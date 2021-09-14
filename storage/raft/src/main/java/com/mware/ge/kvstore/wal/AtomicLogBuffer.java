package com.mware.ge.kvstore.wal;

import com.mware.ge.kvstore.utils.LogIterator;
import com.mware.ge.util.GeLogger;
import com.mware.ge.util.GeLoggerFactory;
import com.mware.ge.util.Preconditions;

import java.util.Arrays;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

/**
 * A wait-free log buffer for single writer, multi readers
 * When deleting the extra node, to avoid read the dangling one,
 * we just mark it to be deleted, and delete it when no readers using it.
 * <p>
 * For write, most of time, it is o(1)
 * For seek, it is o(n), n is the number of nodes inside current list, but in most
 * cases, the seeking log is in the head node, so it equals o(1)
 */
public class AtomicLogBuffer implements AutoCloseable {
    private GeLogger LOGGER = GeLoggerFactory.getLogger(AtomicLogBuffer.class);
    protected static int kMaxLength = 64;

    AtomicReference<Node> head = new AtomicReference<>();
    // The tail is the last valid Node in current list
    // After tail, all nodes should be marked deleted.
    AtomicReference<Node> tail = new AtomicReference<>();
    AtomicInteger refs = new AtomicInteger(0);
    // current size for the buffer.
    AtomicInteger size = new AtomicInteger(0);
    AtomicLong firstLogId = new AtomicLong(0);
    int capacity = 8 * 1024 * 1024;
    AtomicBoolean gcOnGoing = new AtomicBoolean(false);
    AtomicInteger dirtyNodes = new AtomicInteger(0);
    int dirtyNodesLimit = 5;

    public static AtomicLogBuffer instance() {
        return new AtomicLogBuffer(8 * 1024 * 1024);
    }

    public static AtomicLogBuffer instance(int capacity) {
        return new AtomicLogBuffer(capacity);
    }

    private AtomicLogBuffer(int capacity) {
        this.capacity = capacity;
    }

    public void push(long logId, long termId, long clusterId, byte[] msg) {
        push(logId, new Record(clusterId, termId, msg));
    }

    public void push(long logId, Record record) {
        Node h = this.head.get();
        int recSize = record.size();
        if (h == null || h.isFull() || h.markDeleted.get()) {
            Node newNode = new Node();
            newNode.firstLogId = logId;
            newNode.next = h;
            newNode.push_back(record);
            if (h == null || h.markDeleted.get()) {
                // It is the first Node in current list, or head has been marked as deleted
                firstLogId.set(logId);
                tail.set(newNode);
            } else if (h != null) {
                h.prev.set(newNode);
            }
            size.getAndAdd(recSize);
            this.head.set(newNode);
            return;
        }
        if (size.get() + recSize > capacity) {
            Node t = this.tail.get();
            // todo: there is a potential problem: since Node::isFull is judged by
            // log count, we can only add new node when previous node has enough logs. So when tail
            // is equal to head, we need to wait tail is full, after head moves forward, at then
            // tail can be marked as deleted. So the log buffer would takes up more memory than its
            // capacity. Since it does not affect correctness, we could fix it later if necessary.
            if (t != h) {
                // We have more than one nodes in current list.
                // So we mark the tail to be deleted.
                LOGGER.debug("Mark node %d to be deleted", t.firstLogId);
                boolean marked = t.markDeleted.compareAndSet(false, true);
                Node prev = t.prev.get();
                firstLogId.set(prev.firstLogId);
                // All operations above SHOULD NOT be reordered.
                this.tail.set(t.prev.get());
                if (marked) {
                    size.getAndAdd(-t.size);
                    // dirtyNodes_ changes SHOULD after the tail move.
                    dirtyNodes.getAndAdd(1);
                }
            }
        }
        size.getAndAdd(recSize);
        h.push_back(record);
    }

    public long firstLogId() {
        return firstLogId.get();
    }

    public long lastLogId() {
        Node h = this.head.get();
        if (h == null)
            return 0;

        return h.lastLogId();
    }

    public Iterator iterator(long start, long end) {
        return new Iterator(this, start, end);
    }

    /**
     * For reset operation, users should keep it thread-safe with push operation.
     * Just mark all nodes to be deleted.
     *
     * Actually, we don't follow the invariant strictly (node in range [head, tail] are valid),
     * head and tail are not modified. But once an log is pushed after reset, everything will obey
     * the invariant.
     * */
    public void reset() {
        Node p = this.head.get();
        int count = 0;
        while (p != null) {
            if (!p.markDeleted.compareAndSet(false, true)) {
                // The rest nodes has been mark deleted.
                break;
            }
            p = p.next;
            ++count;
        }
        size.set(0);
        firstLogId.set(0);
        dirtyNodes.getAndAdd(count);
    }

    @Override
    public void close() {
        int refs = this.refs.get();
        Preconditions.checkState(refs == 0);
    }

    // Find the non-deleted node contains the logId.
    protected Node seek(long logId) {
        Node head = this.head.get();
        if (head != null && logId > head.lastLogId()) {
            LOGGER.debug("Bad seek, the seeking logId %d is greater than the latest logId in buffer %d",
                    logId, head.lastLogId());
            return null;
        }

        Node p = head;
        if (p == null)
            return null;

        Node tail = this.tail.get();
        Preconditions.checkNotNull(tail);
        while (p != tail.next && !p.markDeleted.get()) {
            LOGGER.debug("current node firstLogId = %d , the seeking logId = ",
                    p.firstLogId, logId);
            if (logId >= p.firstLogId) {
                break;
            }
            p = p.next;
        }

        if (p == null)
            return null;

        return p.markDeleted.get() ? null : p;
    }

    private int addRef() {
        return refs.getAndAdd(1);
    }


    public static class Iterator implements LogIterator, AutoCloseable {
        AtomicLogBuffer logBuffer;
        long currLogId = 0;
        long end = 0;
        Node currNode = null;
        int currIndex = 0;
        boolean valid = true;
        Record currRec = null;

        private Iterator(AtomicLogBuffer logBuffer, long start, long end) {
            this.logBuffer = logBuffer;
            this.currLogId = start;
            this.logBuffer.addRef();
            this.end = Math.min(end, logBuffer.lastLogId());
            seek(currLogId);
        }

        @Override
        public LogIterator next() {
            currIndex++;
            currLogId++;
            if (currLogId > end) {
                valid = false;
                currRec = null;
                return this;
            }
            // Operations after load SHOULD NOT reorder before it.
            int pos = currNode.pos.get();
            if (currIndex >= pos) {
                currNode = currNode.prev.get();
                if (currNode == null) {
                    valid = false;
                    return null;
                } else {
                    currIndex = 0;
                }
            }

            Preconditions.checkState(currIndex < kMaxLength);
            Preconditions.checkNotNull(currNode);
            currRec = currNode.rec(currIndex);
            return this;
        }

        @Override
        public boolean valid() {
            return valid;
        }

        @Override
        public long logId() {
            Preconditions.checkState(valid);
            return currLogId;
        }

        @Override
        public long logSource() {
            return record().clusterId;
        }

        @Override
        public long logTerm() {
            return record().termId;
        }

        @Override
        public byte[] logMsg() {
            return record().msg;
        }

        @Override
        public void close() {
            logBuffer.releaseRef();
        }

        protected Record record() {
            if (!valid)
                return null;

            Preconditions.checkNotNull(currRec);
            return currRec;
        }

        private void seek(long logId) {
            currNode = logBuffer.seek(logId);
            if (currNode != null) {
                currIndex = (int) (logId - currNode.firstLogId);
                currRec = currNode.rec(currIndex);
                valid = true;
            } else {
                valid = false;
            }
        }

        protected Node currNode() {
            return currNode;
        }

        protected int currIndex() {
            return currIndex;
        }
    }

    private void releaseRef() {
        // All operations following SHOULD NOT reordered before tail.load()
        // so we could ensure the tail used in GC is older than new coming readers.
        Node tail = this.tail.get();
        int readers = refs.getAndDecrement();
        if (readers > 1) {
            return;
        }

        // In this position, maybe there are some new readers coming in
        // So we should load tail before refs count down to ensure the tail current thread
        // got is older than the new readers see.
        Preconditions.checkState(readers == 1);
        int dirtyNodes = this.dirtyNodes.get();

        if (dirtyNodes > dirtyNodesLimit) {
            if (gcOnGoing.compareAndSet(false, true)) {
                Preconditions.checkNotNull(tail);
                Node dirtyHead = tail.next;
                tail.next = null;

                // Now we begin to delete the nodes.
                Node curr = dirtyHead;
                while (curr != null) {
                    Preconditions.checkState(curr.markDeleted.get());
                    curr = curr.next;
                    this.dirtyNodes.getAndDecrement();
                    Preconditions.checkState(this.dirtyNodes.get() >= 0);
                }
                gcOnGoing.set(false);
            }
        }
    }

    public static class Record {
        final long clusterId;
        final long termId;
        final byte[] msg;

        public Record(long clusterId, long termId, byte[] msg) {
            this.clusterId = clusterId;
            this.termId = termId;
            this.msg = msg;
        }

        public int size() {
            return Long.BYTES + Long.BYTES + msg.length;
        }

        public byte[] getMsg() {
            return msg;
        }

        public long getTermId() {
            return termId;
        }

        public long getClusterId() {
            return clusterId;
        }
    }

    public static class Node {
        long firstLogId = 0;
        // total size for current Node.
        int size = 0;
        Node next = null;

        // We should ensure the records appended happens-before pos_ increment.
        Record[] records = new Record[kMaxLength];
        // current valid position for the next record.
        AtomicInteger pos = new AtomicInteger(0);
        // The field only be accessed when the refs count down to zero
        AtomicBoolean markDeleted = new AtomicBoolean(false);
        AtomicReference<Node> prev = new AtomicReference<>();

        boolean isFull() {
            return pos.get() == kMaxLength;
        }

        boolean push_back(Record rec) {
            if (isFull()) {
                return false;
            }

            this.size += rec.size();
            int pos = this.pos.get();
            records[pos] = rec;
            this.pos.getAndAdd(1);
            return true;
        }

        Record rec(int index) {
            Preconditions.checkArgument(index >= 0);
            int pos = this.pos.get();
            Preconditions.checkState(index <= pos);
            Preconditions.checkState(index != kMaxLength);
            return records[index];
        }

        long lastLogId() {
            return firstLogId + pos.get();
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Node node = (Node) o;
            return firstLogId == node.firstLogId &&
                    size == node.size &&
                    Arrays.equals(records, node.records) &&
                    Objects.equals(pos.get(), node.pos.get());
        }

        @Override
        public int hashCode() {
            int result = Objects.hash(firstLogId, size, pos.get());
            result = 31 * result + Arrays.hashCode(records);
            return result;
        }
    }
}
