package com.mware.ge.kvstore.raftex;

import com.google.common.primitives.Longs;
import com.mware.ge.collection.Pair;
import com.mware.ge.kvstore.utils.LogIterator;
import com.mware.ge.thrift.ThriftClientManager;
import com.mware.ge.util.GeLogger;
import com.mware.ge.util.GeLoggerFactory;

import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.locks.ReentrantLock;

public class TestShard extends RaftPart {
    private GeLogger LOGGER = GeLoggerFactory.getLogger(TestShard.class);

    interface Callback {
        void callback(int idx, String msg, long termId);
    }

    int commitTimes_ = 0;
    long currLogId_ = -1;
    int idx_;
    RaftexServiceImpl service_;
    List<Pair<Long, byte[]>> data_ = new ArrayList<>();
    long lastCommittedLogId_ = 0L;
    ReentrantLock lock_ = new ReentrantLock();
    Callback leadershipLostCB_;
    Callback becomeLeaderCB_;

    protected TestShard(
            int idx,
            RaftexServiceImpl svc,
            int partId,
            InetSocketAddress addr,
            String walRoot,
            ScheduledExecutorService workers,
            SnapshotManager snapshotMan,
            Callback leadershipLostCB,
            Callback becomeLeaderCB

    ) {
        super(1, 1, partId, addr, walRoot, workers, snapshotMan, new ThriftClientManager(), null);
        this.idx_ = idx;
        this.service_ = svc;
        this.leadershipLostCB_ = leadershipLostCB;
        this.becomeLeaderCB_ = becomeLeaderCB;
    }

    @Override
    public Pair<Long, Long> commitSnapshot(List<ByteBuffer> data, long committedLogId, long committedLogTerm, boolean finished) {
        try {
            lock_.lock();
            long count = 0;
            long size = 0;
            for (ByteBuffer row : data) {
                count++;
                byte[] array = row.array();
                size += array.length;
                Pair<Long, byte[]> idData = decodeSnapshotRow(array);
                LOGGER.info(idStr_ + "Commit row logId " + idData.first() + ", log " + Arrays.toString(idData.other()));
                data_.add(Pair.of(idData.first(), idData.other()));
            }
            if (finished) {
                lastCommittedLogId_ = committedLogId;
                LOGGER.info(idStr_ + "Commit the snapshot committedLogId " + committedLogId
                        + ", term " + committedLogTerm);
            }

            return Pair.of(count, size);
        } finally {
            lock_.unlock();
        }
    }

    private Pair<Long,byte[]> decodeSnapshotRow(byte[] log) {
        ByteBuffer buf = ByteBuffer.wrap(log);
        Long id = buf.getLong();
        byte[] strBuf = new byte[log.length - Long.BYTES];
        System.arraycopy(log, Long.BYTES, strBuf, 0, log.length - Long.BYTES);
        return Pair.of(id, strBuf);
    }

    private InetSocketAddress decodeLearner(byte[] log) {
        throw new UnsupportedOperationException("TBI");
    }

    private InetSocketAddress decodeRemovePeer(byte[] log) {
        throw new UnsupportedOperationException("TBI");
    }

    private InetSocketAddress decodeAddPeer(byte[] log) {
        throw new UnsupportedOperationException("TBI");
    }

    private InetSocketAddress decodeTransferLeader(byte[] log) {
        throw new UnsupportedOperationException("TBI");
    }

    @Override
    protected boolean preProcessLog(long logId, long termId, long clusterId, byte[] log) {
        if (log == null || log.length == 0) {
            return true;
        }

        switch (log[0]) {
            case 0x01: {
                InetSocketAddress learner = decodeLearner(log);
                addLearner(learner);
                LOGGER.info(idStr_ +  "Add learner " + learner);
                break;
            }
            case 0x02: {
                InetSocketAddress leader = decodeTransferLeader(log);
                preProcessTransLeader(leader);
                LOGGER.info(idStr_ +  "Preprocess transleader " + leader);
                break;
            }
            case 0x03: {
                InetSocketAddress peer = decodeAddPeer(log);
                addPeer(peer);
                LOGGER.info(idStr_ +  "Add peer " + peer);
                break;
            }
            case 0x04: {
                InetSocketAddress peer = decodeRemovePeer(log);
                preProcessRemovePeer(peer);
                LOGGER.info(idStr_ +  "Remove peer " + peer);
                break;
            }
        }
        return true;
    }


    @Override
    public Pair<Long, Long> lastCommittedLogId() {
        return Pair.of(committedLogId_, term_);
    }

    @Override
    protected ErrorCode commitLogs(LogIterator iter, boolean wait) {
        long firstId = -1;
        long lastId = -1;
        int commitLogsNum = 0;
        while(iter.valid()) {
            if (firstId < 0) {
                firstId = iter.logId();
            }
            lastId = iter.logId();
            byte[] log = iter.logMsg();
            if (log != null && log.length > 0) {
                switch (log[0]) {
                    case 0x02: {
                        // TRANSFER_LEADER
                        InetSocketAddress leader = decodeTransferLeader(log);
                        commitTransLeader(leader);
                        break;
                    }
                    case 0x04: {
                        InetSocketAddress peer = decodeRemovePeer(log);
                        commitRemovePeer(peer);
                        break;
                    }
                    case 0x03:
                    case 0x01: {
                        break;
                    }
                    default: {
                        try {
                            lock_.lock();
                            currLogId_ = iter.logId();
                            data_.add(Pair.of(currLogId_, log));
                            break;
                        } finally {
                            lock_.unlock();
                        }
                    }
                }
                commitLogsNum++;
            }
            iter.next();
        }

        LOGGER.info("TestShard: "+idStr_+ "Committed log " + firstId + " to " + lastId);
        if (lastId > -1) {
            lastCommittedLogId_ = lastId;
        }
        if (commitLogsNum > 0) {
            commitTimes_++;
        }
        return ErrorCode.SUCCEEDED;
    }

    @Override
    protected void onElected(long term) {
        if (becomeLeaderCB_ != null) {
            becomeLeaderCB_.callback(idx_, idStr(), term);
        }
    }

    public String idStr() {
        return idStr_;
    }

    @Override
    protected void onLostLeadership(long term) {
        if (leadershipLostCB_ != null) {
            leadershipLostCB_.callback(idx_, idStr(), term);
        }
    }

    @Override
    protected void onDiscoverNewLeader(InetSocketAddress nLeader) {

    }

    @Override
    public void cleanup() {
        try {
            lock_.lock();
            data_.clear();
            lastCommittedLogId_ = 0;
        } finally {
            lock_.unlock();
        }
    }

    public int index() {
        return idx_;
    }
}
