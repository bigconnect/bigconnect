package com.mware.ge.kvstore.wal;

import com.mware.ge.kvstore.wal.AtomicLogBuffer.Record;
import com.mware.ge.util.GeLogger;
import com.mware.ge.util.GeLoggerFactory;
import com.mware.ge.util.Preconditions;
import org.apache.commons.lang3.RandomUtils;
import org.apache.commons.lang3.StringUtils;
import org.junit.Assert;
import org.junit.Test;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

import static com.mware.ge.kvstore.wal.AtomicLogBuffer.kMaxLength;

public class AtomicLogBufferTest {
    private GeLogger LOGGER = GeLoggerFactory.getLogger(AtomicLogBufferTest.class);

    public static void checkIterator(AtomicLogBuffer logBuffer, long from, long to, long expected) {
        try (AtomicLogBuffer.Iterator iter = logBuffer.iterator(from, to)) {
            for (; iter.valid(); iter.next()) {
                String log = new String(iter.logMsg(), StandardCharsets.UTF_8);
                Assert.assertEquals(String.format("str_%d", from), log);
                from++;
            }
        }
        Assert.assertEquals(expected, from);
    }

    @Test
    public void readWriteTest() {
        AtomicLogBuffer logBuffer = AtomicLogBuffer.instance();
        for (long logId = 0; logId < 1000L; logId++) {
            logBuffer.push(logId, new Record(0, 0, String.format("str_%d", logId).getBytes(StandardCharsets.UTF_8)));
        }

        checkIterator(logBuffer, 200, 1000, 1000);
        checkIterator(logBuffer, 200, 1500, 1000);
        checkIterator(logBuffer, 200, 800, 801);

        {
            long from = 1200;
            AtomicLogBuffer.Iterator iter = logBuffer.iterator(from, 1800);
            Assert.assertFalse(iter.valid());
        }
    }

    @Test
    public void overflowTest() {
        AtomicLogBuffer logBuffer = AtomicLogBuffer.instance(128);
        for (long logId = 0; logId < 1000L; logId++) {
            logBuffer.push(logId, new Record(0, 0, String.format("str_%d", logId).getBytes(StandardCharsets.UTF_8)));
        }

        long from = 100;
        AtomicLogBuffer.Iterator iter = logBuffer.iterator(from, 1800);
        Assert.assertFalse(iter.valid());
    }

    @Test
    public void SingleWriterMultiReadersTest() throws InterruptedException {
        // The default size is 100K
        AtomicLogBuffer logBuffer = AtomicLogBuffer.instance(100 * 1024);
        AtomicLong writePoint = new AtomicLong(0L);

        Thread writer = new Thread(() -> {
            LOGGER.info("Begin write 1M records");
            for (long logId = 0; logId < 1000000L; logId++) {
                logBuffer.push(logId, new Record(0, 0, String.format("str_%d", logId).getBytes(StandardCharsets.UTF_8)));
                writePoint.set(logId);
            }
            LOGGER.info("Finished writing 1M records");
        });
        writer.start();

        List<Thread> readers = new ArrayList<>();
        for (int i = 0; i < 5; i++) {
            final int i_ = i;
            Thread t = new Thread(() -> {
                try {
                    Thread.sleep(1);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }

                LOGGER.info("Start reader");
                int times = 10000;
                int validSeek = 0;
                while (times-- > 0) {
                    long wp = writePoint.get() - 1;
                    long start = RandomUtils.nextLong(logBuffer.firstLogId(), wp - 1);
                    long end = start + RandomUtils.nextLong(0L, 999L);
                    AtomicLogBuffer.Iterator iter = logBuffer.iterator(start, end);
                    if (!iter.valid())
                        continue;

                    validSeek++;
                    long num = start;
                    for (; iter.valid(); iter.next()) {
                        Record rec = iter.record();
                        long logId = iter.logId();
                        AtomicLogBuffer.Node node = iter.currNode();
                        Preconditions.checkNotNull(rec);
                        String expected = String.format("str_%d", num);
                        Assert.assertEquals(num, logId);
                        Assert.assertEquals(expected.length(), rec.getMsg().length);
                        Assert.assertEquals(expected, new String(rec.getMsg(), StandardCharsets.UTF_8));
                        num++;
                    }
                }
                LOGGER.info("End reader %d, valid seek times ", i_, validSeek);
            });
            t.start();
            readers.add(t);
        }

        writer.join();
        for (Thread r : readers) {
            r.join();
        }
    }

    @Test
    public void resetThenPushExceedLimit() {
        int capacity = 24 * (kMaxLength);
        AtomicLogBuffer logBuffer = AtomicLogBuffer.instance(capacity);
        // One node would save at most kMaxLength logs, so there will be 2 node.
        long logId = 0;
        for (; logId <= kMaxLength; logId++) {
            // The actual size of one record would be sizeof(ClusterID) + sizeof(TermID) + msg_.size(),
            // in this case, it would be 8 + 8 + 8 = 24, total size would be 24 * (kMaxLength + 1)
            logBuffer.push(logId, new Record(0, 0, StringUtils.repeat('a', 8).getBytes(StandardCharsets.UTF_8)));
        }

        // Mark all two node as deleted, log buffer size would be reset to 0
        logBuffer.reset();
        Assert.assertNull(logBuffer.seek(0));
        Assert.assertNull(logBuffer.seek(kMaxLength));

        // Because head has been marked as deleted, this would save in a new node.
        // The record size will be exactly same with capacity of log buffer.
        String logMakeBufferFull = StringUtils.repeat('a', capacity - Long.BYTES - Long.BYTES);
        logBuffer.push(logId, new Record(0, 0, logMakeBufferFull.getBytes(StandardCharsets.UTF_8)));

        Assert.assertEquals(logId, logBuffer.firstLogId.get());
        Assert.assertEquals(capacity, logBuffer.size.get());
        Assert.assertNotNull(logBuffer.seek(logId));

        logId++;

        // At this point, buffer will have three node, head contain the logMakeBufferFull, others
        // are marked as deleted, tail != head. Let's push another log
        logBuffer.push(logId, new Record(0, 0, StringUtils.repeat('a', 8).getBytes(StandardCharsets.UTF_8)));
        Assert.assertNotNull(logBuffer.seek(logId));
    }
}
