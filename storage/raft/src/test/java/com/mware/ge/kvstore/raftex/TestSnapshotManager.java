package com.mware.ge.kvstore.raftex;

import com.mware.ge.collection.Pair;
import com.mware.ge.thrift.ThriftClientManager;
import com.mware.ge.util.GeLogger;
import com.mware.ge.util.GeLoggerFactory;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

public class TestSnapshotManager extends SnapshotManager {
    private GeLogger LOGGER = GeLoggerFactory.getLogger(TestSnapshotManager.class);
    private final RaftexServiceImpl service;

    public TestSnapshotManager(RaftexServiceImpl service) {
        super(new ThriftClientManager());
        this.service = service;
    }

    @Override
    protected void accessAllRowsInSnapshot(int spaceId, int partId, SnapshotCallback cb) {
        TestShard part = (TestShard) service.findPart(spaceId, partId);
        int totalCount = 0;
        int totalSize = 0;
        List<ByteBuffer> data = new ArrayList<>();
        for (Pair<Long, byte[]> row : part.data_) {
            if (data.size() > 100) {
                LOGGER.info(part.idStr_ + "Send snapshot total rows " + data.size()
                        + ", total count sended " + totalCount
                        + ", total size sended " + totalSize
                        + ", finished false");
                cb.apply(data, totalCount, totalSize, SnapshotStatus.IN_PROGRESS);
                data.clear();
            }

            ByteBuffer encoded = encodeSnapshotRow(row.first(), row.other());
            totalSize += encoded.position();
            totalCount++;
            data.add(encoded);
        }

        LOGGER.info(part.idStr_ + "Send snapshot total rows " + data.size()
                + ", total count sended " + totalCount
                + ", total size sended " + totalSize
                + ", finished false");
        cb.apply(data, totalCount, totalSize, SnapshotStatus.DONE);
    }

    private ByteBuffer encodeSnapshotRow(Long logId, byte[] row) {
        ByteBuffer rawData = ByteBuffer.allocate(Long.BYTES + row.length);
        rawData.putLong(logId);
        rawData.put(row);
        return rawData;
    }
}
