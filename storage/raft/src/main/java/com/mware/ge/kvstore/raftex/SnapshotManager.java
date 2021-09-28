package com.mware.ge.kvstore.raftex;

import com.mware.ge.collection.Pair;
import com.mware.ge.thrift.ThriftClientManager;
import com.mware.ge.util.GeLogger;
import com.mware.ge.util.GeLoggerFactory;
import org.apache.thrift.TException;
import org.apache.thrift.async.AsyncMethodCallback;

import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public abstract class SnapshotManager {
    private GeLogger LOGGER = GeLoggerFactory.getLogger(SnapshotManager.class);
    // Retry times if send failed
    public static final int SNAPSHOT_SEND_RETRY_TIMES = 3;
    public static final int SNAPSHOT_SEND_TIMEOUT_MS = 60000;

    ExecutorService executor_ = Executors.newCachedThreadPool();
    ThriftClientManager clientManager;

    public SnapshotManager(ThriftClientManager clientManager) {
        this.clientManager = clientManager;
    }

    public CompletableFuture<Status> sendSnapshot(RaftPart part, InetSocketAddress dst) {
        CompletableFuture<Status> result = new CompletableFuture<>();

        executor_.submit(() -> {
            LOGGER.info(part.idStr_ + "Begin to send the snapshot"
                    + ", commitLogId = " + part.lastCommittedLogId().first()
                    + ", commitLogTerm = " + part.lastCommittedLogId().other());
            accessAllRowsInSnapshot(part.spaceId_, part.partId_, (SnapshotCallback) (data, totalCount, totalSize, status) -> {
                if (status == SnapshotStatus.FAILED) {
                    LOGGER.info(part.idStr_ + "Snapshot send failed, the leader changed?");
                    result.complete(new Status(Status.Error, "Send snapshot failed!"));
                    return false;
                }

                Pair<Long, Long> commitLogIdAndTerm = part.lastCommittedLogId();
                InetSocketAddress localhost = part.address();

                int retry = SNAPSHOT_SEND_RETRY_TIMES;
                while (retry-- > 0) {
                    CompletableFuture<SendSnapshotResponse> f = send(part.spaceId_,
                            part.partId_,
                            part.term_,
                            commitLogIdAndTerm.first(),
                            commitLogIdAndTerm.other(),
                            localhost,
                            data,
                            totalSize,
                            totalCount,
                            dst,
                            status == SnapshotStatus.DONE);

                    try {
                        SendSnapshotResponse resp = f.get();
                        if (ErrorCode.SUCCEEDED == resp.getError_code()) {
                            LOGGER.info(part.idStr_ + "has sended count " + totalCount);
                            if (status == SnapshotStatus.DONE) {
                                LOGGER.info(part.idStr_ + "Finished, totalCount " + totalCount + ", totalSize " + totalSize);
                                result.complete(Status.OK());
                            }
                            return true;
                        } else {
                            LOGGER.info(part.idStr_ + "Sending snapshot failed, we don't retry anymore!. The error code is: "+resp.getError_code());
                            result.complete(new Status(Status.Error, "Send snapshot failed!"));
                            return false;
                        }
                    } catch (Exception e) {
                        LOGGER.info(part.idStr_ + "Send snapshot failed, exception " + e.getMessage());
                        continue;
                    }
                }

                LOGGER.warn(part.idStr_ + "Send snapshot failed!");
                result.complete(new Status(Status.Error, "Send snapshot failed!"));
                return false;
            });
        });
        return result;
    }

    private CompletableFuture<SendSnapshotResponse> send(int spaceId, int partId, long termId, long committedLogId, long committedLogTerm, InetSocketAddress localhost,
                                                         List<ByteBuffer> data, long totalSize, long totalCount, InetSocketAddress addr, boolean finished) {
        LOGGER.info("Sending snapshot to " + addr);
        SendSnapshotRequest req = new SendSnapshotRequest();
        req.setSpace(spaceId);
        req.setPart(partId);
        req.setTerm(termId);
        req.setCommitted_log_id(committedLogId);
        req.setCommitted_log_term(committedLogTerm);
        req.setLeader_addr(localhost.getHostString());
        req.setLeader_port(localhost.getPort());
        req.setRows(data);
        req.setTotal_size(totalSize);
        req.setTotal_count(totalCount);
        req.setDone(finished);
        CompletableFuture<SendSnapshotResponse> resp = new CompletableFuture<>();

        new Thread(() -> {
            try {
                SendSnapshotResponse sendSnapshotResponse = clientManager.client(addr, SNAPSHOT_SEND_TIMEOUT_MS).sendSnapshot(req);
                resp.complete(sendSnapshotResponse);
            } catch (TException e) {
                resp.completeExceptionally(e);
            }
        }).start();

        return resp;
    }

    protected abstract void accessAllRowsInSnapshot(int spaceId, int partId, SnapshotCallback cb);

    enum SnapshotStatus {
        IN_PROGRESS,
        DONE,
        FAILED,
    }

    interface SnapshotCallback {
        boolean apply(List<ByteBuffer> rows, long totalCount, long totalSize, SnapshotStatus status);
    }
}
