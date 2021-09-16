package com.mware.ge.kvstore.raftex;

import com.mware.ge.util.GeLogger;
import com.mware.ge.util.GeLoggerFactory;
import com.mware.ge.util.Preconditions;
import org.apache.thrift.TException;
import org.apache.thrift.async.AsyncMethodCallback;
import org.javatuples.Triplet;

import java.net.InetSocketAddress;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public class Host {
    private GeLogger LOGGER = GeLoggerFactory.getLogger(Host.class);

    // The max number of logs in each appendLog request batch
    public static final int MAX_APPENDLOG_BATCH_SIZE = 128;
    // The max number of outstanding appendLog requests
    public static final int MAX_OUTSTANDING_REQUESTS = 1024;
    // rpc timeout for raft client
    public static final int RAFT_RPC_TIMEOUT_MS = 500;

    private InetSocketAddress addr_;
    private RaftPart part_;
    private ReentrantLock lock_ = new ReentrantLock();
    private boolean isLearner_;
    private String idStr_;
    boolean paused_;
    boolean stopped_;

    boolean requestOnGoing_;
    final Lock noMoreRequestLock = new ReentrantLock();
    final Condition noMoreRequestCV_ = noMoreRequestLock.newCondition();
    CompletableFuture<AppendLogResponse> promise_ = new CompletableFuture<>();
    CompletableFuture<AppendLogResponse> cachingPromise_ =;

    // <term, logId, committedLogId>
    Triplet<Long, Long, Long> pendingReq_ = new Triplet<>(0L, 0L, 0L);

    // These logId and term pointing to the latest log we need to send
    long logIdToSend_ = 0;
    long logTermToSend_ = 0;

    // The previous log before this batch
    long lastLogIdSent_ = 0;
    long lastLogTermSent_ = 0;

    long committedLogId_ = 0;
    AtomicBoolean sendingSnapshot_ = new AtomicBoolean(false);

    // CommittedLogId of follower
    long followerCommittedLogId_ = 0;

    public Host(InetSocketAddress addr, RaftPart part) {
        this(addr, part, false);
    }

    public Host(InetSocketAddress addr, RaftPart part, boolean isLearner) {
        this.addr_ = addr;
        this.part_ = part;
        this.isLearner_ = isLearner;
        this.idStr_ = String.format("%s[Host: %s:%d]", part_.idStr_, addr_.getHostString(), addr_.getPort());
        this.cachingPromise_ = new CompletableFuture<>();
    }

    public String idStr() {
        return idStr_;
    }

    // This will be called when the shard lost its leadership
    public void pause() {
        try {
            lock_.lock();
            paused_ = true;
        } finally {
            lock_.unlock();
        }
    }

    public void resume() {
        try {
            lock_.lock();
            paused_ = false;
        } finally {
            lock_.unlock();
        }
    }

    public void stop() {
        try {
            lock_.lock();
            stopped_ = true;
        } finally {
            lock_.unlock();
        }
    }

    public void reset() {
        try {
            lock_.lock();
            while (requestOnGoing_) {
                try {
                    noMoreRequestCV_.await();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
            logIdToSend_ = 0;
            logTermToSend_ = 0;
            lastLogIdSent_ = 0;
            lastLogTermSent_ = 0;
            committedLogId_ = 0;
            sendingSnapshot_.set(false);
            followerCommittedLogId_ = 0;
        } finally {
            lock_.unlock();
        }
    }

    public void waitForStop() {
        try {
            lock_.lock();
            Preconditions.checkState(stopped_);
            while (requestOnGoing_)
                noMoreRequestCV_.awaitUninterruptibly();
            LOGGER.info("%s: The host has been stopped!", idStr_);
        } finally {
            lock_.unlock();
        }
    }

    private ErrorCode checkStatus() {
        Preconditions.checkState(!lock_.tryLock());
        if (stopped_) {
            LOGGER.info("%s: The host is stopped, just return", idStr_);
            return ErrorCode.E_HOST_STOPPED;
        }

        if (paused_) {
            LOGGER.info("%s: The host is paused, due to losing leadership", idStr_);
            return ErrorCode.E_NOT_A_LEADER;
        }

        return ErrorCode.SUCCEEDED;
    }

    public CompletableFuture<AskForVoteResponse> askForVote(ExecutorService eb, AskForVoteRequest req) {
        try {
            lock_.lock();
            ErrorCode res = checkStatus();
            if (ErrorCode.SUCCEEDED != res) {
                LOGGER.info("%s: The Host is not in a proper status, do not send", idStr_);
                return CompletableFuture.completedFuture(new AskForVoteResponse(res));
            }
        } finally {
            lock_.unlock();
        }

        CompletableFuture<AskForVoteResponse> future = new CompletableFuture<>();

        try {
            part_.clientMan_.askForVote(req, new AsyncMethodCallback<>() {
                @Override
                public void onComplete(AskForVoteResponse askForVoteResponse) {
                    future.complete(askForVoteResponse);
                }

                @Override
                public void onError(Exception e) {
                    future.completeExceptionally(e);
                }
            });
        } catch (TException e) {
            future.completeExceptionally(e);
        }

        return future;
    }

    public CompletableFuture<AppendLogResponse> appendLogs(
            ExecutorService eb,
            long term,
            long logId,
            long committedLogId,
            long prevLogTerm,
            long prevLogId
    ) {
        LOGGER.info("%s: Entering Host::appendLogs()", idStr_);

        try {
            lock_.lock();
            ErrorCode res = checkStatus();
            if (logId <= lastLogIdSent_) {
                LOGGER.info(idStr_ + "The log " + logId + " has been sended"
                        + ", lastLogIdSent " + lastLogIdSent_);
                AppendLogResponse r = new AppendLogResponse();
                r.setError_code(ErrorCode.SUCCEEDED);
                return CompletableFuture.completedFuture(r);
            }

            if (requestOnGoing_ && res == ErrorCode.SUCCEEDED) {
                /**
                 * TODO
                 * TODO
                 * TODO
                 * TODO
                 * TODO
                 * TODO
                 * TODO
                 */
            }
        } finally {
            lock_.unlock();
        }
    }

    public boolean isLearner_() {
        return isLearner_;
    }

    public void setLearner_(boolean learner_) {
        this.isLearner_ = learner_;
    }

    public CompletableFuture<HeartbeatResponse> sendHeartbeat(
            ExecutorService eb,
            long term,
            long latestLogId,
            long commitLogId,
            long lastLogTerm,
            long lastLogId
    ) {
        return null;
    }

    public InetSocketAddress address() {
        return addr_;
    }

    private CompletableFuture<AppendLogResponse> sendAppendLogRequest(AppendLogRequest req) {
        return null;
    }

    private void appendLogsInternal(AppendLogRequest req) {
        throw new UnsupportedOperationException("TBI");
    }

    private CompletableFuture<HeartbeatResponse> sendHeartbeatRequest(HeartbeatRequest req) {
        throw new UnsupportedOperationException("TBI");
    }

    private AppendLogRequest prepareAppendLogRequest() {
        throw new UnsupportedOperationException("TBI");
    }

    private boolean noRequest() {
        throw new UnsupportedOperationException("TBI");
    }

    private void setResponse(AppendLogResponse r) {
        Preconditions.checkState(lock_.tryLock());
        promise_.complete(r);
        cachingPromise_.complete(r);
        cachingPromise_ = new CompletableFuture<>();
        pendingReq_ = new Triplet<>(0L, 0L, 0L);
        requestOnGoing_ = false;
    }
}
