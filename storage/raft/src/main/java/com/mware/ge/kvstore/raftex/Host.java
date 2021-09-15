package com.mware.ge.kvstore.raftex;

import com.mware.ge.util.GeLogger;
import com.mware.ge.util.GeLoggerFactory;
import com.mware.ge.util.Preconditions;
import org.javatuples.Triplet;

import java.net.SocketAddress;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public class Host {
    private GeLogger LOGGER = GeLoggerFactory.getLogger(Host.class);

    private final SocketAddress addr;
    private final RaftPart part;
    private ReentrantLock lock = new ReentrantLock();
    private boolean isLearner;
    private String idStr;
    boolean paused;
    boolean stopped;

    boolean requestOnGoing;
    final Lock noMoreRequestLock = new ReentrantLock();
    final Condition noMoreRequestCV = noMoreRequestLock.newCondition();
    CompletableFuture<AppendLogResponse> promise = new CompletableFuture<>();
    CompletableFuture<AppendLogResponse> cachingPromise = new CompletableFuture<>();

    // <term, logId, committedLogId>
    Triplet<Long, Long, Long> pendingReq = new Triplet<>(0L, 0L, 0L);

    // These logId and term pointing to the latest log we need to send
    long logIdToSend = 0;
    long logTermToSend = 0;

    // The previous log before this batch
    long lastLogIdSent = 0;
    long lastLogTermSent = 0;

    long committedLogId = 0;
    AtomicBoolean sendingSnapshot = new AtomicBoolean(false);

    // CommittedLogId of follower
    long followerCommittedLogId = 0;

    public Host(SocketAddress addr, RaftPart part) {
        this(addr, part, false);
    }

    public Host(SocketAddress addr, RaftPart part, boolean isLearner) {
        this.addr = addr;
        this.part = part;
        this.isLearner = isLearner;
    }

    public String idStr() {
        return idStr;
    }

    // This will be called when the shard lost its leadership
    public void pause() {
        try {
            lock.lock();
            paused = true;
        } finally {
            lock.unlock();
        }
    }

    public void resume() {
        try {
            lock.lock();
            paused = false;
        } finally {
            lock.unlock();
        }
    }

    public void stop() {
        try {
            lock.lock();
            stopped = true;
        } finally {
            lock.unlock();
        }
    }

    public void reset() {
        try {
            lock.lock();
            while (requestOnGoing) {
                try {
                    noMoreRequestCV.await();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
            logIdToSend = 0;
            logTermToSend = 0;
            lastLogIdSent = 0;
            lastLogTermSent = 0;
            committedLogId = 0;
            sendingSnapshot.set(false);
            followerCommittedLogId = 0;
        } finally {
            lock.unlock();
        }
    }

    public void waitForStop() {
        try {
            lock.lock();
            Preconditions.checkState(stopped);
            while (requestOnGoing)
                noMoreRequestCV.awaitUninterruptibly();
            LOGGER.info("%s: The host has been stopped!", idStr);
        } finally {
            lock.unlock();
        }
    }

    public boolean isLearner() {
        return isLearner;
    }

    public void setLearner(boolean learner) {
        this.isLearner = learner;
    }

    public CompletableFuture<AskForVoteResponse> askForVote(AskForVoteRequest req) {
        try {
            lock.lock();
            ErrorCode res = checkStatus();
            if (ErrorCode.SUCCEEDED != res) {
                LOGGER.info("%s: The Host is not in a proper status, do not send", idStr);
                return CompletableFuture.completedFuture(new AskForVoteResponse(res));
            }
        } finally {
            lock.unlock();
        }

//        auto client =
//                part_->clientMan_->client(addr_, eb, false, FLAGS_raft_heartbeat_interval_secs * 1000);
//        return client->future_askForVote(req);
        throw new UnsupportedOperationException("TBI");
    }

    public CompletableFuture<AppendLogResponse> appendLogs(
            long term,
            long logId,
            long committedLogId,
            long prevLogTerm,
            long prevLogId
    ) {
        LOGGER.info("%s: Entering Host::appendLogs()", idStr);

        try {
            lock.lock();
            throw new UnsupportedOperationException("TBI");
        } finally {
            lock.unlock();
        }
    }

    public CompletableFuture<HeartbeatResponse> sendHeartbeat(
            long term,
            long latestLogId,
            long commitLogId,
            long lastLogTerm,
            long lastLogId
    ) {
        return null;
    }

    public SocketAddress address() {
        return addr;
    }

    private ErrorCode checkStatus() {
        Preconditions.checkState(lock.tryLock());
        if (stopped) {
            LOGGER.info("%s: The host is stopped, just return", idStr);
            return ErrorCode.E_HOST_STOPPED;
        }

        if (paused) {
            LOGGER.info("%s: The host is paused, due to losing leadership", idStr);
            return ErrorCode.E_NOT_A_LEADER;
        }

        return ErrorCode.SUCCEEDED;
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
        Preconditions.checkState(lock.tryLock());
        promise.complete(r);
        cachingPromise.complete(r);
        cachingPromise = new CompletableFuture<>();
        pendingReq = new Triplet<>(0L, 0L, 0L);
        requestOnGoing = false;
    }
}
