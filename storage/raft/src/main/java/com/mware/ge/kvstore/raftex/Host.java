package com.mware.ge.kvstore.raftex;

import com.mware.ge.kvstore.utils.LogIterator;
import com.mware.ge.kvstore.utils.SharedFuture;
import com.mware.ge.util.GeLogger;
import com.mware.ge.util.GeLoggerFactory;
import com.mware.ge.util.Preconditions;
import org.apache.thrift.TException;
import org.apache.thrift.async.AsyncMethodCallback;
import org.javatuples.Triplet;

import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.List;
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
    SharedFuture<AppendLogResponse> promise_ = new SharedFuture<>();
    SharedFuture<AppendLogResponse> cachingPromise_ = new SharedFuture<>();

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
            Preconditions.checkState(!stopped_);
            while (requestOnGoing_)
                noMoreRequestCV_.awaitUninterruptibly();
            LOGGER.info("%s: The host has been stopped!", idStr_);
        } finally {
            lock_.unlock();
        }
    }

    private ErrorCode checkStatus() {
        Preconditions.checkState(lock_.tryLock());
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

    public CompletableFuture<AskForVoteResponse> askForVote(AskForVoteRequest req) {
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

        new Thread(() -> {
            try {
                AskForVoteResponse resp = part_.clientMan_.client(addr_, 0).askForVote(req);
                future.complete(resp);
            } catch (Exception e) {
                e.printStackTrace();
                future.completeExceptionally(e);
            }
        }).start();

        return future;
    }

    public CompletableFuture<AppendLogResponse> appendLogs(
            long term,
            long logId,
            long committedLogId,
            long prevLogTerm,
            long prevLogId
    ) {
        LOGGER.info("%s: Entering Host::appendLogs()", idStr_);
        CompletableFuture<AppendLogResponse> ret = new CompletableFuture<>();
        AppendLogRequest req = new AppendLogRequest();

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
                if (cachingPromise_.size() <= MAX_OUTSTANDING_REQUESTS) {
                    pendingReq_ = new Triplet<>(term, logId, committedLogId);
                    return cachingPromise_.getFuture();
                } else {
                    LOGGER.info(idStr_ + "Too many requests are waiting, return error");
                    AppendLogResponse r = new AppendLogResponse();
                    r.setError_code(ErrorCode.E_TOO_MANY_REQUESTS);
                    return CompletableFuture.completedFuture(r);
                }
            }

            if (res != ErrorCode.SUCCEEDED) {
                LOGGER.info(idStr_ + "The host is not in a proper status, just return");
                AppendLogResponse r = new AppendLogResponse();
                r.setError_code(res);
                return CompletableFuture.completedFuture(r);
            }

            LOGGER.info(idStr_ + "About to send the AppendLog request");

            // No request is ongoing, let's send a new request
            if (lastLogIdSent_ == 0 && lastLogTermSent_ == 0) {
                lastLogIdSent_ = prevLogId;
                lastLogTermSent_ = prevLogTerm;
                LOGGER.info(idStr_ + "This is the first time to send the logs to this host"
                        + ", lastLogIdSent = " + lastLogIdSent_
                        + ", lastLogTermSent = " + lastLogTermSent_);
            }

            logTermToSend_ = term;
            logIdToSend_ = logId;
            committedLogId_ = committedLogId;
            pendingReq_ = new Triplet<>(0L, 0L, 0L);
            promise_ = cachingPromise_;
            cachingPromise_ = new SharedFuture<>();
            ret = promise_.getFuture();

            requestOnGoing_ = true;

            req = prepareAppendLogRequest();
        } finally {
            lock_.unlock();
        }

        // Get a new promise
        appendLogsInternal(req);

        return ret;
    }

    private void setResponse(AppendLogResponse r) {
        Preconditions.checkState(lock_.tryLock());
        promise_.complete(r);
        cachingPromise_.complete(r);
        cachingPromise_ = new SharedFuture<>();
        pendingReq_ = new Triplet<>(0L, 0L, 0L);
        requestOnGoing_ = false;
    }

    private void appendLogsInternal(AppendLogRequest req) {
        sendAppendLogRequest(req).whenComplete((resp, e) -> {
            LOGGER.info(idStr_ + "appendLogs() call got response");
            if (e != null) {
                LOGGER.info(idStr_ + e.getMessage());
                AppendLogResponse r = new AppendLogResponse();
                r.setError_code(ErrorCode.E_EXCEPTION);
                try {
                    lock_.lock();
                    setResponse(r);
                    lastLogIdSent_ = lastLogIdSent_ - 1;
                } finally {
                    lock_.unlock();
                }

                try {
                    noMoreRequestLock.lock();
                    noMoreRequestCV_.signalAll();
                } finally {
                    noMoreRequestLock.unlock();
                }

                return;
            }

            LOGGER.info(idStr_ + "AppendLogResponse code " + resp.getError_code()
                    + ", currTerm " + resp.getCurrent_term()
                    + ", lastLogId " + resp.getLast_log_id()
                    + ", lastLogTerm " + resp.getLast_log_term()
                    + ", commitLogId " + resp.getCommitted_log_id()
                    + ", lastLogIdSent_ " + lastLogIdSent_
                    + ", lastLogTermSent_ " + lastLogTermSent_);

            switch (resp.getError_code()) {
                case SUCCEEDED: {
                    LOGGER.info(idStr_ + "AppendLog request sent successfully");

                    AppendLogRequest newReq = null;
                    try {
                        lock_.lock();
                        ErrorCode res = checkStatus();
                        if (res != ErrorCode.SUCCEEDED) {
                            LOGGER.info(idStr_ + "The host is not in a proper status, just return");
                            AppendLogResponse r = new AppendLogResponse();
                            r.setError_code(res);
                            setResponse(r);
                        } else if (lastLogIdSent_ >= resp.getLast_log_id()) {
                            LOGGER.info(idStr_ + "We send nothing in the last request, so we don't send the same logs again");
                            followerCommittedLogId_ = resp.getCommitted_log_id();
                            AppendLogResponse r = new AppendLogResponse();
                            r.setError_code(res);
                            setResponse(r);
                        } else {
                            lastLogIdSent_ = resp.getLast_log_id();
                            lastLogTermSent_ = resp.getLast_log_term();
                            followerCommittedLogId_ = resp.getCommitted_log_id();
                            if (lastLogIdSent_ < logIdToSend_) {
                                // More to send
                                LOGGER.info(idStr_ + "There are more logs to send");
                                newReq = prepareAppendLogRequest();
                            } else {
                                LOGGER.info(idStr_ + "Fulfill the promise, size = " + promise_.size());
                                // Fulfill the promise
                                promise_.complete(resp);
                                if (noRequest()) {
                                    LOGGER.info(idStr_ + "No request any more!");
                                    requestOnGoing_ = false;
                                } else {
                                    Triplet<Long, Long, Long> tup = pendingReq_;
                                    logTermToSend_ = tup.getValue0();
                                    logIdToSend_ = tup.getValue1();
                                    committedLogId_ = tup.getValue2();
                                    LOGGER.info(idStr_ + "Sending the pending request in the queue"
                                            + ", from " + lastLogIdSent_ + 1
                                            + " to " + logIdToSend_);
                                    newReq = prepareAppendLogRequest();
                                    promise_ = cachingPromise_;
                                    cachingPromise_ = new SharedFuture<>();
                                    pendingReq_ = new Triplet<>(0L, 0L, 0L);
                                }
                            }
                        }
                    } finally {
                        lock_.unlock();
                    }

                    if (newReq != null) {
                        appendLogsInternal(newReq);
                    } else {
                        try {
                            noMoreRequestLock.lock();
                            noMoreRequestCV_.signalAll();
                            return;
                        } finally {
                            noMoreRequestLock.unlock();
                        }
                    }
                }
                case E_LOG_GAP: {
                    LOGGER.info(idStr_ + "The host's log is behind, need to catch up");
                    AppendLogRequest newReq = null;
                    try {
                        lock_.lock();
                        ErrorCode res = checkStatus();
                        if (res != ErrorCode.SUCCEEDED) {
                            LOGGER.info(idStr_ + "The host is not in a proper status, skip catching up the gap");
                            AppendLogResponse r = new AppendLogResponse();
                            r.setError_code(res);
                            setResponse(r);
                        } else if (lastLogIdSent_ == resp.getLast_log_id()) {
                            LOGGER.info(idStr_ + "We send nothing in the last request , so we don't send the same logs again");
                            lastLogIdSent_ = resp.getLast_log_id();
                            lastLogTermSent_ = resp.getLast_log_term();
                            followerCommittedLogId_ = resp.getCommitted_log_id();
                            AppendLogResponse r = new AppendLogResponse();
                            r.setError_code(ErrorCode.SUCCEEDED);
                            setResponse(r);
                        } else {
                            lastLogIdSent_ = Math.min(resp.getLast_log_id(), logIdToSend_ - 1);
                            lastLogTermSent_ = resp.getLast_log_term();
                            followerCommittedLogId_ = resp.getCommitted_log_id();
                            newReq = prepareAppendLogRequest();
                        }
                    } finally {
                        lock_.unlock();
                    }

                    if (newReq != null) {
                        appendLogsInternal(newReq);
                    } else {
                        noMoreRequestLock.lock();
                        noMoreRequestCV_.signalAll();
                        noMoreRequestLock.unlock();
                    }
                    return;
                }
                case E_WAITING_SNAPSHOT: {
                    LOGGER.info(idStr_ + "The host is waiting for the snapshot, " +
                            "so we need to send log from current committedLogId " + committedLogId_);
                    AppendLogRequest newReq = null;
                    try {
                        lock_.lock();
                        ErrorCode res = checkStatus();
                        if (res != ErrorCode.SUCCEEDED) {
                            LOGGER.info(idStr_ + "The host is not in a proper status, skip waiting the snapshot");
                            AppendLogResponse r = new AppendLogResponse();
                            r.setError_code(res);
                            setResponse(r);
                        } else {
                            lastLogIdSent_ = committedLogId_;
                            lastLogTermSent_ = logTermToSend_;
                            followerCommittedLogId_ = resp.getCommitted_log_id();
                            newReq = prepareAppendLogRequest();
                        }
                    } finally {
                        lock_.unlock();
                    }

                    if (newReq != null) {
                        appendLogsInternal(newReq);
                    } else {
                        noMoreRequestLock.lock();
                        noMoreRequestCV_.signalAll();
                        noMoreRequestLock.unlock();
                    }
                    return;
                }
                case E_LOG_STALE: {
                    LOGGER.info(idStr_ + "Log stale, reset lastLogIdSent " + lastLogIdSent_
                            + " to the followers lastLodId " + resp.getLast_log_id());
                    AppendLogRequest newReq = null;
                    try {
                        lock_.lock();
                        ErrorCode res = checkStatus();
                        if (res != ErrorCode.SUCCEEDED) {
                            LOGGER.info(idStr_ + "The host is not in a proper status, skip waiting the snapshot");
                            AppendLogResponse r = new AppendLogResponse();
                            r.setError_code(res);
                            setResponse(r);
                        } else if (logIdToSend_ <= resp.getLast_log_id()) {
                            LOGGER.info(idStr_ + "It means the request has been received by follower");
                            lastLogIdSent_ = logIdToSend_ - 1;
                            lastLogTermSent_ = resp.getLast_log_term();
                            followerCommittedLogId_ = resp.getCommitted_log_id();
                            AppendLogResponse r = new AppendLogResponse();
                            r.setError_code(ErrorCode.SUCCEEDED);
                            setResponse(r);
                        } else {
                            lastLogIdSent_ = Math.min(resp.getLast_log_id(), logIdToSend_ - 1);
                            lastLogTermSent_ = resp.getLast_log_term();
                            followerCommittedLogId_ = resp.getCommitted_log_id();
                            newReq = prepareAppendLogRequest();
                        }
                    } finally {
                        lock_.unlock();
                    }

                    if (newReq != null) {
                        appendLogsInternal(newReq);
                    } else {
                        noMoreRequestLock.lock();
                        noMoreRequestCV_.signalAll();
                        noMoreRequestLock.unlock();
                    }
                    return;
                }
                default: {
                    LOGGER.error(idStr_ + "Failed to append logs to the host (Err: " + resp.getError_code() + ")");
                    try {
                        lock_.lock();
                        setResponse(resp);
                        lastLogIdSent_ = logIdToSend_ - 1;
                    } finally {
                        lock_.unlock();
                    }
                    noMoreRequestLock.lock();
                    noMoreRequestCV_.signalAll();
                    noMoreRequestLock.unlock();
                    return;
                }
            }
        });
    }

    private AppendLogRequest prepareAppendLogRequest() {
        Preconditions.checkState(lock_.tryLock());
        AppendLogRequest req = new AppendLogRequest();
        req.setSpace(part_.spaceId());
        req.setPart(part_.partitionId());
        req.setCurrent_term(logTermToSend_);
        req.setLast_log_id(logIdToSend_);
        req.setLeader_addr(part_.address().getHostString());
        req.setLeader_port(part_.address().getPort());
        req.setCommitted_log_id(committedLogId_);
        req.setLast_log_term_sent(lastLogTermSent_);
        req.setLast_log_id_sent(lastLogIdSent_);

        LOGGER.info(idStr_ + "Prepare AppendLogs request from Log "
                + lastLogIdSent_ + 1 + " to " + logIdToSend_);

        if (lastLogIdSent_ + 1 > part_.wal().lastLogId()) {
            LOGGER.info(idStr_ + "My lastLogId in wal is " + part_.wal().lastLogId()
                    + ", but you are seeking " + lastLogIdSent_ + 1
                    + ", so i have nothing to send.");
            return req;
        }

        LogIterator it = part_.wal().iterator(lastLogIdSent_ + 1, logIdToSend_);
        if (it.valid()) {
            LOGGER.info(idStr_ + "Prepare the list of log entries to send");
            long term = it.logTerm();
            req.setLog_term(term);
            List<LogEntry> logs = new ArrayList<>();
            for (int i = 0; it.valid() && it.logTerm() == term && i < MAX_APPENDLOG_BATCH_SIZE; it.next(), ++i) {
                LogEntry le = new LogEntry();
                le.setCluster(it.logSource());
                le.setLog_str(it.logMsg());
                logs.add(le);
            }
            req.setLog_str_list(logs);
            req.setSending_snapshot(false);
        } else {
            req.setSending_snapshot(true);
            if (!sendingSnapshot_.get()) {
                LOGGER.info(idStr_ + "Can't find log " + lastLogIdSent_ + 1
                        + " in wal, send the snapshot"
                        + ", logIdToSend = " + logIdToSend_
                        + ", firstLogId in wal = " + part_.wal().firstLogId()
                        + ", lastLogId in wal = " + part_.wal().lastLogId());
                sendingSnapshot_.set(true);
                part_.snapshot_.sendSnapshot(part_, addr_)
                        .thenAccept(status -> {
                            if (status.ok()) {
                                LOGGER.info(idStr_ + "Send snapshot succeeded!");
                            } else {
                                LOGGER.info(idStr_ + "Send snapshot failed!");
                                // TODO: we should tell the follower i am failed.
                            }
                            sendingSnapshot_.set(false);
                        });
            } else {
                LOGGER.info(idStr_ + "The snapshot req is in queue, please wait for a moment");
            }
        }

        return req;
    }

    private CompletableFuture<AppendLogResponse> sendAppendLogRequest(AppendLogRequest req) {
        LOGGER.info(idStr_ + " Entering Host::sendAppendLogRequest()");

        try {
            lock_.lock();
            ErrorCode res = checkStatus();
            if (res != ErrorCode.SUCCEEDED) {
                LOGGER.warn(idStr_ + " The Host is not in a proper status, do not send");
                AppendLogResponse resp = new AppendLogResponse();
                resp.setError_code(res);
                return CompletableFuture.completedFuture(resp);
            }
        } finally {
            lock_.unlock();
        }

        LOGGER.trace(idStr_ + "Sending appendLog: space " + req.getSpace()
                + ", part " + req.getPart()
                + ", current term " + req.getCurrent_term()
                + ", last_log_id " + req.getLast_log_id()
                + ", committed_id " + req.getCommitted_log_id()
                + ", last_log_term_sent" + req.getLast_log_term_sent()
                + ", last_log_id_sent " + req.getLast_log_id_sent());

        CompletableFuture<AppendLogResponse> response = new CompletableFuture<>();

        new Thread(() -> {
            try {
                AppendLogResponse resp = part_.clientMan_.client(addr_, 0).appendLog(req);
                response.complete(resp);
            } catch (Exception e) {
                e.printStackTrace();
                response.completeExceptionally(e);
            }
        }).start();

        return response;
    }

    public CompletableFuture<HeartbeatResponse> sendHeartbeat(
            long term,
            long latestLogId,
            long commitLogId,
            long lastLogTerm,
            long lastLogId
    ) {
        HeartbeatRequest req = new HeartbeatRequest();
        req.setSpace(part_.spaceId());
        req.setPart(part_.partitionId());
        req.setCurrent_term(term);
        req.setLast_log_id(latestLogId);
        req.setCommitted_log_id(commitLogId);
        req.setLeader_addr(part_.address().getHostString());
        req.setLeader_port(part_.address().getPort());
        req.setLast_log_term_sent(lastLogTerm);
        req.setLast_log_id_sent(lastLogId);

        CompletableFuture<HeartbeatResponse> future = sendHeartbeatRequest(req);
        future.handle((r, t) -> {
            LOGGER.info(idStr_ + " heartbeat call got response");
            if (t != null) {
                HeartbeatResponse resp = new HeartbeatResponse();
                resp.setError_code(ErrorCode.E_EXCEPTION);
                return resp;
            } else {
                return r;
            }
        });
        return future;
    }

    private CompletableFuture<HeartbeatResponse> sendHeartbeatRequest(HeartbeatRequest req) {
        LOGGER.info(idStr_ + " Entering Host::sendHeartbeatRequest()");

        try {
            lock_.lock();
            ErrorCode res = checkStatus();
            if (res != ErrorCode.SUCCEEDED) {
                LOGGER.warn(idStr_ + " The Host is not in a proper status, do not send");
                HeartbeatResponse resp = new HeartbeatResponse();
                resp.setError_code(res);
                return CompletableFuture.completedFuture(resp);
            }
        } finally {
            lock_.unlock();
        }

        LOGGER.trace(idStr_ + "Sending heartbeat: space " + req.getSpace()
                + ", part " + req.getPart()
                + ", current term " + req.getCurrent_term()
                + ", last_log_id " + req.getLast_log_id()
                + ", committed_id " + req.getCommitted_log_id()
                + ", last_log_term_sent" + req.getLast_log_term_sent()
                + ", last_log_id_sent " + req.getLast_log_id_sent());

        CompletableFuture<HeartbeatResponse> response = new CompletableFuture<>();

        new Thread(() -> {
            try {
                HeartbeatResponse resp = part_.clientMan_.client(addr_, 0).heartbeat(req);
                response.complete(resp);
            } catch (Exception e) {
                e.printStackTrace();
                response.completeExceptionally(e);
            }
        }).start();

        return response;
    }


    private boolean noRequest() {
        Preconditions.checkState(lock_.tryLock());
        return pendingReq_.equals(new Triplet<>(0L, 0L, 0L));
    }

    public boolean isLearner_() {
        return isLearner_;
    }

    public void setLearner_(boolean learner_) {
        this.isLearner_ = learner_;
    }

    public InetSocketAddress address() {
        return addr_;
    }
}
