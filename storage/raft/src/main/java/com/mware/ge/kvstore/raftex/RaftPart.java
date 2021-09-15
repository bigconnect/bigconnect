package com.mware.ge.kvstore.raftex;

import com.mware.ge.collection.Pair;
import com.mware.ge.kvstore.DiskManager;
import com.mware.ge.kvstore.wal.FileBasedWal;
import com.mware.ge.kvstore.wal.FileBasedWal.FileBasedWalInfo;
import com.mware.ge.kvstore.wal.FileBasedWalPolicy;
import com.mware.ge.time.Clocks;
import com.mware.ge.util.GeLogger;
import com.mware.ge.util.GeLoggerFactory;
import com.mware.ge.util.Preconditions;
import org.apache.commons.lang3.RandomUtils;

import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public class RaftPart implements AutoCloseable {
    private GeLogger LOGGER = GeLoggerFactory.getLogger(RaftPart.class);

    // The max number of logs in a batch
    public static final int MAX_BATCH_SIZE = 256;

    String idStr_;
    long clusterId_;
    int spaceId_;
    int partId_;
    SocketAddress addr_;
    // hosts_ contains all connection, hosts_ = all peers and listeners
    List<Host> hosts_ = new ArrayList<>();
    int quorum_ = 0;
    // all listener's role is learner (cannot promote to follower)
    List<SocketAddress> listeners_ = new ArrayList<>();
    // The lock is used to protect logs_ and cachingPromise_
    Lock logsLock_ = new ReentrantLock();
    AtomicBoolean replicatingLogs_ = new AtomicBoolean(false);
    AtomicBoolean bufferOverFlow_ = new AtomicBoolean(false);
    Status status_;
    Role role_;

    // Partition level lock to synchronize the access of the partition
    Lock raftLock_ = new ReentrantLock();

    // When the partition is the leader, the leader_ is same as addr_
    SocketAddress leader_;
    // After voted for somebody, it will not be empty anymore.
    // And it will be reset to empty after current election finished.
    SocketAddress votedAddr_;

    // The current term id
    // the term id proposed by that candidate
    long term_ = 0;

    // During normal operation, proposedTerm_ is equal to term_,
    // when the partition becomes a candidate, proposedTerm_ will be
    // bumped up by 1 every time when sending out the AskForVote
    // Request

    // If voted for somebody, the proposeTerm will be reset to the candidate
    // propose term. So we could use it to prevent revote if someone else ask for
    // vote for current proposedTerm.

    // TODO: We should persist it on the disk in the future
    // Otherwise, after restart the whole cluster, maybe the stale
    // leader still has the unsend log with larger term, and after other
    // replicas elected the new leader, the stale one will not join in the
    // Raft group any more.
    long proposedTerm_;

    // The id and term of the last-sent log
    long lastLogId_ = 0;
    long lastLogTerm_ = 0;
    // The id for the last globally committed log (from the leader)
    long committedLogId_ = 0;

    // To record how long ago when the last leader message received
    Duration lastMsgRecvDur_;
    // To record how long ago when the last log message or heartbeat was sent
    Duration lastMsgSentDur_;
    // To record when the last message was accepted by majority peers
    long lastMsgAcceptedTime_ = 0;
    // How long between last message was sent and was accepted by majority peers
    long lastMsgAcceptedCostMs_ = 0;
    // Make sure only one election is in progress
    AtomicBoolean inElection_ = new AtomicBoolean(false);
    // Speed up first election when I don't know who is leader
    boolean isBlindFollower_ = true;
    // Check leader has commit log in this term (accepted by majority is not enough),
    // leader is not allowed to service until it is true.
    boolean commitInThisTerm_ = false;
    // Write-ahead Log
    FileBasedWal wal_;

    // IO Thread pool
    ExecutorService ioThreadPool_;
    // Shared worker thread pool
    ScheduledExecutorService bgWorkers_;
    // Workers pool
    ExecutorService executor_;

    SnapshotManager snapshot_;

    RaftexService.AsyncClient clientMan_;
    // Used in snapshot, record the last total count and total size received from request
    long lastTotalCount_ = 0;
    long lastTotalSize_ = 0;
    Duration lastSnapshotRecvDur_;

    // Check if disk has enough space before write wal
    DiskManager diskMan_;

    // Used to bypass the stale command
    long startTimeMs_ = 0;

    AtomicLong weight_ = new AtomicLong();

    AtomicBoolean blocking_ = new AtomicBoolean(false);
    List<LogTuple> logs_;

    PromiseSet<AppendLogResult> sendingPromise_ = new PromiseSet<>();
    PromiseSet<AppendLogResult> cachingPromise_ = new PromiseSet<>();

    // Protected constructor to prevent from instantiating directly
    protected RaftPart(
            long clusterId,
            int spaceId,
            int partId,
            InetSocketAddress localAddr,
            String walRoot,
            ExecutorService pool,
            ExecutorService workers,
            ExecutorService executor,
            SnapshotManager snapshotMan,
            RaftexService.AsyncClient clientMan,
            DiskManager diskMan
    ) {
        idStr_ = String.format("[Port: %d, Space: %d, Part: %d] ", localAddr.getPort(), spaceId, partId);
        Preconditions.checkNotNull(executor, idStr_ + "executor should not be null");
        clusterId_ = clusterId;
        spaceId_ = spaceId;
        partId_ = partId;
        addr_ = localAddr;
        status_ = Status.STARTING;
        role_ = Role.FOLLOWER;
        leader_ = new InetSocketAddress("", 0);
        ioThreadPool_ = pool;
        bgWorkers_ = workers;
        executor_ = executor;
        snapshot_ = snapshotMan;
        clientMan_ = clientMan;
        diskMan_ = diskMan;
        weight_.set(1);

        FileBasedWalPolicy policy = new FileBasedWalPolicy();
        policy.setFileSize(FileBasedWal.WAL_FILE_SIZE);
        policy.setBufferSize(FileBasedWal.WAL_BUFFER_SIZE);
        policy.setSync(FileBasedWal.WAL_SYNC);

        FileBasedWalInfo info = new FileBasedWalInfo();
        info.idStr = idStr_;
        info.spaceId = spaceId_;
        info.partId = partId_;

        wal_ = FileBasedWal.getWal(
                walRoot, info, policy,
                (logId, logTermId, logClusterId, log) -> preProcessLog(logId, logTermId, logClusterId, log),
                diskMan
        );

        logs_ = new ArrayList<>(MAX_BATCH_SIZE);
    }

    private String roleStr(Role role) {
        switch (role) {
            case LEADER:
                return "Leader";
            case FOLLOWER:
                return "Follower";
            case CANDIDATE:
                return "Candidate";
            case LEARNER:
                return "Learner";
            default:
                LOGGER.error(idStr_ + "Invalid role");
        }
        return null;
    }

    public void start(List<SocketAddress> peers, boolean asLearner) {
        try {
            raftLock_.lock();

            lastLogId_ = wal_.lastLogId();
            lastLogTerm_ = wal_.lastLogTerm();
            term_ = proposedTerm_ = lastLogTerm_;

            // Set the quorum number
            quorum_ = (peers.size() + 1) / 2;

            Pair<Long, Long> logIdAndTerm = lastCommittedLogId();
            committedLogId_ = logIdAndTerm.first();

            if (lastLogId_ < committedLogId_) {
                LOGGER.info(idStr_ + "Reset lastLogId " + lastLogId_ + " to be the committedLogId " + committedLogId_);
                lastLogId_ = committedLogId_;
                lastLogTerm_ = term_;
                wal_.reset();
            }

            LOGGER.info(idStr_ + "There are " + peers.size() + " peer hosts, and total "
                    + peers.size() + 1 + " copies. The quorum is " + (quorum_ + 1) + ", as learner " + asLearner
                    + ", lastLogId " + lastLogId_ + ", lastLogTerm " + lastLogTerm_ + ", committedLogId " + committedLogId_
                    + ", term " + term_);

            // Start all peer hosts
            for (SocketAddress addr : peers) {
                LOGGER.info(idStr_ + "Add peer " + addr);
                hosts_.add(new Host(addr, this));
            }

            // Change the status
            status_ = Status.RUNNING;
            if (asLearner) {
                role_ = Role.LEARNER;
            }

            startTimeMs_ = Clocks.systemClock().millis();

            // Set up a leader election task
            int delayMS = 100 + RandomUtils.nextInt(0, 900);
            bgWorkers_.schedule(() -> statusPolling(startTimeMs_), delayMS, TimeUnit.MILLISECONDS)
        } finally {
            raftLock_.unlock();
        }
    }

    public void stop() {
        LOGGER.info(idStr_ + "Stopping the partition");

        List<Host> hosts;
        try {
            raftLock_.lock();
            status_ = Status.STOPPED;
            leader_ = new InetSocketAddress("", 0);
            role_ = Role.FOLLOWER;
            hosts = this.hosts_;
        } finally {
            raftLock_.unlock();
        }

        for (Host h : hosts) {
            h.stop();
        }

        for (Host h : hosts) {
            LOGGER.info(idStr_ + "Waiting " + h.idStr() + " to stop");
            h.waitForStop();
            LOGGER.info(idStr_ + "Waiting " + h.idStr() + " has stopped");
        }

        hosts.clear();
        LOGGER.info(idStr_ + "Partition has been stopped");
    }

    public void addLearner(SocketAddress addr) {
        assert raftLock_.tryLock();
        if (addr.equals(this.addr_)) {
            LOGGER.info(idStr_ + "I am already a learner!");
            return;
        }

        Optional<Host> existing = hosts_.stream().filter(h -> h.address().equals(addr)).findFirst();
        if (existing.isPresent()) {
            LOGGER.info(idStr_ + "The host " + addr + " already exists as " + ((existing.get().isLearner()) ? "learner" : "group member"));
        } else {
            hosts_.add(new Host(addr, this, true));
            LOGGER.info(idStr_ + "Adding learner " + addr);
        }
    }

    public void preProcessTransLeader(SocketAddress target) {
        assert raftLock_.tryLock();
        LOGGER.info(idStr_ + "Pre process transfer leader to " + target);
        switch (role_) {
            case FOLLOWER: {
                if (!target.equals(addr_) && !target.equals(new InetSocketAddress("", 0))) {
                    LOGGER.info(idStr_ + "I am follower, just wait for the new leader.");
                } else {
                    LOGGER.info(idStr_ + "I will be the new leader, trigger leader election now!";);
                    bgWorkers_.submit(() -> {
                        try {
                            raftLock_.lock();
                            role_ = Role.CANDIDATE;
                            leader_ = new InetSocketAddress("", 0);
                        } finally {
                            raftLock_.unlock();
                        }

                        leaderElection();
                    });
                }
                break;
            }
            default: {
                LOGGER.info(idStr_ + "My role is " + roleStr(role_) + ", so do nothing when pre process transfer leader");
            }
        }
    }

    public void commitTransLeader(SocketAddress target) {
        boolean needToUnlock = raftLock_.tryLock();
        LOGGER.info(idStr_ + "Commit transfer leader to " + target);
        switch (role_) {
            case LEADER: {
                if (!target.equals(addr_) && !hosts_.isEmpty()) {
                    Optional<Host> first = hosts_.stream().filter(h -> !h.isLearner()).findFirst();
                    if (first.isPresent()) {
                        lastMsgRecvDur_ = Duration.ZERO;
                        role_ = Role.FOLLOWER;
                        leader_ = new InetSocketAddress("", 0);
                        LOGGER.info(idStr_ + "Give up my leadership!");
                    }
                } else {
                    LOGGER.info(idStr_ + "I am already the leader!");
                }
                break;
            }

            case FOLLOWER:
            case CANDIDATE: {
                LOGGER.info(idStr_ + "I am " + roleStr(role_) + ", just wait for the new leader!");
                break;
            }
            case LEARNER: {
                LOGGER.info(idStr_ + "I am learner, not in the raft group, skip the log");
                break;
            }
        }
        if (needToUnlock) {
            raftLock_.unlock();
        }
    }

    private void updateQuorum() {
        Preconditions.checkState(!raftLock_.tryLock());
        int total = 0;
        for (Host h : hosts_) {
            if (!h.isLearner())
                total++;
        }
        quorum_ = (total + 1) / 2;
    }

    protected void addPeer(SocketAddress peer) {
        Preconditions.checkState(!raftLock_.tryLock());
        if (peer.equals(addr_)) {
            if (role_ == Role.LEARNER) {
                LOGGER.info(idStr_ + "I am learner, promote myself to be follower");
                role_ = Role.FOLLOWER;
                updateQuorum();
            } else {
                LOGGER.info(idStr_ + "I am already in the raft group!");
            }
            return;
        }
        Optional<Host> existing = hosts_.stream().filter(h -> h.address().equals(peer)).findAny();
        if (!existing.isPresent()) {
            hosts_.add(new Host(peer, this));
            updateQuorum();
            LOGGER.info(idStr_ + "Add peer " + peer);
        } else {
            if (existing.get().isLearner()) {
                LOGGER.info(idStr_ + "The host " + peer + " exists as a learner, promote it!");
                existing.get().setLearner(false);
                updateQuorum();
            } else {
                LOGGER.info(idStr_ + "The host " + peer + " exists as a follower!");
            }
        }
    }

    protected void removePeer(SocketAddress peer) {
        Preconditions.checkState(!raftLock_.tryLock());
        if (peer.equals(addr_)) {
            // The part will be removed in REMOVE_PART_ON_SRC phase
            LOGGER.info(idStr_ + "Remove myself from the raft group.");
            return;
        }
        Optional<Host> existing = hosts_.stream().filter(h -> h.address().equals(peer)).findFirst();
        if (!existing.isPresent()) {
            LOGGER.info(idStr_ + "The peer " + peer + " does not exist!");
        } else {
            if (existing.get().isLearner()) {
                LOGGER.info(idStr_ + "The peer is learner, remove it directly!");
                hosts_.removeIf(h -> h.address().equals(peer));
                return;
            }
            hosts_.removeIf(h -> h.address().equals(peer));
            updateQuorum();
            LOGGER.info(idStr_ + "Remove peer " + peer);
        }
    }

    protected ErrorCode checkPeer(SocketAddress candidate) {
        Preconditions.checkState(!raftLock_.tryLock());
        List<Host> hosts = followers();
        Optional<Host> existing = hosts_.stream().filter(h -> h.address().equals(candidate)).findFirst();
        if (!existing.isPresent()) {
            LOGGER.info(idStr_ + "The candidate " + candidate + " is not in my peers");
            return ErrorCode.E_WRONG_LEADER;
        }
        return ErrorCode.SUCCEEDED;
    }

    public void addListenerPeer(SocketAddress listener) {
        try {
            raftLock_.lock();
            if (listener.equals(addr_)) {
                LOGGER.info(idStr_ + "I am already in the raft group");
                return;
            }
            Optional<Host> existing = hosts_.stream().filter(h -> h.address().equals(listener)).findFirst();
            if (!existing.isPresent()) {
                // Add listener as a raft learner
                hosts_.add(new Host(listener, this, true));
                listeners_.add(listener);
                LOGGER.info(idStr_ + "Add listener " + listener);
            } else {
                LOGGER.info(idStr_ + "The listener " + listener + " already joined the raft group");
            }
        } finally {
            raftLock_.unlock();
        }
    }

    public void removeListenerPeer(SocketAddress listener) {
        try {
            raftLock_.lock();
            if (listener.equals(addr_)) {
                LOGGER.info(idStr_ + "Remove myself from the raft group");
                return;
            }
            Optional<Host> existing = hosts_.stream().filter(h -> h.address().equals(listener)).findFirst();
            if (!existing.isPresent()) {
                LOGGER.info(idStr_ + "The listener " + listener + " not found in the raft group");
            } else {
                hosts_.removeIf(h -> h.address().equals(listener));
                listeners_.removeIf(l -> l.equals(listener));
                LOGGER.info(idStr_ + "Remove listener " + listener);
            }
        } finally {
            raftLock_.unlock();
        }
    }

    public void preProcessRemovePeer(SocketAddress peer) {
        Preconditions.checkState(!raftLock_.tryLock());
        if (role_ == Role.LEADER) {
            LOGGER.info(idStr_ + "I am leader, skip remove peer in preProcessLog");
            return;
        }
        removePeer(peer);
    }

    public void commitRemovePeer(SocketAddress peer) {
        boolean needToUnlock = raftLock_.tryLock();
        try {
            if (role_ == Role.FOLLOWER || role_ == Role.LEARNER) {
                LOGGER.info(idStr_ + "I am " + roleStr(role_)
                        + ", skip remove peer in commit");
                return;
            }
            Preconditions.checkState(role_ == Role.LEADER);
            removePeer(peer);
        } finally {
            if (needToUnlock) {
                raftLock_.unlock();
            }
        }
    }

    /*****************************************************************
     * Asynchronously append a log
     *
     * This is the **PUBLIC** Log Append API, used by storage
     * service
     *
     * The method will take the ownership of the log and returns
     * as soon as possible. Internally it will asynchronously try
     * to send the log to all followers. It will keep trying until
     * majority of followers accept the log, then the future will
     * be fulfilled
     *
     * If the clusterId == -1, the current clusterId will be used
     ****************************************************************/
    public CompletableFuture<AppendLogResult> appendAsync(long clusterId, byte[] log) {
        if (clusterId < 0) {
            clusterId = clusterId_;
        }
        return appendLogAsync(clusterId, LogType.NORMAL, log);
    }

    /****************************************************************
     * Run the op atomically.
     ***************************************************************/
    public CompletableFuture<AppendLogResult> atomicOpAsync(AtomicOp op) {
        return appendLogAsync(clusterId_, LogType.ATOMIC_OP, new byte[0], op);
    }

    /**
     * Asynchronously send one command.
     */
    public CompletableFuture<AppendLogResult> sendCommandAsync(byte[] log) {
        return appendLogAsync(clusterId_, LogType.COMMAND, log, null);
    }

    private CompletableFuture<AppendLogResult> appendLogAsync(long source, LogType logType, byte[] log, AtomicOp op) {
        if (blocking_.get()) {
            // No need to block heartbeats and empty log.
            if ((logType == LogType.NORMAL && (log != null && log.length > 0) || logType == LogType.ATOMIC_OP) {
                return CompletableFuture.completedFuture(AppendLogResult.E_WRITE_BLOCKING);
            }
        }

        CompletableFuture<AppendLogResult> retFuture = new CompletableFuture<>();
        List<LogTuple> swappedOutLogs;

        if (bufferOverFlow_.get()) {
            LOGGER.warn(idStr_ + "The appendLog buffer is full. Please slow down the log appending rate. replicatingLogs_:" + replicatingLogs_.get());
            return CompletableFuture.completedFuture(AppendLogResult.E_BUFFER_OVERFLOW);
        }

        try {
            logsLock_.lock();
            LOGGER.info(idStr_ + "Checking whether buffer overflow");

            if (logs_.size() >= MAX_BATCH_SIZE) {
                // Buffer is full
                LOGGER.warn(idStr_ + "The appendLog buffer is full. Please slow down the log appending rate. replicatingLogs_:" + replicatingLogs_.get());
                bufferOverFlow_.set(true);
                return CompletableFuture.completedFuture(AppendLogResult.E_BUFFER_OVERFLOW);
            }

            LOGGER.info(idStr_ + "Appending logs to the buffer");

            // Append new logs to the buffer
            assert source >= 0;
            logs_.add(new LogTuple(source, logType, log, op));
            switch (logType) {
                case ATOMIC_OP:
                    retFuture = cachingPromise_.getSingleFuture();
                    break;
                case COMMAND:
                    retFuture = cachingPromise_.getAndRollSharedFuture();
                    break;
                case NORMAL:
                    retFuture = cachingPromise_.getSharedFuture();
                    break;
            }

            if (replicatingLogs_.compareAndSet(false, true)) {
                // We need to send logs to all followers
                LOGGER.info(idStr_ + "Preparing to send AppendLog request");
                sendingPromise_ = cachingPromise_.copy();
                cachingPromise_.reset();
                swappedOutLogs = new ArrayList<>(logs_);
                logs_ = new ArrayList<>();
                bufferOverFlow_.set(false);
            } else {
                LOGGER.info(idStr_ + "Another AppendLogs request is ongoing, just return");
                return retFuture;
            }
        } finally {
            logsLock_.unlock();
        }

        long firstId = 0;
        long termId = 0;
        AppendLogResult res;
        try {
            raftLock_.lock();
            res = canAppendLogs();
            if (res == AppendLogResult.SUCCEEDED) {
                firstId = lastLogId_ + 1;
                termId = term_;
            }
        } finally {
            raftLock_.unlock();
        }

        if (!checkAppendLogResult(res)) {
            // Mosy likely failed because the parttion is not leader
            LOGGER.warn(idStr_ + "Cannot append logs, clean the buffer");
            return CompletableFuture.completedFuture(res);
        }

        // Replicate buffered logs to all followers
        // Replication will happen on a separate thread and will block
        // until majority accept the logs, the leadership changes, or
        // the partition stops
        AppendLogsIterator it = new AppendLogsIterator(firstId, termId, swappedOutLogs, opCB -> {
            Preconditions.checkNotNull(opCB);
            Optional<String> opRet = opCB.get();
            if (!opRet.isPresent()) {
                // Failed
                sendingPromise_.setOneSingleValue(AppendLogResult.E_ATOMIC_OP_FAILURE);
            }
            return opRet;
        });
        appendLogsInternal(it, termId);
    }

    /**
     * TODO
     * TODO
     * TODO
     * TODO
     * TODO
     * TODO
     * TODO
     */

    private AppendLogResult canAppendLogs() {
        assert raftLock_.tryLock();
        if (status_ != Status.RUNNING) {
            LOGGER.error(idStr_ + "The partition is not running");
            return AppendLogResult.E_STOPPED;
        }
        if (role_ != Role.LEADER) {
            LOGGER.error(idStr_ + "The partition is not a leader");
            return AppendLogResult.E_NOT_A_LEADER;
        }
        return AppendLogResult.SUCCEEDED;
    }

    private AppendLogResult canAppendLogs(long termId) {
        assert raftLock_.tryLock();
        if (term_ != termId) {
            LOGGER.error(idStr_ + "Term has been updated, origin " + termId + ", new " + term_);
            return AppendLogResult.E_TERM_OUT_OF_DATE;
        }
        return canAppendLogs();
    }

    @Override
    public void close() throws Exception {
        try {
            raftLock_.lock();

            // Make sure the partition has stopped
            Preconditions.checkState(status_ == Status.STOPPED);
            LOGGER.info(idStr_ + "The part has been destroyed...");
        } finally {
            raftLock_.unlock();
        }
    }

    enum Role {
        LEADER,     // the leader
        FOLLOWER,       // following a leader
        CANDIDATE,      // Has sent AskForVote request
        LEARNER         // It is the same with FOLLOWER,
        // except it does not participate in leader election
    }

    enum Status {
        STARTING,   // The part is starting, not ready for service
        RUNNING,        // The part is running
        STOPPED,        // The part has been stopped
        WAITING_SNAPSHOT  // Waiting for the snapshot.
    }

    enum AppendLogResult {
        SUCCEEDED(0),
        E_ATOMIC_OP_FAILURE(-1),
        E_NOT_A_LEADER(-2),
        E_STOPPED(-3),
        E_NOT_READY(-4),
        E_BUFFER_OVERFLOW(-5),
        E_WAL_FAILURE(-6),
        E_TERM_OUT_OF_DATE(-7),
        E_SENDING_SNAPSHOT(-8),
        E_INVALID_PEER(-9),
        E_NOT_ENOUGH_ACKS(-10),
        E_WRITE_BLOCKING(-11);

        int value;

        AppendLogResult(int value) {
            this.value = value;
        }
    }

    enum LogType {
        NORMAL(0x00),
        ATOMIC_OP(0x01),
        /**
         * COMMAND is similar to AtomicOp, but not the same. There are two differences:
         * 1. Normal logs after AtomicOp could be committed together. In opposite, Normal logs
         * after COMMAND should be hold until the COMMAND committed, but the logs before
         * COMMAND could be committed together.
         * 2. AtomicOp maybe failed. So we use SinglePromise for it. But COMMAND not, so it could
         * share one promise with the normal logs before it.
         */
        COMMAND(0x02);

        int value;

        LogType(int value) {
            this.value = value;
        }
    }

    public static class LogTuple {
        long clusterId;
        LogType logType;
        byte[] msg;
        AtomicOp atomicOp;

        public LogTuple() {
        }

        public LogTuple(long clusterId, LogType logType, byte[] msg, AtomicOp atomicOp) {
            this.clusterId = clusterId;
            this.logType = logType;
            this.msg = msg;
            this.atomicOp = atomicOp;
        }
    }
}
