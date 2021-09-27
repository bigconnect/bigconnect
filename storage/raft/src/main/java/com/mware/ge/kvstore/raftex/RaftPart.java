package com.mware.ge.kvstore.raftex;

import com.mware.ge.collection.Pair;
import com.mware.ge.kvstore.DiskManager;
import com.mware.ge.kvstore.utils.DurationCounter;
import com.mware.ge.kvstore.utils.FutureUtils;
import com.mware.ge.kvstore.utils.LogIterator;
import com.mware.ge.kvstore.wal.FileBasedWal;
import com.mware.ge.kvstore.wal.FileBasedWal.FileBasedWalInfo;
import com.mware.ge.kvstore.wal.FileBasedWalPolicy;
import com.mware.ge.kvstore.wal.Wal;
import com.mware.ge.thrift.ThriftClientManager;
import com.mware.ge.time.Clocks;
import com.mware.ge.util.GeLogger;
import com.mware.ge.util.GeLoggerFactory;
import com.mware.ge.util.Preconditions;
import org.apache.commons.lang3.RandomUtils;

import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
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
import java.util.stream.Collectors;

public abstract class RaftPart implements AutoCloseable {
    private GeLogger LOGGER = GeLoggerFactory.getLogger(RaftPart.class);

    // The max number of logs in a batch
    public static final int MAX_BATCH_SIZE = 256;
    // Seconds between each heartbeat
    public static final int RAFT_HEARTBEAT_INTERVAL_SECS = 5;
    // Max seconds between two snapshot requests
    public static final int RAFT_SNAPSHOT_TIMEOUT = 60 * 5;

    String idStr_;
    long clusterId_;
    int spaceId_;
    int partId_;
    InetSocketAddress addr_;
    // hosts_ contains all connection, hosts_ = all peers and listeners
    List<Host> hosts_ = new ArrayList<>();
    int quorum_ = 0;
    // all listener's role is learner (cannot promote to follower)
    List<InetSocketAddress> listeners_ = new ArrayList<>();
    // The lock is used to protect logs_ and cachingPromise_
    Lock logsLock_ = new ReentrantLock();
    AtomicBoolean replicatingLogs_ = new AtomicBoolean(false);
    AtomicBoolean bufferOverFlow_ = new AtomicBoolean(false);
    Status status_;
    Role role_;

    // Partition level lock to synchronize the access of the partition
    Lock raftLock_ = new ReentrantLock();

    // When the partition is the leader, the leader_ is same as addr_
    InetSocketAddress leader_;
    // After voted for somebody, it will not be empty anymore.
    // And it will be reset to empty after current election finished.
    InetSocketAddress votedAddr_;

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
    DurationCounter lastMsgRecvDur_ = new DurationCounter();
    // To record how long ago when the last log message or heartbeat was sent
    DurationCounter lastMsgSentDur_ = new DurationCounter();
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

    // Shared worker thread pool
    ScheduledExecutorService bgWorkers_;
    // Workers pool
    ExecutorService executor_;

    SnapshotManager snapshot_;

    ThriftClientManager clientMan_;
    // Used in snapshot, record the last total count and total size received from request
    long lastTotalCount_ = 0;
    long lastTotalSize_ = 0;
    DurationCounter lastSnapshotRecvDur_ = new DurationCounter();

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
            ScheduledExecutorService workers,
            SnapshotManager snapshotMan,
            ThriftClientManager clientMan,
            DiskManager diskMan
    ) {
        idStr_ = String.format("[Port: %d, Space: %d, Part: %d] ", localAddr.getPort(), spaceId, partId);
        clusterId_ = clusterId;
        spaceId_ = spaceId;
        partId_ = partId;
        addr_ = localAddr;
        status_ = Status.STARTING;
        role_ = Role.FOLLOWER;
        leader_ = new InetSocketAddress("", 0);
        bgWorkers_ = workers;
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

    public void start(List<InetSocketAddress> peers, boolean asLearner) {
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
            for (InetSocketAddress addr : peers) {
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
            bgWorkers_.schedule(() -> statusPolling(startTimeMs_), delayMS, TimeUnit.MILLISECONDS);
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

    public void addLearner(InetSocketAddress addr) {
        assert raftLock_.tryLock();
        if (addr.equals(this.addr_)) {
            LOGGER.info(idStr_ + "I am already a learner!");
            return;
        }

        Optional<Host> existing = hosts_.stream().filter(h -> h.address().equals(addr)).findFirst();
        if (existing.isPresent()) {
            LOGGER.info(idStr_ + "The host " + addr + " already exists as " + ((existing.get().isLearner_()) ? "learner" : "group member"));
        } else {
            hosts_.add(new Host(addr, this, true));
            LOGGER.info(idStr_ + "Adding learner " + addr);
        }
    }

    public void preProcessTransLeader(InetSocketAddress target) {
        assert raftLock_.tryLock();
        LOGGER.info(idStr_ + "Pre process transfer leader to " + target);
        switch (role_) {
            case FOLLOWER: {
                if (!target.equals(addr_) && !target.equals(new InetSocketAddress("", 0))) {
                    LOGGER.info(idStr_ + "I am follower, just wait for the new leader.");
                } else {
                    LOGGER.info(idStr_ + "I will be the new leader, trigger leader election now!");
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

    public void commitTransLeader(InetSocketAddress target) {
        boolean needToUnlock = raftLock_.tryLock();
        LOGGER.info(idStr_ + "Commit transfer leader to " + target);
        switch (role_) {
            case LEADER: {
                if (!target.equals(addr_) && !hosts_.isEmpty()) {
                    Optional<Host> first = hosts_.stream().filter(h -> !h.isLearner_()).findFirst();
                    if (first.isPresent()) {
                        lastMsgRecvDur_.reset(false);
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
            if (!h.isLearner_())
                total++;
        }
        quorum_ = (total + 1) / 2;
    }

    protected void addPeer(InetSocketAddress peer) {
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
            if (existing.get().isLearner_()) {
                LOGGER.info(idStr_ + "The host " + peer + " exists as a learner, promote it!");
                existing.get().setLearner_(false);
                updateQuorum();
            } else {
                LOGGER.info(idStr_ + "The host " + peer + " exists as a follower!");
            }
        }
    }

    protected void removePeer(InetSocketAddress peer) {
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
            if (existing.get().isLearner_()) {
                LOGGER.info(idStr_ + "The peer is learner, remove it directly!");
                hosts_.removeIf(h -> h.address().equals(peer));
                return;
            }
            hosts_.removeIf(h -> h.address().equals(peer));
            updateQuorum();
            LOGGER.info(idStr_ + "Remove peer " + peer);
        }
    }

    protected ErrorCode checkPeer(InetSocketAddress candidate) {
        Preconditions.checkState(!raftLock_.tryLock());
        List<Host> hosts = followers();
        Optional<Host> existing = hosts_.stream().filter(h -> h.address().equals(candidate)).findFirst();
        if (!existing.isPresent()) {
            LOGGER.info(idStr_ + "The candidate " + candidate + " is not in my peers");
            return ErrorCode.E_WRONG_LEADER;
        }
        return ErrorCode.SUCCEEDED;
    }

    public void addListenerPeer(InetSocketAddress listener) {
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

    public void removeListenerPeer(InetSocketAddress listener) {
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

    public void preProcessRemovePeer(InetSocketAddress peer) {
        Preconditions.checkState(!raftLock_.tryLock());
        if (role_ == Role.LEADER) {
            LOGGER.info(idStr_ + "I am leader, skip remove peer in preProcessLog");
            return;
        }
        removePeer(peer);
    }

    public void commitRemovePeer(InetSocketAddress peer) {
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
        return appendLogAsync(clusterId, LogType.NORMAL, log, null);
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
            if ((logType == LogType.NORMAL && (log != null && log.length > 0)) || logType == LogType.ATOMIC_OP) {
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
            if (opRet.isEmpty()) {
                // Failed
                sendingPromise_.setOneSingleValue(AppendLogResult.E_ATOMIC_OP_FAILURE);
            }
            return opRet;
        });
        appendLogsInternal(it, termId);
        return retFuture;
    }

    private void appendLogsInternal(AppendLogsIterator iter, long termId) {
        long currTerm = 0;
        long prevLogId = 0;
        long prevLogTerm = 0;
        long committed = 0;
        long lastId = 0;

        if (iter.valid()) {
            LOGGER.info(idStr_ + "Ready to append logs from id "
                    + iter.logId() + " (Current term is "
                    + currTerm + ")");
        } else {
            LOGGER.error(idStr_ + "Only happend when Atomic op failed");
            replicatingLogs_.set(false);
            return;
        }

        AppendLogResult res = AppendLogResult.SUCCEEDED;
        do {
            try {
                raftLock_.lock();
                res = canAppendLogs(termId);
                if (res != AppendLogResult.SUCCEEDED) {
                    break;
                }
                currTerm = term_;
                prevLogId = lastLogId_;
                prevLogTerm = lastLogTerm_;
                committed = committedLogId_;
                // Step 1: Write WAL
                if (!wal_.appendLogs(iter)) {
                    LOGGER.warn(idStr_ + "Failed to write into WAL");
                    res = AppendLogResult.E_WAL_FAILURE;
                    break;
                }
                lastId = wal_.lastLogId();
                LOGGER.info(idStr_ + "Succeeded writing logs ["
                        + iter.firstLogId() + ", " + lastId + "] to WAL");
            } finally {
                raftLock_.unlock();
            }
        } while (false);

        if (!checkAppendLogResult(res)) {
            LOGGER.warn(idStr_ + "Failed to write wal");
            return;
        }

        // Step 2: Replicate to followers
        replicateLogs(
                iter,
                currTerm,
                lastId,
                committed,
                prevLogTerm,
                prevLogId);
    }

    private void replicateLogs(
            AppendLogsIterator iter,
            long currTerm,
            long lastLogId,
            long committedId,
            long prevLogTerm,
            long prevLogId
    ) {
        AppendLogResult res = AppendLogResult.SUCCEEDED;
        final ArrayList<Host> hosts;

        do {
            try {
                raftLock_.lock();
                res = canAppendLogs(currTerm);
                if (res != AppendLogResult.SUCCEEDED) {
                    hosts = new ArrayList<>();
                    break;
                }
                hosts = new ArrayList<>(hosts_);
            } finally {
                raftLock_.unlock();
            }
        } while (false);


        if (!checkAppendLogResult(res)) {
            LOGGER.warn(idStr_ + "replicateLogs failed because of not leader or term changed");
            return;
        }

        LOGGER.info(idStr_ + "About to replicate logs to all peer hosts");

        lastMsgSentDur_.reset(false);

        FutureUtils.collectNSucceeded(
                hosts.stream().map(h -> {
                    LOGGER.info(idStr_ + "Appending logs to " + h.idStr());
                    return h.appendLogs(currTerm, lastLogId, committedId, prevLogTerm, prevLogId);
                }).collect(Collectors.toList()),
                quorum_,
                pair -> {
                    return pair.other().getError_code() == ErrorCode.SUCCEEDED && !hosts.get(pair.first()).isLearner_();
                }
        ).thenAccept(responses -> {
            LOGGER.info(idStr_ + "Received enough response");
            executor_.submit(() -> processAppendLogResponses(
                    responses, iter, currTerm, lastLogId, committedId, prevLogTerm, prevLogId, hosts));
        });
    }

    private void processAppendLogResponses(
            List<Pair<Integer, AppendLogResponse>> resps,
            AppendLogsIterator iter,
            long currTerm,
            long lastLogId,
            long committedId,
            long prevLogTerm,
            long prevLogId,
            List<Host> hosts
    ) {
        // Make sure majority have succeeded
        int numSucceeded = 0;
        for (Pair<Integer, AppendLogResponse> res : resps) {
            if (!hosts.get(res.first()).isLearner_() && res.other().getError_code() == ErrorCode.SUCCEEDED) {
                ++numSucceeded;
            }
        }

        if (numSucceeded >= quorum_) {
            // Majority have succeeded
            LOGGER.info(idStr_ + numSucceeded + " hosts have accepted the logs");
            long firstLogId = 0;
            AppendLogResult res = AppendLogResult.SUCCEEDED;
            do {
                try {
                    raftLock_.lock();
                    res = canAppendLogs(currTerm);
                    if (res != AppendLogResult.SUCCEEDED) {
                        break;
                    }
                    lastLogId_ = lastLogId;
                    lastLogTerm_ = currTerm;
                } finally {
                    raftLock_.unlock();
                }
            } while (false);

            if (!checkAppendLogResult(res)) {
                LOGGER.warn(idStr_ + "processAppendLogResponses failed because of not leader or term changed");
                return;
            }

            LogIterator walIt = wal_.iterator(committedId + 1, lastLogId);
            // Step 3: Commit the batch
            if (commitLogs(walIt, true) == ErrorCode.SUCCEEDED) {
                try {
                    raftLock_.lock();
                    committedLogId_ = lastLogId;
                    firstLogId = lastLogId_ + 1;
                    lastMsgAcceptedCostMs_ = lastMsgSentDur_.elapsedInMSec();
                    lastMsgAcceptedTime_ = Clocks.systemClock().millis();
                    commitInThisTerm_ = true;
                } finally {
                    raftLock_.unlock();
                }
            } else {
                LOGGER.error(idStr_ + "Failed to commit logs");
            }

            LOGGER.info(idStr_ + "Leader succeeded in committing the logs " + committedId + 1 + " to " + lastLogId);

            // Step 4: Fulfill the promise
            if (iter.hasNonAtomicOpLogs()) {
                sendingPromise_.setOneSharedValue(AppendLogResult.SUCCEEDED);
            }
            if (iter.leadByAtomicOp()) {
                sendingPromise_.setOneSingleValue(AppendLogResult.SUCCEEDED);
            }

            // Step 5: Check whether need to continue
            // the log replication
            try {
                logsLock_.lock();
                Preconditions.checkState(replicatingLogs_.get());
                // Continue to process the original AppendLogsIterator if necessary
                iter.resume();
                // If no more valid logs to be replicated in iter, create a new one if we have new log
                if (iter.empty()) {
                    LOGGER.info(idStr_ + "logs size " + logs_.size());
                    if (logs_.size() > 0) {
                        // continue to replicate the logs
                        sendingPromise_ = cachingPromise_.copy();
                        cachingPromise_.reset();
                        iter = new AppendLogsIterator(firstLogId, currTerm, logs_, op -> {
                            Optional<String> opRet = op.get();
                            if (!opRet.isPresent()) {
                                // Failed
                                sendingPromise_.setOneSingleValue(AppendLogResult.E_ATOMIC_OP_FAILURE);
                            }
                            return opRet;
                        });
                        logs_.clear();
                        bufferOverFlow_.set(false);
                    }

                    // Reset replicatingLogs_ one of the following is true:
                    // 1. old iter is empty && logs_.size() == 0
                    // 2. old iter is empty && logs_.size() > 0, but all logs in new iter is atomic op,
                    //    and all of them failed, which would make iter is empty again
                    if (iter.empty()) {
                        replicatingLogs_.set(false);
                        LOGGER.info(idStr_ + "No more log to be replicated");
                        return;
                    }
                }
            } finally {
                logsLock_.unlock();
            }

            appendLogsInternal(iter, currTerm);
        } else {
            // Not enough hosts accepted the log, re-try
            LOGGER.warn(idStr_ + "Only " + numSucceeded + " hosts succeeded, Need to try again");
            replicateLogs(
                    iter,
                    currTerm,
                    lastLogId,
                    committedId,
                    prevLogTerm,
                    prevLogId);
        }
    }

    private boolean needToSendHeartbeat() {
        try {
            raftLock_.lock();
            return status_ == Status.RUNNING && role_ == Role.LEADER;
        } finally {
            raftLock_.unlock();
        }
    }

    private boolean needToStartElection() {
        try {
            raftLock_.lock();
            if (status_ == Status.RUNNING && role_ == Role.FOLLOWER
                    && (lastMsgRecvDur_.elapsedInMSec() >= weight_.get() * RAFT_HEARTBEAT_INTERVAL_SECS) || isBlindFollower_
            ) {
                LOGGER.info(idStr_ + "Start leader election, reason: lastMsgDur "
                        + lastMsgRecvDur_.elapsedInMSec()
                        + ", term " + term_);
                role_ = Role.CANDIDATE;
                leader_ = new InetSocketAddress("", 0);
            }

            return role_ == Role.CANDIDATE;
        } finally {
            raftLock_.unlock();
        }
    }

    private AskForVoteRequest prepareElectionRequest(List<Host> hosts) {
        try {
            raftLock_.lock();
            // Make sure the partition is running
            if (status_ != Status.RUNNING) {
                LOGGER.info(idStr_ + "The partition is not running");
                return null;
            }

            if (status_ == Status.STOPPED) {
                LOGGER.info(idStr_ + "The part has been stopped, skip the request");
                return null;
            }

            if (status_ == Status.STARTING) {
                LOGGER.info(idStr_ + "The partition is still starting");
                return null;
            }

            if (status_ == Status.WAITING_SNAPSHOT) {
                LOGGER.info(idStr_ + "The partition is still waiting snapshot");
                return null;
            }

            // Make sure the role is still CANDIDATE
            if (role_ != Role.CANDIDATE) {
                LOGGER.info(idStr_ + "A leader has been elected");
                return null;
            }

            // Before start a new election, reset the votedAddr
            votedAddr_ = new InetSocketAddress("", 0);

            AskForVoteRequest req = new AskForVoteRequest();
            req.setSpace(spaceId_);
            req.setPart(partId_);
            req.setCandidate_addr(addr_.getHostString());
            req.setCandidate_port(addr_.getPort());
            req.setTerm(++proposedTerm_);  // Bump up the proposed term
            req.setLast_log_id(lastLogId_);
            req.setLast_log_term(lastLogTerm_);

            hosts.clear();
            hosts.addAll(followers());

            return req;
        } finally {
            raftLock_.unlock();
        }
    }

    private Role processElectionResponses(List<Pair<Integer, AskForVoteResponse>> results, List<Host> hosts, long proposedTerm) {
        try {
            raftLock_.lock();

            if (status_ == Status.STOPPED) {
                LOGGER.info(idStr_ + "The part has been stopped, skip the request");
                return role_;
            }
            if (status_ == Status.STARTING) {
                LOGGER.info(idStr_ + "The partition is still starting");
                return role_;
            }

            if (status_ == Status.WAITING_SNAPSHOT) {
                LOGGER.info(idStr_ + "The partition is still waiting snapshot");
                return role_;
            }

            if (role_ != Role.CANDIDATE) {
                LOGGER.info(idStr_ + "Partition's role has changed to " + roleStr(role_) + " during the election, so discard the results");
                return role_;
            }

            int numSucceeded = 0;
            for (Pair<Integer, AskForVoteResponse> r : results) {
                if (r.other().getError_code() == ErrorCode.SUCCEEDED) {
                    ++numSucceeded;
                } else if (r.other().getError_code() == ErrorCode.E_LOG_STALE) {
                    LOGGER.info(idStr_ + "My last log id is less than " + hosts.get(r.first()).address()
                            + ", double my election interval.");
                    long curWeight = weight_.get();
                    weight_.set(curWeight * 2);
                } else {
                    LOGGER.error(idStr_ + "Receive response about askForVote from "
                            + hosts.get(r.first()).address() + ", error code is " + r.other().getError_code());
                }
            }

            Preconditions.checkState(role_ == Role.CANDIDATE);

            if (numSucceeded >= quorum_) {
                LOGGER.info(idStr_ + "Partition is elected as the new leader for term " + proposedTerm);
                term_ = proposedTerm;
                role_ = Role.LEADER;
                isBlindFollower_ = false;
            }

            return role_;
        } finally {
            raftLock_.unlock();
        }
    }

    private boolean leaderElection() {
        LOGGER.info(idStr_ + "Start leader election...");

        if (!inElection_.compareAndSet(false, true)) {
            return true;
        }

        try {
            final List<Host> hosts = new ArrayList<>(hosts_);
            AskForVoteRequest voteReq = prepareElectionRequest(hosts);
            if (voteReq == null) {
                // Suppose we have three replicas A(leader), B, C, after A crashed,
                // B, C will begin the election. B win, and send hb, C has gap with B
                // and need the snapshot from B. Meanwhile C begin the election,
                // C will be Candidate, but because C is in WAITING_SNAPSHOT,
                // so prepareElectionRequest will return false and go on the election.
                // Becasue C is in Candidate, so it will reject the snapshot request from B.
                // Infinite loop begins.
                // So we neeed to go back to the follower state to avoid the case.
                try {
                    raftLock_.lock();
                    role_ = Role.FOLLOWER;
                    return false;
                } finally {
                    raftLock_.unlock();
                }
            }

            // Send out the AskForVoteRequest
            LOGGER.info(idStr_ + "Sending out an election request "
                    + "(space = " + voteReq.getSpace()
                    + ", part = " + voteReq.getPart()
                    + ", term = " + voteReq.getTerm()
                    + ", lastLogId = " + voteReq.getLast_log_id()
                    + ", lastLogTerm = " + voteReq.getLast_log_term()
                    + ", candidateIP = "
                    + voteReq.getCandidate_addr()
                    + ", candidatePort = " + voteReq.getCandidate_port()
                    + ")");

            long proposedTerm = voteReq.getTerm();
            List<Pair<Integer, AskForVoteResponse>> resps = new ArrayList<>();
            if (hosts_.isEmpty()) {
                LOGGER.info(idStr_ + "No peer found, I will be the leader");
            } else {
                CompletableFuture<List<Pair<Integer, AskForVoteResponse>>> futures = FutureUtils.collectNSucceeded(
                        hosts_.stream().map(h -> {
                            LOGGER.info(idStr_ + "Sending AskForVoteRequest to " + h.address());
                            return h.askForVote(voteReq);
                        }).collect(Collectors.toList()),
                        quorum_,
                        pair -> {
                            return pair.other().getError_code() == ErrorCode.SUCCEEDED && !hosts.get(pair.first()).isLearner_();
                        }
                );

                LOGGER.info(idStr_ + "AskForVoteRequest has been sent to all peers, waiting for responses");
                futures.join();
                if (futures.isCompletedExceptionally()) {
                    throw new RuntimeException("Got exception ");
                }
                LOGGER.info(idStr_ + "Got AskForVote response back");
                resps = futures.get();
            }

            switch (processElectionResponses(resps, hosts, proposedTerm)) {
                case LEADER: {
                    // Elected
                    LOGGER.info(idStr_ + "The partition is elected as the leader");
                    try {
                        raftLock_.lock();
                        if (status_ == Status.RUNNING) {
                            leader_ = addr_;
                            hosts.forEach(Host::reset);
                            bgWorkers_.submit(() -> onElected(voteReq.getTerm()));
                            lastMsgAcceptedTime_ = 0;
                        }
                        weight_.set(1);
                        commitInThisTerm_ = false;
                    } finally {
                        raftLock_.unlock();
                    }
                    sendHeartbeat();
                    return true;
                }
                case FOLLOWER: {
                    // Someone was elected
                    LOGGER.info(idStr_ + "Someone else was elected");
                    return true;
                }
                case CANDIDATE: {
                    // No one has been elected
                    LOGGER.info(idStr_ + "No one is elected, continue the election");
                    return false;
                }
                case LEARNER: {
                    LOGGER.error(idStr_ + " Impossible! There must be some bugs!");
                    return false;
                }
            }

            LOGGER.error(idStr_ + "Should not be here");
            return false;
        } catch (Exception e) {
            throw new RuntimeException("Got exception ", e);
        } finally {
            inElection_.set(false);
        }
    }

    private void statusPolling(long startTime) {
        try {
            raftLock_.lock();
            // If startTime is not same as the time when `statusPolling` is add to event loop,
            // it means the part has been restarted (it only happens in ut for now), so don't
            // add another `statusPolling`.
            if (startTime != startTimeMs_) {
                return;
            }
        } finally {
            raftLock_.unlock();
        }

        long delay = RAFT_HEARTBEAT_INTERVAL_SECS * 1000 / 3;
        if (needToStartElection()) {
            if (leaderElection()) {
                LOGGER.info(idStr_ + "Stop the election");
            } else {
                // No leader has been elected, need to continue
                // (After sleeping a random period betwen [500ms, 2s])
                LOGGER.info(idStr_ + "Wait for a while and continue the leader election");
                delay = (RandomUtils.nextInt(0, 1500) + 500) * weight_.get();
            }
        } else if (needToSendHeartbeat()) {
            LOGGER.info(idStr_ + "Need to send heartbeat");
            sendHeartbeat();
        }
        if (needToCleanupSnapshot()) {
            LOGGER.info(idStr_ + "Clean up the snapshot");
            cleanupSnapshot();
        }
        try {
            raftLock_.lock();
            if (status_ == Status.RUNNING || status_ == Status.WAITING_SNAPSHOT) {
                LOGGER.info(idStr_ + "Schedule new task");
                bgWorkers_.schedule(() -> statusPolling(startTime), delay, TimeUnit.MILLISECONDS);
            }
        } finally {
            raftLock_.unlock();
        }
    }

    private boolean needToCleanupSnapshot() {
        try {
            raftLock_.lock();
            return status_ == Status.WAITING_SNAPSHOT && role_ != Role.LEADER
                    && lastSnapshotRecvDur_.elapsedInSec() != RAFT_SNAPSHOT_TIMEOUT;
        } finally {
            raftLock_.unlock();
        }
    }

    private void cleanupSnapshot() {
        LOGGER.info(idStr_ + "Clean up the snapshot");
        try {
            raftLock_.lock();
            reset();
            status_ = Status.RUNNING;
        } finally {
            raftLock_.unlock();
        }
    }

    private boolean needToCleanWal() {
        try {
            raftLock_.lock();
            if (status_ == Status.STARTING || status_ == Status.WAITING_SNAPSHOT) {
                return false;
            }
            for (Host h : hosts_) {
                if (h.sendingSnapshot_.get())
                    return false;
            }
            return true;
        } finally {
            raftLock_.unlock();
        }
    }

    // Process the incoming leader election request
    protected void processAskForVoteRequest(AskForVoteRequest req, AskForVoteResponse resp) {
        LOGGER.info(idStr_ + "Recieved a VOTING request"
                + ": space = " + req.getSpace()
                + ", partition = " + req.getPart()
                + ", candidateAddr = "
                + req.getCandidate_addr() + ":"
                + req.getCandidate_port()
                + ", term = " + req.getTerm()
                + ", lastLogId = " + req.getLast_log_id()
                + ", lastLogTerm = " + req.getLast_log_term());

        try {
            raftLock_.lock();

            // Make sure the partition is running
            if (status_ == Status.STOPPED) {
                LOGGER.info(idStr_ + "The part has been stopped, skip the request");
                resp.setError_code(ErrorCode.E_BAD_STATE);
                return;
            }

            if (status_ == Status.STARTING) {
                LOGGER.info(idStr_ + "The partition is still starting");
                resp.setError_code(ErrorCode.E_NOT_READY);
                return;
            }

            if (status_ == Status.WAITING_SNAPSHOT) {
                LOGGER.info(idStr_ + "The partition is still waiting snapshot");
                resp.setError_code(ErrorCode.E_NOT_READY);
                return;
            }

            LOGGER.info(idStr_ + "The partition currently is a "
                    + roleStr(role_) + ", lastLogId " + lastLogId_
                    + ", lastLogTerm " + lastLogTerm_
                    + ", committedLogId " + committedLogId_
                    + ", term " + term_);

            if (role_ == Role.LEARNER) {
                resp.setError_code(ErrorCode.E_BAD_ROLE);
                return;
            }

            InetSocketAddress candidate = new InetSocketAddress(req.getCandidate_addr(), req.getCandidate_port());
            // Check term id
            long term = term_;
            if (req.getTerm() <= term) {
                LOGGER.info(idStr_ + (role_ == Role.CANDIDATE
                        ? "The partition is currently proposing term "
                        : "The partition currently is on term ")
                        + term
                        + ". The term proposed by the candidate is no greater, so it will be rejected");
                resp.setError_code(ErrorCode.E_TERM_OUT_OF_DATE);
                return;
            }

            // Check the last term to receive a log
            if (req.getLast_log_term() < lastLogTerm_) {
                LOGGER.info(idStr_ + "The partition's last term to receive a log is "
                        + lastLogTerm_
                        + ", which is newer than the candidate's log "
                        + req.getLast_log_term()
                        + ". So the candidate will be rejected");
                resp.setError_code(ErrorCode.E_TERM_OUT_OF_DATE);
                return;
            }

            if (req.getLast_log_term() == lastLogTerm_) {
                // Check last log id
                if (req.getLast_log_id() < lastLogId_) {
                    LOGGER.info(idStr_ + "The partition's last log id is " + lastLogId_
                            + ". The candidate's last log id " + req.getLast_log_id()
                            + " is smaller, so it will be rejected, candidate is "
                            + candidate);
                    resp.setError_code(ErrorCode.E_LOG_STALE);
                    return;
                }
            }

            // If we have voted for somebody, we will reject other candidates under the proposedTerm.
            if (!new InetSocketAddress("", 0).equals(votedAddr_)) {
                if (proposedTerm_ > req.getTerm() || (proposedTerm_ == req.getTerm() && !candidate.equals(votedAddr_))) {
                    LOGGER.info(idStr_ + "We have voted " + votedAddr_ + " on term " + proposedTerm_
                            + ", so we should reject the candidate " + candidate
                            + " request on term " + req.getTerm());
                    resp.setError_code(ErrorCode.E_TERM_OUT_OF_DATE);
                    return;
                }
            }

            ErrorCode code = checkPeer(candidate);
            if (code != ErrorCode.SUCCEEDED) {
                resp.setError_code(code);
                return;
            }

            // Ok, no reason to refuse, we will vote for the candidate
            LOGGER.info(idStr_ + "The partition will vote for the candidate " + candidate);
            resp.setError_code(ErrorCode.SUCCEEDED);

            // Before change role from leader to follower, check the logs locally.
            if (role_ == Role.LEADER && wal_.lastLogId() > lastLogId_) {
                LOGGER.info(idStr_ + "There is one log " + wal_.lastLogId()
                        + " i did not commit when i was leader, rollback to " + lastLogId_);
                wal_.rollbackToLog(lastLogId_);
            }

            role_ = Role.FOLLOWER;
            votedAddr_ = candidate;
            proposedTerm_ = req.getTerm();
            leader_ = new InetSocketAddress("", 0);

            // Reset the last message time
            lastMsgRecvDur_.reset(false);
            weight_.set(1);
            isBlindFollower_ = false;
        } finally {
            raftLock_.unlock();
        }
    }

    protected void processAppendLogRequest(AppendLogRequest req, AppendLogResponse resp) {
        LOGGER.info(idStr_ + "Received logAppend"
                + ": GraphSpaceId = " + req.getSpace()
                + ", partition = " + req.getPart()
                + ", leaderIp = " + req.getLeader_addr()
                + ", leaderPort = " + req.getLeader_port()
                + ", current_term = " + req.getCurrent_term()
                + ", lastLogId = " + req.getLast_log_id()
                + ", committedLogId = " + req.getCommitted_log_id()
                + ", lastLogIdSent = " + req.getLast_log_id_sent()
                + ", lastLogTermSent = " + req.getLast_log_term_sent()
                + ", num_logs = " + req.getLog_str_list().size()
                + ", logTerm = " + req.getLog_term()
                + ", sendingSnapshot = " + req.isSending_snapshot()
                + ", local lastLogId = " + lastLogId_
                + ", local lastLogTerm = " + lastLogTerm_
                + ", local committedLogId = " + committedLogId_
                + ", local current term = " + term_);

        try {
            raftLock_.lock();
            resp.setCurrent_term(term_);
            resp.setLeader_addr(leader_.getHostString());
            resp.setLeader_port(leader_.getPort());
            resp.setCommitted_log_id(committedLogId_);
            resp.setLast_log_id(lastLogId_ < committedLogId_ ? committedLogId_ : lastLogId_);
            resp.setLast_log_term(lastLogTerm_);

            // Check status
            if (status_ == Status.STOPPED) {
                LOGGER.info(idStr_ + "The part has been stopped, skip the request");
                resp.setError_code(ErrorCode.E_BAD_STATE);
                return;
            }

            if (status_ == Status.STARTING) {
                LOGGER.info(idStr_ + "The partition is still starting");
                resp.setError_code(ErrorCode.E_NOT_READY);
                return;
            }

            // Check leadership
            ErrorCode err = verifyLeader(new VerifyLeaderParams(req));
            if (err != ErrorCode.SUCCEEDED) {
                // Wrong leadership
                LOGGER.info(idStr_ + "Will not follow the leader");
                resp.setError_code(err);
                return;
            }

            // Reset the timeout timer
            lastMsgRecvDur_.reset(false);

            if (req.isSending_snapshot() && status_ != Status.WAITING_SNAPSHOT) {
                LOGGER.info(idStr_ + "Begin to wait for the snapshot " + req.getCommitted_log_id());
                reset();
                status_ = Status.WAITING_SNAPSHOT;
                resp.setError_code(ErrorCode.E_WAITING_SNAPSHOT);
                return;
            }

            if (status_ == Status.WAITING_SNAPSHOT) {
                LOGGER.info(idStr_ + "The part is receiving snapshot,"
                        + "so just accept the new wals, but don't commit them."
                        + "last_log_id_sent " + req.getLast_log_id_sent()
                        + ", total log number " + req.getLog_str_list().size());
                if (lastLogId_ > 0 && req.getLast_log_id_sent() > lastLogId_) {
                    // There is a gap
                    LOGGER.info(idStr_ + "Local is missing logs from id "
                            + lastLogId_ + ". Need to catch up");
                    resp.setError_code(ErrorCode.E_LOG_GAP);
                    return;
                }

                // TODO: if we have 3 node, one is leader, one is wait snapshot and return success,
                // the other is follower, but leader replica log to follow failed,
                // How to deal with leader crash? At this time, no leader will be elected.
                int numLogs = req.getLog_str_list().size();
                long firstId = req.getLast_log_id_sent() + 1;
                LOGGER.info(idStr_ + " Writing log [" + firstId + ", " + (firstId + numLogs - 1) + "] to WAL");
                LogStrListIterator iter = new LogStrListIterator(firstId, req.getLog_term(), req.getLog_str_list());
                if (wal_.appendLogs(iter)) {
                    // When leader has been sending a snapshot already, sometimes it would send a request
                    // with empty log list, and lastLogId in wal may be 0 because of reset.
                    if (numLogs != 0) {
                        if ((firstId + numLogs - 1) != wal_.lastLogId()) {
                            LOGGER.error(idStr_ + " First Id is " + firstId);
                        }
                        lastLogId_ = wal_.lastLogId();
                        lastLogTerm_ = wal_.lastLogTerm();
                        resp.setLast_log_id(lastLogId_);
                        resp.setLast_log_term(lastLogTerm_);
                        resp.setError_code(ErrorCode.SUCCEEDED);
                    }
                } else {
                    LOGGER.info(idStr_ + "Failed to append logs to WAL");
                    resp.setError_code(ErrorCode.E_WAL_FAIL);
                }
                return;
            }

            if (req.getLast_log_id_sent() < committedLogId_ && req.getLast_log_term_sent() <= term_) {
                LOGGER.info(idStr_ + "Stale log! The log " + req.getLast_log_id_sent()
                        + ", term " + req.getLast_log_term_sent()
                        + " i had committed yet. My committedLogId is "
                        + committedLogId_ + ", term is " + term_);
                resp.setError_code(ErrorCode.E_LOG_STALE);
                return;
            } else if (req.getLast_log_id_sent() < committedLogId_) {
                LOGGER.info(idStr_ + "What?? How is this happening ? The log id is "
                        + req.getLast_log_id_sent()
                        + ", the log term is " + req.getLast_log_term_sent()
                        + ", but my committedLogId is " + committedLogId_
                        + ", my term is " + term_
                        + ", to make the cluster stable i will follow the high term"
                        + " candidate and clenaup my data");
                reset();
                resp.setCommitted_log_id(committedLogId_);
                resp.setLast_log_id(lastLogId_);
                resp.setLast_log_term(lastLogTerm_);
                return;
            }

            if (lastLogTerm_ > 0 && req.getLast_log_term_sent() != lastLogTerm_) {
                LOGGER.info(idStr_ + "The local last log term is " + lastLogTerm_
                        + ", which is different from the leader's prevLogTerm "
                        + req.getLast_log_term_sent()
                        + ", the prevLogId is " + req.getLast_log_id_sent()
                        + ". So need to rollback to last committedLogId_ " + committedLogId_);
                if (wal_.rollbackToLog(committedLogId_)) {
                    lastLogId_ = wal_.lastLogId();
                    lastLogTerm_ = wal_.lastLogTerm();
                    resp.setLast_log_id(lastLogId_);
                    resp.setLast_log_term(lastLogTerm_);
                    LOGGER.info(idStr_ + "Rollback succeeded! lastLogId is " + lastLogId_
                            + ", logLogTerm is " + lastLogTerm_
                            + ", committedLogId is " + committedLogId_
                            + ", term is " + term_);
                }
                resp.setError_code(ErrorCode.E_LOG_GAP);
                return;
            } else if (req.getLast_log_id_sent() > lastLogId_) {
                // There is a gap
                LOGGER.info(idStr_ + "Local is missing logs from id "
                        + lastLogId_ + ". Need to catch up");
                resp.setError_code(ErrorCode.E_LOG_GAP);
                return;
            } else if (req.getLast_log_id_sent() < lastLogId_) {
                // TODO: This is a potential bug which would cause data not in consensus. In most
                // case, we would hit this path when leader append logs to follower and timeout (leader
                // would set lastLogIdSent_ = logIdToSend_ - 1 in Host). **But follower actually received
                // it successfully**. Which will explain when leader retry to append these logs, the LOG
                // belows is printed, and lastLogId_ == req.get_last_log_id_sent() + 1 in the LOG.
                //
                // In fact we should always rollback to req.get_last_log_id_sent(), and append the logs from
                // leader (we can't make promise that the logs in range [req.get_last_log_id_sent() + 1,
                // lastLogId_] is same with follower). However, this makes no difference in the above case.
                LOGGER.info(idStr_ + "Stale log! Local lastLogId " + lastLogId_
                        + ", lastLogTerm " + lastLogTerm_
                        + ", lastLogIdSent " + req.getLast_log_id_sent()
                        + ", lastLogTermSent " + req.getLast_log_term_sent());
                resp.setError_code(ErrorCode.E_LOG_GAP);
                return;
            }

            // Append new logs
            int numLogs = req.getLog_str_list().size();
            long firstId = req.getLast_log_id_sent() + 1;
            LOGGER.info(idStr_ + "Writing log [" + firstId + ", " + (firstId + numLogs - 1) + "] to WAL");
            LogStrListIterator iter = new LogStrListIterator(firstId, req.getLog_term(), req.getLog_str_list());
            if (wal_.appendLogs(iter)) {
                if (numLogs != 0) {
                    if ((firstId + numLogs - 1) != wal_.lastLogId()) {
                        LOGGER.error(idStr_ + "First Id is " + firstId);
                    }
                    lastLogId_ = wal_.lastLogId();
                    lastLogTerm_ = wal_.lastLogTerm();
                    resp.setLast_log_id(lastLogId_);
                    resp.setLast_log_term(lastLogTerm_);
                }
            } else {
                LOGGER.info(idStr_ + "Failed to append logs to WAL");
                resp.setError_code(ErrorCode.E_WAL_FAIL);
                return;
            }

            long lastLogIdCanCommit = Math.max(lastLogId_, req.getCommitted_log_id());
            if (lastLogIdCanCommit > committedLogId_) {
                // Commit some logs
                // We can only commit logs from firstId to min(lastLogId_, leader's commit log id),
                // follower can't always commit to leader's commit id because of lack of log
                ErrorCode code = commitLogs(wal_.iterator(committedLogId_ + 1, lastLogIdCanCommit), false);
                if (code == ErrorCode.SUCCEEDED) {
                    LOGGER.info(idStr_ + "Follower succeeded committing log "
                            + committedLogId_ + 1 + " to "
                            + lastLogIdCanCommit);
                    committedLogId_ = lastLogIdCanCommit;
                    resp.setCommitted_log_id(lastLogIdCanCommit);
                    resp.setError_code(ErrorCode.SUCCEEDED);
                } else if (code == ErrorCode.E_WRITE_STALLED) {
                    LOGGER.info(idStr_ + "Follower delay committing log "
                            + committedLogId_ + 1 + " to "
                            + lastLogIdCanCommit);
                    // Even if log is not applied to state machine, still regard as succeded:
                    // 1. As a follower, upcoming request will try to commit them
                    // 2. If it is elected as leader later, it will try to commit them as well
                    resp.setCommitted_log_id(committedLogId_);
                    resp.setError_code(ErrorCode.SUCCEEDED);
                } else {
                    LOGGER.info(idStr_ + "Failed to commit log "
                            + committedLogId_ + 1 + " to "
                            + req.getCommitted_log_id());
                    resp.setError_code(ErrorCode.E_WAL_FAIL);
                }
            } else {
                resp.setError_code(ErrorCode.SUCCEEDED);
            }

            // Reset the timeout timer again in case wal and commit takes longer time than expected
            lastMsgRecvDur_.reset(false);
        } finally {
            raftLock_.unlock();
        }
    }

    static class VerifyLeaderParams {
        public String leader_addr;
        public int leader_port;
        public long current_term;

        public VerifyLeaderParams(AppendLogRequest req) {
            this.leader_addr = req.getLeader_addr();
            this.leader_port = req.getLeader_port();
            this.current_term = req.getCurrent_term();
        }

        public VerifyLeaderParams(HeartbeatRequest req) {
            this.leader_addr = req.getLeader_addr();
            this.leader_port = req.getLeader_port();
            this.current_term = req.getCurrent_term();
        }
    }

    private ErrorCode verifyLeader(VerifyLeaderParams req) {
        assert !raftLock_.tryLock();
        InetSocketAddress candidate = new InetSocketAddress(req.leader_addr, req.leader_port);
        ErrorCode code = checkPeer(candidate);
        if (code != ErrorCode.SUCCEEDED) {
            return code;
        }

        LOGGER.info(idStr_ + "The current role is " + roleStr(role_));
        // Make sure the remote term is greater than local's
        if (req.current_term < term_) {
            LOGGER.info(idStr_ + "The current role is " + roleStr(role_)
                    + ". The local term is " + term_
                    + ". The remote term is not newer");
            return ErrorCode.E_TERM_OUT_OF_DATE;
        } else if (req.current_term > term_) {
            // Leader stickness, no matter the term in Request is larger or not.
            // TODO Maybe we should reconsider the logic
            if (new InetSocketAddress("", 0).equals(leader_) && !candidate.equals(leader_)
                    && lastMsgRecvDur_.elapsedInMSec() < (RAFT_HEARTBEAT_INTERVAL_SECS * 1000)) {
                LOGGER.info(idStr_ + "I believe the leader " + leader_ + " exists. "
                        + "Refuse to append logs of " + candidate);
                return ErrorCode.E_WRONG_LEADER;
            }
        } else {
            do {
                if (role_ != Role.LEADER && new InetSocketAddress("", 0).equals(leader_)) {
                    LOGGER.info(idStr_ + "I dont know who is leader for current term " + term_
                            + ", so accept the candidate " + candidate);
                    break;
                }

                // Same leader
                if (role_ != Role.LEADER && candidate.equals(leader_)) {
                    return ErrorCode.SUCCEEDED;
                } else {
                    LOGGER.info(idStr_ + "The local term is same as remote term " + term_
                            + ", my role is " + roleStr(role_) + ", reject it!");
                    return ErrorCode.E_TERM_OUT_OF_DATE;
                }
            } while (false);
        }

        // Update my state.
        Role oldRole = role_;
        long oldTerm = term_;

        // Ok, no reason to refuse, just follow the leader
        LOGGER.info(idStr_ + "The current role is " + roleStr(role_)
                + ". Will follow the new leader "
                + req.leader_addr
                + ":" + req.leader_addr
                + " [Term: " + req.current_term + "]");

        if (role_ != Role.LEARNER) {
            role_ = Role.FOLLOWER;
        }

        leader_ = candidate;
        term_ = proposedTerm_ = req.current_term;
        votedAddr_ = new InetSocketAddress("", 0);
        weight_.set(1);
        isBlindFollower_ = false;

        // Before accept the logs from the new leader, check the logs locally.
        if (wal_.lastLogId() > lastLogId_) {
            LOGGER.info(idStr_ + "There is one log " + wal_.lastLogId()
                    + " i did not commit when i was leader, rollback to " + lastLogId_);
            wal_.rollbackToLog(lastLogId_);
        }
        if (oldRole == Role.LEADER) {
            // Need to invoke onLostLeadership callback
            bgWorkers_.submit(() -> onLostLeadership(oldTerm));
        }
        bgWorkers_.submit(() -> onDiscoverNewLeader(leader_));
        return ErrorCode.SUCCEEDED;
    }

    protected void processHeartbeatRequest(HeartbeatRequest req, HeartbeatResponse resp) {
        LOGGER.info(idStr_ + "Received heartbeat"
                + ": GraphSpaceId = " + req.getSpace()
                + ", partition = " + req.getPart()
                + ", leaderIp = " + req.getLeader_addr()
                + ", leaderPort = " + req.getLeader_port()
                + ", current_term = " + req.getCurrent_term()
                + ", lastLogId = " + req.getLast_log_id()
                + ", committedLogId = " + req.getCommitted_log_id()
                + ", lastLogIdSent = " + req.getLast_log_id_sent()
                + ", lastLogTermSent = " + req.getLast_log_term_sent()
                + ", local lastLogId = " + lastLogId_
                + ", local lastLogTerm = " + lastLogTerm_
                + ", local committedLogId = " + committedLogId_
                + ", local current term = " + term_);

        try {
            raftLock_.lock();

            resp.setCurrent_term(term_);
            resp.setLeader_addr(leader_.getHostString());
            resp.setLeader_port(leader_.getPort());
            resp.setCommitted_log_id(committedLogId_);
            resp.setLast_log_id(lastLogId_ < committedLogId_ ? committedLogId_ : lastLogId_);
            resp.setLast_log_term(lastLogTerm_);

            // Check status
            if (status_ == Status.STOPPED) {
                LOGGER.info(idStr_ + "The part has been stopped, skip the request");
                resp.setError_code(ErrorCode.E_BAD_STATE);
                return;
            }

            if (status_ == Status.STARTING) {
                LOGGER.info(idStr_ + "The partition is still starting");
                resp.setError_code(ErrorCode.E_NOT_READY);
                return;
            }

            // Check leadership
            ErrorCode err = verifyLeader(new VerifyLeaderParams(req));
            if (err != ErrorCode.SUCCEEDED) {
                // Wrong leadership
                LOGGER.info(idStr_ + "Will not follow the leader");
                resp.setError_code(err);
                return;
            }

            // Reset the timeout timer
            lastMsgRecvDur_.reset(false);

            // As for heartbeat, return ok after verifyLeader
            resp.setError_code(ErrorCode.SUCCEEDED);
        } finally {
            raftLock_.unlock();
        }
    }

    protected void processSendSnapshotRequest(SendSnapshotRequest req, SendSnapshotResponse resp) {
        LOGGER.info(idStr_ + "Receive snapshot, total rows " + req.getRows().size()
                + ", total count received " + req.getTotal_count()
                + ", total size received " + req.getTotal_size()
                + ", finished " + req.isDone());

        try {
            raftLock_.lock();

            if (status_ == Status.STOPPED) {
                LOGGER.error(idStr_ + "The part has been stopped, skip the request");
                resp.setError_code(ErrorCode.E_BAD_STATE);
                return;
            }

            if (status_ == Status.STARTING) {
                LOGGER.error(idStr_ + "The partition is still starting");
                resp.setError_code(ErrorCode.E_NOT_READY);
                return;
            }

            if (role_ != Role.FOLLOWER && role_ != Role.LEARNER) {
                LOGGER.error(idStr_ + "Bad role " + roleStr(role_));
                resp.setError_code(ErrorCode.E_BAD_STATE);
                return;
            }

            if (!new InetSocketAddress(req.getLeader_addr(), req.getLeader_port()).equals(leader_) || term_ != req.getTerm()) {
                LOGGER.error(idStr_ + "Term out of date, current term " + term_
                        + ", received term " + req.getTerm());
                resp.setError_code(ErrorCode.E_TERM_OUT_OF_DATE);
                return;
            }

            if (status_ != Status.WAITING_SNAPSHOT) {
                LOGGER.info(idStr_ + "Begin to receive the snapshot");
                reset();
                status_ = Status.WAITING_SNAPSHOT;
            }

            lastSnapshotRecvDur_.reset(false);

            // TODO: Maybe we should save them into one sst firstly?
            Pair<Long, Long> ret = commitSnapshot(req.getRows(),
                    req.getCommitted_log_id(),
                    req.getCommitted_log_term(),
                    req.isDone());
            lastTotalCount_ += ret.first();
            lastTotalSize_ += ret.other();
            if (lastTotalCount_ != req.getTotal_count()
                    || lastTotalSize_ != req.getTotal_size()) {
                LOGGER.error(idStr_ + "Bad snapshot, total rows received " + lastTotalCount_
                        + ", total rows sended " + req.getTotal_count()
                        + ", total size received " + lastTotalSize_
                        + ", total size sended " + req.getTotal_size());
                resp.setError_code(ErrorCode.E_PERSIST_SNAPSHOT_FAILED);
                return;
            }

            if (req.isDone()) {
                committedLogId_ = req.getCommitted_log_id();
                if (lastLogId_ < committedLogId_) {
                    lastLogId_ = committedLogId_;
                    lastLogTerm_ = req.getCommitted_log_term();
                }
                if (wal_.lastLogId() <= committedLogId_) {
                    LOGGER.info(idStr_ + "Reset invalid wal after snapshot received");
                    wal_.reset();
                }
                status_ = Status.RUNNING;
                LOGGER.info(idStr_ + "Receive all snapshot, committedLogId_ " + committedLogId_
                        + ", lastLodId " + lastLogId_ + ", lastLogTermId " + lastLogTerm_);
            }
            resp.setError_code(ErrorCode.SUCCEEDED);
        } finally {
            raftLock_.unlock();
        }
    }

    private void sendHeartbeat() {
        // If leader has not commit any logs in this term, it must commit all logs in previous term,
        // so heartbeat is send by appending one empty log.
        if (!replicatingLogs_.get()) {
            executor_.submit(() -> appendLogAsync(clusterId_, LogType.NORMAL, new byte[0], null));
        }

        LOGGER.info(idStr_ + "Send heartbeat");
        final long currTerm;
        final long latestLogId;
        final long commitLogId;
        final long prevLogTerm;
        final long prevLogId;
        final int replica;
        final List<Host> hosts;

        try {
            raftLock_.lock();
            currTerm = term_;
            latestLogId = wal_.lastLogId();
            commitLogId = committedLogId_;
            prevLogTerm = lastLogTerm_;
            prevLogId = lastLogId_;
            replica = quorum_;
            hosts = new ArrayList<>(hosts_);
        } finally {
            raftLock_.unlock();
        }

        long startMs = Clocks.systemClock().millis();
        FutureUtils.collectNSucceeded(
                hosts.stream().map(h -> {
                    LOGGER.info(idStr_ + "Send heartbeat to " + h.idStr());
                    return h.sendHeartbeat(currTerm, latestLogId, commitLogId, prevLogTerm, prevLogId);
                }).collect(Collectors.toList()),
                hosts.size(),
                pair -> {
                    return pair.other().getError_code() == ErrorCode.SUCCEEDED && !hosts.get(pair.first()).isLearner_();
                }
        ).whenComplete((pairs, throwable) -> {
            if (throwable != null) {
                throw new RuntimeException("ERROR");
            }

            int numSucceeded = 0;
            for (Pair<Integer, HeartbeatResponse> resp : pairs) {
                if (!hosts.get(resp.first()).isLearner_() && resp.other().getError_code() == ErrorCode.SUCCEEDED) {
                    ++numSucceeded;
                }
            }
            if (numSucceeded > replica) {
                LOGGER.info(idStr_, "Heartbeat is accepted by quorum");
                try {
                    raftLock_.lock();
                    long now = Clocks.systemClock().millis();
                    lastMsgAcceptedCostMs_ = now - startMs;
                    lastMsgAcceptedTime_ = now;
                } finally {
                    raftLock_.unlock();
                }
            }
        });
    }

    // followers return Host of which could vote, in other words, learner is not counted in
    public List<Host> followers() {
        Preconditions.checkState(!raftLock_.tryLock());
        List<Host> hosts = new ArrayList<>();
        for (Host h : hosts_) {
            if (!h.isLearner_()) {
                hosts.add(h);
            }
        }
        return hosts;
    }

    public List<InetSocketAddress> peers() {
        try {
            raftLock_.lock();
            List<InetSocketAddress> peers = new ArrayList<>();
            peers.add(addr_);
            for (Host h : hosts_) {
                peers.add(h.address());
            }
            return peers;
        } finally {
            raftLock_.unlock();
        }
    }

    public List<InetSocketAddress> listeners() {
        try {
            raftLock_.lock();
            return listeners_;
        } finally {
            raftLock_.unlock();
        }
    }

    public Pair<Long, Long> lastLogInfo() {
        return Pair.of(wal_.lastLogId(), wal_.lastLogTerm());
    }

    boolean checkAppendLogResult(AppendLogResult res) {
        if (res != AppendLogResult.SUCCEEDED) {
            try {
                logsLock_.lock();
                logs_.clear();
                cachingPromise_.setValue(res);
                cachingPromise_.reset();
                bufferOverFlow_.set(false);
            } finally {
                logsLock_.unlock();
            }

            sendingPromise_.setValue(res);
            replicatingLogs_.set(false);
            return false;
        }
        return true;
    }

    public void reset() {
        Preconditions.checkState(!raftLock_.tryLock());
        wal_.reset();
        cleanup();
        lastLogId_ = committedLogId_ = 0;
        lastLogTerm_ = 0;
        lastTotalCount_ = 0;
        lastTotalSize_ = 0;
    }

    /**
     * Check if the peer has catched up data from leader. If leader is sending the snapshot,
     * the method will return false.
     */
    public AppendLogResult isCatchedUp(InetSocketAddress peer) {
        try {
            raftLock_.lock();
            LOGGER.info(idStr_ + "Check whether I catch up");
            if (role_ != Role.LEADER) {
                LOGGER.info(idStr_ + "I am not the leader");
                return AppendLogResult.E_NOT_A_LEADER;
            }
            if (peer.equals(addr_)) {
                LOGGER.info(idStr_ + "I am the leader");
                return AppendLogResult.SUCCEEDED;
            }
            for (Host h : hosts_) {
                if (h.address().equals(peer)) {
                    if (h.followerCommittedLogId_ == 0 || h.followerCommittedLogId_ < wal_.firstLogId()) {
                        LOGGER.info(idStr_ + "The committed log id of peer is " + h.followerCommittedLogId_
                                + ", which is invalid or less than my first wal log id");
                        return AppendLogResult.E_SENDING_SNAPSHOT;
                    }
                    return h.sendingSnapshot_.get() ? AppendLogResult.E_SENDING_SNAPSHOT
                            : AppendLogResult.SUCCEEDED;
                }
            }
            return AppendLogResult.E_INVALID_PEER;
        } finally {
            raftLock_.unlock();
        }
    }

    public boolean linkCurrentWAL(String path) {
        Preconditions.checkNotNull(path);
        try {
            raftLock_.lock();
            return wal_.linkCurrentWAL(path);
        } finally {
            raftLock_.unlock();
        }
    }

    /**
     * Reset my peers if not equals the argument
     */
    public void checkAndResetPeers(List<InetSocketAddress> peers) {
        try {
            raftLock_.lock();
            List<Host> hosts = new ArrayList<>(hosts_);
            for (Host h : hosts) {
                LOGGER.info(idStr_ + "Check host " + h.address());
                if (!peers.contains(h.address())) {
                    LOGGER.info(idStr_ + "The peer " + h.address() + " should not exist in my peers");
                    removePeer(h.address());
                }
            }
            for (InetSocketAddress p : peers) {
                LOGGER.info(idStr_ + "Add peer " + p + " if not exist!");
                addPeer(p);
            }
        } finally {
            raftLock_.unlock();
        }
    }

    /**
     * Add listener into peers or remove from peers
     */
    public void checkRemoteListeners(List<InetSocketAddress> expected) {
        List<InetSocketAddress> actual = listeners();
        for (InetSocketAddress host : actual) {
            if (!expected.contains(host)) {
                LOGGER.info(idStr_ + "The listener " + host + " should not exist in my peers");
                removeListenerPeer(host);
            }
        }
        for (InetSocketAddress host : expected) {
            if (!actual.contains(host)) {
                LOGGER.info(idStr_ + "Add listener " + host + " to my peers");
                addListenerPeer(host);
            }
        }
    }

    public boolean leaseValid() {
        try {
            raftLock_.lock();

            if (hosts_.isEmpty()) {
                return true;
            }
            if (!commitInThisTerm_) {
                return false;
            }

            // When majority has accepted a log, leader obtains a lease which last for heartbeat.
            // However, we need to take off the net io time. On the left side of the inequality is
            // the time duration since last time leader send a log (the log has been accepted as well)
            return Clocks.systemClock().millis() - lastMsgAcceptedTime_ < RAFT_HEARTBEAT_INTERVAL_SECS * 1000 - lastMsgAcceptedCostMs_;
        } finally {
            raftLock_.unlock();
        }
    }

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

    public boolean isRunning() {
        try {
            raftLock_.lock();
            return status_ == Status.RUNNING;
        } finally {
            raftLock_.unlock();
        }
    }

    public boolean isLearner() {
        try {
            raftLock_.lock();
            return role_ == Role.LEARNER;
        } finally {
            raftLock_.unlock();
        }
    }

    public InetSocketAddress leader() {
        try {
            raftLock_.lock();
            return leader_;
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

    public int spaceId() {
        return spaceId_;
    }

    public int partitionId() {
        return partId_;
    }

    public InetSocketAddress address() {
        return addr_;
    }

    public Wal wal() {
        return wal_;
    }

    // Clean up extra data about the part, usually related to state machine
    public abstract void cleanup();

    // Return <size, count> committed;
    public abstract Pair<Long, Long> commitSnapshot(List<ByteBuffer> data, long committedLogId, long committedLogTerm, boolean finished);

    protected abstract boolean preProcessLog(long logId, long termId, long clusterId, byte[] log);

    // The method will be invoked by start()
    //
    // Inherited classes should implement this method to provide the last
    // committed log id
    public abstract Pair<Long, Long> lastCommittedLogId();

    // The inherited classes need to implement this method to commit
    // a batch of log messages
    protected abstract ErrorCode commitLogs(LogIterator iter, boolean wait);

    // This method is called when this partition is elected as a new leader
    protected abstract void onElected(long term);

    // This method is called when this partition's leader term
    // is finished, either by receiving a new leader election
    // request, or a new leader heartbeat
    protected abstract void onLostLeadership(long term);

    protected abstract void onDiscoverNewLeader(InetSocketAddress nLeader);
}
