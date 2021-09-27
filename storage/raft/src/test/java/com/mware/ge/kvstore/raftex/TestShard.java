package com.mware.ge.kvstore.raftex;

import com.mware.ge.collection.Pair;
import com.mware.ge.kvstore.utils.LogIterator;
import com.mware.ge.thrift.ThriftClientManager;
import com.mware.ge.util.GeLogger;
import com.mware.ge.util.GeLoggerFactory;

import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.locks.ReentrantLock;

public class TestShard extends RaftPart {
    private GeLogger LOGGER = GeLoggerFactory.getLogger(TestShard.class);

    interface Callback {
        void callback(int idx, String msg, long termId);
    }

    int commitTimes_ = 0;
    int currLogId_ = -1;
    int idx_;
    RaftexServiceImpl service_;
    List<Pair<Integer, byte[]>> data_ = new ArrayList<>();
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
        super(1, 1, partId, addr, walRoot, workers, snapshotMan, ThriftClientManager.getInstance(), null);
        this.idx_ = idx;
        this.service_ = svc;
        this.leadershipLostCB_ = leadershipLostCB;
        this.becomeLeaderCB_ = becomeLeaderCB;
    }

    @Override
    public Pair<Long, Long> commitSnapshot(List<ByteBuffer> data, long committedLogId, long committedLogTerm, boolean finished) {
        return null;
    }

    @Override
    protected boolean preProcessLog(long logId, long termId, long clusterId, byte[] log) {
        return false;
    }

    @Override
    public Pair<Long, Long> lastCommittedLogId() {
        return Pair.of(committedLogId_, term_);
    }

    @Override
    protected ErrorCode commitLogs(LogIterator iter, boolean wait) {
        return null;
    }

    @Override
    protected void onElected(long term) {

    }

    @Override
    protected void onLostLeadership(long term) {

    }

    @Override
    protected void onDiscoverNewLeader(InetSocketAddress nLeader) {

    }

    @Override
    public void cleanup() {

    }
}
