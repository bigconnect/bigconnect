package com.mware.ge.kvstore.raftex;

import com.mware.ge.util.GeLogger;
import com.mware.ge.util.GeLoggerFactory;
import com.mware.ge.util.Preconditions;
import org.junit.Assert;

import java.io.File;
import java.net.InetSocketAddress;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

public class RaftexTestBase {
    private GeLogger LOGGER = GeLoggerFactory.getLogger(RaftexTestBase.class);
    private ReentrantLock leaderMutex = new ReentrantLock();
    private Condition leaderCV = leaderMutex.newCondition();

    public void setupRaft(
            int numCopies,
            Path walRoot,
            ScheduledExecutorService workers,
            List<String> wals,
            List<InetSocketAddress> allHosts,
            List<RaftexServiceImpl> services,
            List<TestShard> copies,
            AtomicReference<TestShard> leader,
            List<Boolean> isLearner
    ) {
        // Set up WAL folders (Create one extra for leader crash test)
        for (int i = 0; i < numCopies + 1; ++i) {
            String path = String.format("%s/copy%d", walRoot.toString(), i + 1);
            wals.add(path);
            new File(path).mkdirs();
        }

        // Set up services
        for (int i = 0; i < numCopies; ++i) {
            int port = 22333 + i;
            services.add(RaftexServiceImpl.createService(port));
            allHosts.add(new InetSocketAddress("127.0.0.1", port));
        }

        List<SnapshotManager> sps = snapshots(services);

        // Create one copy of the shard for each service
        for (int i = 0; i < services.size(); i++) {
            TestShard part = new TestShard(
                    copies.size(),
                    services.get(i),
                    1,
                    allHosts.get(i),
                    wals.get(i),
                    workers,
                    sps.get(i),
                    (idx, idStr, termId) -> onLeadershipLost(copies, leader, idx, idStr, termId),
                    (idx, idStr, termId) -> onLeaderElected(copies, leader, idx, idStr, termId)
            );
            copies.add(part);
            services.get(i).addPartition(part);
            part.start(getPeers(allHosts, allHosts.get(i), isLearner), isLearner.get(i));
        }

        // Wait untill all copies agree on the same leader
        waitUntilLeaderElected(copies, leader, isLearner);
    }

    private List<InetSocketAddress> getPeers(List<InetSocketAddress> allHosts, InetSocketAddress self, List<Boolean> isLearner) {
        if (isLearner.isEmpty()) {
            allHosts.forEach(h -> isLearner.add(false));
        }

        int index = 0;
        List<InetSocketAddress> peers = new ArrayList<>();
        for (InetSocketAddress host : allHosts) {
            if (!host.equals(self) && !isLearner.get(index)) {
                LOGGER.info("Adding host "+host);
                peers.add(host);
            }
            index++;
        }
        return peers;
    }

    private void onLeaderElected(List<TestShard> copies, AtomicReference<TestShard> leader, int idx, String idStr, long termId) {
        LOGGER.info(idStr + " I'm elected as the leader for the term "+termId);
        try {
            leaderMutex.lock();
            leader.set(copies.get(idx));
            leaderCV.signal();
        }  finally {
            leaderMutex.unlock();
        }
    }

    private void onLeadershipLost(List<TestShard> copies, AtomicReference<TestShard> leader, int idx, String idStr, long termId) {
        LOGGER.info(idStr + " Term "+termId+" is passed. I'm not the leader any more");
        try {
            leaderMutex.lock();
            TestShard shard = copies.get(idx);
            TestShard l = leader.get();
            if (l.idStr_.equals(shard.idStr_)) {
                copies.remove(l);
                leader.set(null);
            }
        } finally {
            leaderMutex.unlock();
        }
    }

    public void waitUntilLeaderElected(List<TestShard> copies, AtomicReference<TestShard> leader, List<Boolean> isLearner) {
        while (true) {
            boolean sameLeader = false;
            try {
                leaderMutex.lock();
                leaderCV.awaitUninterruptibly();

                if (leader.get() == null)
                    leaderCV.awaitUninterruptibly();

                // Sleep some time to wait until resp of heartbeat has come back when elected as leader
                try {
                    Thread.sleep(1000);
                } catch (InterruptedException e) {
                }
                sameLeader = checkLeader(copies, leader, isLearner);
            } finally {
                leaderMutex.unlock();
            }

            if (sameLeader) {
                // Sleep a while in case concurrent leader election occurs
                try {
                    Thread.sleep(1000);
                } catch (InterruptedException e) {
                }

                if (checkLeader(copies, leader, isLearner))
                    break;
            }
        }

        // Wait for one second
        try {
            Thread.sleep(1000);
        } catch (InterruptedException e) {
        }
    }

    private boolean checkLeader(List<TestShard> copies, AtomicReference<TestShard> leader, List<Boolean> isLearner) {
        int index = 0;
        for (TestShard c : copies) {
            // if a copy in abnormal state, its leader is (0, 0)
            if (!isLearner.get(index) && !leader.get().idStr_.equals(c.idStr_) && c.isRunning()) {
                if (!c.leader().equals(leader.get().address())) {
                    return false;
                }
            }
            index++;
        }
        return true;
    }

    public void waitUntilAllHasLeader(List<TestShard> copies) {
        while (true) {
            boolean allHaveLeader = true;
            for (TestShard c : copies) {
                if (c.isRunning()) {
                    if (c.leader().equals(new InetSocketAddress("", 0))) {
                        allHaveLeader = false;
                        break;
                    }
                }
            }

            if (allHaveLeader)
                return;

            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
            }
        }
    }

    public int checkLeadership(List<TestShard> copies, AtomicReference<TestShard> leader) {
        try {
            leaderMutex.lock();
            int leaderIndex = -1;
            int i = 0;
            for (TestShard c : copies) {
                if (!c.idStr_.equals(leader.get().idStr_) && !c.isLearner() && c.isRunning()) {
                    Assert.assertEquals(c.leader(), leader.get().address());
                } else if (c.idStr_.equals(leader.get().idStr_)) {
                    leaderIndex = i;
                }
                i++;
            }
            Assert.assertTrue(leaderIndex >= 0);
            return leaderIndex;
        } finally {
            leaderMutex.unlock();
        }
    }

    public void finishRaft(List<RaftexServiceImpl> services, List<TestShard> copies, ScheduledExecutorService workers, AtomicReference<TestShard> leader) {
        try {
            leader.set(null);
            copies.clear();

            for (RaftexServiceImpl svc : services)
                svc.stop();

            workers.shutdown();
            workers.awaitTermination(1000, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
        }
    }

    private List<SnapshotManager> snapshots(List<RaftexServiceImpl> services) {
        List<SnapshotManager> snapshots = new ArrayList<>();
        for (RaftexServiceImpl service : services) {
            snapshots.add(new TestSnapshotManager(service));
        }
        return snapshots;
    }
}
