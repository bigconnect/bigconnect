package com.mware.ge.kvstore.raftex;

import com.mware.ge.util.GeLogger;
import com.mware.ge.util.GeLoggerFactory;
import org.apache.thrift.async.AsyncMethodCallback;
import org.apache.thrift.async.TAsyncClientManager;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TNonblockingSocket;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;
import org.junit.Test;

import java.net.InetSocketAddress;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicReference;

public class LeaderElectionTest extends RaftexTestBase {
    private GeLogger LOGGER = GeLoggerFactory.getLogger(LeaderElectionTest.class);

    @Test
    public void ElectionWithThreeCopies() throws Exception {
        LOGGER.info("=====> Start ElectionWithThreeCopies test");
        Path walRoot = Files.createTempDirectory("election_with_three_copies.");

        List<String> wals = new ArrayList<>();
        List<InetSocketAddress> allHosts = new ArrayList<>();
        List<RaftexServiceImpl> services = new ArrayList<>();
        List<TestShard> copies = new ArrayList<>();
        ScheduledExecutorService workers = Executors.newScheduledThreadPool(4);

        AtomicReference<TestShard> leader = new AtomicReference<>();

        setupRaft(3, walRoot, workers, wals, allHosts, services, copies, leader, new ArrayList<>());

        checkLeadership(copies, leader);
        finishRaft(services, copies, workers, leader);

        LOGGER.info("=====> Done ElectionWithThreeCopies test");
    }

    @Test
    public void TestThrift() throws Exception {
        RaftexServiceImpl service = RaftexServiceImpl.createService(22333);
        service.start();

        TTransport transport = new TSocket("127.0.0.1", 22333);
        TProtocol protocol = new TBinaryProtocol(transport);

        RaftexService.Client c1 = new RaftexService.Client(protocol);

        c1.askForVote(new AskForVoteRequest(1, 1, "127.0.0.1", 22333, 0L, 0L, 0L));
//

        Thread.sleep(50000L);
    }
}
