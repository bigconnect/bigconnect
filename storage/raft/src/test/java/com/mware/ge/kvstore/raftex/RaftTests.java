package com.mware.ge.kvstore.raftex;

import com.mware.ge.util.GeLogger;
import com.mware.ge.util.GeLoggerFactory;
import org.apache.commons.io.FileUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicReference;

public class RaftTests extends RaftexTestBase {
    private GeLogger LOGGER = GeLoggerFactory.getLogger(RaftTests.class);
    @Rule
    public TestName name = new TestName();

    Path walRoot_;
    int size_ = 3;
    ScheduledExecutorService workers_ = Executors.newScheduledThreadPool(10);
    List<String> wals = new ArrayList<>();
    List<InetSocketAddress> allHosts = new ArrayList<>();
    List<RaftexServiceImpl> services = new ArrayList<>();
    List<TestShard> copies = new ArrayList<>();
    AtomicReference<TestShard> leader = new AtomicReference<>();

    @Before
    public void before() throws IOException  {
        walRoot_ = Files.createTempDirectory(String.format("/tmp/%s", name.getMethodName()));
        setupRaft(size_, walRoot_, workers_,  wals, allHosts, services, copies, leader, Collections.EMPTY_LIST);
        checkLeadership(copies, leader);
    }

    @After
    public void after() throws IOException {
        finishRaft(services, copies, workers_, leader);
        FileUtils.deleteDirectory(walRoot_.toFile());
    }

    @Test
    public void LeaderCrashReboot() {
        LOGGER.info("=====> Now let's kill the old leader");
        int idx = leader.get().index();
        killOneCopy(services, copies, leader, idx);
    }
}
