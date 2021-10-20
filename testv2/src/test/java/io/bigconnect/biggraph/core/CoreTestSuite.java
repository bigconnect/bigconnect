package io.bigconnect.biggraph.core;

import io.bigconnect.biggraph.BigGraph;
import io.bigconnect.biggraph.backend.id.IdGenerator;
import io.bigconnect.biggraph.dist.RegisterUtil;
import io.bigconnect.biggraph.testutil.Utils;
import io.bigconnect.biggraph.type.define.NodeRole;
import io.bigconnect.biggraph.util.Log;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.runner.RunWith;
import org.junit.runners.Suite;
import org.slf4j.Logger;

@RunWith(Suite.class)
@Suite.SuiteClasses({
//        RamTableTest.class,
        VertexCoreTest.class
})
public class CoreTestSuite {
    private static final Logger LOG = Log.logger(CoreTestSuite.class);
    private static BigGraph graph = null;

    @BeforeClass
    public static void initEnv() {
        RegisterUtil.registerBackends();
    }

    @BeforeClass
    public static void init() {
        graph = Utils.open();
        graph.clearBackend();
        graph.initBackend();
        graph.serverStarted(IdGenerator.of("server1"), NodeRole.MASTER);
    }

    @AfterClass
    public static void clear() {
        if (graph == null) {
            return;
        }

        try {
            graph.clearBackend();
        } finally {
            try {
                graph.close();
            } catch (Throwable e) {
                LOG.error("Error when close()", e);
            }
            graph = null;
        }
    }

    protected static BigGraph graph() {
        Assert.assertNotNull(graph);
        //Assert.assertFalse(graph.closed());
        return graph;
    }
}
