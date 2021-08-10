package com.mware.ge.hbase;

import com.mware.core.model.schema.SchemaConstants;
import com.mware.ge.Authorizations;
import com.mware.ge.Graph;
import com.mware.ge.Vertex;
import com.mware.ge.Visibility;
import com.mware.ge.base.GraphBaseTests;
import com.mware.ge.base.GraphTestSetup;
import com.mware.ge.base.TestGraphFactory;
import com.mware.ge.event.GraphEvent;
import com.mware.ge.event.GraphEventListener;
import com.mware.ge.util.GeLogger;
import com.mware.ge.util.GeLoggerFactory;
import com.mware.ge.values.storable.Values;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static com.mware.ge.util.GeAssert.addGraphEvent;
import static com.mware.ge.util.GeAssert.clearGraphEvents;

public class HBaseBaseTests extends GraphBaseTests implements GraphTestSetup {
    private static final GeLogger LOGGER = GeLoggerFactory.getLogger(HBaseBaseTests.class);

    @Before
    public void before() throws Exception {
        graph = graphFactory().createGraph();
        clearGraphEvents();
        getGraph().addGraphEventListener(new GraphEventListener() {
            @Override
            public void onGraphEvent(GraphEvent graphEvent) {
                addGraphEvent(graphEvent);
            }
        });
    }

    @After
    public void after() throws Exception {
        if (getGraph() != null) {
            getGraph().drop();
            getGraph().shutdown();
            graph = null;
        }
    }

    @Override
    public Graph getGraph() {
        return graph;
    }

    @Override
    public TestGraphFactory graphFactory() {
        return new HBaseGraphFactory();
    }

    @Test
    public void test1() {
        Vertex v1 = getGraph().prepareVertex(Visibility.EMPTY, SchemaConstants.CONCEPT_TYPE_THING)
                .setProperty("p1", Values.stringValue("v1"), Visibility.EMPTY)
                .save(new Authorizations());

    }
}
