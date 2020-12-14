/*
 * This file is part of the BigConnect project.
 *
 * Copyright (c) 2013-2020 MWARE SOLUTIONS SRL
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License version 3
 * as published by the Free Software Foundation with the addition of the
 * following permission added to Section 15 as permitted in Section 7(a):
 * FOR ANY PART OF THE COVERED WORK IN WHICH THE COPYRIGHT IS OWNED BY
 * MWARE SOLUTIONS SRL, MWARE SOLUTIONS SRL DISCLAIMS THE WARRANTY OF
 * NON INFRINGEMENT OF THIRD PARTY RIGHTS

 * This program is distributed in the hope that it will be useful, but
 * WITHOUT ANY WARRANTY; without even the implied warranty of MERCHANTABILITY
 * or FITNESS FOR A PARTICULAR PURPOSE.
 * See the GNU Affero General Public License for more details.
 * You should have received a copy of the GNU Affero General Public License
 * along with this program; if not, see http://www.gnu.org/licenses or write to
 * the Free Software Foundation, Inc., 51 Franklin Street, Fifth Floor,
 * Boston, MA, 02110-1301 USA, or download the license from the following URL:
 * https://www.gnu.org/licenses/agpl-3.0.txt
 *
 * The interactive user interfaces in modified source and object code versions
 * of this program must display Appropriate Legal Notices, as required under
 * Section 5 of the GNU Affero General Public License.
 *
 * You can be released from the requirements of the license by purchasing
 * a commercial license. Buying such a license is mandatory as soon as you
 * develop commercial activities involving the BigConnect software without
 * disclosing the source code of your own applications.
 *
 * These activities include: offering paid services to customers as an ASP,
 * embedding the product in a web application, shipping BigConnect with a
 * closed source product.
 */
package com.mware.core.ingest.dataworker;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.mware.core.config.Configuration;
import com.mware.core.model.role.AuthorizationRepository;
import com.mware.core.model.workQueue.Priority;
import com.mware.core.model.workQueue.WebQueueRepository;
import com.mware.core.model.workQueue.WorkQueueRepository;
import com.mware.core.status.JmxMetricsManager;
import com.mware.core.status.MetricsManager;
import com.mware.core.status.StatusRepository;
import com.mware.ge.*;
import com.mware.ge.values.storable.StringValue;
import com.mware.ge.values.storable.TextValue;
import com.mware.ge.values.storable.Value;
import com.mware.ge.values.storable.Values;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import java.io.InputStream;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class DataWorkerRunnerTest {
    private static final String VERTEX_ID = "vertexID";
    private static final String EDGE_ID = "edgeID";

    private static final String PROP_NAME = "propName";
    private static final String PROP_KEY = "propKey";
    private static final TextValue PROP_VALUE = Values.stringValue("propValue");

    private DataWorkerRunner testSubject;
    private Graph graph;
    private MetricsManager metricsManager = new JmxMetricsManager();

    @Mock
    private WorkQueueRepository workQueueRepository;

    @Mock
    private WebQueueRepository webQueueRepository;

    @Mock
    private Configuration configuration;

    @Mock
    private AuthorizationRepository authorizationRepository;

    @Mock
    private StatusRepository statusRepository;

    @Before
    public void before() {
        testSubject = new DataWorkerRunner(
                workQueueRepository,
                webQueueRepository,
                statusRepository,
                configuration,
                metricsManager,
                authorizationRepository
        );
        graph = mock(Graph.class);
        testSubject.setGraph(graph);
    }

    @Test
    public void testHandlePropertyOnVertexIsHandledByDWS() throws Exception {
        TestCountingDWStub countingDWStub = new TestCountingDWStub();

        DataWorkerMessage message = createVertexPropertyGPWMessage(VERTEX_ID, PROP_NAME + "0", PROP_KEY + "0");
        inflateVertexAndAddToGraph(VERTEX_ID, 1L);
        runTests(countingDWStub, message);

        assertThat(countingDWStub.isExecutingCount.get(), is(1L));
        assertThat(countingDWStub.isHandledCount.get(), is(1L));
    }

    @Test
    public void testAllPropertiesOnVertexAreProcessedByDataWorkers() throws Exception {
        TestCountingDWStub countingDWStub = new TestCountingDWStub();

        DataWorkerMessage message = createVertexIdJSONGPWMessage(VERTEX_ID);
        inflateVertexAndAddToGraph(VERTEX_ID, 11L);
        runTests(countingDWStub, message);

        assertThat(countingDWStub.isExecutingCount.get(), is(12L));
        assertThat(countingDWStub.isHandledCount.get(), is(12L));
    }

    @Test
    public void testHandlePropertyOnEdgeIsHandledByDWS() throws Exception {
        TestCountingDWStub countingDWStub = new TestCountingDWStub();

        DataWorkerMessage message = createEdgeIdJSONGPWMessage(EDGE_ID, PROP_NAME + "0", PROP_KEY + "0");
        inflateEdgeAndAddToGraph(EDGE_ID, 1L);
        runTests(countingDWStub, message);

        assertThat(countingDWStub.isExecutingCount.get(), is(2L));
        assertThat(countingDWStub.isHandledCount.get(), is(2L));
    }

    @Test
    public void testAllPropertiesOnEdgeAreProcessedByDataWorkers() throws Exception {
        TestCountingDWStub countingGPWStub = new TestCountingDWStub();

        DataWorkerMessage message = createEdgeIdJSONGPWMessage(EDGE_ID);
        inflateEdgeAndAddToGraph(EDGE_ID, 14L);
        runTests(countingGPWStub, message);

        assertThat(countingGPWStub.isExecutingCount.get(), is(15L));
        assertThat(countingGPWStub.isHandledCount.get(), is(15L));
    }

    @Test
    public void testMultipleEdgesAreProcessedInMultiEdgeMessage() throws Exception {
        int numMessages = 5;
        int numProperties = 11;
        String[] ids = new String[numMessages];

        for (int i = 0; i < numMessages; i++) {
            ids[i] = EDGE_ID + "_" + i;
            inflateEdgeAndAddToGraph(ids[i], numProperties);
        }

        DataWorkerMessage message = createMultiEdgeIdJSONGPWMessage(ids);

        testMultiElementMessage(numMessages, numProperties, message);
    }

    @Test
    public void testMultipleVerticesAreProcessedInMultiVertexMessage() throws Exception {
        int numMessages = 5;
        int numProperties = 11;
        String[] ids = new String[numMessages];

        for (int i = 0; i < numMessages; i++) {
            ids[i] = VERTEX_ID + "_" + i;
            inflateVertexAndAddToGraph(ids[i], numProperties);
        }

        testMultiElementMessage(numMessages, numProperties, createMultiVertexIdJSONGPWMessage(ids));
    }

    @Test
    public void testMultipleElementsOnSinglePropertyRunsOnPropertyOnAllElements() throws Exception {
        int numElements = 5;
        Property prop = createProperty(PROP_NAME, PROP_KEY, PROP_VALUE);
        String[] ids = new String[numElements];

        for (int i = 0; i < numElements; i++) {
            ids[i] = VERTEX_ID + "_" + i;
            inflateVertexAndAddToGraph(ids[i], prop);
        }

        DataWorkerMessage message = createMultiVertexPropertyMessage(ids, prop);
        TestCountingDWStub countingGPWStub = new TestCountingDWStub();
        runTests(countingGPWStub, message);

        long expectedNumProperties = (long) (numElements);

        assertThat(countingGPWStub.isExecutingCount.get(), is(expectedNumProperties));
        assertThat(countingGPWStub.isHandledCount.get(), is(expectedNumProperties));
        assertThat(countingGPWStub.workedOnProperties.size(), is(1));
        Property next = countingGPWStub.workedOnProperties.iterator().next();
        assertThat(next.getName(), is(prop.getName()));
        assertThat(next.getName(), is(prop.getName()));
        assertThat(next.getKey(), is(prop.getKey()));
        assertThat(next.getValue(), is(prop.getValue()));
    }

    private void testMultiElementMessage(int numMessages, int numProperties, DataWorkerMessage message) throws Exception {
        TestCountingDWStub countingGPWStub = new TestCountingDWStub();
        runTests(countingGPWStub, message);

        long expectedNumProperties = (long) (numMessages * numProperties + numMessages);

        assertThat(countingGPWStub.isExecutingCount.get(), is(expectedNumProperties));
        assertThat(countingGPWStub.isHandledCount.get(), is(expectedNumProperties));
    }

    private void runTests(DataWorker worker, DataWorkerMessage message) throws Exception {
        DataWorkerThreadedWrapper dataWorkerThreadedWrapper = startInThread(worker);

        testSubject.addDataWorkerThreadedWrappers(dataWorkerThreadedWrapper);

        DataWorkerItem workerItem = testSubject.tupleDataToWorkerItem(message.toBytes());
        testSubject.process(workerItem);

        stopInThread(dataWorkerThreadedWrapper);
    }

    private void inflateVertexAndAddToGraph(String vertexId, long numProperties) {
        inflateVertexAndAddToGraph(vertexId, createNumProperties(numProperties));
    }

    private void inflateVertexAndAddToGraph(String vertexId, Property... properties) {
        Vertex mockedVertex = createMockedVertex(vertexId, properties);
        registerVertexWithGraph(vertexId, mockedVertex);
    }

    private void inflateEdgeAndAddToGraph(String edgeId, long numProperties) {
        Property[] props = createNumProperties(numProperties);
        Edge mockedEdge = createMockedEdge(edgeId, props);
        registerEdgeWithGraph(edgeId, mockedEdge);
    }

    private DataWorkerThreadedWrapper createTestGPWThreadedWrapper(DataWorker worker) {
        DataWorkerThreadedWrapper stubDataWorkerThreadedWrapper = new DataWorkerThreadedWrapper(worker);
        stubDataWorkerThreadedWrapper.setMetricsManager(metricsManager);
        return stubDataWorkerThreadedWrapper;
    }

    private DataWorkerThreadedWrapper startInThread(DataWorker worker) throws InterruptedException {
        DataWorkerThreadedWrapper testGPWThreadedWrapper = createTestGPWThreadedWrapper(worker);
        Thread thread = new Thread(testGPWThreadedWrapper);
        thread.start();
        return testGPWThreadedWrapper;
    }

    private void stopInThread(DataWorkerThreadedWrapper... wrappers) throws InterruptedException {
        for (DataWorkerThreadedWrapper wrapper : wrappers) {
            sleep();
            wrapper.stop();
            sleep();
        }
    }

    private void sleep() throws InterruptedException {
        Thread.sleep(50L);
    }

    private class TestCountingDWStub extends DataWorker {
        public AtomicLong isHandledCount = new AtomicLong(0);
        public AtomicLong isExecutingCount = new AtomicLong(0);
        public Set<Property> workedOnProperties = Sets.newHashSet();

        @Override
        public void execute(InputStream in, DataWorkerData data) throws Exception {
            isExecutingCount.incrementAndGet();
        }

        @Override
        public boolean isHandled(Element element, Property property) {
            isHandledCount.incrementAndGet();
            workedOnProperties.add(property);
            return true;
        }
    }

    private Edge createMockedEdge(String edgeId, Property... props) {
        List<Property> propList = Lists.newArrayList(props);
        Edge e = mock(Edge.class);
        when(e.getId()).thenReturn(edgeId);
        when(e.getProperties()).thenReturn(propList);
        return e;
    }

    private Property[] createNumProperties(long num) {
        List<Property> props = Lists.newArrayList();

        for (long i = 0; i < num; i++) {
            props.add(createProperty(PROP_NAME + i, PROP_KEY + i, Values.stringValue(PROP_VALUE.stringValue() + i)));
        }

        return props.toArray(new Property[0]);
    }

    private Property createProperty(String name, String key, Value value) {
        Property prop = mock(Property.class);
        when(prop.getName()).thenReturn(name);
        when(prop.getKey()).thenReturn(key);
        when(prop.getValue()).thenReturn(value);
        when(prop.getVisibility()).thenReturn(Visibility.EMPTY);
        return prop;
    }

    private Vertex createMockedVertex(String id, Property... properties) {
        List<Property> propList = Lists.newArrayList(properties);
        Vertex v = mock(Vertex.class);
        when(v.getId()).thenReturn(id);
        when(v.getProperties()).thenReturn(propList);
        for (Property property : properties) {
            String key = property.getKey();
            String name = property.getName();
            when(v.getProperty(key, name)).thenReturn(property);
            when(v.getProperty(name)).thenReturn(property);
            when(v.getProperties(name)).thenReturn(Collections.singletonList(property));
            when(v.getProperties(key, name)).thenReturn(Collections.singletonList(property));
        }

        return v;
    }

    private void registerVertexWithGraph(String id, Vertex v) {
        when(graph.getVertex(eq(id), any(Authorizations.class))).thenReturn(v);
        when(graph.getVertex(eq(id), any(FetchHints.class), any(Authorizations.class))).thenReturn(v);
    }

    private void registerEdgeWithGraph(String edgeId, Edge e) {
        when(graph.getEdge(eq(edgeId), any(Authorizations.class))).thenReturn(e);
        when(graph.getEdge(eq(edgeId), any(FetchHints.class), any(Authorizations.class))).thenReturn(e);
    }

    private static DataWorkerMessage createMultiEdgeIdJSONGPWMessage(String... edgeIds) {
        return createTestJSONGPWMessage().setGraphEdgeId(edgeIds);
    }

    private static DataWorkerMessage createMultiVertexPropertyMessage(String[] vertexIds, Property property) {
        return createTestJSONGPWMessage()
                .setGraphVertexId(vertexIds)
                .setPropertyKey(property.getKey())
                .setPropertyName(property.getName());
    }

    private static DataWorkerMessage createMultiVertexIdJSONGPWMessage(String... vertexIds) {
        return createTestJSONGPWMessage()
                .setGraphVertexId(vertexIds);
    }

    private static DataWorkerMessage createVertexPropertyGPWMessage(String vertexId, String propertyName, String propertyKey) {
        return createVertexIdJSONGPWMessage(vertexId)
                .setPropertyKey(propertyKey)
                .setPropertyName(propertyName);
    }

    private static DataWorkerMessage createEdgeIdJSONGPWMessage(String edgeId, String propertyName, String propertyKey) {
        return createTestJSONGPWMessage()
                .setGraphEdgeId(new String[]{edgeId});
    }

    private static DataWorkerMessage createVertexIdJSONGPWMessage(String vertexId) {
        return createTestJSONGPWMessage()
                .setGraphVertexId(new String[]{vertexId});
    }

    private static DataWorkerMessage createEdgeIdJSONGPWMessage(String edgeId) {
        return createTestJSONGPWMessage()
                .setGraphEdgeId(new String[]{edgeId});
    }

    private static DataWorkerMessage createTestJSONGPWMessage() {
        DataWorkerMessage message = new DataWorkerMessage();
        message.setPriority(Priority.LOW);
        message.setVisibilitySource("");
        message.setWorkspaceId("wsId");
        return message;
    }
}
