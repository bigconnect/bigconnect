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
import com.mware.core.InMemoryGraphTestBase;
import com.mware.core.model.schema.SchemaConstants;
import com.mware.core.model.workQueue.Priority;
import com.mware.ge.*;
import com.mware.ge.inmemory.InMemoryExtendedDataRow;
import com.mware.ge.values.storable.TextValue;
import com.mware.ge.values.storable.Value;
import com.mware.ge.values.storable.Values;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import java.util.ArrayList;
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
public class DataWorkerRunnerTest extends InMemoryGraphTestBase {
    private static final String VERTEX_ID = "vertexID";
    private static final String EDGE_ID = "edgeID";

    private static final String PROP_NAME = "propName";
    private static final String PROP_KEY = "propKey";
    private static final TextValue PROP_VALUE = Values.stringValue("propValue");

    private DataWorkerRunner dataWorkerRunner;
    private Authorizations AUTHS;

    @Mock
    private PluginStateRepository pluginStateRepository;

    @Before
    public void before() throws Exception {
        super.before();

        AUTHS = getGraphAuthorizations("A");

        dataWorkerRunner = createRunner();
        getWorkQueueRepository().setDataWorkerRunner(dataWorkerRunner);
    }

    private DataWorkerRunner createRunner() {
        DataWorkerRunner runner = new DataWorkerRunner(
                workQueueRepository,
                webQueueRepository,
                configuration,
                authorizationRepository,
                graph,
                pluginStateRepository
        );
        runner.setGraph(graph);
        runner.setAuthorizations(AUTHS);
        runner.setConfiguration(getConfiguration());

        return runner;
    }

    @Test
    public void testHandlePropertyOnVertexIsHandledByDWS() throws Exception {
        TestCountingDWStub1 countingDWStub = new TestCountingDWStub1();

        DataWorkerMessage message = createVertexPropertyDWMessage(VERTEX_ID, PROP_NAME + "0", PROP_KEY + "0");
        createVertex(VERTEX_ID, createNumProperties(1));
        runTests(countingDWStub, message);

        assertThat(countingDWStub.isExecutingCount.get(), is(1L));
        assertThat(countingDWStub.isHandledCount.get(), is(1L));
    }

    @Test
    public void testAllPropertiesOnVertexAreProcessedByDataWorkers() throws Exception {
        TestCountingDWStub1 countingDWStub = new TestCountingDWStub1();

        DataWorkerMessage message = createVertexIdJSONDWMessage(VERTEX_ID);
        createVertex(VERTEX_ID, createNumProperties(11));
        runTests(countingDWStub, message);

        assertThat(countingDWStub.isExecutingCount.get(), is(12L));
        assertThat(countingDWStub.isHandledCount.get(), is(12L));
    }

    @Test
    public void testHandlePropertyOnEdgeIsHandledByDWS() throws Exception {
        TestCountingDWStub1 countingDWStub = new TestCountingDWStub1();

        DataWorkerMessage message = createEdgeIdJSONDWMessage(PROP_NAME + "0", PROP_KEY + "0");
        createEdge(EDGE_ID, 1);
        runTests(countingDWStub, message);

        assertThat(countingDWStub.isExecutingCount.get(), is(2L));
        assertThat(countingDWStub.isHandledCount.get(), is(2L));
    }

    @Test
    public void testAllPropertiesOnEdgeAreProcessedByDataWorkers() throws Exception {
        TestCountingDWStub1 countingGPWStub = new TestCountingDWStub1();

        DataWorkerMessage message = createEdgeIdJSONDWMessage(EDGE_ID);
        createEdge(EDGE_ID, 14);
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
            createEdge(ids[i], numProperties);
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
            createVertex(ids[i], createNumProperties(numProperties));
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
            createVertex(ids[i], prop);
        }

        DataWorkerMessage message = createMultiVertexPropertyMessage(ids, prop);
        TestCountingDWStub1 countingGPWStub = new TestCountingDWStub1();
        runTests(countingGPWStub, message);

        assertThat(countingGPWStub.isExecutingCount.get(), is((long) numElements));
        assertThat(countingGPWStub.isHandledCount.get(), is((long) numElements));
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

    @Test
    public void testMultithreading() throws Exception {
        List<DataWorkerRunner> runners = new ArrayList<>();
        for (int i = 0; i < 4; i++) {
            DataWorkerRunner runner = createRunner();
            runner.addDataWorkerThreadedWrappers(startInThread(new TestCountingDWStub1()));
            runner.prepare(getUserRepository().getSystemUser());
            runner.run();
            runners.add(runner);
        }

        int numElements = 50;

        DataWorkerMemoryTracer.ENABLED = true;

        for (int i = 0; i < numElements; i++) {
            Vertex v = createVertex(VERTEX_ID + "_" + i, createNumProperties(1));
            getWorkQueueRepository().pushOnDwQueue(
                    v,
                    null,
                    null,
                    null,
                    null,
                    Priority.HIGH,
                    ElementOrPropertyStatus.UPDATE,
                    null
            );
        }

//        while (countingDWStub.isExecutingCount.get() < numElements * 2L) {
//            Thread.sleep(1000L);
//        }
//
//        DataWorkerMemoryTracer.print();
//
//        assertThat(countingDWStub.isExecutingCount.get(), is((long) numElements * 2));
//        assertThat(countingDWStub.isHandledCount.get(), is((long) numElements * 2));
//        assertThat(countingDWStub.workedOnProperties.size(), is(2));
    }

    private void runTests(DataWorker worker, DataWorkerMessage message) throws Exception {
        DataWorkerThreadedWrapper dataWorkerThreadedWrapper = startInThread(worker);
        dataWorkerRunner.addDataWorkerThreadedWrappers(dataWorkerThreadedWrapper);

        DataWorkerItem workerItem = dataWorkerRunner.tupleDataToWorkerItem(message.toBytes());
        dataWorkerRunner.process(workerItem);

        stopInThread(dataWorkerThreadedWrapper);
    }

    private Vertex createVertex(String vertexId, Property... properties) {
        VertexBuilder v = graph.prepareVertex(vertexId, Visibility.EMPTY, SchemaConstants.CONCEPT_TYPE_THING);
        for (Property property : properties) {
            v.addPropertyValue(property.getKey(), property.getName(), property.getValue(), property.getVisibility());
        }
        return v.save(AUTHS);
    }

    private Edge createEdge(String edgeId, int numProperties) {
        Property[] props = createNumProperties(numProperties);
        EdgeBuilderByVertexId e = graph.prepareEdge(edgeId, VERTEX_ID, VERTEX_ID, "label", Visibility.EMPTY);
        for (Property property : props) {
            e.addPropertyValue(property.getKey(), property.getName(), property.getValue(), property.getVisibility());
        }
        return e.save(AUTHS);
    }

    private DataWorkerThreadedWrapper startInThread(DataWorker worker) {
        DataWorkerThreadedWrapper threadedWrapper = new DataWorkerThreadedWrapper(worker, graph.getMetricsRegistry());

        Thread thread = new Thread(threadedWrapper);
        thread.start();
        return threadedWrapper;
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

    private Property[] createNumProperties(int num) {
        List<Property> props = Lists.newArrayList();

        for (long i = 0; i < num; i++) {
            props.add(createProperty(PROP_NAME + i, PROP_KEY + i, Values.stringValue(PROP_VALUE.stringValue() + i)));
        }

        return props.toArray(new Property[0]);
    }

    private Property createProperty(String name, String key, Value value) {
        return new InMemoryExtendedDataRow.InMemoryProperty(name, key, value, FetchHints.ALL, 0L, Visibility.EMPTY);
    }

    private Vertex createMockedVertex(String id, Property... properties) {
        VertexBuilder v = graph.prepareVertex(id, Visibility.EMPTY, SchemaConstants.CONCEPT_TYPE_THING);
        for (Property property : properties) {
            v.addPropertyValue(property.getKey(), property.getName(), property.getValue(), property.getVisibility());
        }

        return v.save(new Authorizations());
    }

    private static DataWorkerMessage createMultiEdgeIdJSONGPWMessage(String... edgeIds) {
        return createTestJSONDWMessage().setGraphEdgeId(edgeIds);
    }

    private static DataWorkerMessage createMultiVertexPropertyMessage(String[] vertexIds, Property property) {
        return createTestJSONDWMessage()
                .setGraphVertexId(vertexIds)
                .setPropertyKey(property.getKey())
                .setPropertyName(property.getName());
    }

    private static DataWorkerMessage createMultiVertexIdJSONGPWMessage(String... vertexIds) {
        return createTestJSONDWMessage()
                .setGraphVertexId(vertexIds);
    }

    private static DataWorkerMessage createVertexPropertyDWMessage(String vertexId, String propertyName, String propertyKey) {
        return createVertexIdJSONDWMessage(vertexId)
                .setPropertyKey(propertyKey)
                .setPropertyName(propertyName);
    }

    private static DataWorkerMessage createEdgeIdJSONDWMessage(String propertyName, String propertyKey) {
        return createTestJSONDWMessage()
                .setGraphEdgeId(new String[]{DataWorkerRunnerTest.EDGE_ID});
    }

    private static DataWorkerMessage createVertexIdJSONDWMessage(String vertexId) {
        return createTestJSONDWMessage()
                .setGraphVertexId(new String[]{vertexId});
    }

    private static DataWorkerMessage createEdgeIdJSONDWMessage(String edgeId) {
        return createTestJSONDWMessage()
                .setGraphEdgeId(new String[]{edgeId});
    }

    private static DataWorkerMessage createTestJSONDWMessage() {
        DataWorkerMessage message = new DataWorkerMessage();
        message.setPriority(Priority.LOW);
        message.setVisibilitySource("");
        message.setWorkspaceId("wsId");
        return message;
    }
}
