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
package com.mware.ge.base;

import com.mware.ge.*;
import com.mware.ge.event.GraphEvent;
import com.mware.ge.event.GraphEventListener;
import com.mware.ge.mutation.ElementMutation;
import com.mware.ge.query.builder.GeQueryBuilders;
import com.mware.ge.util.GeLogger;
import com.mware.ge.util.GeLoggerFactory;
import com.mware.ge.values.storable.TextValue;
import com.mware.ge.values.storable.Value;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import java.util.*;

import static com.mware.core.model.schema.SchemaConstants.CONCEPT_TYPE_THING;
import static com.mware.ge.util.GeAssert.*;
import static com.mware.ge.util.IterableUtils.count;
import static com.mware.ge.util.IterableUtils.toList;
import static com.mware.ge.values.storable.Values.stringValue;
import static org.junit.Assume.assumeTrue;

@RunWith(JUnit4.class)
public abstract class GraphBenchmarkTests implements GraphTestSetup {
    private static final GeLogger LOGGER = GeLoggerFactory.getLogger(GraphBenchmarkTests.class);
    protected Graph graph;

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

    @Test
    public void benchmark() {
        assumeTrue(benchmarkEnabled());
        Random random = new Random(1);
        int vertexCount = 10000;
        int edgeCount = 10000;
        int findVerticesByIdCount = 10000;

        benchmarkAddVertices(vertexCount);
        benchmarkAddEdges(random, vertexCount, edgeCount);
        benchmarkFindVerticesById(random, vertexCount, findVerticesByIdCount);
        benchmarkFindConnectedVertices();
    }

    @Test
    public void benchmarkGetPropertyByName() {
        assumeTrue(benchmarkEnabled());

        final int propertyCount = 100;

        VertexBuilder m = getGraph().prepareVertex("111111", VISIBILITY_A, CONCEPT_TYPE_THING);
        for (int i = 0; i < propertyCount; i++) {
            m.addPropertyValue("key", "prop" + i, stringValue("value " + i), VISIBILITY_A);
        }
        m.save(AUTHORIZATIONS_ALL);
        getGraph().flush();

        Vertex v1 = getGraph().getVertex("111111", AUTHORIZATIONS_ALL);

        double startTime = System.currentTimeMillis();
        StringBuilder optimizationBuster = new StringBuilder();
        for (int i = 0; i < 10000; i++) {
            for (int propIndex = 0; propIndex < propertyCount; propIndex++) {
                Value value = v1.getPropertyValue("key", "prop" + propIndex);
                optimizationBuster.append(value.toString().substring(0, 1));
            }
        }
        double endTime = System.currentTimeMillis();
        LOGGER.trace("optimizationBuster: %s", optimizationBuster.substring(0, 1));
        LOGGER.info("get property by name and key in %.3fs", (endTime - startTime) / 1000);

        startTime = System.currentTimeMillis();
        optimizationBuster = new StringBuilder();
        for (int i = 0; i < 10000; i++) {
            for (int propIndex = 0; propIndex < propertyCount; propIndex++) {
                Value value = v1.getPropertyValue("prop" + propIndex);
                optimizationBuster.append(value.toString().substring(0, 1));
            }
        }
        endTime = System.currentTimeMillis();
        LOGGER.trace("optimizationBuster: %s", optimizationBuster.substring(0, 1));
        LOGGER.info("get property by name in %.3fs", (endTime - startTime) / 1000);
    }

    @Test
    public void benchmarkSaveElementMutations() {
        assumeTrue(benchmarkEnabled());

        int vertexCount = 1000000;

        benchmarkAddVertices(vertexCount);
        benchmarkAddVerticesSaveElementMutations(vertexCount);
        benchmarkAddVertices(vertexCount);
    }

    @Test
    public void benchmarkAddManyVertices() {
        assumeTrue(benchmarkEnabled());

        int vertexCount = 1000000;

        benchmarkAddVertices(vertexCount);
    }

    @Test
    public void benchmarkLotsOfProperties() {
        assumeTrue(benchmarkEnabled());

        int vertexCount = 100;
        int propertyNameCount = 10;
        int propertiesPerName = 50;
        int metadataPerProperty = 5;
        System.out.println("Defining properties");
        for (int i = 0; i < propertyNameCount; i++) {
            getGraph().defineProperty("prop" + i)
                    .textIndexHint(TextIndexHint.NONE)
                    .dataType(TextValue.class)
                    .define();
        }
        System.out.println("Writing vertices");
        for (int vertexIndex = 0; vertexIndex < vertexCount; vertexIndex++) {
            String vertexId = "v" + vertexIndex;
            VertexBuilder m = getGraph().prepareVertex(vertexId, VISIBILITY_A, CONCEPT_TYPE_THING);
            for (int propertyNameIndex = 0; propertyNameIndex < propertyNameCount; propertyNameIndex++) {
                for (int propertyPerNameIndex = 0; propertyPerNameIndex < propertiesPerName; propertyPerNameIndex++) {
                    Metadata metadata = Metadata.create();
                    for (int metadataIndex = 0; metadataIndex < metadataPerProperty; metadataIndex++) {
                        metadata.add("m" + UUID.randomUUID().toString(), stringValue("value" + metadataIndex), VISIBILITY_A);
                    }
                    m.addPropertyValue("k" + UUID.randomUUID().toString(), "prop" + propertyNameIndex, stringValue("value" + propertyNameIndex), metadata, VISIBILITY_A);
                }
            }
            m.save(AUTHORIZATIONS_ALL);
        }
        getGraph().flush();
        System.out.println("Reading vertices");
        for (Vertex vertex : getGraph().getVertices(FetchHints.ALL, AUTHORIZATIONS_A)) {
            vertex.getId();
        }
    }

    @Test
    public void benchmarkDeletes() {
        assumeTrue(benchmarkEnabled());

        Random random = new Random(1);
        int vertexCount = 10000;
        int edgeCount = 10000;
        int extendedDataRowCount = 10000;

        benchmarkAddVertices(vertexCount);
        benchmarkAddEdges(random, vertexCount, edgeCount);
        benchmarkAddExtendedDataRows(random, vertexCount, extendedDataRowCount);

        double startTime = System.currentTimeMillis();
        List<ElementId> elementIds = new ArrayList<>();
        for (int i = 0; i < vertexCount; i++) {
            String vertexId = "v" + i;
            elementIds.add(ElementId.vertex(vertexId));
        }
        getGraph().deleteElements(elementIds.stream(), AUTHORIZATIONS_A);
        ;
        getGraph().flush();
        double endTime = System.currentTimeMillis();
        LOGGER.info("delete vertices in %.3fs", (endTime - startTime) / 1000.0);
    }

    @Test
    public void benchmarkLotsOfEdges() {
        assumeTrue(benchmarkEnabled());

        int edgeCount = 100_000;
        String sourceVertexId = "v12345678901234567890";

        graph.prepareVertex(sourceVertexId, VISIBILITY_A, CONCEPT_TYPE_THING)
                .setProperty("name", stringValue(sourceVertexId), VISIBILITY_A)
                .save(AUTHORIZATIONS_A);

        for (int i = 0; i < edgeCount; i++) {
            if (i % 10000 == 0) {
                System.out.println(String.format("Creating vertex %,d / %,d", i, edgeCount));
            }
            String vertexId = "v12345678901234567890_" + i;
            graph.prepareVertex(vertexId, VISIBILITY_A, CONCEPT_TYPE_THING)
                    .setProperty("name", stringValue(vertexId), VISIBILITY_A)
                    .save(AUTHORIZATIONS_A);
        }

        for (int i = 0; i < edgeCount; i++) {
            if (i % 10000 == 0) {
                System.out.println(String.format("Creating edge %,d / %,d", i, edgeCount));
            }
            String vertexId = "v12345678901234567890_" + i;
            String edgeId = "v12345678901234567890_label1_" + vertexId;
            graph.prepareEdge(edgeId, sourceVertexId, vertexId, "label1", VISIBILITY_A)
                    .save(AUTHORIZATIONS_A);
        }
        graph.flush();

        FetchHints fetchHints = new FetchHintsBuilder(FetchHints.ALL)
                .build();
        graph.getVertex(sourceVertexId, fetchHints, AUTHORIZATIONS_A);

        fetchHints = new FetchHintsBuilder(FetchHints.ALL)
                .setIncludeEdgeIds(false)
                .build();
        graph.getVertex(sourceVertexId, fetchHints, AUTHORIZATIONS_A);
    }

    @Test
    public void testFindRelatedEdgesPerformance() {
        int totalNumberOfVertices = 1000;
        int totalNumberOfEdges = 100000;
        int totalVerticesToCheck = 1000;

        Date startTime, endTime;
        Random random = new Random(100);

        startTime = new Date();
        List<Vertex> vertices = new ArrayList<>();
        for (int i = 0; i < totalNumberOfVertices; i++) {
            vertices.add(getGraph().addVertex("v" + i, VISIBILITY_A, AUTHORIZATIONS_A, CONCEPT_TYPE_THING));
        }
        getGraph().flush();
        endTime = new Date();
        long insertVerticesTime = endTime.getTime() - startTime.getTime();

        startTime = new Date();
        for (int i = 0; i < totalNumberOfEdges; i++) {
            Vertex outVertex = vertices.get(random.nextInt(vertices.size()));
            Vertex inVertex = vertices.get(random.nextInt(vertices.size()));
            getGraph().addEdge("e" + i, outVertex, inVertex, LABEL_LABEL1, VISIBILITY_A, AUTHORIZATIONS_A);
        }
        getGraph().flush();
        endTime = new Date();
        long insertEdgesTime = endTime.getTime() - startTime.getTime();

        List<String> vertexIds = new ArrayList<>();
        for (int i = 0; i < totalVerticesToCheck; i++) {
            Vertex v = vertices.get(random.nextInt(vertices.size()));
            vertexIds.add(v.getId());
        }

        startTime = new Date();
        Iterable<String> edgeIds = toList(getGraph().findRelatedEdgeIds(vertexIds, AUTHORIZATIONS_A));
        count(edgeIds);
        endTime = new Date();
        long findRelatedEdgesTime = endTime.getTime() - startTime.getTime();

        LOGGER.info(
                "RESULTS\ntotalNumberOfVertices,totalNumberOfEdges,totalVerticesToCheck,insertVerticesTime,insertEdgesTime,findRelatedEdgesTime\n%d,%d,%d,%d,%d,%d",
                totalNumberOfVertices,
                totalNumberOfEdges,
                totalVerticesToCheck,
                insertVerticesTime,
                insertEdgesTime,
                findRelatedEdgesTime
        );
    }

    @Test
    public void simpleAddVertices() {
        double startTime = System.currentTimeMillis();
        for (int i = 0; i < 100_000; i++) {
            getGraph().prepareVertex(VISIBILITY_A, CONCEPT_TYPE_THING)
                    .addPropertyValue("k1", "prop1", stringValue("value1"), VISIBILITY_A)
                    .addPropertyValue("k1", "prop2", stringValue("value2"), VISIBILITY_A)
                    .addPropertyValue("k1", "prop3", stringValue("value3"), VISIBILITY_A)
                    .addPropertyValue("k1", "prop4", stringValue("value4"), VISIBILITY_A)
                    .addPropertyValue("k1", "prop5", stringValue("value5"), VISIBILITY_A)
                    .save(AUTHORIZATIONS_ALL);
        }
        getGraph().flush();
        double endTime = System.currentTimeMillis();
        LOGGER.info("add vertices in %.3fs", (endTime - startTime) / 1000);
    }

    @Test
    public void bulkAddVertices() {
        double startTime = System.currentTimeMillis();
        List builders = new ArrayList();
        for (int i = 0; i < 100_000; i++) {
            builders.add(
                    getGraph().prepareVertex(VISIBILITY_A, CONCEPT_TYPE_THING)
                            .addPropertyValue("k1", "prop1", stringValue("value1"), VISIBILITY_A)
                            .addPropertyValue("k1", "prop2", stringValue("value2"), VISIBILITY_A)
                            .addPropertyValue("k1", "prop3", stringValue("value3"), VISIBILITY_A)
                            .addPropertyValue("k1", "prop4", stringValue("value4"), VISIBILITY_A)
                            .addPropertyValue("k1", "prop5", stringValue("value5"), VISIBILITY_A)
            );
        }
        getGraph().saveElementMutations(builders, AUTHORIZATIONS_ALL);
        getGraph().flush();
        double endTime = System.currentTimeMillis();
        LOGGER.info("add vertices in %.3fs", (endTime - startTime) / 1000);
    }

    private void benchmarkAddVertices(int vertexCount) {
        getGraph().prepareVertex("warm_up", VISIBILITY_A, CONCEPT_TYPE_THING)
                .addPropertyValue("k1", "prop1", stringValue("value1"), VISIBILITY_A)
                .addPropertyValue("k1", "prop2", stringValue("value2"), VISIBILITY_A)
                .addPropertyValue("k1", "prop3", stringValue("value3"), VISIBILITY_A)
                .addPropertyValue("k1", "prop4", stringValue("value4"), VISIBILITY_A)
                .addPropertyValue("k1", "prop5", stringValue("value5"), VISIBILITY_A)
                .save(AUTHORIZATIONS_ALL);
        getGraph().flush();

        List<ElementMutation<? extends Element>> elements = new ArrayList<>();
        int batchSize = 2000;

        double startTime = System.currentTimeMillis();
        ((GraphBaseWithSearchIndex) getGraph()).getSearchIndex().enableBulkIngest(true);
        for (int i = 0; i < vertexCount; i++) {
            String vertexId = "v" + i;

            elements.add(
                    getGraph().prepareVertex(vertexId, VISIBILITY_A, CONCEPT_TYPE_THING)
                            .addPropertyValue("k1", "prop1", stringValue("value1 " + i), VISIBILITY_A)
                            .addPropertyValue("k1", "prop2", stringValue("value2 " + i), VISIBILITY_A)
                            .addPropertyValue("k1", "prop3", stringValue("value3 " + i), VISIBILITY_A)
                            .addPropertyValue("k1", "prop4", stringValue("value4 " + i), VISIBILITY_A)
                            .addPropertyValue("k1", "prop5", stringValue("value5 " + i), VISIBILITY_A)
            );

            if (i % batchSize == 0) {
                graph.saveElementMutations(elements, AUTHORIZATIONS_ALL);
                elements.clear();
            }
        }
        graph.saveElementMutations(elements, AUTHORIZATIONS_ALL);
        getGraph().flush();
        ((GraphBaseWithSearchIndex) getGraph()).getSearchIndex().enableBulkIngest(false);

        assertEquals(vertexCount + 1, getGraph().query(GeQueryBuilders.searchAll().limit(0L), AUTHORIZATIONS_ALL)
                .vertices()
                .getTotalHits()
        );
        double endTime = System.currentTimeMillis();
        LOGGER.info("add vertices in %.3fs", (endTime - startTime) / 1000);
    }

    private void benchmarkAddVerticesSaveElementMutations(int vertexCount) {
        double startTime = System.currentTimeMillis();
        List<ElementMutation<? extends Element>> mutations = new ArrayList<>();
        for (int i = 0; i < vertexCount; i++) {
            String vertexId = "v" + i;
            ElementBuilder<Vertex> m = getGraph().prepareVertex(vertexId, VISIBILITY_A, CONCEPT_TYPE_THING)
                    .addPropertyValue("k1", "prop1", stringValue("value1 " + i), VISIBILITY_A)
                    .addPropertyValue("k1", "prop2", stringValue("value2 " + i), VISIBILITY_A)
                    .addPropertyValue("k1", "prop3", stringValue("value3 " + i), VISIBILITY_A)
                    .addPropertyValue("k1", "prop4", stringValue("value4 " + i), VISIBILITY_A)
                    .addPropertyValue("k1", "prop5", stringValue("value5 " + i), VISIBILITY_A);
            mutations.add(m);
        }
        getGraph().saveElementMutations(mutations, AUTHORIZATIONS_ALL);
        getGraph().flush();
        double endTime = System.currentTimeMillis();
        LOGGER.info("save element mutations in %.3fs", (endTime - startTime) / 1000);
    }

    private void benchmarkAddEdges(Random random, int vertexCount, int edgeCount) {
        double startTime = System.currentTimeMillis();
        for (int i = 0; i < edgeCount; i++) {
            String edgeId = "e" + i;
            String outVertexId = "v" + random.nextInt(vertexCount);
            String inVertexId = "v" + random.nextInt(vertexCount);
            getGraph().prepareEdge(edgeId, outVertexId, inVertexId, LABEL_LABEL1, VISIBILITY_A)
                    .addPropertyValue("k1", "prop1", stringValue("value1 " + i), VISIBILITY_A)
                    .addPropertyValue("k1", "prop2", stringValue("value2 " + i), VISIBILITY_A)
                    .addPropertyValue("k1", "prop3", stringValue("value3 " + i), VISIBILITY_A)
                    .addPropertyValue("k1", "prop4", stringValue("value4 " + i), VISIBILITY_A)
                    .addPropertyValue("k1", "prop5", stringValue("value5 " + i), VISIBILITY_A)
                    .save(AUTHORIZATIONS_ALL);
        }
        getGraph().flush();
        double endTime = System.currentTimeMillis();
        LOGGER.info("add edges in %.3fs", (endTime - startTime) / 1000);
    }

    private void benchmarkAddExtendedDataRows(Random random, int vertexCount, int extendedDataRowCount) {
        double startTime = System.currentTimeMillis();
        for (int i = 0; i < extendedDataRowCount; i++) {
            String row = "row" + i;
            String vertexId = "v" + random.nextInt(vertexCount);
            getGraph().prepareVertex(vertexId, VISIBILITY_A, CONCEPT_TYPE_THING)
                    .addExtendedData("table1", row, "column1", stringValue("value1"), VISIBILITY_A)
                    .save(AUTHORIZATIONS_ALL);
        }
        getGraph().flush();
        double endTime = System.currentTimeMillis();
        LOGGER.info("add rows in %.3fs", (endTime - startTime) / 1000);
    }

    private void benchmarkFindVerticesById(Random random, int vertexCount, int findVerticesByIdCount) {
        double startTime = System.currentTimeMillis();
        for (int i = 0; i < findVerticesByIdCount; i++) {
            String vertexId = "v" + random.nextInt(vertexCount);
            getGraph().getVertex(vertexId, AUTHORIZATIONS_ALL);
        }
        getGraph().flush();
        double endTime = System.currentTimeMillis();
        LOGGER.info("find vertices by id in %.3fs", (endTime - startTime) / 1000);
    }


    private void benchmarkFindConnectedVertices() {
        double startTime = System.currentTimeMillis();
        for (Vertex vertex : getGraph().getVertices(AUTHORIZATIONS_ALL)) {
            for (Vertex connectedVertex : vertex.getVertices(Direction.BOTH, AUTHORIZATIONS_ALL)) {
                connectedVertex.getId();
            }
        }
        double endTime = System.currentTimeMillis();
        LOGGER.info("find connected vertices in %.3fs", (endTime - startTime) / 1000);
    }

    protected boolean benchmarkEnabled() {
        return Boolean.parseBoolean(System.getProperty("benchmark", "false"));
    }
}
