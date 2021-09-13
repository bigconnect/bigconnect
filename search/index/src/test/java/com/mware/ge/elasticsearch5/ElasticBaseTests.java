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
package com.mware.ge.elasticsearch5;

import com.mware.ge.Graph;
import com.mware.ge.GraphWithSearchIndex;
import com.mware.ge.Vertex;
import com.mware.ge.base.GraphBaseTests;
import com.mware.ge.base.GraphTestSetup;
import com.mware.ge.base.TestGraphFactory;
import com.mware.ge.query.QueryResultsIterable;
import org.elasticsearch.action.admin.indices.stats.IndicesStatsResponse;
import org.junit.Assert;
import org.junit.ClassRule;
import org.junit.Test;

import java.util.Iterator;
import java.util.List;

import static com.mware.core.model.schema.SchemaConstants.CONCEPT_TYPE_THING;
import static com.mware.ge.query.builder.GeQueryBuilders.hasFilter;
import static com.mware.ge.util.GeAssert.*;
import static com.mware.ge.util.IterableUtils.count;
import static com.mware.ge.util.IterableUtils.toList;
import static com.mware.ge.values.storable.Values.stringValue;

public class ElasticBaseTests extends GraphBaseTests implements GraphTestSetup {
    private int expectedTestElasticsearchExceptionHandlerNumberOfTimesCalled = 0;

    @ClassRule
    public static ElasticsearchResource elasticsearchResource = new ElasticsearchResource(ElasticBaseTests.class.getName());

    @Override
    public TestGraphFactory graphFactory() {
        return new ElasticGraphFactory().
                withElasticsearchResource(elasticsearchResource);
    }

    @Override
    public void before() throws Exception {
        expectedTestElasticsearchExceptionHandlerNumberOfTimesCalled = 0;
        TestElasticsearch5ExceptionHandler.clearNumberOfTimesCalled();
        elasticsearchResource.dropIndices();
        super.before();
    }

    @Override
    public void after() throws Exception {
        assertEquals(
                expectedTestElasticsearchExceptionHandlerNumberOfTimesCalled,
                TestElasticsearch5ExceptionHandler.getNumberOfTimesCalled()
        );
        super.after();
    }

    @Test
    public void testUpdateVertexWithDeletedElasticsearchDocument() {
        expectedTestElasticsearchExceptionHandlerNumberOfTimesCalled = 1;
        TestElasticsearch5ExceptionHandler.authorizations = AUTHORIZATIONS_A;

        graph.prepareVertex("v1", VISIBILITY_A, CONCEPT_TYPE_THING)
                .addPropertyValue("k1", "prop1", stringValue("joe"), VISIBILITY_A)
                .save(AUTHORIZATIONS_A);
        graph.flush();

        Vertex v1 = graph.getVertex("v1", AUTHORIZATIONS_A);
        getSearchIndex().deleteElement(graph, v1, AUTHORIZATIONS_A);
        graph.flush();

        v1 = graph.getVertex("v1", AUTHORIZATIONS_A);
        v1.prepareMutation()
                .addPropertyValue("k1", "prop2", stringValue("bob"), VISIBILITY_A)
                .save(AUTHORIZATIONS_A);
        graph.flush();

        // Missing documents are treated as new documents (see BulkUpdateService#handleFailure) and thus are not part
        // of the initial flush.
        graph.flush();

        List<String> results = toList(graph.query("joe", AUTHORIZATIONS_A).vertexIds());
        assertEquals(1, results.size());
        assertEquals("v1", results.get(0));

        results = toList(graph.query("bob", AUTHORIZATIONS_A).vertexIds());
        assertEquals(1, results.size());
        assertEquals("v1", results.get(0));
    }

    @Test
    public void testManyWritesToSameElement() throws InterruptedException {
        int threadCount = 10;
        int numberOfTimerToWrite = 100;

        Thread[] threads = new Thread[threadCount];
        for (int i = 0; i < threads.length; i++) {
            threads[i] = new Thread(() -> {
                for (int write = 0; write < numberOfTimerToWrite; write++) {
                    String keyAndValue = Thread.currentThread().getId() + "-" + write;
                    getGraph().prepareVertex("v1", VISIBILITY_EMPTY, CONCEPT_TYPE_THING)
                            .addPropertyValue(keyAndValue, "name", stringValue(keyAndValue), VISIBILITY_EMPTY)
                            .save(AUTHORIZATIONS_EMPTY);
                    getGraph().flush();
                }
            });
            threads[i].setName("testManyWritesToSameElement-" + threads[i].getId());
        }
        for (Thread thread : threads) {
            thread.start();
        }
        for (Thread thread : threads) {
            thread.join();
        }
        Vertex v1 = getGraph().getVertex("v1", AUTHORIZATIONS_EMPTY);
        assertEquals(threadCount * numberOfTimerToWrite, count(v1.getProperties("name")));
    }

    @Test
    public void testManyWritesToSameElementNoFlushTillEnd() throws InterruptedException {
        int threadCount = 5;
        int numberOfTimerToWrite = 20;

        Thread[] threads = new Thread[threadCount];
        for (int i = 0; i < threads.length; i++) {
            threads[i] = new Thread(() -> {
                for (int write = 0; write < numberOfTimerToWrite; write++) {
                    String keyAndValue = Thread.currentThread().getId() + "-" + write;
                    getGraph().prepareVertex("v1", VISIBILITY_EMPTY, CONCEPT_TYPE_THING)
                            .addPropertyValue(keyAndValue, "name", stringValue(keyAndValue), VISIBILITY_EMPTY)
                            .save(AUTHORIZATIONS_EMPTY);
                }
                getGraph().flush();
            });
            threads[i].setName("testManyWritesToSameElementNoFlushTillEnd-" + threads[i].getId());
        }
        for (Thread thread : threads) {
            thread.start();
        }
        for (Thread thread : threads) {
            thread.join();
        }
        Vertex v1 = getGraph().getVertex("v1", AUTHORIZATIONS_EMPTY);
        assertEquals(threadCount * numberOfTimerToWrite, count(v1.getProperties("name")));
    }

    @Test
    public void testNumberOfRefreshes() throws InterruptedException {
        getGraph().prepareVertex("vPRIME", VISIBILITY_EMPTY, CONCEPT_TYPE_THING)
                .addPropertyValue("k1", "name", stringValue("value1"), VISIBILITY_EMPTY)
                .save(AUTHORIZATIONS_EMPTY);
        getGraph().flush();

        int verticesToCreate = 100;
        int insertIterations = 50;
        int queryIterations = 10;

        long startRefreshes = getRefreshCount();
        Thread insertThread = new Thread(() -> {
            for (int it = 0; it < insertIterations; it++) {
                System.out.println("update " + it);
                for (int i = 0; i < verticesToCreate; i++) {
                    getGraph().prepareVertex("v", VISIBILITY_EMPTY, CONCEPT_TYPE_THING)
                            .addPropertyValue("k" + i, "name", stringValue("value" + i), VISIBILITY_EMPTY)
                            .save(AUTHORIZATIONS_EMPTY);
                }
                getGraph().flush();
            }
        });
        Thread queryThread = new Thread(() -> {
            for (int it = 0; it < queryIterations; it++) {
                System.out.println("query " + it);
                for (int i = 0; i < verticesToCreate; i++) {
                    toList(getGraph().query(hasFilter("name", stringValue("value" + i)), AUTHORIZATIONS_EMPTY)
                            .vertices());
                }
            }
        });
        long startTime = System.currentTimeMillis();
        queryThread.start();
        insertThread.start();
        queryThread.join();
        insertThread.join();
        long endTime = System.currentTimeMillis();
        System.out.println("time: " + (endTime - startTime));

        long endRefreshCount = getRefreshCount();
        long totalRefreshes = endRefreshCount - startRefreshes;
        System.out.println("refreshes: " + totalRefreshes);

        assertTrue(
                "total refreshes should be well below insert iterations times the number of vertices inserted",
                totalRefreshes < insertIterations * 2
        );
    }


    @Test
    public void testUnclosedScrollApi() {
        int verticesToCreate = ElasticsearchResource.TEST_QUERY_PAGE_SIZE * 2;
        for (int i = 0; i < verticesToCreate; i++) {
            getGraph().prepareVertex("v" + i, VISIBILITY_EMPTY, CONCEPT_TYPE_THING)
                    .addPropertyValue("k1", "name", stringValue("value1"), VISIBILITY_EMPTY)
                    .save(AUTHORIZATIONS_EMPTY);
        }
        getGraph().flush();

        QueryResultsIterable<Vertex> vertices = getGraph().query(
                hasFilter("name", stringValue("value1"))
                        .limit((Long) null),
                AUTHORIZATIONS_EMPTY
        ).vertices();
        assertEquals(verticesToCreate, vertices.getTotalHits());
        Iterator<Vertex> it = vertices.iterator();
        Assert.assertTrue(it.hasNext());
        it.next();

        System.gc();
        System.gc();
    }

    /**
     * Tests that the Elasticsearch batching handles multiple properties with the same name in the same batch
     */
    @Test
    public void testAddVertexMultipleTimesInSameBatch() {
        graph.prepareVertex("v1", VISIBILITY_EMPTY, CONCEPT_TYPE_THING)
                .addPropertyValue("keyJoe", "name", stringValue("Joe"), VISIBILITY_EMPTY)
                .addPropertyValue("keyJanet", "name", stringValue("Janet"), VISIBILITY_EMPTY)
                .addPropertyValue("keyBob", "name", stringValue("Bob"), VISIBILITY_A)
                .save(AUTHORIZATIONS_A);
        graph.prepareVertex("v1", VISIBILITY_EMPTY, CONCEPT_TYPE_THING)
                .addPropertyValue("keyTom", "name", stringValue("Tom"), VISIBILITY_EMPTY)
                .save(AUTHORIZATIONS_A);
        graph.flush();

        assertIdsAnyOrder(graph.query(AUTHORIZATIONS_EMPTY).vertexIds(), "v1");
        assertIdsAnyOrder(graph.query(AUTHORIZATIONS_A).vertexIds(), "v1");

        assertIdsAnyOrder(graph.query(hasFilter("name", stringValue("Joe")), AUTHORIZATIONS_EMPTY).vertexIds(), "v1");
        assertIdsAnyOrder(graph.query(hasFilter("name", stringValue("Tom")), AUTHORIZATIONS_EMPTY).vertexIds(), "v1");
        assertIdsAnyOrder(graph.query(hasFilter("name", stringValue("Janet")), AUTHORIZATIONS_EMPTY).vertexIds(), "v1");
        assertIdsAnyOrder(graph.query(hasFilter("name", stringValue("Bob")), AUTHORIZATIONS_EMPTY).vertexIds());

        assertIdsAnyOrder(graph.query(hasFilter("name", stringValue("Joe")), AUTHORIZATIONS_A).vertexIds(), "v1");
        assertIdsAnyOrder(graph.query(hasFilter("name", stringValue("Tom")), AUTHORIZATIONS_A).vertexIds(), "v1");
        assertIdsAnyOrder(graph.query(hasFilter("name", stringValue("Janet")), AUTHORIZATIONS_A).vertexIds(), "v1");
        assertIdsAnyOrder(graph.query(hasFilter("name", stringValue("Bob")), AUTHORIZATIONS_A).vertexIds(), "v1");
    }

    /**
     * Tests that the Elasticsearch batching handles multiple property visibility changes with the same name in the same batch
     */
    @Test
    public void testChangePropertyVisibilityMultipleTimesInSameBatch() {
        graph.prepareVertex("v1", VISIBILITY_EMPTY, CONCEPT_TYPE_THING)
                .addPropertyValue("k1", "name", stringValue("Joe"), VISIBILITY_A)
                .save(AUTHORIZATIONS_A_AND_B);
        graph.flush();

        Vertex v1a = graph.getVertex("v1", AUTHORIZATIONS_A_AND_B);
        Vertex v1b = graph.getVertex("v1", AUTHORIZATIONS_A_AND_B);

        v1a.prepareMutation()
                .alterPropertyVisibility("k1", "name", VISIBILITY_B)
                .save(AUTHORIZATIONS_A_AND_B);
        v1b.prepareMutation()
                .alterPropertyVisibility("k1", "name", VISIBILITY_B)
                .save(AUTHORIZATIONS_A_AND_B);
        graph.flush();

        assertIdsAnyOrder(graph.query(hasFilter("name", stringValue("Joe")), AUTHORIZATIONS_A).vertexIds());
        assertIdsAnyOrder(graph.query(hasFilter("name", stringValue("Joe")), AUTHORIZATIONS_B).vertexIds(), "v1");
    }

    /**
     * Tests that the Elasticsearch batching handles multiple property visibility changes with the same name in the same batch
     */
    @Test
    public void testChangePropertyVisibilityMultipleTimesInSameBatchDifferentVisibilities() {
        graph.prepareVertex("v1", VISIBILITY_EMPTY, CONCEPT_TYPE_THING)
                .addPropertyValue("k1", "name", stringValue("Joe"), VISIBILITY_EMPTY)
                .save(AUTHORIZATIONS_A_AND_B);
        graph.flush();

        Vertex v1a = graph.getVertex("v1", AUTHORIZATIONS_A_AND_B);
        Vertex v1b = graph.getVertex("v1", AUTHORIZATIONS_A_AND_B);

        try {
            v1a.prepareMutation()
                    .alterPropertyVisibility("k1", "name", VISIBILITY_A)
                    .save(AUTHORIZATIONS_A_AND_B);
            v1b.prepareMutation()
                    .alterPropertyVisibility("k1", "name", VISIBILITY_B)
                    .save(AUTHORIZATIONS_A_AND_B);
            graph.flush();
            // If this isn't caught it's also ok, it could be supported by the search index/graph or it could be
            // in different batches
        } catch (Exception ex) {
            // OK
        }
    }

    @Override
    public boolean disableEdgeIndexing(Graph graph) {
        Elasticsearch5SearchIndex searchIndex = (Elasticsearch5SearchIndex) ((GraphWithSearchIndex) graph).getSearchIndex();
        searchIndex.getConfig().getGraphConfiguration().set(ElasticsearchOptions.INDEX_EDGES.name(), false);
        return true;
    }

    @Override
    public boolean isLuceneQueriesSupported() {
        return true;
    }

    @Override
    public boolean isPainlessDateMath() {
        return true;
    }

    private Elasticsearch5SearchIndex getSearchIndex() {
        return (Elasticsearch5SearchIndex) ((GraphWithSearchIndex) graph).getSearchIndex();
    }

    private long getRefreshCount() {
        IndicesStatsResponse resp = getSearchIndex().getClient().admin().indices().prepareStats().get();
        return resp.getTotal().getRefresh().getTotal();
    }
}
