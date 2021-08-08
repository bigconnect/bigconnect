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

import com.mware.ge.*;
import com.mware.ge.base.GraphMetadataTests;
import com.mware.ge.base.GraphQueryTests;
import com.mware.ge.base.GraphTestSetup;
import com.mware.ge.base.TestGraphFactory;
import com.mware.ge.elasticsearch5.lucene.DefaultQueryStringTransformer;
import com.mware.ge.mutation.ExistingElementMutation;
import com.mware.ge.query.QueryResultsIterable;
import com.mware.ge.query.builder.GeQueryBuilders;
import com.mware.ge.values.storable.TextValue;
import org.elasticsearch.action.admin.cluster.node.stats.NodeStats;
import org.elasticsearch.action.admin.cluster.node.stats.NodesStatsAction;
import org.elasticsearch.action.admin.cluster.node.stats.NodesStatsRequestBuilder;
import org.elasticsearch.action.admin.cluster.node.stats.NodesStatsResponse;
import org.elasticsearch.common.io.stream.NotSerializableExceptionWrapper;
import org.elasticsearch.index.search.stats.SearchStats;
import org.junit.Assert;
import org.junit.ClassRule;
import org.junit.Test;

import java.util.List;

import static com.mware.core.model.schema.SchemaConstants.CONCEPT_TYPE_THING;
import static com.mware.ge.query.builder.GeQueryBuilders.searchAll;
import static com.mware.ge.util.CloseableUtils.closeQuietly;
import static com.mware.ge.util.GeAssert.*;
import static com.mware.ge.util.IterableUtils.count;
import static com.mware.ge.util.IterableUtils.toList;
import static com.mware.ge.values.storable.Values.stringValue;

public class ElasticQueryTests extends GraphQueryTests implements GraphTestSetup {
    @ClassRule
    public static ElasticsearchResource elasticsearchResource = new ElasticsearchResource(ElasticQueryTests.class.getName());

    @Override
    public TestGraphFactory graphFactory() {
        return new ElasticGraphFactory().
                withElasticsearchResource(elasticsearchResource);
    }

    @Override
    public void before() throws Exception {
        elasticsearchResource.dropIndices();
        super.before();
    }

    @Test
    public void testCustomQueryStringTransformer() {
        Elasticsearch5SearchIndex searchIndex = (Elasticsearch5SearchIndex) ((GraphWithSearchIndex) graph).getSearchIndex();
        searchIndex.getConfig().setQueryStringTransformer(new DefaultQueryStringTransformer(graph) {
            @Override
            protected String[] expandFieldName(String fieldName, Authorizations authorizations) {
                if ("knownAs".equals(fieldName)) {
                    fieldName = "name";
                }
                return super.expandFieldName(fieldName, authorizations);
            }
        });

        graph.defineProperty("name").dataType(TextValue.class).textIndexHint(TextIndexHint.ALL).define();
        graph.defineProperty("food").dataType(TextValue.class).textIndexHint(TextIndexHint.ALL).define();

        graph.prepareVertex("v1", VISIBILITY_A, CONCEPT_TYPE_THING)
                .setProperty("name", stringValue("Joe Ferner"), VISIBILITY_A)
                .setProperty("food", stringValue("pizza"), VISIBILITY_A)
                .save(AUTHORIZATIONS_A_AND_B);
        graph.prepareVertex("v2", VISIBILITY_A, CONCEPT_TYPE_THING)
                .setProperty("name", stringValue("Joe Smith"), VISIBILITY_A)
                .setProperty("food", stringValue("salad"), VISIBILITY_A)
                .save(AUTHORIZATIONS_A_AND_B);
        graph.flush();

        Iterable<Vertex> vertices = graph.query("joe", AUTHORIZATIONS_A).vertices();
        Assert.assertEquals(2, count(vertices));

        vertices = graph.query("\"joe ferner\"", AUTHORIZATIONS_A).vertices();
        Assert.assertEquals(1, count(vertices));

        vertices = graph.query("name:\"joe ferner\"", AUTHORIZATIONS_A).vertices();
        Assert.assertEquals(1, count(vertices));

        vertices = graph.query("knownAs:\"joe ferner\"", AUTHORIZATIONS_A).vertices();
        Assert.assertEquals(1, count(vertices));

        vertices = graph.query("knownAs:joe", AUTHORIZATIONS_A).vertices();
        Assert.assertEquals(2, count(vertices));

        vertices = graph.query(
                GeQueryBuilders.boolQuery()
                        .and(GeQueryBuilders.search("knownAs:joe"))
                        .and(GeQueryBuilders.hasFilter("food", stringValue("pizza"))),
                AUTHORIZATIONS_A
        ).vertices();
        Assert.assertEquals(1, count(vertices));

        vertices = graph.query("food:pizza", AUTHORIZATIONS_A).vertices();
        Assert.assertEquals(1, count(vertices));

        vertices = graph.query("eats:pizza", AUTHORIZATIONS_A).vertices();
        Assert.assertEquals(0, count(vertices));
    }

    @Test
    public void testQueryCreateAndUpdate() {
        Authorizations a1 = graph.createAuthorizations("workspace", "junit-workspace", "ontology");
        Authorizations a2 = graph.createAuthorizations("workspace", "ontology");
        graph.prepareVertex("v1", new Visibility("((ontology)|administrator)&(junit-workspace)"), CONCEPT_TYPE_THING).save(a1);
        graph.flush();

        QueryResultsIterable<Vertex> vertices = graph.query(a1).vertices();
        assertResultsCount(1, 1, vertices);

        Vertex v1 = graph.getVertex("v1", a1);
        ExistingElementMutation<Vertex> m1 = v1.prepareMutation().alterElementVisibility(new Visibility("((ontology)|administrator)"));
        m1.save(a1);
        graph.flush();

        vertices = graph.query(a2).vertices();
        assertResultsCount(1, 1, vertices);
    }

    @Test
    public void testQueryExecutionCountWhenPaging() {
        graph.prepareVertex("v1", VISIBILITY_A, CONCEPT_TYPE_THING).save(AUTHORIZATIONS_A);
        graph.prepareVertex("v2", VISIBILITY_A, CONCEPT_TYPE_THING).save(AUTHORIZATIONS_A);
        graph.flush();

        long startingNumQueries = getNumQueries();

        QueryResultsIterable<Vertex> vertices = graph.query(AUTHORIZATIONS_A).vertices();
        assertEquals(startingNumQueries, getNumQueries());

        assertResultsCount(2, 2, vertices);
        assertEquals(startingNumQueries + 2, getNumQueries());

        vertices = graph.query(searchAll().limit(1), AUTHORIZATIONS_A).vertices();
        assertEquals(startingNumQueries + 4, getNumQueries());

        assertResultsCount(1, 2, vertices);
        assertEquals(startingNumQueries + 4, getNumQueries());

        vertices = graph.query(searchAll().limit(10), AUTHORIZATIONS_A).vertices();
        assertEquals(startingNumQueries + 6, getNumQueries());

        assertResultsCount(2, 2, vertices);
        assertEquals(startingNumQueries + 6, getNumQueries());
    }

    @Test
    public void testQueryExecutionCountWhenScrollingApi() {
        Elasticsearch5SearchIndex searchIndex = (Elasticsearch5SearchIndex) ((GraphWithSearchIndex) graph).getSearchIndex();
        searchIndex.getConfig().getGraphConfiguration().set(GraphConfiguration.SEARCH_INDEX_PROP_PREFIX + "." + ElasticsearchSearchIndexConfiguration.QUERY_PAGE_SIZE, 1);

        graph.prepareVertex("v1", VISIBILITY_A, CONCEPT_TYPE_THING).save(AUTHORIZATIONS_A);
        graph.prepareVertex("v2", VISIBILITY_A, CONCEPT_TYPE_THING).save(AUTHORIZATIONS_A);
        graph.flush();

        long startingNumQueries = getNumQueries();

        QueryResultsIterable<Vertex> vertices = graph.query(AUTHORIZATIONS_A).vertices();
        assertResultsCount(2, vertices);
        assertEquals(startingNumQueries + 4, getNumQueries());

        searchIndex = (Elasticsearch5SearchIndex) ((GraphWithSearchIndex) graph).getSearchIndex();
        searchIndex.getConfig().getGraphConfiguration().set(GraphConfiguration.SEARCH_INDEX_PROP_PREFIX + "." + ElasticsearchSearchIndexConfiguration.QUERY_PAGE_SIZE, 2);

        graph.prepareVertex("v3", VISIBILITY_A, CONCEPT_TYPE_THING).save(AUTHORIZATIONS_A);
        graph.flush();

        vertices = graph.query(AUTHORIZATIONS_A).vertices();
        assertResultsCount(3, vertices);
        assertEquals(startingNumQueries + 8, getNumQueries());
    }

    @Test
    public void testDisallowLeadingWildcardsInQueryString() {
        graph.prepareVertex("v1", VISIBILITY_A, CONCEPT_TYPE_THING).setProperty("prop1", stringValue("value1"), VISIBILITY_A).save(AUTHORIZATIONS_A);
        graph.flush();

        try {
            graph.query("*alue1", AUTHORIZATIONS_A).search().getTotalHits();
            fail("Wildcard prefix of query string should have caused an exception");
        } catch (Exception e) {
            if (!(getRootCause(e) instanceof NotSerializableExceptionWrapper)) {
                fail("Wildcard prefix of query string should have caused a NotSerializableExceptionWrapper exception");
            }
        }
    }

    @Test
    public void testLimitingNumberOfQueryStringTerms() {
        graph.prepareVertex("v1", VISIBILITY_A, CONCEPT_TYPE_THING).setProperty("prop1", stringValue("value1"), VISIBILITY_A).save(AUTHORIZATIONS_A);
        graph.flush();

        StringBuilder q = new StringBuilder();
        for (int i = 0; i < getSearchIndex().getConfig().getMaxQueryStringTerms(); i++) {
            q.append("jeff").append(i).append(" ");
        }

        // should succeed
        graph.query(q.toString(), AUTHORIZATIONS_A).search().getTotalHits();

        try {
            q.append("done");
            graph.query(q.toString(), AUTHORIZATIONS_A).search().getTotalHits();
            fail("Exceeding max query terms should have thrown an exception");
        } catch (GeException e) {
            // expected
        }
    }

    @Test
    public void testQueryReturningElasticsearchEdge() {
        graph.prepareVertex("v1", VISIBILITY_A, CONCEPT_TYPE_THING).save(AUTHORIZATIONS_A);
        graph.prepareVertex("v2", VISIBILITY_A, CONCEPT_TYPE_THING).save(AUTHORIZATIONS_A);
        graph.prepareEdge("e1", "v1", "v2", LABEL_LABEL1, VISIBILITY_A).save(AUTHORIZATIONS_A);
        graph.flush();

        QueryResultsIterable<Edge> edges = graph.query(AUTHORIZATIONS_A)
                .edges(FetchHints.NONE);

        assertResultsCount(1, 1, edges);
        Edge e1 = toList(edges).get(0);
        assertEquals(LABEL_LABEL1, e1.getLabel());
        assertEquals("v1", e1.getVertexId(Direction.OUT));
        assertEquals("v2", e1.getVertexId(Direction.IN));
        assertEquals("e1", e1.getId());
    }

    @Test
    public void testQueryReturningElasticsearchVertex() {
        graph.prepareVertex("v1", VISIBILITY_A, CONCEPT_TYPE_THING).save(AUTHORIZATIONS_A);
        graph.prepareVertex("v2", VISIBILITY_B, CONCEPT_TYPE_THING).save(AUTHORIZATIONS_B);
        graph.prepareEdge("e1", "v1", "v2", LABEL_LABEL1, VISIBILITY_A).save(AUTHORIZATIONS_A);
        graph.flush();

        QueryResultsIterable<Vertex> vertices = graph.query(AUTHORIZATIONS_B)
                .vertices(FetchHints.NONE);

        assertResultsCount(1, 1, vertices);
        Vertex vertex = toList(vertices).get(0);
        assertEquals("v2", vertex.getId());
    }

    @Test
    public void testQueryPagingVsScrollApi() {
        for (int i = 0; i < ElasticsearchResource.TEST_QUERY_PAGING_LIMIT * 2; i++) {
            graph.prepareVertex("v" + i, VISIBILITY_A, CONCEPT_TYPE_THING)
                    .addPropertyValue("k1", "prop1", stringValue("joe"), VISIBILITY_A)
                    .save(AUTHORIZATIONS_A);
        }
        graph.flush();

        int resultCount = count(graph.query(searchAll().limit(ElasticsearchResource.TEST_QUERY_PAGING_LIMIT - 1), AUTHORIZATIONS_A)
                .vertices());
        assertEquals(ElasticsearchResource.TEST_QUERY_PAGING_LIMIT - 1, resultCount);

        resultCount = count(graph.query(searchAll().limit(ElasticsearchResource.TEST_QUERY_PAGING_LIMIT + 1), AUTHORIZATIONS_A)
                .vertices());
        assertEquals(ElasticsearchResource.TEST_QUERY_PAGING_LIMIT + 1, resultCount);

        resultCount = count(graph.query(AUTHORIZATIONS_A)
                .vertices());
        assertEquals(ElasticsearchResource.TEST_QUERY_PAGING_LIMIT * 2, resultCount);
    }

    @Test
    public void testLargeTotalHitsPaged() throws InterruptedException {
        int vertexCount = 10_045;
        for (int write = 0; write < vertexCount; write++) {
            getGraph().prepareVertex("v" + write, VISIBILITY_EMPTY, CONCEPT_TYPE_THING).save(AUTHORIZATIONS_EMPTY);
        }
        getGraph().flush();
        QueryResultsIterable<String> queryResults = getGraph()
                .query(searchAll().limit(0), AUTHORIZATIONS_EMPTY)
                .vertexIds();
        assertEquals(vertexCount, queryResults.getTotalHits());
        closeQuietly(queryResults);
    }

    @Test
    public void testLargeTotalHitsScroll() {
        int vertexCount = 10_045;
        for (int write = 0; write < vertexCount; write++) {
            getGraph().prepareVertex("v" + write, VISIBILITY_EMPTY, CONCEPT_TYPE_THING).save(AUTHORIZATIONS_EMPTY);
        }
        getGraph().flush();
        QueryResultsIterable<String> queryResults = getGraph()
                .query(searchAll().limit((Long) null), AUTHORIZATIONS_EMPTY)
                .vertexIds();
        assertEquals(vertexCount, queryResults.getTotalHits());
        closeQuietly(queryResults);
    }

    @Test(expected = GeNotSupportedException.class)
    public void testRetrievingVerticesFromElasticsearchEdge() {
        graph.prepareVertex("v1", VISIBILITY_A, CONCEPT_TYPE_THING).save(AUTHORIZATIONS_A);
        graph.prepareVertex("v2", VISIBILITY_A, CONCEPT_TYPE_THING).save(AUTHORIZATIONS_A);
        graph.prepareEdge("e1", "v1", "v2", LABEL_LABEL1, VISIBILITY_A).save(AUTHORIZATIONS_A);
        graph.flush();

        QueryResultsIterable<Edge> edges = graph.query(AUTHORIZATIONS_A)
                .edges(FetchHints.NONE);

        assertResultsCount(1, 1, edges);
        toList(edges).get(0).getVertices(AUTHORIZATIONS_A);
    }

    @Override
    public boolean disableEdgeIndexing(Graph graph) {
        Elasticsearch5SearchIndex searchIndex = (Elasticsearch5SearchIndex) ((GraphWithSearchIndex) graph).getSearchIndex();
        searchIndex.getConfig().getGraphConfiguration().set(GraphConfiguration.SEARCH_INDEX_PROP_PREFIX + "." + ElasticsearchSearchIndexConfiguration.INDEX_EDGES, "false");
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

    @Override
    protected boolean isFieldNamesInQuerySupported() {
        return true;
    }

    private Throwable getRootCause(Throwable e) {
        if (e.getCause() == null) {
            return e;
        }
        return getRootCause(e.getCause());
    }

    private Elasticsearch5SearchIndex getSearchIndex() {
        return (Elasticsearch5SearchIndex) ((GraphWithSearchIndex) graph).getSearchIndex();
    }

    private long getNumQueries() {
        NodesStatsResponse nodeStats = new NodesStatsRequestBuilder(getSearchIndex().getClient(), NodesStatsAction.INSTANCE).get();

        List<NodeStats> nodes = nodeStats.getNodes();
        assertEquals(1, nodes.size());

        SearchStats searchStats = nodes.get(0).getIndices().getSearch();
        return searchStats.getTotal().getQueryCount();
    }
}
