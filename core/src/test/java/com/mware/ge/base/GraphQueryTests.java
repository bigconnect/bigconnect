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

import com.google.common.collect.Lists;
import com.mware.ge.*;
import com.mware.ge.event.GraphEvent;
import com.mware.ge.event.GraphEventListener;
import com.mware.ge.query.*;
import com.mware.ge.query.aggregations.*;
import com.mware.ge.util.ConvertingIterable;
import com.mware.ge.util.GeLogger;
import com.mware.ge.util.GeLoggerFactory;
import com.mware.ge.util.IterableUtils;
import com.mware.ge.values.storable.*;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import java.io.Closeable;
import java.io.IOException;
import java.text.ParseException;
import java.time.DayOfWeek;
import java.time.Month;
import java.time.ZoneOffset;
import java.time.temporal.ChronoField;
import java.util.*;
import java.util.stream.Collectors;

import static com.mware.core.model.schema.SchemaConstants.CONCEPT_TYPE_THING;
import static com.mware.ge.query.builder.GeQueryBuilders.*;
import static com.mware.ge.util.GeAssert.*;
import static com.mware.ge.util.GeAssert.assertResultsCount;
import static com.mware.ge.util.IterableUtils.count;
import static com.mware.ge.util.IterableUtils.toList;
import static com.mware.ge.util.StreamUtils.stream;
import static com.mware.ge.values.storable.Values.*;
import static com.mware.ge.values.storable.Values.floatValue;
import static org.junit.Assume.assumeTrue;

@RunWith(JUnit4.class)
public abstract class GraphQueryTests implements GraphTestSetup {
    private static final GeLogger LOGGER = GeLoggerFactory.getLogger(GraphQueryTests.class);

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
    public void testGraphQueryPagingForUniqueIdsSortedOrder() {
        String namePropertyName = "first.name";
        getGraph().defineProperty(namePropertyName).dataType(TextValue.class).sortable(true).textIndexHint(TextIndexHint.ALL).define();

        getGraph().prepareVertex("v1", VISIBILITY_A, CONCEPT_TYPE_THING)
                .addPropertyValue("k1", namePropertyName, stringValue("B"), VISIBILITY_A)
                .save(AUTHORIZATIONS_A);
        getGraph().prepareVertex("v2", VISIBILITY_A, CONCEPT_TYPE_THING)
                .addPropertyValue("k1", namePropertyName, stringValue("A"), VISIBILITY_A)
                .save(AUTHORIZATIONS_A);
        getGraph().prepareVertex("v3", VISIBILITY_A, CONCEPT_TYPE_THING)
                .addPropertyValue("k1", namePropertyName, stringValue("C"), VISIBILITY_A)
                .save(AUTHORIZATIONS_A);
        getGraph().flush();

        QueryResultsIterable<String> idsIterable = getGraph().query(
                searchAll()
                        .sort(Element.ID_PROPERTY_NAME, SortDirection.ASCENDING)
                        .skip(0)
                        .limit(1),
                AUTHORIZATIONS_A
        ).vertexIds();
        assertIdsAnyOrder(idsIterable, "v1");
        assertResultsCount(1, 3, idsIterable);

        idsIterable = getGraph().query(
                searchAll()
                        .sort(Element.ID_PROPERTY_NAME, SortDirection.ASCENDING)
                        .skip(1)
                        .limit(1),
                AUTHORIZATIONS_A
        ).vertexIds();
        assertIdsAnyOrder(idsIterable, "v2");

        idsIterable = getGraph().query(
                searchAll()
                        .sort(Element.ID_PROPERTY_NAME, SortDirection.ASCENDING)
                        .skip(2)
                        .limit(1),
                AUTHORIZATIONS_A
        ).vertexIds();
        assertIdsAnyOrder(idsIterable, "v3");

        idsIterable = getGraph().query(searchAll().sort(namePropertyName, SortDirection.ASCENDING), AUTHORIZATIONS_A).vertexIds();
        assertResultsCount(3, 3, idsIterable);

        idsIterable = getGraph().query(searchAll().limit((Long) null), AUTHORIZATIONS_A).vertexIds();
        assertResultsCount(3, 3, idsIterable);

        List<Vertex> vertices = toList(getGraph().query(
                searchAll()
                        .sort(namePropertyName, SortDirection.ASCENDING)
                        .skip(0)
                        .limit(1),
                AUTHORIZATIONS_A
        ).vertices());
        assertEquals(1, vertices.size());
        assertEquals("v2", vertices.get(0).getId());
    }

    @Test
    public void testGraphQueryForIds() {
        String namePropertyName = "first.name";
        Vertex v1 = getGraph().prepareVertex("v1", VISIBILITY_A, CONCEPT_TYPE_THING)
                .addPropertyValue("k1", namePropertyName, stringValue("joe"), VISIBILITY_A)
                .save(AUTHORIZATIONS_A);
        Vertex v2 = getGraph().addVertex("v2", VISIBILITY_A, AUTHORIZATIONS_A, CONCEPT_TYPE_THING);
        getGraph().prepareVertex("v3", VISIBILITY_A, CONCEPT_TYPE_THING)
                .addExtendedData("table1", "row1", namePropertyName, stringValue("value 1"), VISIBILITY_A)
                .addExtendedData("table1", "row2", namePropertyName, stringValue("value 2"), VISIBILITY_A)
                .save(AUTHORIZATIONS_A);

        getGraph().addEdge("e1", v1, v2, LABEL_LABEL1, VISIBILITY_A, AUTHORIZATIONS_A);
        getGraph().addEdge("e2", v1, v2, LABEL_LABEL2, VISIBILITY_A, AUTHORIZATIONS_A);
        getGraph().flush();

        QueryResultsIterable<String> idsIterable = getGraph().query(AUTHORIZATIONS_A).vertexIds();
        assertIdsAnyOrder(idsIterable, "v1", "v2", "v3");
        assertResultsCount(3, 3, idsIterable);

        idsIterable = getGraph().query(searchAll().skip(1), AUTHORIZATIONS_A).vertexIds();
        assertResultsCount(2, 3, idsIterable);

        idsIterable = getGraph().query(searchAll().limit(1), AUTHORIZATIONS_A).vertexIds();
        assertResultsCount(1, 3, idsIterable);

        idsIterable = getGraph().query(searchAll().skip(1).limit(1), AUTHORIZATIONS_A).vertexIds();
        assertResultsCount(1, 3, idsIterable);

        idsIterable = getGraph().query(searchAll().skip(3), AUTHORIZATIONS_A).vertexIds();
        assertResultsCount(0, 3, idsIterable);

        idsIterable = getGraph().query(searchAll().skip(2).limit(2), AUTHORIZATIONS_A).vertexIds();
        assertResultsCount(1, 3, idsIterable);

        idsIterable = getGraph().query(AUTHORIZATIONS_A).edgeIds();
        assertIdsAnyOrder(idsIterable, "e1", "e2");
        assertResultsCount(2, 2, idsIterable);

        idsIterable = getGraph().query(hasEdgeLabel(LABEL_LABEL1), AUTHORIZATIONS_A).edgeIds();
        assertIdsAnyOrder(idsIterable, "e1");
        assertResultsCount(1, 1, idsIterable);

        idsIterable = getGraph().query(hasEdgeLabel(LABEL_LABEL1, LABEL_LABEL2), AUTHORIZATIONS_A).edgeIds();
        assertResultsCount(2, 2, idsIterable);

        idsIterable = getGraph().query(AUTHORIZATIONS_A).elementIds();
        assertIdsAnyOrder(idsIterable, "v1", "v2", "v3", "e1", "e2");
        assertResultsCount(5, 5, idsIterable);

        assumeTrue("FetchHints.NONE vertex queries are not supported", isFetchHintNoneVertexQuerySupported());

        idsIterable = getGraph().query(exists(namePropertyName), AUTHORIZATIONS_A).vertexIds();
        assertIdsAnyOrder(idsIterable, "v1");
        assertResultsCount(1, 1, idsIterable);

        QueryResultsIterable<ExtendedDataRowId> extendedDataRowIds = getGraph().query(hasExtendedData("table1"), AUTHORIZATIONS_A).extendedDataRowIds();
        List<String> rowIds = stream(extendedDataRowIds).map(ExtendedDataRowId::getRowId).collect(Collectors.toList());
        assertIdsAnyOrder(rowIds, "row1", "row2");
        assertResultsCount(2, 2, extendedDataRowIds);

        idsIterable = getGraph().query(boolQuery().andNot(exists(namePropertyName)), AUTHORIZATIONS_A).vertexIds();
        assertIdsAnyOrder(idsIterable, "v2", "v3");
        assertResultsCount(2, 2, idsIterable);

        idsIterable = getGraph().query(exists("notSetProp"), AUTHORIZATIONS_A).vertexIds();
        assertResultsCount(0, 0, idsIterable);

        idsIterable = getGraph().query(boolQuery().andNot(exists("notSetProp")), AUTHORIZATIONS_A).vertexIds();
        assertIdsAnyOrder(idsIterable, "v1", "v2", "v3");
        assertResultsCount(3, 3, idsIterable);

        assertResultsCount(3, 3, getGraph().query(hasFilter("notSetProp", Compare.NOT_EQUAL, intValue(5)), AUTHORIZATIONS_A).vertexIds());
    }

    @Test
    public void testGraphQueryForEdgesUsingInOutVertexIds() {
        Vertex v1 = getGraph().addVertex("v1", VISIBILITY_A, AUTHORIZATIONS_A, CONCEPT_TYPE_THING);
        Vertex v2 = getGraph().addVertex("v2", VISIBILITY_A, AUTHORIZATIONS_A, CONCEPT_TYPE_THING);
        Vertex v3 = getGraph().addVertex("v3", VISIBILITY_A, AUTHORIZATIONS_A, CONCEPT_TYPE_THING);
        getGraph().addEdge("e1", v1, v2, LABEL_LABEL1, VISIBILITY_A, AUTHORIZATIONS_A);
        getGraph().addEdge("e2", v1, v3, LABEL_LABEL1, VISIBILITY_A, AUTHORIZATIONS_A);
        getGraph().addEdge("e3", v3, v1, LABEL_LABEL1, VISIBILITY_A, AUTHORIZATIONS_A);
        getGraph().flush();
        QueryResultsIterable<Edge> edges = getGraph().query(
                boolQuery()
                        .and(hasFilter(Edge.OUT_VERTEX_ID_PROPERTY_NAME, Compare.EQUAL, stringValue("v1")))
                        .and(hasFilter(Edge.IN_VERTEX_ID_PROPERTY_NAME, Compare.EQUAL, stringValue("v2"))),
                AUTHORIZATIONS_A
        ).edges();
        assertEdgeIdsAnyOrder(edges, "e1");
        edges = getGraph().query(hasFilter(Edge.OUT_VERTEX_ID_PROPERTY_NAME, Compare.EQUAL, stringValue("v1")), AUTHORIZATIONS_A)
                .edges();
        assertEdgeIdsAnyOrder(edges, "e1", "e2");
    }

    @Test
    public void testGraphQueryForEdgesUsingEdgeLabel() {
        Vertex v1 = getGraph().addVertex("v1", VISIBILITY_A, AUTHORIZATIONS_A, CONCEPT_TYPE_THING);
        Vertex v2 = getGraph().addVertex("v2", VISIBILITY_A, AUTHORIZATIONS_A, CONCEPT_TYPE_THING);
        Vertex v3 = getGraph().addVertex("v3", VISIBILITY_A, AUTHORIZATIONS_A, CONCEPT_TYPE_THING);
        getGraph().addEdge("e1", v1, v2, LABEL_LABEL1, VISIBILITY_A, AUTHORIZATIONS_A);
        getGraph().addEdge("e2", v1, v3, LABEL_LABEL2, VISIBILITY_A, AUTHORIZATIONS_A);
        getGraph().addEdge("e3", v3, v1, LABEL_LABEL2, VISIBILITY_A, AUTHORIZATIONS_A);
        getGraph().flush();
        QueryResultsIterable<Edge> edges = getGraph().query(hasFilter(Edge.LABEL_PROPERTY_NAME, Compare.EQUAL, stringValue(LABEL_LABEL1)), AUTHORIZATIONS_A)
                .edges();
        assertEdgeIdsAnyOrder(edges, "e1");
        edges = getGraph().query(hasFilter(Edge.LABEL_PROPERTY_NAME, Compare.EQUAL, stringValue(LABEL_LABEL2)), AUTHORIZATIONS_A)
                .edges();
        assertEdgeIdsAnyOrder(edges, "e2", "e3");
    }

    @Test
    public void testGraphQueryForEdgesUsingInOrOutVertexId() {
        Vertex v1 = getGraph().addVertex("v1", VISIBILITY_A, AUTHORIZATIONS_A, CONCEPT_TYPE_THING);
        Vertex v2 = getGraph().addVertex("v2", VISIBILITY_A, AUTHORIZATIONS_A, CONCEPT_TYPE_THING);
        Vertex v3 = getGraph().addVertex("v3", VISIBILITY_A, AUTHORIZATIONS_A, CONCEPT_TYPE_THING);
        getGraph().addEdge("e1", v1, v2, LABEL_LABEL1, VISIBILITY_A, AUTHORIZATIONS_A);
        getGraph().addEdge("e2", v1, v3, LABEL_LABEL1, VISIBILITY_A, AUTHORIZATIONS_A);
        getGraph().addEdge("e3", v2, v3, LABEL_LABEL1, VISIBILITY_A, AUTHORIZATIONS_A);
        getGraph().flush();
        QueryResultsIterable<Edge> edges = getGraph().query(hasFilter(Edge.IN_OR_OUT_VERTEX_ID_PROPERTY_NAME, stringValue("v1")), AUTHORIZATIONS_A)
                .edges();
        assertEdgeIdsAnyOrder(edges, "e1", "e2");
    }

    @Test
    public void testGraphQuery() {
        String namePropertyName = "first.name";
        Vertex v1 = getGraph().prepareVertex("v1", VISIBILITY_A, CONCEPT_TYPE_THING)
                .addPropertyValue("k1", namePropertyName, stringValue("joe"), VISIBILITY_A)
                .save(AUTHORIZATIONS_A);
        Vertex v2 = getGraph().addVertex("v2", VISIBILITY_A, AUTHORIZATIONS_A, CONCEPT_TYPE_THING);
        getGraph().addEdge("e1", v1, v2, LABEL_LABEL1, VISIBILITY_A, AUTHORIZATIONS_A);
        getGraph().addEdge("e2", v1, v2, LABEL_LABEL2, VISIBILITY_A, AUTHORIZATIONS_A);
        getGraph().flush();

        QueryResultsIterable<Vertex> vertices = getGraph().query(AUTHORIZATIONS_A).vertices();
        assertResultsCount(2, 2, vertices);

        vertices = getGraph().query(searchAll().skip(1), AUTHORIZATIONS_A).vertices();
        assertResultsCount(1, 2, vertices);

        vertices = getGraph().query(searchAll().limit(1), AUTHORIZATIONS_A).vertices();
        assertResultsCount(1, 2, vertices);

        vertices = getGraph().query(searchAll().skip(1).limit(1), AUTHORIZATIONS_A).vertices();
        assertResultsCount(1, 2, vertices);

        vertices = getGraph().query(searchAll().skip(2), AUTHORIZATIONS_A).vertices();
        assertResultsCount(0, 2, vertices);

        vertices = getGraph().query(searchAll().skip(1).limit(2), AUTHORIZATIONS_A).vertices();
        assertResultsCount(1, 2, vertices);

        QueryResultsIterable<Edge> edges = getGraph().query(AUTHORIZATIONS_A).edges();
        assertResultsCount(2, 2, edges);

        edges = getGraph().query(hasEdgeLabel(LABEL_LABEL1), AUTHORIZATIONS_A).edges();
        assertResultsCount(1, 1, edges);

        edges = getGraph().query(hasEdgeLabel(LABEL_LABEL1, LABEL_LABEL2), AUTHORIZATIONS_A).edges();
        assertResultsCount(2, 2, edges);

        QueryResultsIterable<Element> elements = getGraph().query(AUTHORIZATIONS_A).elements();
        assertResultsCount(4, 4, elements);

        vertices = getGraph().query(exists(namePropertyName), AUTHORIZATIONS_A).vertices();
        assertResultsCount(1, 1, vertices);

        vertices = getGraph().query(boolQuery().andNot(exists(namePropertyName)), AUTHORIZATIONS_A).vertices();
        assertResultsCount(1, 1, vertices);

        vertices = getGraph().query(exists("notSetProp"), AUTHORIZATIONS_A).vertices();
        assertResultsCount(0, 0, vertices);

        vertices = getGraph().query(boolQuery().andNot(exists("notSetProp")), AUTHORIZATIONS_A).vertices();
        assertResultsCount(2, 2, vertices);

        vertices = getGraph().query(hasIds("v1"), AUTHORIZATIONS_A).vertices();
        assertResultsCount(1, 1, vertices);

        vertices = getGraph().query(hasIds("v1", "v2"), AUTHORIZATIONS_A).vertices();
        assertResultsCount(2, 2, vertices);

        vertices = getGraph().query(boolQuery().and(hasIds("v1", "v2")).and(hasIds("v1")), AUTHORIZATIONS_A).vertices();
        assertResultsCount(1, 1, vertices);
        assertVertexIds(vertices, "v1");

        vertices = getGraph().query(boolQuery().and(hasIds("v1")).and(hasIds("v2")), AUTHORIZATIONS_A).vertices();
        assertResultsCount(0, 0, vertices);

        edges = getGraph().query(hasIds("e1"), AUTHORIZATIONS_A).edges();
        assertResultsCount(1, 1, edges);

        edges = getGraph().query(hasIds("e1", "e2"), AUTHORIZATIONS_A).edges();
        assertResultsCount(2, 2, edges);

        assertResultsCount(0, getGraph().query(hasFilter("notSetProp", Compare.NOT_EQUAL, intValue(5)), AUTHORIZATIONS_A).vertices());
    }

    @Test
    public void testGraphQueryWithBoolean() {
        getGraph().defineProperty("boolean").dataType(BooleanValue.class).define();

        getGraph().prepareVertex("v1", VISIBILITY_A, CONCEPT_TYPE_THING)
                .addPropertyValue("k1", "boolean", BooleanValue.TRUE, VISIBILITY_A)
                .save(AUTHORIZATIONS_A);
        getGraph().flush();

        QueryResultsIterable<Vertex> vertices = getGraph().query("zzzzz", AUTHORIZATIONS_A).vertices();
        assertResultsCount(0, 0, vertices);

        vertices = getGraph().query(hasFilter("boolean", BooleanValue.TRUE), AUTHORIZATIONS_A).vertices();
        assertResultsCount(1, 1, vertices);

        vertices = getGraph().query(hasFilter("boolean", BooleanValue.FALSE), AUTHORIZATIONS_A).vertices();
        assertResultsCount(0, 0, vertices);
    }

    @Test
    public void testClosingIterables() throws IOException {
        getGraph().prepareVertex("v1", VISIBILITY_A, CONCEPT_TYPE_THING)
                .addPropertyValue("k1", "name", stringValue("joe"), VISIBILITY_A)
                .save(AUTHORIZATIONS_A);
        getGraph().prepareVertex("v2", VISIBILITY_A, CONCEPT_TYPE_THING)
                .addPropertyValue("k1", "name", stringValue("matt"), VISIBILITY_A)
                .save(AUTHORIZATIONS_A);
        getGraph().flush();
        // Ensure that closing doesn't cause an error if we haven't iterated yet
        Iterable<Vertex> vertices1 = getGraph().getVertices(AUTHORIZATIONS_A);
        if (vertices1 instanceof Closeable) {
            ((Closeable) vertices1).close();
        }
        // Ensure that closing doesn't cause an error if the iterable was fully traversed
        vertices1 = getGraph().getVertices(AUTHORIZATIONS_A);
        toList(vertices1);
        if (vertices1 instanceof Closeable) {
            ((Closeable) vertices1).close();
        }
        // Ensure that closing query results doesn't cause an error if we haven't iterated yet
        QueryResultsIterable<Vertex> queryResults = getGraph().query(hasIds("v1"), AUTHORIZATIONS_A).vertices();
        queryResults.close();
        // Ensure that closing query results doesn't cause an error if the iterable was fully traversed
        queryResults = getGraph().query(hasIds("v1"), AUTHORIZATIONS_A).vertices();
        toList(queryResults);
        queryResults.close();
    }

    @Test
    public void testGraphQueryWithFetchHints() {
        getGraph().prepareVertex("v1", VISIBILITY_A, CONCEPT_TYPE_THING)
                .addPropertyValue("k1", "name", stringValue("joe"), VISIBILITY_A)
                .save(AUTHORIZATIONS_A);

        getGraph().prepareVertex("v2", VISIBILITY_A, CONCEPT_TYPE_THING)
                .addPropertyValue("k1", "name", stringValue("matt"), VISIBILITY_A)
                .save(AUTHORIZATIONS_A);

        getGraph().prepareVertex("v3", VISIBILITY_A, CONCEPT_TYPE_THING)
                .addPropertyValue("k1", "name", stringValue("joe"), VISIBILITY_A)
                .save(AUTHORIZATIONS_A);

        getGraph().addEdge("e1", "v1", "v2", LABEL_LABEL1, VISIBILITY_A, AUTHORIZATIONS_A);

        getGraph().flush();

        assertTrue(getGraph().getVertexCount(AUTHORIZATIONS_A) == 3);

        FetchHints propertiesFetchHints = FetchHints.builder()
                .setIncludeAllProperties(true)
                .build();
        QueryResultsIterable<Vertex> vertices = getGraph().query(hasFilter("name", stringValue("joe")), AUTHORIZATIONS_A)
                .vertices(propertiesFetchHints);

        assertResultsCount(2, 2, vertices);

        assumeTrue("FetchHints.NONE vertex queries are not supported", isFetchHintNoneVertexQuerySupported());

        vertices = getGraph().query(hasFilter("name", stringValue("joe")), AUTHORIZATIONS_A)
                .vertices(FetchHints.ALL);

        assertResultsCount(2, 2, vertices);

        QueryResultsIterable<Edge> edges = getGraph().query(hasFilter(Edge.LABEL_PROPERTY_NAME, Compare.EQUAL, stringValue(LABEL_LABEL1)), AUTHORIZATIONS_A)
                .edges(FetchHints.EDGE_REFS);

        assertResultsCount(1, 1, edges);
    }

    @Test
    public void testGraphQueryWithQueryString() {
        Vertex v1 = getGraph().addVertex("v1", VISIBILITY_A, AUTHORIZATIONS_ALL, CONCEPT_TYPE_THING);
        v1.setProperty("description", stringValue("This is vertex 1 - dog."), VISIBILITY_A, AUTHORIZATIONS_ALL);
        Vertex v2 = getGraph().addVertex("v2", VISIBILITY_A, AUTHORIZATIONS_ALL, CONCEPT_TYPE_THING);
        v2.setProperty("description", stringValue("This is vertex 2 - cat."), VISIBILITY_B, AUTHORIZATIONS_ALL);
        Edge e1 = getGraph().addEdge("e1", v1, v2, LABEL_LABEL1, VISIBILITY_A, AUTHORIZATIONS_A);
        e1.setProperty("description", stringValue("This is edge 1 - dog to cat."), VISIBILITY_A, AUTHORIZATIONS_ALL);
        getGraph().flush();

        Iterable<Vertex> vertices = getGraph().query("vertex", AUTHORIZATIONS_A_AND_B).vertices();
        assertEquals(2, count(vertices));

        vertices = getGraph().query("vertex", AUTHORIZATIONS_A).vertices();
        assertEquals(1, count(vertices));

        vertices = getGraph().query("dog", AUTHORIZATIONS_A).vertices();
        assertEquals(1, count(vertices));

        vertices = getGraph().query("dog", AUTHORIZATIONS_B).vertices();
        assertEquals(0, count(vertices));

        Iterable<Element> elements = getGraph().query("dog", AUTHORIZATIONS_A_AND_B).elements();
        assertEquals(2, count(elements));
    }

    @Test
    public void testGraphQueryWithQueryStringWithAuthorizations() {
        Vertex v1 = getGraph().addVertex("v1", VISIBILITY_A, AUTHORIZATIONS_ALL, CONCEPT_TYPE_THING);
        v1.setProperty("description", stringValue("This is vertex 1 - dog."), VISIBILITY_A, AUTHORIZATIONS_ALL);
        Vertex v2 = getGraph().addVertex("v2", VISIBILITY_B, AUTHORIZATIONS_ALL, CONCEPT_TYPE_THING);
        v2.setProperty("description", stringValue("This is vertex 2 - cat."), VISIBILITY_B, AUTHORIZATIONS_ALL);
        Edge e1 = getGraph().addEdge("e1", v1, v2, LABEL_LABEL1, VISIBILITY_A, AUTHORIZATIONS_ALL);
        e1.setProperty("edgeDescription", stringValue("This is edge 1 - dog to cat."), VISIBILITY_A, AUTHORIZATIONS_ALL);
        getGraph().flush();

        Iterable<Vertex> vertices = getGraph().query(AUTHORIZATIONS_A).vertices();
        assertEquals(1, count(vertices));
        if (isIterableWithTotalHitsSupported(vertices)) {
            IterableWithTotalHits hits = (IterableWithTotalHits) vertices;
            assertEquals(1, hits.getTotalHits());
        }

        Iterable<Edge> edges = getGraph().query(AUTHORIZATIONS_A).edges();
        assertEquals(1, count(edges));
    }


    @Test
    public void testGraphQueryHas() {
        String agePropertyName = "age.property";
        getGraph().prepareVertex("v1", VISIBILITY_A, CONCEPT_TYPE_THING)
                .setProperty("text", stringValue("hello"), VISIBILITY_A)
                .setProperty(agePropertyName, intValue(25), VISIBILITY_EMPTY)
                .setProperty("birthDate", createDate(1989, 1, 5), VISIBILITY_A)
                .setProperty("lastAccessed", createDateTime(2014, 2, 24, 13, 0, 5), VISIBILITY_A)
                .save(AUTHORIZATIONS_A_AND_B);
        getGraph().prepareVertex("v2", VISIBILITY_A, CONCEPT_TYPE_THING)
                .setProperty("text", stringValue("world"), VISIBILITY_A)
                .setProperty(agePropertyName, intValue(30), VISIBILITY_A)
                .setProperty("birthDate", createDate(1984, 1, 5), VISIBILITY_A)
                .setProperty("lastAccessed", createDateTime(2014, 2, 25, 13, 0, 5), VISIBILITY_A)
                .save(AUTHORIZATIONS_A_AND_B);
        getGraph().flush();

        Iterable<Vertex> vertices = getGraph().query(exists(agePropertyName), AUTHORIZATIONS_A)
                .vertices();
        assertEquals(2, count(vertices));

        try {
            vertices = getGraph().query(boolQuery().andNot(exists(agePropertyName)), AUTHORIZATIONS_A)
                    .vertices();
            assertEquals(0, count(vertices));
        } catch (GeNotSupportedException ex) {
            LOGGER.warn("skipping. Not supported", ex);
        }

        vertices = getGraph().query(hasFilter(agePropertyName, Compare.EQUAL, intValue(25)), AUTHORIZATIONS_A)
                .vertices();
        assertEquals(1, count(vertices));

        vertices = getGraph().query(
                boolQuery()
                        .and(hasFilter(agePropertyName, Compare.EQUAL, intValue(25)))
                        .and(hasFilter("birthDate", Compare.EQUAL, createDate(1989, 1, 5))),
                AUTHORIZATIONS_A
        ).vertices();
        assertEquals(1, count(vertices));

        vertices = getGraph().query(
                boolQuery()
                        .and(search("hello"))
                        .and(hasFilter(agePropertyName, Compare.EQUAL, intValue(25)))
                        .and(hasFilter("birthDate", Compare.EQUAL, createDate(1989, 1, 5))),
                AUTHORIZATIONS_A
        ).vertices();
        assertEquals(1, count(vertices));

        vertices = getGraph().query(hasFilter("birthDate", Compare.EQUAL, createDate(1989, 1, 5)), AUTHORIZATIONS_A)
                .vertices();
        assertEquals(1, count(vertices));

        vertices = getGraph().query(hasFilter("lastAccessed", Compare.EQUAL, createDateTime(2014, 2, 24, 13, 0, 5)), AUTHORIZATIONS_A)
                .vertices();
        assertEquals(1, count(vertices));

        vertices = getGraph().query(hasFilter(agePropertyName, intValue(25)), AUTHORIZATIONS_A)
                .vertices();
        assertEquals(1, count(vertices));
        assertEquals(intValue(25), toList(vertices).get(0).getPropertyValue(agePropertyName));

        try {
            vertices = getGraph().query(boolQuery().andNot(hasFilter(agePropertyName, intValue(25))), AUTHORIZATIONS_A)
                    .vertices();
            assertEquals(1, count(vertices));
            assertEquals(intValue(30), toList(vertices).get(0).getPropertyValue(agePropertyName));
        } catch (GeNotSupportedException ex) {
            LOGGER.warn("skipping. Not supported", ex);
        }

        vertices = getGraph().query(hasFilter(agePropertyName, Compare.GREATER_THAN_EQUAL, intValue(25)), AUTHORIZATIONS_A)
                .vertices();
        assertEquals(2, count(vertices));

        vertices = getGraph().query(hasFilter(agePropertyName, Contains.IN, intArray(new int[]{25})), AUTHORIZATIONS_A)
                .vertices();
        assertEquals(1, count(vertices));
        assertEquals(intValue(25), toList(vertices).get(0).getPropertyValue(agePropertyName));

        try {
            vertices = getGraph().query(hasFilter(agePropertyName, Contains.NOT_IN, intArray(new int[]{25})), AUTHORIZATIONS_A)
                    .vertices();
            assertEquals(1, count(vertices));
            assertEquals(intValue(30), toList(vertices).get(0).getPropertyValue(agePropertyName));
        } catch (GeNotSupportedException ex) {
            LOGGER.warn("skipping. Not supported", ex);
        }

        vertices = getGraph().query(hasFilter(agePropertyName, Contains.IN, intArray(new int[]{25, 30})), AUTHORIZATIONS_A)
                .vertices();
        assertEquals(2, count(vertices));

        vertices = getGraph().query(hasFilter(agePropertyName, Compare.GREATER_THAN, intValue(25)), AUTHORIZATIONS_A)
                .vertices();
        assertEquals(1, count(vertices));

        vertices = getGraph().query(hasFilter(agePropertyName, Compare.LESS_THAN, intValue(26)), AUTHORIZATIONS_A)
                .vertices();
        assertEquals(1, count(vertices));

        vertices = getGraph().query(hasFilter(agePropertyName, Compare.LESS_THAN_EQUAL, intValue(25)), AUTHORIZATIONS_A)
                .vertices();
        assertEquals(1, count(vertices));

        vertices = getGraph().query(hasFilter(agePropertyName, Compare.NOT_EQUAL, intValue(25)), AUTHORIZATIONS_A)
                .vertices();
        assertEquals(1, count(vertices));

        vertices = getGraph().query(hasFilter(Element.ID_PROPERTY_NAME, Compare.NOT_EQUAL, stringValue("v1")), AUTHORIZATIONS_A)
                .vertices();
        assertElementIds(vertices, "v2");

        vertices = getGraph().query(hasFilter("lastAccessed", Compare.EQUAL, createDate(2014, 2, 24)), AUTHORIZATIONS_A)
                .vertices();
        assertEquals(1, count(vertices));

        vertices = getGraph().query(
                boolQuery()
                        .and(searchAll())
                        .and(hasFilter(agePropertyName, Contains.IN, intArray(new int[]{25, 30}))),
                AUTHORIZATIONS_A
        ).vertices();
        assertEquals(2, count(vertices));
    }

    @Test
    public void testStartsWithQuery() {
        getGraph().defineProperty("location").dataType(GeoPointValue.class).textIndexHint(TextIndexHint.ALL).define();
        getGraph().defineProperty("text").dataType(TextValue.class).textIndexHint(TextIndexHint.ALL).define();
        getGraph().prepareVertex("v1", VISIBILITY_A, CONCEPT_TYPE_THING)
                .setProperty("text", stringValue("hello world"), VISIBILITY_A)
                .save(AUTHORIZATIONS_A_AND_B);
        getGraph().prepareVertex("v2", VISIBILITY_A, CONCEPT_TYPE_THING)
                .setProperty("text", stringValue("junit says hello"), VISIBILITY_A)
                .save(AUTHORIZATIONS_A_AND_B);
        getGraph().flush();

        QueryResultsIterable<Vertex> vertices = getGraph().query(hasFilter("text", Compare.STARTS_WITH, stringValue("hel")), AUTHORIZATIONS_A)
                .vertices();
        assertResultsCount(1, vertices);
        assertVertexIdsAnyOrder(vertices, "v1");

        vertices = getGraph().query(hasFilter("text", Compare.STARTS_WITH, stringValue("foo")), AUTHORIZATIONS_A)
                .vertices();
        assertResultsCount(0, vertices);
    }

    @Test
    public void testGraphQueryMultiPropertyHas() {
        getGraph().defineProperty("unusedFloatProp").dataType(FloatValue.class).define();
        getGraph().defineProperty("unusedDateProp").dataType(DateValue.class).define();
        getGraph().defineProperty("unusedStringProp").dataType(TextValue.class).textIndexHint(TextIndexHint.ALL).define();

        String agePropertyName = "age.property";
        getGraph().prepareVertex("v1", VISIBILITY_A, CONCEPT_TYPE_THING)
                .setProperty("text", stringValue("hello"), VISIBILITY_A)
                .setProperty("text2", stringValue("foo"), VISIBILITY_A)
                .setProperty("text3", stringValue("bar"), VISIBILITY_A)
                .setProperty(agePropertyName, intValue(25), VISIBILITY_A)
                .setProperty("birthDate", createDate(1989, 1, 5), VISIBILITY_A)
                .setProperty("lastAccessed", createDateTime(2014, 2, 24, 13, 0, 5), VISIBILITY_A)
                .setProperty("location", geoPointValue(38.9544, -77.3464), VISIBILITY_A)
                .addExtendedData("table1", "row1", "column1", stringValue("value1"), VISIBILITY_A)
                .save(AUTHORIZATIONS_A_AND_B);
        getGraph().prepareVertex("v2", VISIBILITY_A, CONCEPT_TYPE_THING)
                .setProperty("text", stringValue("world"), VISIBILITY_A)
                .setProperty("text2", stringValue("foo"), VISIBILITY_A)
                .setProperty(agePropertyName, intValue(30), VISIBILITY_A)
                .setProperty("birthDate", createDate(1984, 1, 5), VISIBILITY_A)
                .setProperty("lastAccessed", createDateTime(2014, 2, 25, 13, 0, 5), VISIBILITY_A)
                .setProperty("location", geoPointValue(38.9186, -77.2297), VISIBILITY_A)
                .save(AUTHORIZATIONS_A_AND_B);
        getGraph().flush();

        QueryResultsIterable<Vertex> vertices = getGraph().query(exists(getGraph(), TextValue.class), AUTHORIZATIONS_A)
                .vertices();
        assertResultsCount(2, vertices);
        assertVertexIdsAnyOrder(vertices, "v1", "v2");

        vertices = getGraph().query(boolQuery().andNot(exists(getGraph(), TextValue.class)), AUTHORIZATIONS_A)
                .vertices();
        assertResultsCount(0, vertices);

        try {
            getGraph().query(exists(getGraph(), DoubleValue.class), AUTHORIZATIONS_A)
                    .vertices();
            fail("Should not allow searching for a dataType that there are no mappings for");
        } catch (GeException ex) {
            // expected
        }

        vertices = getGraph().query(exists(getGraph(), FloatValue.class), AUTHORIZATIONS_A)
                .vertices();
        assertResultsCount(0, vertices);

        vertices = getGraph().query(boolQuery().andNot(exists(getGraph(), FloatValue.class)), AUTHORIZATIONS_A)
                .vertices();
        assertResultsCount(2, vertices);
        assertVertexIdsAnyOrder(vertices, "v1", "v2");

        vertices = getGraph().query(
                boolQuery()
                        .or(exists("text3"))
                        .or(exists("unusedStringProp")),
                AUTHORIZATIONS_A
        ).vertices();
        assertResultsCount(1, vertices);
        assertVertexIds(vertices, "v1");

        vertices = getGraph().query(boolQuery().andNot(exists("text3")).andNot(exists("unusedStringProp")), AUTHORIZATIONS_A)
                .vertices();
        assertResultsCount(1, vertices);
        assertVertexIds(vertices, "v2");

        vertices = getGraph().query(hasFilter(getGraph(), TextValue.class, Compare.EQUAL, stringValue("hello")), AUTHORIZATIONS_A)
                .vertices();
        assertResultsCount(1, vertices);
        assertVertexIds(vertices, "v1");

        vertices = getGraph().query(hasFilter(getGraph(), TextValue.class, Compare.EQUAL, stringValue("foo")), AUTHORIZATIONS_A)
                .vertices();
        assertResultsCount(2, vertices);
        assertVertexIdsAnyOrder(vertices, "v1", "v2");

        vertices = getGraph().query(
                boolQuery()
                        .or(hasFilter("text", stringValue("hello")))
                        .or(hasFilter("text2", stringValue("hello"))),
                AUTHORIZATIONS_A
        ).vertices();
        assertResultsCount(1, vertices);
        assertVertexIds(vertices, "v1");

        vertices = getGraph().query(
                        boolQuery()
                                .or(hasFilter("text", Compare.EQUAL, stringValue("foo")))
                                .or(hasFilter("text2", Compare.EQUAL, stringValue("foo"))),
                        AUTHORIZATIONS_A
                )
                .vertices();
        assertResultsCount(2, vertices);
        assertVertexIdsAnyOrder(vertices, "v1", "v2");

        vertices = getGraph().query(hasFilter("text", Compare.EQUAL, stringValue("foo")), AUTHORIZATIONS_A)
                .vertices();
        assertResultsCount(0, vertices);

        vertices = getGraph().query(hasFilter(getGraph(), NumberValue.class, Compare.EQUAL, intValue(25)), AUTHORIZATIONS_A)
                .vertices();
        assertResultsCount(1, vertices);
        assertVertexIds(vertices, "v1");

        vertices = getGraph().query(hasFilter(getGraph(), DateTimeValue.class, Compare.GREATER_THAN, createDateTime(2014, 2, 25, 0, 0, 0)), AUTHORIZATIONS_A)
                .vertices();
        assertResultsCount(1, vertices);
        assertVertexIds(vertices, "v2");

        vertices = getGraph().query(hasFilter(getGraph(), DateTimeValue.class, Compare.LESS_THAN, createDateTime(2014, 2, 25, 0, 0, 0)), AUTHORIZATIONS_A)
                .vertices();
        assertResultsCount(1, vertices);
        assertVertexIdsAnyOrder(vertices, "v1");

        vertices = getGraph().query(hasFilter(getGraph(), DateValue.class, Compare.LESS_THAN, createDate(1985, 1, 5)), AUTHORIZATIONS_A)
                .vertices();
        assertResultsCount(1, vertices);
        assertVertexIds(vertices, "v2");

        vertices = getGraph().query(hasFilter(getGraph(), DateValue.class, Compare.GREATER_THAN, createDate(2000, 1, 1)), AUTHORIZATIONS_A)
                .vertices();
        assertResultsCount(0, vertices);

        vertices = getGraph().query(hasFilter(getGraph(), GeoShapeValue.class, GeoCompare.WITHIN, geoCircleValue(38.9186, -77.2297, 1)), AUTHORIZATIONS_A)
                .vertices();
        assertResultsCount(1, vertices);
        assertVertexIds(vertices, "v2");

        vertices = getGraph().query(hasFilter(getGraph(), GeoPointValue.class, GeoCompare.WITHIN, geoCircleValue(38.9186, -77.2297, 1)), AUTHORIZATIONS_A)
                .vertices();
        assertResultsCount(1, vertices);
        assertVertexIds(vertices, "v2");

        vertices = getGraph().query(hasFilter(getGraph(), GeoPointValue.class, GeoCompare.WITHIN, geoCircleValue(38.9186, -77.2297, 25)), AUTHORIZATIONS_A)
                .vertices();
        assertResultsCount(2, vertices);
        assertVertexIdsAnyOrder(vertices, "v1", "v2");

        try {
            getGraph().query(hasFilter(getGraph(), DoubleValue.class, Compare.EQUAL, doubleValue(25)), AUTHORIZATIONS_A)
                    .vertices();
            fail("Should not allow searching for a dataType that there are no mappings for");
        } catch (GeException e) {
            // expected
        }

        // If given a property that is defined in the graph but never used, return no results
        vertices = getGraph().query(hasFilter(getGraph(), FloatValue.class, Compare.EQUAL, floatValue(25)), AUTHORIZATIONS_A)
                .vertices();
        assertResultsCount(0, vertices);
    }


    @Test
    public void testGraphQueryHasAuthorization() {
        getGraph().prepareVertex("v1", VISIBILITY_A, CONCEPT_TYPE_THING)
                .setProperty("text", stringValue("hello"), VISIBILITY_A)
                .save(AUTHORIZATIONS_ALL);
        getGraph().prepareVertex("v2", VISIBILITY_A, CONCEPT_TYPE_THING)
                .setProperty("text.with.dots", stringValue("world"), VISIBILITY_B)
                .save(AUTHORIZATIONS_ALL);
        getGraph().prepareVertex("v3", VISIBILITY_C, CONCEPT_TYPE_THING)
                .setProperty("text", stringValue("world"), VISIBILITY_A)
                .save(AUTHORIZATIONS_ALL);
        getGraph().flush();

        QueryResultsIterable<Vertex> vertices = getGraph().query(hasAuthorization(VISIBILITY_A_STRING), AUTHORIZATIONS_ALL)
                .vertices();
        assertResultsCount(3, 3, vertices);
        assertVertexIdsAnyOrder(vertices, "v1", "v2", "v3");

        vertices = getGraph().query(hasAuthorization(VISIBILITY_B_STRING), AUTHORIZATIONS_ALL)
                .vertices();
        assertResultsCount(1, 1, vertices);
        assertVertexIdsAnyOrder(vertices, "v2");

        vertices = getGraph().query(hasAuthorization(VISIBILITY_C_STRING), AUTHORIZATIONS_ALL)
                .vertices();
        assertResultsCount(1, 1, vertices);
        assertVertexIdsAnyOrder(vertices, "v3");
    }

    @Test
    public void testGraphQueryHasAuthorizationWithHidden() {
        Vertex v1 = getGraph().addVertex("v1", Visibility.EMPTY, AUTHORIZATIONS_A, CONCEPT_TYPE_THING);
        Vertex v2 = getGraph().addVertex("v2", Visibility.EMPTY, AUTHORIZATIONS_A, CONCEPT_TYPE_THING);
        Vertex v3 = getGraph().prepareVertex("v3", VISIBILITY_EMPTY, CONCEPT_TYPE_THING)
                .addPropertyValue("junit", "name", stringValue("value"), VISIBILITY_B)
                .save(AUTHORIZATIONS_B_AND_C);
        Edge e1 = getGraph().addEdge("e1", v1.getId(), v2.getId(), "junit edge", Visibility.EMPTY, AUTHORIZATIONS_A);
        getGraph().flush();

        getGraph().markEdgeHidden(e1, VISIBILITY_A, AUTHORIZATIONS_A);
        getGraph().markVertexHidden(v1, VISIBILITY_A, AUTHORIZATIONS_A);
        v3.markPropertyHidden("junit", "name", VISIBILITY_B, VISIBILITY_C, AUTHORIZATIONS_B_AND_C);
        getGraph().flush();

        QueryResultsIterable<Vertex> vertices = getGraph().query(hasAuthorization(VISIBILITY_A_STRING), AUTHORIZATIONS_A).vertices(FetchHints.ALL);
        assertResultsCount(0, vertices);

        QueryResultsIterable<String> vertexIds = getGraph().query(hasAuthorization(VISIBILITY_A_STRING), AUTHORIZATIONS_A).vertexIds(IdFetchHint.NONE);
        assertResultsCount(0, 0, vertexIds);

        vertexIds = getGraph().query(hasAuthorization(VISIBILITY_A_STRING), AUTHORIZATIONS_A).vertexIds();
        assertResultsCount(0, 0, vertexIds);

        vertices = getGraph().query(hasAuthorization(VISIBILITY_A_STRING), AUTHORIZATIONS_A).vertices(FetchHints.ALL_INCLUDING_HIDDEN);
        assertResultsCount(1, vertices);
        assertVertexIdsAnyOrder(vertices, v1.getId());

        vertices = getGraph().query(hasAuthorization(VISIBILITY_C_STRING), AUTHORIZATIONS_B_AND_C).vertices(FetchHints.ALL_INCLUDING_HIDDEN);
        assertResultsCount(1, vertices);
        assertVertexIdsAnyOrder(vertices, v3.getId());

        vertexIds = getGraph().query(hasAuthorization(VISIBILITY_A_STRING), AUTHORIZATIONS_A).vertexIds(IdFetchHint.ALL_INCLUDING_HIDDEN);
        assertResultsCount(1, 1, vertexIds);
        assertIdsAnyOrder(vertexIds, v1.getId());

        vertexIds = getGraph().query(hasAuthorization(VISIBILITY_C_STRING), AUTHORIZATIONS_B_AND_C).vertexIds(IdFetchHint.ALL_INCLUDING_HIDDEN);
        assertResultsCount(1, 1, vertexIds);
        assertIdsAnyOrder(vertexIds, v3.getId());

        QueryResultsIterable<Edge> edges = getGraph().query(hasAuthorization(VISIBILITY_A_STRING), AUTHORIZATIONS_A).edges(FetchHints.ALL);
        assertResultsCount(0, edges);

        QueryResultsIterable<String> edgeIds = getGraph().query(hasAuthorization(VISIBILITY_A_STRING), AUTHORIZATIONS_A).edgeIds(IdFetchHint.NONE);
        assertResultsCount(0, 0, edgeIds);

        edgeIds = getGraph().query(hasAuthorization(VISIBILITY_A_STRING), AUTHORIZATIONS_A).edgeIds();
        assertResultsCount(0, 0, edgeIds);

        edges = getGraph().query(hasAuthorization(VISIBILITY_A_STRING), AUTHORIZATIONS_A).edges(FetchHints.ALL_INCLUDING_HIDDEN);
        assertResultsCount(1, edges);
        assertEdgeIdsAnyOrder(edges, e1.getId());

        edgeIds = getGraph().query(hasAuthorization(VISIBILITY_A_STRING), AUTHORIZATIONS_A).edgeIds(IdFetchHint.ALL_INCLUDING_HIDDEN);
        assertResultsCount(1, 1, edgeIds);
        assertIdsAnyOrder(edgeIds, e1.getId());

        QueryResultsIterable<Element> elements = getGraph().query(hasAuthorization(VISIBILITY_A_STRING), AUTHORIZATIONS_A).elements(FetchHints.ALL);
        assertResultsCount(0, elements);

        QueryResultsIterable<String> elementIds = getGraph().query(hasAuthorization(VISIBILITY_A_STRING), AUTHORIZATIONS_A).elementIds(IdFetchHint.NONE);
        assertResultsCount(0, 0, elementIds);

        elementIds = getGraph().query(hasAuthorization(VISIBILITY_A_STRING), AUTHORIZATIONS_A).elementIds();
        assertResultsCount(0, 0, elementIds);

        elements = getGraph().query(hasAuthorization(VISIBILITY_A_STRING), AUTHORIZATIONS_A).elements(FetchHints.ALL_INCLUDING_HIDDEN);
        assertResultsCount(2, elements);
        assertElementIdsAnyOrder(elements, v1.getId(), e1.getId());

        elementIds = getGraph().query(hasAuthorization(VISIBILITY_A_STRING), AUTHORIZATIONS_A).elementIds(IdFetchHint.ALL_INCLUDING_HIDDEN);
        assertResultsCount(2, 2, elementIds);
        assertIdsAnyOrder(elementIds, v1.getId(), e1.getId());
    }

    @Test
    public void testGraphQueryContainsNotIn() {
        getGraph().prepareVertex("v1", VISIBILITY_A, CONCEPT_TYPE_THING)
                .setProperty("status", stringValue("0"), VISIBILITY_A)
                .setProperty("name", stringValue("susan"), VISIBILITY_A)
                .save(AUTHORIZATIONS_A_AND_B);

        getGraph().prepareVertex("v2", VISIBILITY_A, CONCEPT_TYPE_THING)
                .setProperty("status", stringValue("1"), VISIBILITY_A)
                .setProperty("name", stringValue("joe"), VISIBILITY_A)
                .save(AUTHORIZATIONS_A_AND_B);

        getGraph().prepareVertex("v3", VISIBILITY_A, CONCEPT_TYPE_THING)
                .save(AUTHORIZATIONS_A_AND_B);

        getGraph().prepareVertex("v4", VISIBILITY_A, CONCEPT_TYPE_THING)
                .save(AUTHORIZATIONS_A_AND_B);

        getGraph().prepareVertex("v5", VISIBILITY_A, CONCEPT_TYPE_THING)
                .save(AUTHORIZATIONS_A_AND_B);

        getGraph().prepareVertex("v6", VISIBILITY_A, CONCEPT_TYPE_THING)
                .setProperty("status", stringValue("0"), VISIBILITY_A)
                .save(AUTHORIZATIONS_A_AND_B);
        getGraph().flush();

        try {
            Iterable<Vertex> vertices = getGraph().query(hasFilter("status", Contains.NOT_IN, stringArray("0")), AUTHORIZATIONS_A)
                    .vertices();
            assertEquals(4, count(vertices));

            vertices = getGraph().query(hasFilter("status", Contains.NOT_IN, stringArray("0", "1")), AUTHORIZATIONS_A)
                    .vertices();
            assertEquals(3, count(vertices));
        } catch (GeNotSupportedException ex) {
            LOGGER.warn("skipping. Not supported", ex);
        }
    }

    @Test
    public void testGraphQueryHasGeoPointAndExact() {
        getGraph().defineProperty("location").dataType(GeoPointValue.class).define();
        getGraph().defineProperty("exact").dataType(TextValue.class).textIndexHint(TextIndexHint.EXACT_MATCH).define();

        getGraph().prepareVertex("v1", VISIBILITY_A, CONCEPT_TYPE_THING)
                .setProperty("prop1", stringValue("val1"), VISIBILITY_A)
                .setProperty("exact", stringValue("val1"), VISIBILITY_A)
                .setProperty("location", geoPointValue(38.9186, -77.2297), VISIBILITY_A)
                .save(AUTHORIZATIONS_A_AND_B);
        getGraph().prepareVertex("v2", VISIBILITY_A, CONCEPT_TYPE_THING)
                .setProperty("prop2", stringValue("val2"), VISIBILITY_A)
                .save(AUTHORIZATIONS_A_AND_B);
        getGraph().flush();

        Iterable<Element> results = getGraph().query(
                boolQuery()
                        .and(searchAll())
                        .and(exists("prop1")),
                AUTHORIZATIONS_A_AND_B
        ).elements();
        assertEquals(1, count(results));
        assertEquals(1, ((IterableWithTotalHits) results).getTotalHits());
        assertEquals("v1", results.iterator().next().getId());

        results = getGraph().query(
                boolQuery()
                        .and(searchAll())
                        .and(exists("exact")),
                AUTHORIZATIONS_A_AND_B
        ).elements();
        assertEquals(1, count(results));
        assertEquals(1, ((IterableWithTotalHits) results).getTotalHits());
        assertEquals("v1", results.iterator().next().getId());

        results = getGraph().query(
                boolQuery()
                        .and(searchAll())
                        .and(exists("location")),
                AUTHORIZATIONS_A_AND_B
        ).elements();
        assertEquals(1, count(results));
        assertEquals(1, ((IterableWithTotalHits) results).getTotalHits());
        assertEquals("v1", results.iterator().next().getId());
    }

    @Test
    public void testGraphQueryHasNotGeoPointAndExact() {
        getGraph().defineProperty("location").dataType(GeoPointValue.class).define();
        getGraph().defineProperty("exact").dataType(TextValue.class).textIndexHint(TextIndexHint.EXACT_MATCH).define();

        getGraph().prepareVertex("v1", VISIBILITY_A, CONCEPT_TYPE_THING)
                .setProperty("prop1", stringValue("val1"), VISIBILITY_A)
                .setProperty("exact", stringValue("val2"), VISIBILITY_A)
                .setProperty("location", geoPointValue(38.9186, -77.2297), VISIBILITY_A)
                .save(AUTHORIZATIONS_A_AND_B);
        getGraph().prepareVertex("v2", VISIBILITY_A, CONCEPT_TYPE_THING)
                .setProperty("prop2", stringValue("val2"), VISIBILITY_A)
                .save(AUTHORIZATIONS_A_AND_B);
        getGraph().flush();

        Iterable<Element> results = getGraph().query(
                boolQuery()
                        .and(searchAll())
                        .andNot(exists("prop1"))
                , AUTHORIZATIONS_A_AND_B
        ).elements();
        assertEquals(1, count(results));
        assertEquals(1, ((IterableWithTotalHits) results).getTotalHits());
        assertEquals("v2", results.iterator().next().getId());

        results = getGraph().query(
                boolQuery()
                        .and(searchAll())
                        .andNot(exists("prop3"))
                        .sort(Element.ID_PROPERTY_NAME, SortDirection.ASCENDING),
                AUTHORIZATIONS_A_AND_B
        ).elements();
        assertEquals(2, count(results));
        Iterator<Element> iterator = results.iterator();
        assertEquals("v1", iterator.next().getId());
        assertEquals("v2", iterator.next().getId());

        results = getGraph().query(
                boolQuery()
                        .and(searchAll())
                        .andNot(exists("exact")),
                AUTHORIZATIONS_A_AND_B
        ).elements();
        assertEquals(1, count(results));
        assertEquals(1, ((IterableWithTotalHits) results).getTotalHits());
        assertEquals("v2", results.iterator().next().getId());

        results = getGraph().query(
                boolQuery()
                        .and(searchAll())
                        .andNot(exists("location")),
                AUTHORIZATIONS_A_AND_B
        ).elements();
        assertEquals(1, count(results));
        assertEquals(1, ((IterableWithTotalHits) results).getTotalHits());
        assertEquals("v2", results.iterator().next().getId());
    }

    @Test
    public void testGraphQueryHasTwoVisibilities() {
        String agePropertyName = "age.property";
        getGraph().prepareVertex("v1", VISIBILITY_A, CONCEPT_TYPE_THING)
                .setProperty("name", stringValue("v1"), VISIBILITY_A)
                .setProperty(agePropertyName, intValue(25), VISIBILITY_A)
                .save(AUTHORIZATIONS_A_AND_B);
        getGraph().prepareVertex("v2", VISIBILITY_A, CONCEPT_TYPE_THING)
                .setProperty("name", stringValue("v2"), VISIBILITY_A)
                .addPropertyValue("k1", agePropertyName, intValue(30), VISIBILITY_A)
                .addPropertyValue("k2", agePropertyName, intValue(35), VISIBILITY_B)
                .save(AUTHORIZATIONS_A_AND_B);
        getGraph().prepareVertex("v3", VISIBILITY_A, CONCEPT_TYPE_THING)
                .setProperty("name", stringValue("v3"), VISIBILITY_A)
                .save(AUTHORIZATIONS_A_AND_B);
        getGraph().flush();

        Iterable<Vertex> vertices = getGraph().query(exists(agePropertyName), AUTHORIZATIONS_A_AND_B)
                .vertices();
        assertEquals(2, count(vertices));

        vertices = getGraph().query(boolQuery().andNot(exists(agePropertyName)), AUTHORIZATIONS_A_AND_B)
                .vertices();
        assertEquals(1, count(vertices));
    }

    @Test
    public void testGraphQueryIn() {
        String namePropertyName = "full.name";
        getGraph().defineProperty("age").dataType(IntValue.class).sortable(true).define();
        getGraph().defineProperty(namePropertyName).dataType(TextValue.class).sortable(true).textIndexHint(TextIndexHint.ALL).define();

        getGraph().prepareVertex("v1", VISIBILITY_A, CONCEPT_TYPE_THING)
                .setProperty(namePropertyName, stringValue("joe ferner"), VISIBILITY_A)
                .save(AUTHORIZATIONS_A_AND_B);
        getGraph().prepareVertex("v2", VISIBILITY_A, CONCEPT_TYPE_THING)
                .setProperty(namePropertyName, stringValue("bob smith"), VISIBILITY_B)
                .setProperty("age", intValue(25), VISIBILITY_A)
                .save(AUTHORIZATIONS_A_AND_B);
        getGraph().prepareVertex("v3", VISIBILITY_A, CONCEPT_TYPE_THING)
                .setProperty(namePropertyName, stringValue("tom thumb"), VISIBILITY_A)
                .setProperty("age", intValue(30), VISIBILITY_B)
                .save(AUTHORIZATIONS_A_AND_B);
        getGraph().flush();

        Iterable<Vertex> results = getGraph().query(
                hasFilter(namePropertyName, Contains.IN, stringArray("joe ferner", "tom thumb")),
                AUTHORIZATIONS_A_AND_B
        ).vertices();
        assertEquals(2, ((IterableWithTotalHits) results).getTotalHits());
        assertVertexIdsAnyOrder(results, "v1", "v3");
    }

    @Test
    public void testGraphQueryHidden() {
        Vertex v1 = getGraph().addVertex("v1", VISIBILITY_A, AUTHORIZATIONS_A, CONCEPT_TYPE_THING);
        Vertex v2 = getGraph().addVertex("v2", VISIBILITY_A, AUTHORIZATIONS_A, CONCEPT_TYPE_THING);
        Vertex v3 = getGraph().addVertex("v3", VISIBILITY_A, AUTHORIZATIONS_A, CONCEPT_TYPE_THING);
        Edge e1 = getGraph().addEdge("e1", "v1", "v2", "junit edge", VISIBILITY_A, AUTHORIZATIONS_A);
        Edge e2 = getGraph().addEdge("e2", "v2", "v3", "junit edge", VISIBILITY_A, AUTHORIZATIONS_A);
        getGraph().flush();

        getGraph().markEdgeHidden(e1, VISIBILITY_A, AUTHORIZATIONS_A);
        getGraph().markVertexHidden(v1, VISIBILITY_A, AUTHORIZATIONS_A);
        getGraph().flush();

        QueryResultsIterable<Vertex> vertices = getGraph().query(AUTHORIZATIONS_A).vertices(FetchHints.ALL);
        assertResultsCount(2, vertices);
        assertVertexIdsAnyOrder(vertices, v2.getId(), v3.getId());

        QueryResultsIterable<String> vertexIds = getGraph().query(AUTHORIZATIONS_A).vertexIds(IdFetchHint.NONE);
        assertResultsCount(2, 2, vertexIds);
        assertIdsAnyOrder(vertexIds, v2.getId(), v3.getId());

        vertexIds = getGraph().query(AUTHORIZATIONS_A).vertexIds();
        assertResultsCount(2, 2, vertexIds);
        assertIdsAnyOrder(vertexIds, v2.getId(), v3.getId());

        vertices = getGraph().query(AUTHORIZATIONS_A).vertices(FetchHints.ALL_INCLUDING_HIDDEN);
        assertResultsCount(3, vertices);
        assertVertexIdsAnyOrder(vertices, v1.getId(), v2.getId(), v3.getId());

        vertexIds = getGraph().query(AUTHORIZATIONS_A).vertexIds(IdFetchHint.ALL_INCLUDING_HIDDEN);
        assertResultsCount(3, 3, vertexIds);
        assertIdsAnyOrder(vertexIds, v1.getId(), v2.getId(), v3.getId());

        QueryResultsIterable<Edge> edges = getGraph().query(AUTHORIZATIONS_A).edges(FetchHints.ALL);
        assertResultsCount(1, edges);
        assertEdgeIdsAnyOrder(edges, e2.getId());

        QueryResultsIterable<String> edgeIds = getGraph().query(AUTHORIZATIONS_A).edgeIds(IdFetchHint.NONE);
        assertResultsCount(1, 1, edgeIds);
        assertIdsAnyOrder(edgeIds, e2.getId());

        edgeIds = getGraph().query(AUTHORIZATIONS_A).edgeIds();
        assertResultsCount(1, 1, edgeIds);
        assertIdsAnyOrder(edgeIds, e2.getId());

        edges = getGraph().query(AUTHORIZATIONS_A).edges(FetchHints.ALL_INCLUDING_HIDDEN);
        assertResultsCount(2, edges);
        assertEdgeIdsAnyOrder(edges, e1.getId(), e2.getId());

        edgeIds = getGraph().query(AUTHORIZATIONS_A).edgeIds(IdFetchHint.ALL_INCLUDING_HIDDEN);
        assertResultsCount(2, 2, edgeIds);
        assertIdsAnyOrder(edgeIds, e1.getId(), e2.getId());

        QueryResultsIterable<Element> elements = getGraph().query(AUTHORIZATIONS_A).elements(FetchHints.ALL);
        assertResultsCount(3, elements);
        assertElementIdsAnyOrder(elements, v2.getId(), v3.getId(), e2.getId());

        QueryResultsIterable<String> elementIds = getGraph().query(AUTHORIZATIONS_A).elementIds(IdFetchHint.NONE);
        assertResultsCount(3, 3, elementIds);
        assertIdsAnyOrder(elementIds, v2.getId(), v3.getId(), e2.getId());

        elementIds = getGraph().query(AUTHORIZATIONS_A).elementIds();
        assertResultsCount(3, 3, elementIds);
        assertIdsAnyOrder(elementIds, v2.getId(), v3.getId(), e2.getId());

        elements = getGraph().query(AUTHORIZATIONS_A).elements(FetchHints.ALL_INCLUDING_HIDDEN);
        assertResultsCount(5, elements);
        assertElementIdsAnyOrder(elements, v1.getId(), v2.getId(), v3.getId(), e1.getId(), e2.getId());

        elementIds = getGraph().query(AUTHORIZATIONS_A).elementIds(IdFetchHint.ALL_INCLUDING_HIDDEN);
        assertResultsCount(5, 5, elementIds);
        assertIdsAnyOrder(elementIds, v1.getId(), v2.getId(), v3.getId(), e1.getId(), e2.getId());
    }

    @Test
    public void testGraphQueryVertexWithVisibilityChange() {
        Vertex v1 = getGraph().prepareVertex("v1", VISIBILITY_A, CONCEPT_TYPE_THING)
                .save(AUTHORIZATIONS_A);
        getGraph().flush();

        QueryResultsIterable<Vertex> vertices = getGraph().query(AUTHORIZATIONS_A).vertices();
        assertResultsCount(1, vertices);
        assertVertexIds(vertices, v1.getId());

        // change to same visibility
        v1 = getGraph().getVertex("v1", AUTHORIZATIONS_A);
        v1 = v1.prepareMutation()
                .alterElementVisibility(VISIBILITY_EMPTY)
                .save(AUTHORIZATIONS_A);
        getGraph().flush();
        assertEquals(VISIBILITY_EMPTY, v1.getVisibility());

        vertices = getGraph().query(AUTHORIZATIONS_A).vertices();
        assertResultsCount(1, vertices);
        assertVertexIds(vertices, v1.getId());

        // change to new visibility
        v1 = getGraph().getVertex("v1", AUTHORIZATIONS_A);
        v1.prepareMutation()
                .alterElementVisibility(VISIBILITY_B)
                .save(AUTHORIZATIONS_A_AND_B);
        getGraph().flush();

        vertices = getGraph().query(AUTHORIZATIONS_A).vertices();
        assertResultsCount(0, vertices);
        assertEquals(0, count(vertices));
    }

    @Test
    public void testGraphQueryVertexHasWithSecurityCantSeeVertex() {
        getGraph().prepareVertex("v1", VISIBILITY_B, CONCEPT_TYPE_THING)
                .setProperty("age", intValue(25), VISIBILITY_A)
                .save(AUTHORIZATIONS_A_AND_B);
        getGraph().flush();

        Iterable<Vertex> vertices = getGraph().query(hasFilter("age", Compare.EQUAL, intValue(25)), AUTHORIZATIONS_A)
                .vertices();
        assertEquals(0, count(vertices));

        vertices = getGraph().query(hasFilter("age", Compare.EQUAL, intValue(25)), AUTHORIZATIONS_A_AND_B)
                .vertices();
        assertEquals(1, count(vertices));
    }

    @Test
    public void testGraphQueryVertexHasWithSecurityCantSeeProperty() {
        getGraph().prepareVertex("v1", VISIBILITY_A, CONCEPT_TYPE_THING)
                .setProperty("age", intValue(25), VISIBILITY_B)
                .save(AUTHORIZATIONS_A_AND_B);

        Iterable<Vertex> vertices = getGraph().query(hasFilter("age", Compare.EQUAL, intValue(25)), AUTHORIZATIONS_A)
                .vertices();
        assertEquals(0, count(vertices));
    }

    @Test
    public void testGraphQueryEdgeHasWithSecurity() {
        Vertex v1 = getGraph().prepareVertex("v1", VISIBILITY_A, CONCEPT_TYPE_THING).save(AUTHORIZATIONS_A_AND_B);
        Vertex v2 = getGraph().prepareVertex("v2", VISIBILITY_A, CONCEPT_TYPE_THING).save(AUTHORIZATIONS_A_AND_B);
        Vertex v3 = getGraph().prepareVertex("v3", VISIBILITY_A, CONCEPT_TYPE_THING).save(AUTHORIZATIONS_A_AND_B);

        getGraph().prepareEdge("e1", v1, v2, LABEL_LABEL1, VISIBILITY_A)
                .setProperty("age", intValue(25), VISIBILITY_A)
                .save(AUTHORIZATIONS_A_AND_B);
        getGraph().prepareEdge("e2", v1, v3, LABEL_LABEL1, VISIBILITY_A)
                .setProperty("age", intValue(25), VISIBILITY_B)
                .save(AUTHORIZATIONS_A_AND_B);
        getGraph().flush();

        Iterable<Edge> edges = getGraph().query(hasFilter("age", Compare.EQUAL, intValue(25)), AUTHORIZATIONS_A)
                .edges();
        assertEquals(1, count(edges));
    }

    @Test
    public void testGraphQueryUpdateVertex() {
        getGraph().prepareVertex("v1", VISIBILITY_EMPTY, CONCEPT_TYPE_THING)
                .setProperty("age", intValue(25), VISIBILITY_EMPTY)
                .save(AUTHORIZATIONS_A_AND_B);
        Vertex v2 = getGraph().prepareVertex("v2", VISIBILITY_EMPTY, CONCEPT_TYPE_THING)
                .save(AUTHORIZATIONS_A_AND_B);
        Vertex v3 = getGraph().prepareVertex("v3", VISIBILITY_EMPTY, CONCEPT_TYPE_THING)
                .save(AUTHORIZATIONS_A_AND_B);
        getGraph().addEdge("v2tov3", v2, v3, LABEL_LABEL1, VISIBILITY_EMPTY, AUTHORIZATIONS_A_AND_B);
        getGraph().flush();

        getGraph().prepareVertex("v1", VISIBILITY_EMPTY, CONCEPT_TYPE_THING)
                .setProperty("name", stringValue("Joe"), VISIBILITY_EMPTY)
                .save(AUTHORIZATIONS_A_AND_B);
        getGraph().prepareVertex("v2", VISIBILITY_EMPTY, CONCEPT_TYPE_THING)
                .setProperty("name", stringValue("Bob"), VISIBILITY_EMPTY)
                .save(AUTHORIZATIONS_A_AND_B);
        getGraph().prepareVertex("v3", VISIBILITY_EMPTY, CONCEPT_TYPE_THING)
                .setProperty("name", stringValue("Same"), VISIBILITY_EMPTY)
                .save(AUTHORIZATIONS_A_AND_B);
        getGraph().flush();

        List<Vertex> allVertices = toList(getGraph().query(AUTHORIZATIONS_A_AND_B).vertices());
        assertEquals(3, count(allVertices));

        Iterable<Vertex> vertices = getGraph().query(hasFilter("age", Compare.EQUAL, intValue(25)), AUTHORIZATIONS_A_AND_B)
                .vertices();
        assertEquals(1, count(vertices));

        vertices = getGraph().query(hasFilter("name", Compare.EQUAL, stringValue("Joe")), AUTHORIZATIONS_A_AND_B)
                .vertices();
        assertEquals(1, count(vertices));

        vertices = getGraph().query(
                boolQuery()
                        .and(hasFilter("age", Compare.EQUAL, intValue(25)))
                        .and(hasFilter("name", Compare.EQUAL, stringValue("Joe"))),
                AUTHORIZATIONS_A_AND_B
        ).vertices();
        assertEquals(1, count(vertices));
    }

    @Test
    public void testQueryWithVertexIds() {
        getGraph().prepareVertex("v1", VISIBILITY_A, CONCEPT_TYPE_THING)
                .setProperty("age", intValue(25), VISIBILITY_A)
                .save(AUTHORIZATIONS_A);
        getGraph().prepareVertex("v2", VISIBILITY_A, CONCEPT_TYPE_THING)
                .setProperty("age", intValue(30), VISIBILITY_A)
                .save(AUTHORIZATIONS_A);
        getGraph().prepareVertex("v3", VISIBILITY_A, CONCEPT_TYPE_THING)
                .setProperty("age", intValue(35), VISIBILITY_A)
                .save(AUTHORIZATIONS_A);
        getGraph().flush();

        List<Vertex> vertices = toList(getGraph().query(
                boolQuery()
                        .and(hasIds("v1", "v2"))
                        .and(hasFilter("age", Compare.GREATER_THAN, intValue(27))),
                AUTHORIZATIONS_A
        ).vertices());
        assertEquals(1, vertices.size());
        assertEquals("v2", vertices.get(0).getId());

        vertices = toList(getGraph().query(
                boolQuery()
                        .and(hasIds("v1", "v2", "v3"))
                        .and(hasFilter("age", Compare.GREATER_THAN, intValue(27))),
                AUTHORIZATIONS_A
        ).vertices());
        assertEquals(2, vertices.size());
        List<String> vertexIds = toList(new ConvertingIterable<Vertex, String>(vertices) {
            @Override
            protected String convert(Vertex o) {
                return o.getId();
            }
        });
        assertTrue("v2 not found", vertexIds.contains("v2"));
        assertTrue("v3 not found", vertexIds.contains("v3"));
    }

    @Test
    public void testDisableEdgeIndexing() {
        assumeTrue("disabling indexing not supported", disableEdgeIndexing(getGraph()));

        Vertex v1 = getGraph().prepareVertex("v1", VISIBILITY_A, CONCEPT_TYPE_THING).save(AUTHORIZATIONS_A_AND_B);
        Vertex v2 = getGraph().prepareVertex("v2", VISIBILITY_A, CONCEPT_TYPE_THING).save(AUTHORIZATIONS_A_AND_B);

        getGraph().prepareEdge("e1", v1, v2, LABEL_LABEL1, VISIBILITY_A)
                .setProperty("prop1", stringValue("value1"), VISIBILITY_A)
                .save(AUTHORIZATIONS_A_AND_B);
        getGraph().flush();

        Iterable<Edge> edges = getGraph().query(hasFilter("prop1", stringValue("value1")), AUTHORIZATIONS_A)
                .edges();
        assertEquals(0, count(edges));
    }

    @Test
    public void testGraphQueryHasWithSpaces() {
        getGraph().prepareVertex("v1", VISIBILITY_A, CONCEPT_TYPE_THING)
                .setProperty("name", stringValue("Joe Ferner"), VISIBILITY_A)
                .setProperty("propWithNonAlphaCharacters", stringValue("hyphen-word, etc."), VISIBILITY_A)
                .save(AUTHORIZATIONS_A_AND_B);
        getGraph().prepareVertex("v2", VISIBILITY_A, CONCEPT_TYPE_THING)
                .setProperty("name", stringValue("Joe Smith"), VISIBILITY_A)
                .save(AUTHORIZATIONS_A_AND_B);
        getGraph().flush();

        QueryResultsIterable<Vertex> vertices = getGraph().query("Ferner", AUTHORIZATIONS_A)
                .vertices();
        assertEquals(1, vertices.getTotalHits());
        assertEquals(1, count(vertices));

        vertices = getGraph().query("joe", AUTHORIZATIONS_A)
                .vertices();
        assertEquals(2, vertices.getTotalHits());
        assertEquals(2, count(vertices));

        if (isLuceneQueriesSupported()) {
            vertices = getGraph().query("joe AND ferner", AUTHORIZATIONS_A)
                    .vertices();
            assertEquals(1, vertices.getTotalHits());
            assertEquals(1, count(vertices));
        }

        if (isLuceneQueriesSupported()) {
            vertices = getGraph().query("joe smith", AUTHORIZATIONS_A)
                    .vertices();
            List<Vertex> verticesList = toList(vertices);
            assertEquals(2, verticesList.size());
            boolean foundV1 = false;
            boolean foundV2 = false;
            for (Vertex v : verticesList) {
                if (v.getId().equals("v1")) {
                    foundV1 = true;
                } else if (v.getId().equals("v2")) {
                    foundV2 = true;
                } else {
                    throw new RuntimeException("Invalid vertex id: " + v.getId());
                }
            }
            assertTrue(foundV1);
            assertTrue(foundV2);
        }

        vertices = getGraph().query(hasFilter("name", TextPredicate.CONTAINS, stringValue("Ferner")), AUTHORIZATIONS_A)
                .vertices();
        assertEquals(1, vertices.getTotalHits());
        assertEquals(1, count(vertices));

        vertices = getGraph().query(
                boolQuery()
                        .and(hasFilter("name", TextPredicate.CONTAINS, stringValue("Joe")))
                        .and(hasFilter("name", TextPredicate.CONTAINS, stringValue("Ferner"))),
                AUTHORIZATIONS_A
        ).vertices();
        assertEquals(1, vertices.getTotalHits());
        assertEquals(1, count(vertices));

        vertices = getGraph().query(hasFilter("name", TextPredicate.CONTAINS, stringValue("Joe Ferner")), AUTHORIZATIONS_A)
                .vertices();
        assertEquals(1, vertices.getTotalHits());
        assertEquals(1, count(vertices));

        vertices = getGraph().query(hasFilter("propWithNonAlphaCharacters", TextPredicate.CONTAINS, stringValue("hyphen-word, etc.")), AUTHORIZATIONS_A)
                .vertices();
        assertEquals(1, vertices.getTotalHits());
        assertEquals(1, count(vertices));
    }

    @Test
    public void testGraphQueryWithANDOperatorAndWithExactMatchFields() {
        getGraph().defineProperty("firstName").dataType(TextValue.class).textIndexHint(TextIndexHint.EXACT_MATCH).define();

        getGraph().prepareVertex("v1", VISIBILITY_A, CONCEPT_TYPE_THING)
                .setProperty("firstName", stringValue("Joe"), VISIBILITY_A)
                .setProperty("lastName", stringValue("Ferner"), VISIBILITY_A)
                .save(AUTHORIZATIONS_A_AND_B);
        getGraph().prepareVertex("v2", VISIBILITY_A, CONCEPT_TYPE_THING)
                .setProperty("firstName", stringValue("Joe"), VISIBILITY_A)
                .setProperty("lastName", stringValue("Smith"), VISIBILITY_A)
                .save(AUTHORIZATIONS_A_AND_B);
        getGraph().flush();

        assumeTrue("lucene and queries not supported", isLuceneQueriesSupported() && isLuceneAndQueriesSupported());

        Iterable<Vertex> vertices = getGraph().query("Joe AND ferner", AUTHORIZATIONS_A)
                .vertices();
        assertEquals(1, count(vertices));
    }

    @Test
    public void testGraphQueryHasWithSpacesAndFieldedQueryString() {
        assumeTrue("fielded query not supported", isFieldNamesInQuerySupported());

        getGraph().defineProperty("name").dataType(TextValue.class).textIndexHint(TextIndexHint.ALL).define();

        getGraph().prepareVertex("v1", VISIBILITY_A, CONCEPT_TYPE_THING)
                .setProperty("name", stringValue("Joe Ferner"), VISIBILITY_A)
                .setProperty("propWithHyphen", stringValue("hyphen-word"), VISIBILITY_A)
                .save(AUTHORIZATIONS_A_AND_B);
        getGraph().prepareVertex("v2", VISIBILITY_A, CONCEPT_TYPE_THING)
                .setProperty("name", stringValue("Joe Smith"), VISIBILITY_A)
                .save(AUTHORIZATIONS_A_AND_B);
        getGraph().flush();

        assumeTrue("lucene queries", isLuceneQueriesSupported());

        Iterable<Vertex> vertices = getGraph().query("name:Joe", AUTHORIZATIONS_A)
                .vertices();
        assertEquals(2, count(vertices));

        vertices = getGraph().query("name:\"Joe Ferner\"", AUTHORIZATIONS_A)
                .vertices();
        assertEquals(1, count(vertices));

        vertices = getGraph().query("name:Fer*", AUTHORIZATIONS_A)
                .vertices();
        Assert.assertEquals(1, count(vertices));

        vertices = getGraph().query("name:Fer*er", AUTHORIZATIONS_A)
                .vertices();
        Assert.assertEquals(1, count(vertices));

        vertices = getGraph().query("name:/f.*r/", AUTHORIZATIONS_A)
                .vertices();
        Assert.assertEquals(1, count(vertices));

        vertices = getGraph().query("name:terner~", AUTHORIZATIONS_A)
                .vertices();
        Assert.assertEquals(1, count(vertices));

        vertices = getGraph().query("name:{Fern TO Gern}", AUTHORIZATIONS_A)
                .vertices();
        Assert.assertEquals(1, count(vertices));
    }

    @Test
    public void testGraphQueryRange() {
        getGraph().prepareVertex("v1", VISIBILITY_A, CONCEPT_TYPE_THING)
                .setProperty("age", intValue(25), VISIBILITY_A)
                .save(AUTHORIZATIONS_A_AND_B);
        getGraph().prepareVertex("v2", VISIBILITY_A, CONCEPT_TYPE_THING)
                .setProperty("age", intValue(30), VISIBILITY_A)
                .save(AUTHORIZATIONS_A_AND_B);
        getGraph().flush();

        Iterable<Vertex> vertices = getGraph().query(range("age", intValue(25), intValue(25)), AUTHORIZATIONS_A)
                .vertices();
        assertEquals(1, count(vertices));

        vertices = getGraph().query(range("age", intValue(20), intValue(29)), AUTHORIZATIONS_A)
                .vertices();
        assertEquals(1, count(vertices));

        vertices = getGraph().query(range("age", intValue(25), intValue(30)), AUTHORIZATIONS_A)
                .vertices();
        assertEquals(2, count(vertices));

        vertices = getGraph().query(range("age", intValue(25), true, intValue(30), false), AUTHORIZATIONS_A)
                .vertices();
        assertEquals(1, count(vertices));
        assertEquals(intValue(25), toList(vertices).get(0).getPropertyValue("age"));

        vertices = getGraph().query(range("age", intValue(25), false, intValue(30), true), AUTHORIZATIONS_A)
                .vertices();
        assertEquals(1, count(vertices));
        assertEquals(intValue(30), toList(vertices).get(0).getPropertyValue("age"));

        vertices = getGraph().query(range("age", intValue(25), true, intValue(30), true), AUTHORIZATIONS_A)
                .vertices();
        assertEquals(2, count(vertices));

        vertices = getGraph().query(range("age", intValue(25), false, intValue(30), false), AUTHORIZATIONS_A)
                .vertices();
        assertEquals(0, count(vertices));
    }

    @Test
    public void testVertexQuery() {
        String propertyName = "prop.one";
        Vertex v1 = getGraph().addVertex("v1", VISIBILITY_A, AUTHORIZATIONS_ALL, CONCEPT_TYPE_THING);
        v1.setProperty(propertyName, stringValue("value1"), VISIBILITY_A, AUTHORIZATIONS_ALL);

        Vertex v2 = getGraph().addVertex("v2", VISIBILITY_A, AUTHORIZATIONS_ALL, CONCEPT_TYPE_THING);
        v2.setProperty(propertyName, stringValue("value2"), VISIBILITY_A, AUTHORIZATIONS_ALL);

        Vertex v3 = getGraph().addVertex("v3", VISIBILITY_A, AUTHORIZATIONS_ALL, CONCEPT_TYPE_THING);
        v3.setProperty(propertyName, stringValue("value3"), VISIBILITY_A, AUTHORIZATIONS_ALL);

        Edge ev1v2 = getGraph().prepareEdge("e v1->v2", v1, v2, LABEL_LABEL1, VISIBILITY_A)
                .setProperty(propertyName, stringValue("value1"), VISIBILITY_A)
                .save(AUTHORIZATIONS_A);
        Edge ev1v3 = getGraph().prepareEdge("e v1->v3", v1, v3, LABEL_LABEL2, VISIBILITY_A)
                .setProperty(propertyName, stringValue("value2"), VISIBILITY_A)
                .save(AUTHORIZATIONS_A);
        getGraph().prepareEdge("e v2->v3", v2, v3, LABEL_LABEL2, VISIBILITY_A)
                .setProperty(propertyName, stringValue("value2"), VISIBILITY_A)
                .save(AUTHORIZATIONS_A);
        getGraph().flush();

        v1 = getGraph().getVertex("v1", AUTHORIZATIONS_A);
        Iterable<Vertex> vertices = v1.query(AUTHORIZATIONS_A).vertices();
        assertEquals(2, count(vertices));
        IterableUtils.assertContains(v2, vertices);
        IterableUtils.assertContains(v3, vertices);
        if (isIterableWithTotalHitsSupported(vertices)) {
            assertEquals(2, ((IterableWithTotalHits) vertices).getTotalHits());

            vertices = v1.query(searchAll().limit(1), AUTHORIZATIONS_A).vertices();
            assertEquals(1, count(vertices));
            assertEquals(2, ((IterableWithTotalHits) vertices).getTotalHits());
        }

        vertices = v1.query(hasFilter(propertyName, stringValue("value2")), AUTHORIZATIONS_A)
                .vertices();
        assertEquals(1, count(vertices));
        IterableUtils.assertContains(v2, vertices);

        Iterable<Edge> edges = v1.query(AUTHORIZATIONS_A).edges();
        assertEquals(2, count(edges));
        IterableUtils.assertContains(ev1v2, edges);
        IterableUtils.assertContains(ev1v3, edges);

        edges = v1.query(hasEdgeLabel(LABEL_LABEL1, LABEL_LABEL2), AUTHORIZATIONS_A).edges();
        assertEquals(2, count(edges));
        IterableUtils.assertContains(ev1v2, edges);
        IterableUtils.assertContains(ev1v3, edges);

        edges = v1.query(hasEdgeLabel(LABEL_LABEL1), AUTHORIZATIONS_A).edges();
        assertEquals(1, count(edges));
        IterableUtils.assertContains(ev1v2, edges);

        vertices = v1.query(hasEdgeLabel(LABEL_LABEL1), AUTHORIZATIONS_A).vertices();
        assertEquals(1, count(vertices));
        IterableUtils.assertContains(v2, vertices);

        assertVertexIdsAnyOrder(v1.query(AUTHORIZATIONS_A).hasDirection(Direction.OUT).vertices(), "v2", "v3");
        assertVertexIdsAnyOrder(v1.query(AUTHORIZATIONS_A).hasDirection(Direction.IN).vertices());
        assertEdgeIdsAnyOrder(v1.query(AUTHORIZATIONS_A).hasDirection(Direction.OUT).edges(), "e v1->v2", "e v1->v3");
        assertEdgeIdsAnyOrder(v1.query(AUTHORIZATIONS_A).hasDirection(Direction.IN).edges());

        assertVertexIdsAnyOrder(v1.query(AUTHORIZATIONS_A).hasOtherVertexId("v2").vertices(), "v2");
        assertEdgeIds(v1.query(AUTHORIZATIONS_A).hasOtherVertexId("v2").edges(), "e v1->v2");
    }

    @Test
    public void testGraphQueryEdgeWithTermsAggregationAlterElementVisibility() {
        getGraph().prepareVertex("v1", VISIBILITY_EMPTY, CONCEPT_TYPE_THING)
                .save(AUTHORIZATIONS_A_AND_B);
        getGraph().prepareVertex("v2", VISIBILITY_EMPTY, CONCEPT_TYPE_THING)
                .save(AUTHORIZATIONS_A_AND_B);
        getGraph().prepareEdge("e1", "v1", "v2", LABEL_LABEL1, VISIBILITY_A)
                .addPropertyValue("k1", "age", intValue(25), VISIBILITY_EMPTY)
                .save(AUTHORIZATIONS_A_AND_B);
        getGraph().flush();

        Edge e1 = getGraph().getEdge("e1", AUTHORIZATIONS_A_AND_B);
        e1.prepareMutation()
                .alterElementVisibility(VISIBILITY_B)
                .save(AUTHORIZATIONS_A_AND_B);
        getGraph().flush();

        Map<Object, Long> propertyCountByValue = queryGraphQueryWithTermsAggregation("age", ElementType.EDGE, AUTHORIZATIONS_A_AND_B);
        assumeTrue("terms aggregation not supported", propertyCountByValue != null);
        assertEquals(1, propertyCountByValue.size());

        propertyCountByValue = queryGraphQueryWithTermsAggregation("age", ElementType.EDGE, AUTHORIZATIONS_A);
        assumeTrue("terms aggregation not supported", propertyCountByValue != null);
        assertEquals(0, propertyCountByValue.size());

        propertyCountByValue = queryGraphQueryWithTermsAggregation("age", ElementType.EDGE, AUTHORIZATIONS_B);
        assumeTrue("terms aggregation not supported", propertyCountByValue != null);
        assertEquals(1, propertyCountByValue.size());
    }


    @Test
    public void testGraphQueryWithNestedTermsAggregation() {
        getGraph().defineProperty("name").dataType(TextValue.class).textIndexHint(TextIndexHint.EXACT_MATCH, TextIndexHint.FULL_TEXT).define();
        getGraph().defineProperty("gender").dataType(TextValue.class).textIndexHint(TextIndexHint.EXACT_MATCH).define();

        getGraph().prepareVertex("v1", VISIBILITY_EMPTY, CONCEPT_TYPE_THING)
                .addPropertyValue("k1", "name", stringValue("Joe"), VISIBILITY_EMPTY)
                .addPropertyValue("k1", "gender", stringValue("male"), VISIBILITY_EMPTY)
                .save(AUTHORIZATIONS_A_AND_B);
        getGraph().prepareVertex("v2", VISIBILITY_EMPTY, CONCEPT_TYPE_THING)
                .addPropertyValue("k1", "name", stringValue("Sam"), VISIBILITY_EMPTY)
                .addPropertyValue("k1", "gender", stringValue("male"), VISIBILITY_EMPTY)
                .save(AUTHORIZATIONS_A_AND_B);
        getGraph().prepareVertex("v3", VISIBILITY_EMPTY, CONCEPT_TYPE_THING)
                .addPropertyValue("k1", "name", stringValue("Sam"), VISIBILITY_EMPTY)
                .addPropertyValue("k1", "gender", stringValue("female"), VISIBILITY_EMPTY)
                .save(AUTHORIZATIONS_A_AND_B);
        getGraph().prepareVertex("v4", VISIBILITY_EMPTY, CONCEPT_TYPE_THING)
                .addPropertyValue("k1", "name", stringValue("Sam"), VISIBILITY_EMPTY)
                .addPropertyValue("k1", "gender", stringValue("female"), VISIBILITY_EMPTY)
                .save(AUTHORIZATIONS_A_AND_B);
        getGraph().flush();

        Query q = getGraph().query(searchAll().limit(0), AUTHORIZATIONS_A_AND_B);
        TermsAggregation agg = new TermsAggregation("terms-count", "name");
        agg.addNestedAggregation(new TermsAggregation("nested", "gender"));
        assumeTrue("terms aggregation not supported", q.isAggregationSupported(agg));
        q.addAggregation(agg);
        TermsResult aggregationResult = q.vertices().getAggregationResult("terms-count", TermsResult.class);
        Map<Object, Map<Object, Long>> vertexPropertyCountByValue = nestedTermsBucketToMap(aggregationResult.getBuckets(), "nested");

        assertEquals(2, vertexPropertyCountByValue.size());
        assertEquals(1, vertexPropertyCountByValue.get("Joe").size());
        assertEquals(1L, (long) vertexPropertyCountByValue.get("Joe").get("male"));
        assertEquals(2, vertexPropertyCountByValue.get("Sam").size());
        assertEquals(1L, (long) vertexPropertyCountByValue.get("Sam").get("male"));
        assertEquals(2L, (long) vertexPropertyCountByValue.get("Sam").get("female"));
    }

    @Test
    public void testVertexQueryWithNestedTermsAggregation() {
        getGraph().defineProperty("name").dataType(TextValue.class).textIndexHint(TextIndexHint.EXACT_MATCH, TextIndexHint.FULL_TEXT).define();
        getGraph().defineProperty("gender").dataType(TextValue.class).textIndexHint(TextIndexHint.EXACT_MATCH).define();

        getGraph().prepareVertex("v1", VISIBILITY_EMPTY, CONCEPT_TYPE_THING)
                .save(AUTHORIZATIONS_A_AND_B);
        getGraph().prepareVertex("v2", VISIBILITY_EMPTY, CONCEPT_TYPE_THING)
                .addPropertyValue("k1", "name", stringValue("Joe"), VISIBILITY_EMPTY)
                .addPropertyValue("k1", "gender", stringValue("male"), VISIBILITY_EMPTY)
                .save(AUTHORIZATIONS_A_AND_B);
        getGraph().prepareVertex("v3", VISIBILITY_EMPTY, CONCEPT_TYPE_THING)
                .addPropertyValue("k1", "name", stringValue("Sam"), VISIBILITY_EMPTY)
                .addPropertyValue("k1", "gender", stringValue("male"), VISIBILITY_EMPTY)
                .save(AUTHORIZATIONS_A_AND_B);
        getGraph().prepareVertex("v4", VISIBILITY_EMPTY, CONCEPT_TYPE_THING)
                .addPropertyValue("k1", "name", stringValue("Sam"), VISIBILITY_EMPTY)
                .addPropertyValue("k1", "gender", stringValue("female"), VISIBILITY_EMPTY)
                .save(AUTHORIZATIONS_A_AND_B);
        getGraph().prepareVertex("v5", VISIBILITY_EMPTY, CONCEPT_TYPE_THING)
                .addPropertyValue("k1", "name", stringValue("Sam"), VISIBILITY_EMPTY)
                .addPropertyValue("k1", "gender", stringValue("female"), VISIBILITY_EMPTY)
                .save(AUTHORIZATIONS_A_AND_B);
        getGraph().addEdge("v1", "v2", LABEL_LABEL1, VISIBILITY_EMPTY, AUTHORIZATIONS_A_AND_B);
        getGraph().addEdge("v1", "v3", LABEL_LABEL1, VISIBILITY_EMPTY, AUTHORIZATIONS_A_AND_B);
        getGraph().addEdge("v1", "v4", LABEL_LABEL1, VISIBILITY_EMPTY, AUTHORIZATIONS_A_AND_B);
        getGraph().addEdge("v1", "v5", LABEL_LABEL1, VISIBILITY_EMPTY, AUTHORIZATIONS_A_AND_B);
        getGraph().flush();

        Query q = getGraph().getVertex("v1", AUTHORIZATIONS_A_AND_B).query(searchAll().limit(0), AUTHORIZATIONS_A_AND_B);
        TermsAggregation agg = new TermsAggregation("terms-count", "name");
        agg.addNestedAggregation(new TermsAggregation("nested", "gender"));
        assumeTrue("terms aggregation not supported", q.isAggregationSupported(agg));
        q.addAggregation(agg);
        TermsResult aggregationResult = q.vertices().getAggregationResult("terms-count", TermsResult.class);
        Map<Object, Map<Object, Long>> vertexPropertyCountByValue = nestedTermsBucketToMap(aggregationResult.getBuckets(), "nested");

        assertEquals(2, vertexPropertyCountByValue.size());
        assertEquals(1, vertexPropertyCountByValue.get("Joe").size());
        assertEquals(1L, (long) vertexPropertyCountByValue.get("Joe").get("male"));
        assertEquals(2, vertexPropertyCountByValue.get("Sam").size());
        assertEquals(1L, (long) vertexPropertyCountByValue.get("Sam").get("male"));
        assertEquals(2L, (long) vertexPropertyCountByValue.get("Sam").get("female"));
    }

    @Test

    public void testVertexQueryWithNestedTermsAggregationOnExtendedData() {
        getGraph().defineProperty("name").dataType(TextValue.class).textIndexHint(TextIndexHint.EXACT_MATCH, TextIndexHint.FULL_TEXT).define();
        getGraph().defineProperty("gender").dataType(TextValue.class).textIndexHint(TextIndexHint.EXACT_MATCH).define();

        getGraph().prepareVertex("v1", VISIBILITY_EMPTY, CONCEPT_TYPE_THING)
                .addExtendedData("t1", "r1", "name", stringValue("Joe"), VISIBILITY_EMPTY)
                .addExtendedData("t1", "r1", "gender", stringValue("male"), VISIBILITY_EMPTY)
                .addExtendedData("t1", "r2", "name", stringValue("Sam"), VISIBILITY_EMPTY)
                .addExtendedData("t1", "r2", "gender", stringValue("male"), VISIBILITY_EMPTY)
                .addExtendedData("t1", "r3", "name", stringValue("Sam"), VISIBILITY_EMPTY)
                .addExtendedData("t1", "r3", "gender", stringValue("female"), VISIBILITY_EMPTY)
                .addExtendedData("t1", "r4", "name", stringValue("Sam"), VISIBILITY_EMPTY)
                .addExtendedData("t1", "r4", "gender", stringValue("female"), VISIBILITY_EMPTY)
                .save(AUTHORIZATIONS_A_AND_B);
        getGraph().flush();

        Vertex v1 = getGraph().getVertex("v1", AUTHORIZATIONS_A_AND_B);
        Query q = v1.getExtendedData("t1").query(searchAll().limit(0), AUTHORIZATIONS_A_AND_B);
        TermsAggregation agg = new TermsAggregation("terms-count", "name");
        agg.addNestedAggregation(new TermsAggregation("nested", "gender"));
        assumeTrue("terms aggregation not supported", q.isAggregationSupported(agg));
        q.addAggregation(agg);
        TermsResult aggregationResult = q.extendedDataRows().getAggregationResult("terms-count", TermsResult.class);
        Map<Object, Map<Object, Long>> vertexPropertyCountByValue = nestedTermsBucketToMap(aggregationResult.getBuckets(), "nested");

        assertEquals(2, vertexPropertyCountByValue.size());
        assertEquals(1, vertexPropertyCountByValue.get("Joe").size());
        assertEquals(1L, (long) vertexPropertyCountByValue.get("Joe").get("male"));
        assertEquals(2, vertexPropertyCountByValue.get("Sam").size());
        assertEquals(1L, (long) vertexPropertyCountByValue.get("Sam").get("male"));
        assertEquals(2L, (long) vertexPropertyCountByValue.get("Sam").get("female"));

        q = v1.getExtendedData().query(searchAll().limit(0), AUTHORIZATIONS_A_AND_B);
        agg = new TermsAggregation("terms-count", "name");
        agg.addNestedAggregation(new TermsAggregation("nested", "gender"));
        assumeTrue("terms aggregation not supported", q.isAggregationSupported(agg));
        q.addAggregation(agg);
        aggregationResult = q.extendedDataRows().getAggregationResult("terms-count", TermsResult.class);
        vertexPropertyCountByValue = nestedTermsBucketToMap(aggregationResult.getBuckets(), "nested");

        assertEquals(2, vertexPropertyCountByValue.size());
        assertEquals(1, vertexPropertyCountByValue.get("Joe").size());
        assertEquals(1L, (long) vertexPropertyCountByValue.get("Joe").get("male"));
        assertEquals(2, vertexPropertyCountByValue.get("Sam").size());
        assertEquals(1L, (long) vertexPropertyCountByValue.get("Sam").get("male"));
        assertEquals(2L, (long) vertexPropertyCountByValue.get("Sam").get("female"));
    }

    @Test
    public void testGraphQueryWithHistogramAggregation() {
        boolean searchIndexFieldLevelSecurity = isSearchIndexFieldLevelSecuritySupported();

        getGraph().defineProperty("emptyField").dataType(IntValue.class).define();

        String agePropertyName = "age.property";
        getGraph().prepareVertex("v1", VISIBILITY_EMPTY, CONCEPT_TYPE_THING)
                .addPropertyValue("", agePropertyName, intValue(25), VISIBILITY_EMPTY)
                .addPropertyValue("", "birthDate", DateValue.parse("1990-09-04"), VISIBILITY_EMPTY)
                .save(AUTHORIZATIONS_A_AND_B);
        getGraph().prepareVertex("v2", VISIBILITY_EMPTY, CONCEPT_TYPE_THING)
                .addPropertyValue("", agePropertyName, intValue(20), VISIBILITY_EMPTY)
                .addPropertyValue("", "birthDate", DateValue.parse("1995-09-04"), VISIBILITY_EMPTY)
                .save(AUTHORIZATIONS_A_AND_B);
        getGraph().prepareVertex("v3", VISIBILITY_EMPTY, CONCEPT_TYPE_THING)
                .addPropertyValue("", agePropertyName, intValue(20), VISIBILITY_EMPTY)
                .addPropertyValue("", "birthDate", DateValue.parse("1995-08-15"), VISIBILITY_EMPTY)
                .save(AUTHORIZATIONS_A_AND_B);
        getGraph().prepareVertex("v4", VISIBILITY_EMPTY, CONCEPT_TYPE_THING)
                .addPropertyValue("", agePropertyName, intValue(20), VISIBILITY_A)
                .addPropertyValue("", "birthDate", DateValue.parse("1995-03-02"), VISIBILITY_EMPTY)
                .save(AUTHORIZATIONS_A_AND_B);
        getGraph().flush();

        Map<Object, Long> histogram = queryGraphQueryWithHistogramAggregation(agePropertyName, "1", 0L, new HistogramAggregation.ExtendedBounds<>(longValue(20L), longValue(25L)), AUTHORIZATIONS_EMPTY);
        assumeTrue("histogram aggregation not supported", histogram != null);
        assertEquals(6, histogram.size());
        assertEquals(1L, (long) histogram.get("25"));
        assertEquals(searchIndexFieldLevelSecurity ? 2L : 3L, (long) histogram.get("20"));

        histogram = queryGraphQueryWithHistogramAggregation(agePropertyName, "1", null, null, AUTHORIZATIONS_A_AND_B);
        assumeTrue("histogram aggregation not supported", histogram != null);
        assertEquals(2, histogram.size());
        assertEquals(1L, (long) histogram.get("25"));
        assertEquals(3L, (long) histogram.get("20"));

        // field that doesn't have any values
        histogram = queryGraphQueryWithHistogramAggregation("emptyField", "1", null, null, AUTHORIZATIONS_A_AND_B);
        assumeTrue("histogram aggregation not supported", histogram != null);
        assertEquals(0, histogram.size());

        // date by 'year'
        histogram = queryGraphQueryWithHistogramAggregation("birthDate", "year", null, null, AUTHORIZATIONS_EMPTY);
        assumeTrue("histogram aggregation not supported", histogram != null);
        assertEquals(2, histogram.size());

        // date by milliseconds
        histogram = queryGraphQueryWithHistogramAggregation("birthDate", (365L * 24L * 60L * 60L * 1000L) + "", null, null, AUTHORIZATIONS_EMPTY);
        assumeTrue("histogram aggregation not supported", histogram != null);
        assertEquals(2, histogram.size());
    }

    private Map<Object, Long> queryGraphQueryWithHistogramAggregation(
            String propertyName,
            String interval,
            Long minDocCount,
            HistogramAggregation.ExtendedBounds extendedBounds,
            Authorizations authorizations
    ) {
        Query q = getGraph().query(searchAll().limit(0), authorizations);
        HistogramAggregation agg = new HistogramAggregation("hist-count", propertyName, interval, minDocCount);
        agg.setExtendedBounds(extendedBounds);
        if (!q.isAggregationSupported(agg)) {
            LOGGER.warn("%s unsupported", HistogramAggregation.class.getName());
            return null;
        }
        q.addAggregation(agg);
        return histogramBucketToMap(q.vertices().getAggregationResult("hist-count", HistogramResult.class).getBuckets());
    }

    @Test
    public void testGraphQueryWithRangeAggregation() throws ParseException {
        boolean searchIndexFieldLevelSecurity = isSearchIndexFieldLevelSecuritySupported();

        getGraph().defineProperty("emptyField").dataType(IntValue.class).define();

        getGraph().prepareVertex("v1", VISIBILITY_EMPTY, CONCEPT_TYPE_THING)
                .addPropertyValue("", "age", intValue(25), VISIBILITY_EMPTY)
                .addPropertyValue("", "birthDate", DateValue.parse("1990-09-04"), VISIBILITY_EMPTY)
                .save(AUTHORIZATIONS_A_AND_B);
        getGraph().prepareVertex("v2", VISIBILITY_EMPTY, CONCEPT_TYPE_THING)
                .addPropertyValue("", "age", intValue(20), VISIBILITY_EMPTY)
                .addPropertyValue("", "birthDate", DateValue.parse("1995-09-04"), VISIBILITY_EMPTY)
                .save(AUTHORIZATIONS_A_AND_B);
        getGraph().prepareVertex("v3", VISIBILITY_EMPTY, CONCEPT_TYPE_THING)
                .addPropertyValue("", "age", intValue(20), VISIBILITY_EMPTY)
                .addPropertyValue("", "birthDate", DateValue.parse("1995-08-15"), VISIBILITY_EMPTY)
                .save(AUTHORIZATIONS_A_AND_B);
        getGraph().prepareVertex("v4", VISIBILITY_EMPTY, CONCEPT_TYPE_THING)
                .addPropertyValue("", "age", intValue(20), VISIBILITY_A)
                .addPropertyValue("", "birthDate", DateValue.parse("1995-03-02"), VISIBILITY_EMPTY)
                .save(AUTHORIZATIONS_A_AND_B);
        getGraph().prepareEdge("e1", "v1", "v2", "v1Tov2", VISIBILITY_EMPTY)
                .addPropertyValue("", "birthDate", DateValue.parse("1995-03-02"), VISIBILITY_EMPTY)
                .save(AUTHORIZATIONS_A_AND_B);
        getGraph().flush();

        // numeric range
        RangeResult aggregationResult = queryGraphQueryWithRangeAggregation(
                "age",
                null,
                "lower",
                intValue(21),
                "middle",
                intValue(23),
                "upper",
                AUTHORIZATIONS_EMPTY
        );
        assumeTrue("range aggregation not supported", aggregationResult != null);
        assertEquals(searchIndexFieldLevelSecurity ? 2 : 3, aggregationResult.getBucketByKey("lower").getCount());
        assertEquals(0, aggregationResult.getBucketByKey("middle").getCount());
        assertEquals(1, aggregationResult.getBucketByKey("upper").getCount());

        // numeric range with permission to see more data
        aggregationResult = queryGraphQueryWithRangeAggregation(
                "age",
                null,
                "lower",
                intValue(21),
                "middle",
                intValue(23),
                "upper",
                AUTHORIZATIONS_A_AND_B
        );
        assumeTrue("range aggregation not supported", aggregationResult != null);
        assertEquals(3, aggregationResult.getBucketByKey("lower").getCount());
        assertEquals(0, aggregationResult.getBucketByKey("middle").getCount());
        assertEquals(1, aggregationResult.getBucketByKey("upper").getCount());

        // range for a field with no values
        aggregationResult = queryGraphQueryWithRangeAggregation(
                "emptyField",
                null,
                "lower",
                intValue(21),
                "middle",
                intValue(23),
                "upper",
                AUTHORIZATIONS_EMPTY
        );
        assumeTrue("range aggregation not supported", aggregationResult != null);
        assertEquals(0, IterableUtils.count(aggregationResult.getBuckets()));

        // date range with dates specified as strings
        aggregationResult = queryGraphQueryWithRangeAggregation(
                "birthDate",
                null,
                "lower",
                DateValue.parse("1991-01-01"),
                "middle",
                DateValue.parse("1995-08-30"),
                "upper",
                AUTHORIZATIONS_EMPTY
        );
        assumeTrue("range aggregation not supported", aggregationResult != null);
        assertEquals(1, aggregationResult.getBucketByKey("lower").getCount());
        assertEquals(2, aggregationResult.getBucketByKey("middle").getCount());
        assertEquals(1, aggregationResult.getBucketByKey("upper").getCount());

        // date range without user specified keys
        aggregationResult = queryGraphQueryWithRangeAggregation(
                "birthDate",
                "yyyy-MM-dd",
                null,
                Values.stringValue("1991-01-01"),
                null,
                Values.stringValue("1995-08-30"),
                null,
                AUTHORIZATIONS_EMPTY
        );
        assumeTrue("range aggregation not supported", aggregationResult != null);
        assertEquals(1, aggregationResult.getBucketByKey("*-1991-01-01").getCount());
        assertEquals(2, aggregationResult.getBucketByKey("1991-01-01-1995-08-30").getCount());
        assertEquals(1, aggregationResult.getBucketByKey("1995-08-30-*").getCount());

        // date range with dates specified as date objects
        aggregationResult = queryGraphQueryWithRangeAggregation(
                "birthDate",
                null,
                "lower",
                DateValue.parse("1991-01-01"),
                "middle",
                DateValue.parse("1995-08-30"),
                "upper",
                AUTHORIZATIONS_EMPTY
        );
        assumeTrue("range aggregation not supported", aggregationResult != null);
        assertEquals(1, aggregationResult.getBucketByKey("lower").getCount());
        assertEquals(2, aggregationResult.getBucketByKey("middle").getCount());
        assertEquals(1, aggregationResult.getBucketByKey("upper").getCount());
    }

    private RangeResult queryGraphQueryWithRangeAggregation(
            String propertyName,
            String format,
            String keyOne,
            Value boundaryOne,
            String keyTwo,
            Value boundaryTwo,
            String keyThree,
            Authorizations authorizations
    ) {
        Query q = getGraph().query(searchAll().limit(0), authorizations);
        RangeAggregation agg = new RangeAggregation("range-count", propertyName, format);
        if (!q.isAggregationSupported(agg)) {
            LOGGER.warn("%s unsupported", RangeAggregation.class.getName());
            return null;

        }
        agg.addUnboundedTo(keyOne, boundaryOne);
        agg.addRange(keyTwo, boundaryOne, boundaryTwo);
        agg.addUnboundedFrom(keyThree, boundaryTwo);
        q.addAggregation(agg);
        return q.vertices().getAggregationResult("range-count", RangeResult.class);
    }

    @Test
    public void testGraphQueryWithRangeAggregationAndNestedTerms() throws ParseException {
        String agePropertyName = "age.property";
        getGraph().prepareVertex("v1", VISIBILITY_EMPTY, CONCEPT_TYPE_THING)
                .addPropertyValue("", agePropertyName, intValue(25), VISIBILITY_EMPTY)
                .addPropertyValue("", "name", stringValue("Alice"), VISIBILITY_EMPTY)
                .save(AUTHORIZATIONS_A_AND_B);
        getGraph().prepareVertex("v2", VISIBILITY_EMPTY, CONCEPT_TYPE_THING)
                .addPropertyValue("", agePropertyName, intValue(20), VISIBILITY_EMPTY)
                .addPropertyValue("", "name", stringValue("Alice"), VISIBILITY_EMPTY)
                .save(AUTHORIZATIONS_A_AND_B);
        getGraph().prepareVertex("v3", VISIBILITY_EMPTY, CONCEPT_TYPE_THING)
                .addPropertyValue("", agePropertyName, intValue(21), VISIBILITY_EMPTY)
                .addPropertyValue("", "name", stringValue("Alice"), VISIBILITY_EMPTY)
                .save(AUTHORIZATIONS_A_AND_B);
        getGraph().prepareVertex("v4", VISIBILITY_EMPTY, CONCEPT_TYPE_THING)
                .addPropertyValue("", agePropertyName, intValue(22), VISIBILITY_EMPTY)
                .addPropertyValue("", "name", stringValue("Bob"), VISIBILITY_EMPTY)
                .save(AUTHORIZATIONS_A_AND_B);
        getGraph().flush();

        Query q = getGraph().query(searchAll().limit(0), AUTHORIZATIONS_A_AND_B);
        RangeAggregation rangeAggregation = new RangeAggregation("range-count", agePropertyName);
        TermsAggregation termsAggregation = new TermsAggregation("name-count", "name");
        rangeAggregation.addNestedAggregation(termsAggregation);

        assumeTrue("range aggregation not supported", q.isAggregationSupported(rangeAggregation));
        assumeTrue("terms aggregation not supported", q.isAggregationSupported(termsAggregation));

        rangeAggregation.addUnboundedTo("lower", intValue(23));
        rangeAggregation.addUnboundedFrom("upper", intValue(23));
        q.addAggregation(rangeAggregation);

        RangeResult rangeAggResult = q.vertices().getAggregationResult("range-count", RangeResult.class);
        assertEquals(3, rangeAggResult.getBucketByKey("lower").getCount());
        assertEquals(1, rangeAggResult.getBucketByKey("upper").getCount());

        Comparator<TermsBucket> bucketComparator = (b1, b2) -> Long.compare(b2.getCount(), b1.getCount());

        Map<String, AggregationResult> lowerNestedResult = rangeAggResult.getBucketByKey("lower").getNestedResults();
        TermsResult lowerTermsResult = (TermsResult) lowerNestedResult.get(termsAggregation.getAggregationName());
        List<TermsBucket> lowerTermsBuckets = toList(lowerTermsResult.getBuckets());
        Collections.sort(lowerTermsBuckets, bucketComparator);
        assertEquals(1, lowerNestedResult.size());
        assertEquals(2, lowerTermsBuckets.size());
        assertEquals("Alice", lowerTermsBuckets.get(0).getKey());
        assertEquals(2, lowerTermsBuckets.get(0).getCount());
        assertEquals("Bob", lowerTermsBuckets.get(1).getKey());
        assertEquals(1, lowerTermsBuckets.get(1).getCount());

        Map<String, AggregationResult> upperNestedResult = rangeAggResult.getBucketByKey("upper").getNestedResults();
        TermsResult upperTermsResult = (TermsResult) upperNestedResult.get(termsAggregation.getAggregationName());
        List<TermsBucket> upperTermsBuckets = toList(upperTermsResult.getBuckets());
        assertEquals(1, upperNestedResult.size());
        assertEquals(1, upperTermsBuckets.size());
        assertEquals("Alice", upperTermsBuckets.get(0).getKey());
        assertEquals(1, upperTermsBuckets.get(0).getCount());
    }

    @Test
    public void testGraphQueryWithStatisticsAggregation() throws ParseException {
        getGraph().defineProperty("emptyField").dataType(IntValue.class).define();

        getGraph().prepareVertex("v1", VISIBILITY_EMPTY, CONCEPT_TYPE_THING)
                .addPropertyValue("", "age", intValue(25), VISIBILITY_EMPTY)
                .save(AUTHORIZATIONS_A_AND_B);
        getGraph().prepareVertex("v2", VISIBILITY_EMPTY, CONCEPT_TYPE_THING)
                .addPropertyValue("", "age", intValue(20), VISIBILITY_EMPTY)
                .save(AUTHORIZATIONS_A_AND_B);
        getGraph().prepareVertex("v3", VISIBILITY_EMPTY, CONCEPT_TYPE_THING)
                .addPropertyValue("", "age", intValue(20), VISIBILITY_EMPTY)
                .save(AUTHORIZATIONS_A_AND_B);
        getGraph().prepareVertex("v4", VISIBILITY_EMPTY, CONCEPT_TYPE_THING)
                .addPropertyValue("", "age", intValue(30), VISIBILITY_A)
                .save(AUTHORIZATIONS_A_AND_B);
        getGraph().flush();

        StatisticsResult stats = queryGraphQueryWithStatisticsAggregation("age", AUTHORIZATIONS_EMPTY);
        assumeTrue("statistics aggregation not supported", stats != null);
        assertEquals(3, stats.getCount());
        assertEquals(65.0, stats.getSum(), 0.1);
        assertEquals(20.0, stats.getMin(), 0.1);
        assertEquals(25.0, stats.getMax(), 0.1);
        assertEquals(2.35702, stats.getStandardDeviation(), 0.1);
        assertEquals(21.666666, stats.getAverage(), 0.1);

        stats = queryGraphQueryWithStatisticsAggregation("emptyField", AUTHORIZATIONS_EMPTY);
        assumeTrue("statistics aggregation not supported", stats != null);
        assertEquals(0, stats.getCount());
        assertEquals(0.0, stats.getSum(), 0.1);
        assertEquals(0.0, stats.getMin(), 0.1);
        assertEquals(0.0, stats.getMax(), 0.1);
        assertEquals(0.0, stats.getAverage(), 0.1);
        assertEquals(0.0, stats.getStandardDeviation(), 0.1);

        stats = queryGraphQueryWithStatisticsAggregation("age", AUTHORIZATIONS_A_AND_B);
        assumeTrue("statistics aggregation not supported", stats != null);
        assertEquals(4, stats.getCount());
        assertEquals(95.0, stats.getSum(), 0.1);
        assertEquals(20.0, stats.getMin(), 0.1);
        assertEquals(30.0, stats.getMax(), 0.1);
        assertEquals(23.75, stats.getAverage(), 0.1);
        assertEquals(4.14578, stats.getStandardDeviation(), 0.1);
    }

    private StatisticsResult queryGraphQueryWithStatisticsAggregation(String propertyName, Authorizations authorizations) {
        Query q = getGraph().query(searchAll().limit(0), authorizations);
        StatisticsAggregation agg = new StatisticsAggregation("stats", propertyName);
        if (!q.isAggregationSupported(agg)) {
            LOGGER.warn("%s unsupported", StatisticsAggregation.class.getName());
            return null;
        }
        q.addAggregation(agg);
        return q.vertices().getAggregationResult("stats", StatisticsResult.class);
    }

    @Test
    public void testGraphQueryWithCardinalityAggregation() {
        getGraph().defineProperty("emptyField").dataType(IntValue.class).define();
        getGraph().prepareVertex("v1", VISIBILITY_EMPTY, CONCEPT_TYPE_THING)
                .addPropertyValue("", "age", intValue(25), VISIBILITY_EMPTY)
                .save(AUTHORIZATIONS_A_AND_B);
        getGraph().prepareVertex("v2", VISIBILITY_EMPTY, CONCEPT_TYPE_THING)
                .addPropertyValue("", "age", intValue(20), VISIBILITY_EMPTY)
                .save(AUTHORIZATIONS_A_AND_B);
        getGraph().prepareVertex("v3", VISIBILITY_EMPTY, CONCEPT_TYPE_THING)
                .addPropertyValue("", "age", intValue(20), VISIBILITY_EMPTY)
                .save(AUTHORIZATIONS_A_AND_B);
        getGraph().prepareVertex("v4", VISIBILITY_A, CONCEPT_TYPE_THING)
                .addPropertyValue("", "age", intValue(30), VISIBILITY_A)
                .save(AUTHORIZATIONS_A_AND_B);
        getGraph().flush();
        CardinalityResult stats = queryGraphQueryWithCardinalityAggregation(Element.ID_PROPERTY_NAME, AUTHORIZATIONS_EMPTY);
        assumeTrue("Cardinality aggregation not supported", stats != null);
        assertEquals(3, (long) stats.value());
        stats = queryGraphQueryWithCardinalityAggregation(Element.ID_PROPERTY_NAME, AUTHORIZATIONS_A_AND_B);
        assumeTrue("Cardinality aggregation not supported", stats != null);
        assertEquals(4, (long) stats.value());
        CardinalityResult r = queryGraphQueryWithCardinalityAggregation("age", AUTHORIZATIONS_A_AND_B);
        assertEquals(r.value().intValue(), 3);
    }


    @Test
    public void testGraphQueryWithPercentilesAggregation() throws ParseException {
        getGraph().defineProperty("emptyField").dataType(IntValue.class).define();

        for (int i = 0; i <= 100; i++) {
            getGraph().prepareVertex("v" + i, VISIBILITY_EMPTY, CONCEPT_TYPE_THING)
                    .addPropertyValue("", "age", intValue(i), VISIBILITY_EMPTY)
                    .save(AUTHORIZATIONS_A_AND_B);
        }
        getGraph().prepareVertex("v200", VISIBILITY_EMPTY, CONCEPT_TYPE_THING)
                .addPropertyValue("", "age", intValue(30), VISIBILITY_A)
                .save(AUTHORIZATIONS_A_AND_B);
        getGraph().flush();

        PercentilesResult percentilesResult = queryGraphQueryWithPercentilesAggregation("age", VISIBILITY_EMPTY, AUTHORIZATIONS_EMPTY);
        assumeTrue("percentiles aggregation not supported", percentilesResult != null);
        List<Percentile> percentiles = toList(percentilesResult.getPercentiles());
        percentiles.sort(Comparator.comparing(Percentile::getPercentile));
        Assert.assertEquals(7, percentiles.size());
        Assert.assertEquals(1.0, percentiles.get(0).getPercentile(), 0.1);
        Assert.assertEquals(1.0, percentiles.get(0).getValue(), 0.5);
        Assert.assertEquals(5.0, percentiles.get(1).getPercentile(), 0.1);
        Assert.assertEquals(5.0, percentiles.get(1).getValue(), 0.5);
        Assert.assertEquals(25.0, percentiles.get(2).getPercentile(), 0.1);
        Assert.assertEquals(25.0, percentiles.get(2).getValue(), 0.5);
        Assert.assertEquals(50.0, percentiles.get(3).getPercentile(), 0.1);
        Assert.assertEquals(50.0, percentiles.get(3).getValue(), 0.5);
        Assert.assertEquals(75.0, percentiles.get(4).getPercentile(), 0.1);
        Assert.assertEquals(75.0, percentiles.get(4).getValue(), 0.5);
        Assert.assertEquals(95.0, percentiles.get(5).getPercentile(), 0.1);
        Assert.assertEquals(95.0, percentiles.get(5).getValue(), 0.5);
        Assert.assertEquals(99.0, percentiles.get(6).getPercentile(), 0.1);
        Assert.assertEquals(99.0, percentiles.get(6).getValue(), 0.5);

        percentilesResult = queryGraphQueryWithPercentilesAggregation("age", VISIBILITY_EMPTY, AUTHORIZATIONS_EMPTY, 60, 99.99);
        assumeTrue("statistics aggregation not supported", percentilesResult != null);
        percentiles = toList(percentilesResult.getPercentiles());
        percentiles.sort(Comparator.comparing(Percentile::getPercentile));
        Assert.assertEquals(2, percentiles.size());
        Assert.assertEquals(60.0, percentiles.get(0).getValue(), 0.1);
        Assert.assertEquals(60.0, percentiles.get(0).getValue(), 0.1);
        Assert.assertEquals(99.99, percentiles.get(1).getValue(), 0.1);
        Assert.assertEquals(99.99, percentiles.get(1).getValue(), 0.1);

        percentilesResult = queryGraphQueryWithPercentilesAggregation("emptyField", VISIBILITY_EMPTY, AUTHORIZATIONS_EMPTY);
        assumeTrue("statistics aggregation not supported", percentilesResult != null);
        percentiles = toList(percentilesResult.getPercentiles());
        Assert.assertEquals(0, percentiles.size());

        percentilesResult = queryGraphQueryWithPercentilesAggregation("age", VISIBILITY_A, AUTHORIZATIONS_A_AND_B);
        assumeTrue("statistics aggregation not supported", percentilesResult != null);
        percentiles = toList(percentilesResult.getPercentiles());
        percentiles.sort(Comparator.comparing(Percentile::getPercentile));
        Assert.assertEquals(7, percentiles.size());
        Assert.assertEquals(1.0, percentiles.get(0).getPercentile(), 0.1);
        Assert.assertEquals(30.0, percentiles.get(0).getValue(), 0.1);
        Assert.assertEquals(5.0, percentiles.get(1).getPercentile(), 0.1);
        Assert.assertEquals(30.0, percentiles.get(1).getValue(), 0.1);
        Assert.assertEquals(25.0, percentiles.get(2).getPercentile(), 0.1);
        Assert.assertEquals(30.0, percentiles.get(2).getValue(), 0.1);
        Assert.assertEquals(50.0, percentiles.get(3).getPercentile(), 0.1);
        Assert.assertEquals(30.0, percentiles.get(3).getValue(), 0.1);
        Assert.assertEquals(75.0, percentiles.get(4).getPercentile(), 0.1);
        Assert.assertEquals(30.0, percentiles.get(4).getValue(), 0.1);
        Assert.assertEquals(95.0, percentiles.get(5).getPercentile(), 0.1);
        Assert.assertEquals(30.0, percentiles.get(5).getValue(), 0.1);
        Assert.assertEquals(99.0, percentiles.get(6).getPercentile(), 0.1);
        Assert.assertEquals(30.0, percentiles.get(6).getValue(), 0.1);
    }

    private PercentilesResult queryGraphQueryWithPercentilesAggregation(
            String propertyName,
            Visibility visibility,
            Authorizations authorizations,
            double... percents
    ) {
        Query q = getGraph().query(searchAll().limit(0), authorizations);
        PercentilesAggregation agg = new PercentilesAggregation("percentiles", propertyName, visibility);
        agg.setPercents(percents);
        if (!q.isAggregationSupported(agg)) {
            LOGGER.warn("%s unsupported", StatisticsAggregation.class.getName());
            return null;
        }
        q.addAggregation(agg);
        return q.vertices().getAggregationResult("percentiles", PercentilesResult.class);
    }


    @Test
    public void testGraphQueryWithGeohashAggregation() {
        boolean searchIndexFieldLevelSecurity = isSearchIndexFieldLevelSecuritySupported();

        getGraph().defineProperty("emptyField").dataType(GeoPointValue.class).define();
        getGraph().defineProperty("location").dataType(GeoPointValue.class).define();

        getGraph().prepareVertex("v1", VISIBILITY_EMPTY, CONCEPT_TYPE_THING)
                .addPropertyValue("", "location", geoPointValue(50, -10), VISIBILITY_EMPTY)
                .save(AUTHORIZATIONS_A_AND_B);
        getGraph().prepareVertex("v2", VISIBILITY_EMPTY, CONCEPT_TYPE_THING)
                .addPropertyValue("", "location", geoPointValue(39, -77), VISIBILITY_EMPTY)
                .save(AUTHORIZATIONS_A_AND_B);
        getGraph().prepareVertex("v3", VISIBILITY_EMPTY, CONCEPT_TYPE_THING)
                .addPropertyValue("", "location", geoPointValue(39.1, -77.1), VISIBILITY_EMPTY)
                .save(AUTHORIZATIONS_A_AND_B);
        getGraph().prepareVertex("v4", VISIBILITY_EMPTY, CONCEPT_TYPE_THING)
                .addPropertyValue("", "location", geoPointValue(39.2, -77.2), VISIBILITY_A)
                .save(AUTHORIZATIONS_A_AND_B);
        getGraph().flush();

        Map<String, Long> histogram = queryGraphQueryWithGeohashAggregation("location", 2, AUTHORIZATIONS_EMPTY);
        assumeTrue("geo hash histogram aggregation not supported", histogram != null);
        assertEquals(2, histogram.size());
        assertEquals(1L, (long) histogram.get("gb"));
        assertEquals(searchIndexFieldLevelSecurity ? 2L : 3L, (long) histogram.get("dq"));

        histogram = queryGraphQueryWithGeohashAggregation("emptyField", 2, AUTHORIZATIONS_EMPTY);
        assumeTrue("geo hash histogram aggregation not supported", histogram != null);
        assertEquals(0, histogram.size());

        histogram = queryGraphQueryWithGeohashAggregation("location", 2, AUTHORIZATIONS_A_AND_B);
        assumeTrue("geo hash histogram aggregation not supported", histogram != null);
        assertEquals(2, histogram.size());
        assertEquals(1L, (long) histogram.get("gb"));
        assertEquals(3L, (long) histogram.get("dq"));
    }

    @Test
    public void testGraphQueryWithCalendarFieldAggregation() {
        String dateFieldName = "agg_date_field";

        // wed
        getGraph().prepareVertex("v0", VISIBILITY_EMPTY, CONCEPT_TYPE_THING)
                .addPropertyValue("", "other_field", createDateTime(2016, Month.APRIL.getValue(), 27, 10, 18, 56), VISIBILITY_A)
                .save(AUTHORIZATIONS_ALL);

        // wed
        getGraph().prepareVertex("v1", VISIBILITY_EMPTY, CONCEPT_TYPE_THING)
                .addPropertyValue("", dateFieldName, createDateTime(2016, Month.APRIL.getValue(), 27, 10, 18, 56), VISIBILITY_A)
                .save(AUTHORIZATIONS_ALL);
        // fri
        getGraph().prepareVertex("v2", VISIBILITY_EMPTY, CONCEPT_TYPE_THING)
                .addPropertyValue("", dateFieldName, createDateTime(2017, Month.MAY.getValue(), 26, 10, 18, 56), VISIBILITY_EMPTY)
                .save(AUTHORIZATIONS_ALL);

        // wed
        getGraph().prepareVertex("v3", VISIBILITY_A_AND_B, CONCEPT_TYPE_THING)
                .addPropertyValue("", dateFieldName, createDateTime(2016, Month.APRIL.getValue(), 27, 12, 18, 56), VISIBILITY_EMPTY)
                .save(AUTHORIZATIONS_ALL);

        // sun
        getGraph().prepareVertex("v4", VISIBILITY_A_AND_B, CONCEPT_TYPE_THING)
                .addPropertyValue("", dateFieldName, createDateTime(2016, Month.APRIL.getValue(), 24, 12, 18, 56), VISIBILITY_EMPTY)
                .save(AUTHORIZATIONS_ALL);

        // mon
        getGraph().prepareVertex("v5", VISIBILITY_A_AND_B, CONCEPT_TYPE_THING)
                .addPropertyValue("", dateFieldName, createDateTime(2016, Month.APRIL.getValue(), 25, 12, 18, 56), VISIBILITY_EMPTY)
                .save(AUTHORIZATIONS_ALL);

        // sat
        getGraph().prepareVertex("v6", VISIBILITY_A_AND_B, CONCEPT_TYPE_THING)
                .addPropertyValue("", dateFieldName, createDateTime(2016, Month.APRIL.getValue(), 30, 12, 18, 56), VISIBILITY_EMPTY)
                .save(AUTHORIZATIONS_ALL);
        getGraph().flush();

        // hour of day
        TimeZone timeZone = TimeZone.getTimeZone(ZoneOffset.UTC);
        QueryResultsIterable<Vertex> results = getGraph().query(searchAll().limit(0), AUTHORIZATIONS_ALL)
                .addAggregation(new ChronoFieldAggregation("agg1", dateFieldName, null, timeZone, ChronoField.HOUR_OF_DAY))
                .vertices();

        HistogramResult aggResult = results.getAggregationResult("agg1", ChronoFieldAggregation.RESULT_CLASS);
        assertEquals(2, count(aggResult.getBuckets()));
        assertEquals(2, aggResult.getBucketByKey(10).getCount());
        assertEquals(4, aggResult.getBucketByKey(12).getCount());

        // day of week
        results = getGraph().query(searchAll().limit(0), AUTHORIZATIONS_ALL)
                .addAggregation(new ChronoFieldAggregation("agg1", dateFieldName, null, timeZone, ChronoField.DAY_OF_WEEK))
                .vertices();

        aggResult = results.getAggregationResult("agg1", ChronoFieldAggregation.RESULT_CLASS);
        assertEquals(5, count(aggResult.getBuckets()));
        assertEquals(1, aggResult.getBucketByKey(DayOfWeek.SUNDAY.getValue()).getCount());
        assertEquals(1, aggResult.getBucketByKey(DayOfWeek.MONDAY.getValue()).getCount());
        assertEquals(2, aggResult.getBucketByKey(DayOfWeek.WEDNESDAY.getValue()).getCount());
        assertEquals(1, aggResult.getBucketByKey(DayOfWeek.FRIDAY.getValue()).getCount());
        assertEquals(1, aggResult.getBucketByKey(DayOfWeek.SATURDAY.getValue()).getCount());

        // day of month
        results = getGraph().query(searchAll().limit(0), AUTHORIZATIONS_ALL)
                .addAggregation(new ChronoFieldAggregation("agg1", dateFieldName, null, timeZone, ChronoField.DAY_OF_MONTH))
                .vertices();

        aggResult = results.getAggregationResult("agg1", ChronoFieldAggregation.RESULT_CLASS);
        assertEquals(5, count(aggResult.getBuckets()));
        assertEquals(1, aggResult.getBucketByKey(24).getCount());
        assertEquals(1, aggResult.getBucketByKey(25).getCount());
        assertEquals(1, aggResult.getBucketByKey(26).getCount());
        assertEquals(2, aggResult.getBucketByKey(27).getCount());
        assertEquals(1, aggResult.getBucketByKey(30).getCount());

        // month
        results = getGraph().query(searchAll().limit(0), AUTHORIZATIONS_ALL)
                .addAggregation(new ChronoFieldAggregation("agg1", dateFieldName, null, timeZone, ChronoField.MONTH_OF_YEAR))
                .vertices();

        aggResult = results.getAggregationResult("agg1", ChronoFieldAggregation.RESULT_CLASS);
        assertEquals(2, count(aggResult.getBuckets()));
        assertEquals(5, aggResult.getBucketByKey(Month.APRIL.getValue()).getCount());
        assertEquals(1, aggResult.getBucketByKey(Month.MAY.getValue()).getCount());

        // year
        results = getGraph().query(searchAll().limit(0), AUTHORIZATIONS_ALL)
                .addAggregation(new ChronoFieldAggregation("agg1", dateFieldName, null, timeZone, ChronoField.YEAR))
                .vertices();

        aggResult = results.getAggregationResult("agg1", ChronoFieldAggregation.RESULT_CLASS);
        assertEquals(2, count(aggResult.getBuckets()));
        assertEquals(5, aggResult.getBucketByKey(2016).getCount());
        assertEquals(1, aggResult.getBucketByKey(2017).getCount());

        // week of year
        results = getGraph().query(searchAll().limit(0), AUTHORIZATIONS_ALL)
                .addAggregation(new ChronoFieldAggregation("agg1", dateFieldName, null, timeZone, ChronoField.ALIGNED_WEEK_OF_YEAR))
                .vertices();

        aggResult = results.getAggregationResult("agg1", ChronoFieldAggregation.RESULT_CLASS);
        if (isPainlessDateMath()) {
            assertEquals(3, count(aggResult.getBuckets()));
            assertEquals(1, aggResult.getBucketByKey(18).getCount());
            assertEquals(4, aggResult.getBucketByKey(17).getCount());
            assertEquals(1, aggResult.getBucketByKey(21).getCount());
        } else {
            assertEquals(3, count(aggResult.getBuckets()));
            assertEquals(4, aggResult.getBucketByKey(17).getCount());
            assertEquals(1, aggResult.getBucketByKey(18).getCount());
            assertEquals(1, aggResult.getBucketByKey(21).getCount());
        }
    }

    @Test
    public void testGraphQueryWithCalendarFieldAggregationNested() {
        getGraph().prepareVertex("v1", VISIBILITY_EMPTY, CONCEPT_TYPE_THING)
                .addPropertyValue("", "date", createDateTime(2016, Month.APRIL.getValue(), 27, 10, 18, 56), VISIBILITY_EMPTY)
                .save(AUTHORIZATIONS_ALL);
        getGraph().prepareVertex("v2", VISIBILITY_EMPTY, CONCEPT_TYPE_THING)
                .addPropertyValue("", "date", createDateTime(2016, Month.APRIL.getValue(), 27, 10, 18, 56), VISIBILITY_EMPTY)
                .save(AUTHORIZATIONS_ALL);
        getGraph().prepareVertex("v3", VISIBILITY_EMPTY, CONCEPT_TYPE_THING)
                .addPropertyValue("", "date", createDateTime(2016, Month.APRIL.getValue(), 27, 12, 18, 56), VISIBILITY_EMPTY)
                .save(AUTHORIZATIONS_A);
        getGraph().prepareVertex("v4", VISIBILITY_EMPTY, CONCEPT_TYPE_THING)
                .addPropertyValue("", "date", createDateTime(2016, Month.APRIL.getValue(), 28, 10, 18, 56), VISIBILITY_EMPTY)
                .save(AUTHORIZATIONS_A);
        getGraph().flush();

        ChronoFieldAggregation agg = new ChronoFieldAggregation("agg1", "date", null, TimeZone.getTimeZone(ZoneOffset.UTC), ChronoField.DAY_OF_WEEK);
        agg.addNestedAggregation(new ChronoFieldAggregation("aggNested", "date", null, TimeZone.getTimeZone(ZoneOffset.UTC), ChronoField.HOUR_OF_DAY));
        QueryResultsIterable<Vertex> results = getGraph().query(searchAll().limit(0), AUTHORIZATIONS_ALL)
                .addAggregation(agg)
                .vertices();

        HistogramResult aggResult = results.getAggregationResult("agg1", ChronoFieldAggregation.RESULT_CLASS);

        HistogramBucket bucket = aggResult.getBucketByKey(DayOfWeek.WEDNESDAY.getValue());
        assertEquals(3, bucket.getCount());
        HistogramResult nestedResult = (HistogramResult) bucket.getNestedResults().get("aggNested");
        assertEquals(2, nestedResult.getBucketByKey(10).getCount());
        assertEquals(1, nestedResult.getBucketByKey(12).getCount());

        bucket = aggResult.getBucketByKey(DayOfWeek.THURSDAY.getValue());
        assertEquals(1, bucket.getCount());
        nestedResult = (HistogramResult) bucket.getNestedResults().get("aggNested");
        assertEquals(1, nestedResult.getBucketByKey(10).getCount());
    }

    @Test
    public void testIteratorWithLessThanPageSizeResultsPageOne() {
        DefaultGraphQueryIterable<Vertex> iterable = new DefaultGraphQueryIterable<>(
                searchAll().skip(0).limit(5),
                getVertices(3),
                false,
                false,
                false,
                AUTHORIZATIONS_ALL
        );
        int count = 0;
        Iterator<Vertex> iterator = iterable.iterator();
        Vertex v = null;
        while (iterator.hasNext()) {
            count++;
            v = iterator.next();
            assertNotNull(v);
        }
        assertEquals(3, count);
        assertNotNull("v was null", v);
        assertEquals("2", v.getId());
    }

    @Test
    public void testIteratorWithPageSizeResultsPageOne() {
        DefaultGraphQueryIterable<Vertex> iterable = new DefaultGraphQueryIterable<>(
                searchAll().skip(0).limit(5),
                getVertices(5),
                false,
                false,
                false,
                AUTHORIZATIONS_ALL
        );
        int count = 0;
        Iterator<Vertex> iterator = iterable.iterator();
        Vertex v = null;
        while (iterator.hasNext()) {
            count++;
            v = iterator.next();
            assertNotNull(v);
        }
        assertEquals(5, count);
        assertNotNull("v was null", v);
        assertEquals("4", v.getId());
    }

    @Test
    public void testIteratorWithMoreThanPageSizeResultsPageOne() {
        DefaultGraphQueryIterable<Vertex> iterable = new DefaultGraphQueryIterable<>(
                searchAll().skip(0).limit(5),
                getVertices(7),
                false,
                false,
                false,
                AUTHORIZATIONS_ALL
        );
        int count = 0;
        Iterator<Vertex> iterator = iterable.iterator();
        Vertex v = null;
        while (iterator.hasNext()) {
            count++;
            v = iterator.next();
            assertNotNull(v);
        }
        assertEquals(5, count);
        assertNotNull("v was null", v);
        assertEquals("4", v.getId());
    }

    @Test
    public void testIteratorWithMoreThanPageSizeResultsPageTwo() {
        DefaultGraphQueryIterable<Vertex> iterable = new DefaultGraphQueryIterable<>(
                searchAll().skip(5).limit(5),
                getVertices(12),
                false,
                false,
                false,
                AUTHORIZATIONS_ALL
        );
        int count = 0;
        Iterator<Vertex> iterator = iterable.iterator();
        Vertex v = null;
        while (iterator.hasNext()) {
            count++;
            v = iterator.next();
            assertNotNull(v);
        }
        assertEquals(5, count);
        assertNotNull("v was null", v);
        assertEquals("9", v.getId());
    }

    @Test
    public void testIteratorWithMoreThanPageSizeResultsPageThree() {
        DefaultGraphQueryIterable<Vertex> iterable = new DefaultGraphQueryIterable<>(
                searchAll().skip(10).limit(5),
                getVertices(12),
                false,
                false,
                false,
                AUTHORIZATIONS_ALL);
        int count = 0;
        Iterator<Vertex> iterator = iterable.iterator();
        Vertex v = null;
        while (iterator.hasNext()) {
            count++;
            v = iterator.next();
            assertNotNull(v);
        }
        assertEquals(2, count);
        assertNotNull("v was null", v);
        assertEquals("11", v.getId());
    }


    @Test
    public void testSimilarityByText() {
        assumeTrue("query similar", getGraph().isQuerySimilarToTextSupported());

        getGraph().prepareVertex("v1", VISIBILITY_A, CONCEPT_TYPE_THING)
                .setProperty("text", stringValue("Mary had a little lamb, His fleece was white as snow."), VISIBILITY_A)
                .save(AUTHORIZATIONS_A);
        getGraph().prepareVertex("v2", VISIBILITY_A, CONCEPT_TYPE_THING)
                .setProperty("text", stringValue("Mary had a little tiger, His fleece was white as snow."), VISIBILITY_B)
                .save(AUTHORIZATIONS_B);
        getGraph().prepareVertex("v3", VISIBILITY_B, CONCEPT_TYPE_THING)
                .setProperty("text", stringValue("Mary had a little lamb, His fleece was white as snow"), VISIBILITY_A)
                .save(AUTHORIZATIONS_B);
        getGraph().prepareVertex("v4", VISIBILITY_A, CONCEPT_TYPE_THING)
                .setProperty("text", stringValue("Mary had a little lamb."), VISIBILITY_A)
                .save(AUTHORIZATIONS_A);
        getGraph().prepareVertex("v5", VISIBILITY_A, CONCEPT_TYPE_THING)
                .setProperty("text", stringValue("His fleece was white as snow."), VISIBILITY_A)
                .save(AUTHORIZATIONS_A);
        getGraph().prepareVertex("v6", VISIBILITY_A, CONCEPT_TYPE_THING)
                .setProperty("text", stringValue("Mary had a little lamb, His fleece was black as snow."), VISIBILITY_A)
                .save(AUTHORIZATIONS_A);
        getGraph().prepareVertex("v6", VISIBILITY_A, CONCEPT_TYPE_THING)
                .setProperty("text", stringValue("Jack and Jill went up the hill to fetch a pail of water."), VISIBILITY_A)
                .save(AUTHORIZATIONS_A);
        getGraph().flush();


        Query query = getGraph().query(
                similarTo(new String[]{"text"}, "Mary had a little lamb, His fleece was white as snow")
                        .minTermFrequency(1)
                        .maxQueryTerms(25)
                        .minDocFrequency(1)
                        .maxDocFrequency(10)
                        .boost(2.0f),
                AUTHORIZATIONS_A_AND_B
        );
        QueryResultsIterable<? extends GeObject> searchResults = query.search();
        List<Vertex> vertices = toList(query.vertices());

        assertTrue(vertices.size() > 0);
        assertEquals(vertices.size(), searchResults.getTotalHits());


        query = getGraph().query(
                similarTo(new String[]{"text"}, "Mary had a little lamb, His fleece was white as snow")
                        .minTermFrequency(1)
                        .maxQueryTerms(25)
                        .minDocFrequency(1)
                        .maxDocFrequency(10)
                        .boost(2.0f),
                AUTHORIZATIONS_A
        );
        searchResults = query.search();
        vertices = toList(query.vertices());

        assertTrue(vertices.size() > 0);
        assertTrue(vertices.stream().noneMatch(vertex -> vertex.getId().equals("v3")));
        assertEquals(vertices.size(), searchResults.getTotalHits());
    }

    @Test
    public void testGraphQueryWithTermsAggregation() {
        boolean searchIndexFieldLevelSecurity = isSearchIndexFieldLevelSecuritySupported();
        getGraph().defineProperty("name").dataType(TextValue.class).textIndexHint(TextIndexHint.EXACT_MATCH, TextIndexHint.FULL_TEXT).define();

        getGraph().defineProperty("emptyField").dataType(IntValue.class).define();

        getGraph().prepareVertex("v1", VISIBILITY_EMPTY, CONCEPT_TYPE_THING)
                .addPropertyValue("k1", "name", stringValue("Joe"), VISIBILITY_EMPTY)
                .addPropertyValue("k2", "name", stringValue("Joseph"), VISIBILITY_EMPTY)
                .addPropertyValue("", "age", intValue(25), VISIBILITY_EMPTY)
                .save(AUTHORIZATIONS_A_AND_B);
        getGraph().prepareVertex("v2", VISIBILITY_EMPTY, CONCEPT_TYPE_THING)
                .addPropertyValue("k1", "name", stringValue("Joe"), VISIBILITY_EMPTY)
                .addPropertyValue("k2", "name", stringValue("Joseph"), VISIBILITY_B)
                .addPropertyValue("", "age", intValue(20), VISIBILITY_EMPTY)
                .save(AUTHORIZATIONS_A_AND_B);
        getGraph().prepareEdge("e1", "v1", "v2", LABEL_LABEL1, VISIBILITY_EMPTY)
                .addPropertyValue("k1", "name", stringValue("Joe"), VISIBILITY_EMPTY)
                .save(AUTHORIZATIONS_A_AND_B);
        getGraph().prepareEdge("e2", "v1", "v2", LABEL_LABEL1, VISIBILITY_EMPTY)
                .save(AUTHORIZATIONS_A_AND_B);
        getGraph().prepareEdge("e3", "v1", "v2", LABEL_LABEL2, VISIBILITY_EMPTY)
                .save(AUTHORIZATIONS_A_AND_B);
        getGraph().flush();

        TermsResult aggregationResult = queryGraphQueryWithTermsAggregationResult(null, "name", ElementType.VERTEX, 10, AUTHORIZATIONS_EMPTY);
        assertEquals(0, aggregationResult.getSumOfOtherDocCounts());
        Map<Object, Long> vertexPropertyCountByValue = termsBucketToMap(aggregationResult.getBuckets());
        assumeTrue("terms aggregation not supported", vertexPropertyCountByValue != null);
        assertEquals(2, vertexPropertyCountByValue.size());
        assertEquals(2L, (long) vertexPropertyCountByValue.get("Joe"));
        assertEquals(searchIndexFieldLevelSecurity ? 1L : 2L, (long) vertexPropertyCountByValue.get("Joseph"));

        aggregationResult = queryGraphQueryWithTermsAggregationResult(null, "name", ElementType.VERTEX, 1, AUTHORIZATIONS_EMPTY);
        assertEquals(1, aggregationResult.getSumOfOtherDocCounts());

        vertexPropertyCountByValue = queryGraphQueryWithTermsAggregation("emptyField", ElementType.VERTEX, AUTHORIZATIONS_EMPTY);
        assumeTrue("terms aggregation not supported", vertexPropertyCountByValue != null);
        assertEquals(0, vertexPropertyCountByValue.size());

        vertexPropertyCountByValue = queryGraphQueryWithTermsAggregation("name", ElementType.VERTEX, AUTHORIZATIONS_A_AND_B);
        assumeTrue("terms aggregation not supported", vertexPropertyCountByValue != null);
        assertEquals(2, vertexPropertyCountByValue.size());
        assertEquals(2L, (long) vertexPropertyCountByValue.get("Joe"));
        assertEquals(2L, (long) vertexPropertyCountByValue.get("Joseph"));

        Map<Object, Long> edgePropertyCountByValue = queryGraphQueryWithTermsAggregation(Edge.LABEL_PROPERTY_NAME, ElementType.EDGE, AUTHORIZATIONS_A_AND_B);
        assumeTrue("terms aggregation not supported", edgePropertyCountByValue != null);
        assertEquals(2, edgePropertyCountByValue.size());
        assertEquals(2L, (long) edgePropertyCountByValue.get(LABEL_LABEL1));
        assertEquals(1L, (long) edgePropertyCountByValue.get(LABEL_LABEL2));

        vertexPropertyCountByValue = queryGraphQueryWithTermsAggregation("Joe", "name", ElementType.VERTEX, AUTHORIZATIONS_EMPTY);
        assumeTrue("terms aggregation not supported", vertexPropertyCountByValue != null);
        assertEquals(2, vertexPropertyCountByValue.size());
        assertEquals(2L, (long) vertexPropertyCountByValue.get("Joe"));
        assertEquals(searchIndexFieldLevelSecurity ? 1L : 2L, (long) vertexPropertyCountByValue.get("Joseph"));
    }

    @Test
    public void testGraphQueryVertexWithTermsAggregationAlterElementVisibility() {
        getGraph().prepareVertex("v1", VISIBILITY_A, CONCEPT_TYPE_THING)
                .addPropertyValue("k1", "age", intValue(25), VISIBILITY_EMPTY)
                .save(AUTHORIZATIONS_A_AND_B);
        getGraph().flush();

        Vertex v1 = getGraph().getVertex("v1", AUTHORIZATIONS_A_AND_B);
        v1.prepareMutation()
                .alterElementVisibility(VISIBILITY_B)
                .save(AUTHORIZATIONS_A_AND_B);
        getGraph().flush();

        Map<Object, Long> propertyCountByValue = queryGraphQueryWithTermsAggregation("age", ElementType.VERTEX, AUTHORIZATIONS_A_AND_B);
        assumeTrue("terms aggregation not supported", propertyCountByValue != null);
        assertEquals(1, propertyCountByValue.size());

        propertyCountByValue = queryGraphQueryWithTermsAggregation("age", ElementType.VERTEX, AUTHORIZATIONS_A);
        assumeTrue("terms aggregation not supported", propertyCountByValue != null);
        assertEquals(0, propertyCountByValue.size());

        propertyCountByValue = queryGraphQueryWithTermsAggregation("age", ElementType.VERTEX, AUTHORIZATIONS_B);
        assumeTrue("terms aggregation not supported", propertyCountByValue != null);
        assertEquals(1, propertyCountByValue.size());
    }

    @Test
    public void testCaseSensitivityOfExactMatch() {
        getGraph().defineProperty("text").dataType(TextValue.class).textIndexHint(TextIndexHint.EXACT_MATCH).define();
        getGraph().prepareVertex("v1", VISIBILITY_A, CONCEPT_TYPE_THING)
                .setProperty("text", stringValue("Joe"), VISIBILITY_A)
                .save(AUTHORIZATIONS_A);
        graph.flush(); // Not sure why this needs to be here
        getGraph().prepareVertex("v2", VISIBILITY_A, CONCEPT_TYPE_THING)
                .setProperty("text", stringValue("joe"), VISIBILITY_A)
                .save(AUTHORIZATIONS_A);
        getGraph().prepareVertex("v3", VISIBILITY_A, CONCEPT_TYPE_THING)
                .setProperty("text", stringValue("JOE"), VISIBILITY_A)
                .save(AUTHORIZATIONS_A);
        getGraph().prepareVertex("v4", VISIBILITY_A, CONCEPT_TYPE_THING)
                .setProperty("text", stringValue("Joe"), VISIBILITY_A)
                .save(AUTHORIZATIONS_A);
        getGraph().flush();

        QueryResultsIterable<Vertex> vertices = getGraph().query(hasFilter("text", Compare.EQUAL, stringValue("Joe")), AUTHORIZATIONS_A)
                .addAggregation(new TermsAggregation("agg1", "text"))
                .vertices();
        assertVertexIdsAnyOrder(vertices, "v1", "v2", "v3", "v4");

        TermsResult agg = vertices.getAggregationResult("agg1", TermsResult.class);
        ArrayList<TermsBucket> buckets = Lists.newArrayList(agg.getBuckets());
        assertEquals(1, buckets.size());
        assertEquals("Joe", buckets.get(0).getKey());
        assertEquals(4L, buckets.get(0).getCount());
    }

    @Test
    public void testTextIndex() throws Exception {
        getGraph().defineProperty("none").dataType(TextValue.class).textIndexHint(TextIndexHint.NONE).define();
        getGraph().defineProperty("none").dataType(TextValue.class).textIndexHint(TextIndexHint.NONE).define(); // try calling define twice
        getGraph().defineProperty("both").dataType(TextValue.class).textIndexHint(TextIndexHint.ALL).define();
        getGraph().defineProperty("fullText").dataType(TextValue.class).textIndexHint(TextIndexHint.FULL_TEXT).define();
        getGraph().defineProperty("exactMatch").dataType(TextValue.class).textIndexHint(TextIndexHint.EXACT_MATCH).define();

        getGraph().prepareVertex("v1", VISIBILITY_A, CONCEPT_TYPE_THING)
                .setProperty("none", stringValue("Test Value"), VISIBILITY_A)
                .setProperty("both", stringValue("Test Value"), VISIBILITY_A)
                .setProperty("fullText", stringValue("Test Value"), VISIBILITY_A)
                .setProperty("exactMatch", stringValue("Test Value"), VISIBILITY_A)
                .save(AUTHORIZATIONS_A_AND_B);
        getGraph().flush();

        Vertex v1 = getGraph().getVertex("v1", AUTHORIZATIONS_A);
        assertEquals(stringValue("Test Value"), v1.getPropertyValue("none"));
        assertEquals(stringValue("Test Value"), v1.getPropertyValue("both"));
        assertEquals(stringValue("Test Value"), v1.getPropertyValue("fullText"));
        assertEquals(stringValue("Test Value"), v1.getPropertyValue("exactMatch"));

        assertEquals(1, count(getGraph().query(hasFilter("both", TextPredicate.CONTAINS, stringValue("Test")), AUTHORIZATIONS_A).vertices()));
        assertEquals(1, count(getGraph().query(hasFilter("fullText", TextPredicate.CONTAINS, stringValue("Test")), AUTHORIZATIONS_A).vertices()));
        assertEquals("exact match shouldn't match partials", 0, count(getGraph().query(hasFilter("exactMatch", stringValue("Test")), AUTHORIZATIONS_A).vertices()));
        assertEquals("un-indexed property shouldn't match partials", 0, count(getGraph().query(hasFilter("none", stringValue("Test")), AUTHORIZATIONS_A).vertices()));

        assertEquals(1, count(getGraph().query(hasFilter("both", stringValue("Test Value")), AUTHORIZATIONS_A).vertices()));
        assertEquals("default has predicate is equals which shouldn't work for full text", 0, count(getGraph().query(hasFilter("fullText", stringValue("Test Value")), AUTHORIZATIONS_A).vertices()));
        assertEquals(1, count(getGraph().query(hasFilter("exactMatch", stringValue("Test Value")), AUTHORIZATIONS_A).vertices()));
        if (count(getGraph().query(hasFilter("none", stringValue("Test Value")), AUTHORIZATIONS_A).vertices()) != 0) {
            LOGGER.warn("default has predicate is equals which shouldn't work for un-indexed");
        }
    }

    @Test
    public void testTextIndexDoesNotContain() {
        getGraph().defineProperty("both").dataType(TextValue.class).textIndexHint(TextIndexHint.ALL).define();
        getGraph().defineProperty("fullText").dataType(TextValue.class).textIndexHint(TextIndexHint.FULL_TEXT).define();
        getGraph().defineProperty("exactMatch").dataType(TextValue.class).textIndexHint(TextIndexHint.EXACT_MATCH).define();

        getGraph().prepareVertex("v1", VISIBILITY_A, CONCEPT_TYPE_THING)
                .setProperty("exactMatch", stringValue("Test Value"), VISIBILITY_A)
                .setProperty("both", stringValue("Test123"), VISIBILITY_A)
                .save(AUTHORIZATIONS_A_AND_B);

        getGraph().prepareVertex("v2", VISIBILITY_A, CONCEPT_TYPE_THING)
                .setProperty("both", stringValue("Test Value"), VISIBILITY_A)
                .save(AUTHORIZATIONS_A_AND_B);

        getGraph().prepareVertex("v3", VISIBILITY_A, CONCEPT_TYPE_THING)
                .setProperty("both", stringValue("Temp"), VISIBILITY_A)
                .save(AUTHORIZATIONS_A_AND_B);

        getGraph().prepareVertex("v4", VISIBILITY_A, CONCEPT_TYPE_THING)
                .save(AUTHORIZATIONS_A_AND_B);

        getGraph().prepareVertex("v5", VISIBILITY_A, CONCEPT_TYPE_THING)
                .setProperty("both", stringValue("Test123 test"), VISIBILITY_A)
                .save(AUTHORIZATIONS_A_AND_B);
        getGraph().flush();

        QueryResultsIterable<Vertex> vertices = getGraph().query(hasFilter("both", TextPredicate.DOES_NOT_CONTAIN, stringValue("Test")), AUTHORIZATIONS_A)
                .vertices();
        assertVertexIdsAnyOrder(vertices, "v1", "v3", "v4");

        vertices = getGraph().query(hasFilter("exactMatch", stringValue("Test Value")), AUTHORIZATIONS_A)
                .vertices();
        assertVertexIds(vertices, "v1");

        getGraph().query(hasFilter("exactMatch", TextPredicate.DOES_NOT_CONTAIN, stringValue("Test")), AUTHORIZATIONS_A)
                .vertices();

        getGraph().prepareVertex("v6", VISIBILITY_A, CONCEPT_TYPE_THING)
                .setProperty("both", stringValue("susan-test"), VISIBILITY_A)
                .save(AUTHORIZATIONS_A_AND_B);

        getGraph().prepareVertex("v7", VISIBILITY_A, CONCEPT_TYPE_THING)
                .setProperty("both", stringValue("susan-test"), Visibility.EMPTY)
                .save(AUTHORIZATIONS_A_AND_B);
        getGraph().flush();

        vertices = getGraph().query(hasFilter("both", TextPredicate.DOES_NOT_CONTAIN, stringValue("susan")), AUTHORIZATIONS_A)
                .vertices();
        assertVertexIdsAnyOrder(vertices, "v1", "v2", "v3", "v4", "v5");
    }

    @Test
    public void testFieldBoost() throws Exception {
        assumeTrue("Boost not supported", getGraph().isFieldBoostSupported());

        getGraph().defineProperty("a")
                .dataType(TextValue.class)
                .textIndexHint(TextIndexHint.ALL)
                .boost(1)
                .define();
        getGraph().defineProperty("b")
                .dataType(TextValue.class)
                .textIndexHint(TextIndexHint.ALL)
                .boost(2)
                .define();

        getGraph().prepareVertex("v1", VISIBILITY_A, CONCEPT_TYPE_THING)
                .setProperty("a", stringValue("Test Value"), VISIBILITY_A)
                .save(AUTHORIZATIONS_A_AND_B);

        getGraph().prepareVertex("v2", VISIBILITY_A, CONCEPT_TYPE_THING)
                .setProperty("b", stringValue("Test Value"), VISIBILITY_A)
                .save(AUTHORIZATIONS_A_AND_B);

        assertVertexIds(getGraph().query("Test", AUTHORIZATIONS_A).vertices(), "v2", "v1");
    }

    @Test
    public void testValueTypes() throws Exception {
        DateTimeValue date = createDateTime(2014, 2, 24, 13, 0, 5);

        getGraph().prepareVertex("v1", VISIBILITY_A, CONCEPT_TYPE_THING)
                .setProperty("int", intValue(5), VISIBILITY_A)
                .setProperty("double", doubleValue(5.6d), VISIBILITY_A)
                .setProperty("float", floatValue(6.4f), VISIBILITY_A)
                .setProperty("string", stringValue("test"), VISIBILITY_A)
                .setProperty("byte", byteValue((byte) 5), VISIBILITY_A)
                .setProperty("long", longValue(5L), VISIBILITY_A)
                .setProperty("boolean", BooleanValue.TRUE, VISIBILITY_A)
                .setProperty("geopoint", geoPointValue(77, -33), VISIBILITY_A)
                .setProperty("short", shortValue((short) 5), VISIBILITY_A)
                .setProperty("date", date, VISIBILITY_A)
                .save(AUTHORIZATIONS_A_AND_B);
        getGraph().flush();

        assertEquals(1, count(getGraph().query(hasFilter("int", intValue(5)), AUTHORIZATIONS_A).vertices()));
        assertEquals(1, count(getGraph().query(hasFilter("double", doubleValue(5.6d)), AUTHORIZATIONS_A).vertices()));
        assertEquals(1, count(getGraph().query(range("float", floatValue(6.3f), floatValue(6.5f)), AUTHORIZATIONS_A).vertices())); // can't search for 6.4f her because of float precision
        assertEquals(1, count(getGraph().query(hasFilter("string", stringValue("test")), AUTHORIZATIONS_A).vertices()));
        assertEquals(1, count(getGraph().query(hasFilter("byte", byteValue((byte) 5)), AUTHORIZATIONS_A).vertices()));
        assertEquals(1, count(getGraph().query(hasFilter("long", longValue(5L)), AUTHORIZATIONS_A).vertices()));
        assertEquals(1, count(getGraph().query(hasFilter("boolean", BooleanValue.TRUE), AUTHORIZATIONS_A).vertices()));
        assertEquals(1, count(getGraph().query(hasFilter("short", shortValue((short) 5)), AUTHORIZATIONS_A).vertices()));
        assertEquals(1, count(getGraph().query(hasFilter("date", date), AUTHORIZATIONS_A).vertices()));
        assertEquals(1, count(getGraph().query(hasFilter("geopoint", GeoCompare.WITHIN, geoCircleValue(77, -33, 1)), AUTHORIZATIONS_A).vertices()));
    }

    @Test
    public void testValueTypesUpdatingWithMutations() throws Exception {
        DateTimeValue date = createDateTime(2014, 2, 24, 13, 0, 5);
        getGraph().prepareVertex("v1", VISIBILITY_A, CONCEPT_TYPE_THING).save(AUTHORIZATIONS_A_AND_B);
        getGraph().flush();
        getGraph().getVertex("v1", AUTHORIZATIONS_A_AND_B)
                .prepareMutation()
                .addPropertyValue("", "int", intValue(5), VISIBILITY_A)
                .addPropertyValue("", "double", doubleValue(5.6d), VISIBILITY_A)
                .addPropertyValue("", "float", floatValue(6.4f), VISIBILITY_A)
                .addPropertyValue("", "string", stringValue("test"), VISIBILITY_A)
                .addPropertyValue("", "byte", byteValue((byte) 5), VISIBILITY_A)
                .addPropertyValue("", "long", longValue(5L), VISIBILITY_A)
                .addPropertyValue("", "boolean", BooleanValue.TRUE, VISIBILITY_A)
                .addPropertyValue("", "geopoint", geoPointValue(77, -33), VISIBILITY_A)
                .addPropertyValue("", "short", shortValue((short) 5), VISIBILITY_A)
                .addPropertyValue("", "date", date, VISIBILITY_A)
                .save(AUTHORIZATIONS_A_AND_B);
        getGraph().flush();
        assertEquals(1, count(getGraph().query(hasFilter("int", intValue(5)), AUTHORIZATIONS_A).vertices()));
        assertEquals(1, count(getGraph().query(hasFilter("double", doubleValue(5.6d)), AUTHORIZATIONS_A).vertices()));
        assertEquals(1, count(getGraph().query(range("float", floatValue(6.3f), floatValue(6.5f)), AUTHORIZATIONS_A).vertices())); // can't search for 6.4f her because of float precision
        assertEquals(1, count(getGraph().query(hasFilter("string", stringValue("test")), AUTHORIZATIONS_A).vertices()));
        assertEquals(1, count(getGraph().query(hasFilter("byte", byteValue((byte) 5)), AUTHORIZATIONS_A).vertices()));
        assertEquals(1, count(getGraph().query(hasFilter("long", longValue(5L)), AUTHORIZATIONS_A).vertices()));
        assertEquals(1, count(getGraph().query(hasFilter("boolean", BooleanValue.TRUE), AUTHORIZATIONS_A).vertices()));
        assertEquals(1, count(getGraph().query(hasFilter("short", shortValue((short) 5)), AUTHORIZATIONS_A).vertices()));
        assertEquals(1, count(getGraph().query(hasFilter("date", date), AUTHORIZATIONS_A).vertices()));
        assertEquals(1, count(getGraph().query(hasFilter("geopoint", GeoCompare.WITHIN, geoCircleValue(77, -33, 1)), AUTHORIZATIONS_A).vertices()));
    }

    @Test
    public void testPartialUpdateOfVertex() {
        getGraph().prepareVertex("v1", VISIBILITY_A, CONCEPT_TYPE_THING)
                .setProperty("prop1", stringValue("value1"), VISIBILITY_A)
                .setProperty("prop2", stringValue("value2"), VISIBILITY_A)
                .save(AUTHORIZATIONS_A_AND_B);
        getGraph().flush();

        getGraph().prepareVertex("v1", VISIBILITY_A, CONCEPT_TYPE_THING)
                .setProperty("prop1", stringValue("value1New"), VISIBILITY_A)
                .save(AUTHORIZATIONS_A_AND_B);
        getGraph().flush();

        Iterable<Vertex> vertices = getGraph().query(hasFilter("prop2", stringValue("value2")), AUTHORIZATIONS_A_AND_B)
                .vertices();
        assertVertexIds(vertices, "v1");
    }

    @Test
    public void testPartialUpdateOfVertexPropertyKey() {
        assumeTrue("Known bug in partial updates", isParitalUpdateOfVertexPropertyKeySupported());

        getGraph().prepareVertex("v1", VISIBILITY_A, CONCEPT_TYPE_THING)
                .addPropertyValue("key1", "prop", stringValue("value1"), VISIBILITY_A)
                .addPropertyValue("key2", "prop", stringValue("value2"), VISIBILITY_A)
                .save(AUTHORIZATIONS_A_AND_B);
        getGraph().flush();

        Iterable<Vertex> vertices = getGraph().query(hasFilter("prop", stringValue("value1")), AUTHORIZATIONS_A_AND_B)
                .vertices();
        assertVertexIds(vertices, "v1");

        vertices = getGraph().query(hasFilter("prop", stringValue("value2")), AUTHORIZATIONS_A_AND_B)
                .vertices();
        assertVertexIds(vertices, "v1");

        getGraph().prepareVertex("v1", VISIBILITY_A, CONCEPT_TYPE_THING)
                .addPropertyValue("key1", "prop", stringValue("value1New"), VISIBILITY_A)
                .save(AUTHORIZATIONS_A_AND_B);
        getGraph().flush();

        vertices = getGraph().query(hasFilter("prop", stringValue("value1New")), AUTHORIZATIONS_A_AND_B)
                .vertices();
        assertVertexIds(vertices, "v1");

        vertices = getGraph().query(hasFilter("prop", stringValue("value2")), AUTHORIZATIONS_A_AND_B)
                .vertices();
        assertVertexIds(vertices, "v1");

        vertices = getGraph().query(hasFilter("prop", stringValue("value1")), AUTHORIZATIONS_A_AND_B)
                .vertices();
        assertVertexIds(vertices);
    }

    @Test
    public void testGetVertexPropertyCountByValue() {
        boolean searchIndexFieldLevelSecurity = isSearchIndexFieldLevelSecuritySupported();
        getGraph().defineProperty("name").dataType(TextValue.class).textIndexHint(TextIndexHint.EXACT_MATCH).define();

        getGraph().prepareVertex("v1", VISIBILITY_EMPTY, CONCEPT_TYPE_THING)
                .addPropertyValue("k1", "name", stringValue("Joe"), VISIBILITY_EMPTY)
                .addPropertyValue("k2", "name", stringValue("Joseph"), VISIBILITY_EMPTY)
                .addPropertyValue("", "age", intValue(25), VISIBILITY_EMPTY)
                .save(AUTHORIZATIONS_A_AND_B);
        getGraph().prepareVertex("v2", VISIBILITY_EMPTY, CONCEPT_TYPE_THING)
                .addPropertyValue("k1", "name", stringValue("Joe"), VISIBILITY_EMPTY)
                .addPropertyValue("k2", "name", stringValue("Joseph"), VISIBILITY_B)
                .addPropertyValue("", "age", intValue(20), VISIBILITY_EMPTY)
                .save(AUTHORIZATIONS_A_AND_B);
        getGraph().prepareEdge("v1", "v2", LABEL_LABEL1, VISIBILITY_EMPTY)
                .addPropertyValue("k1", "name", stringValue("Joe"), VISIBILITY_EMPTY)
                .save(AUTHORIZATIONS_A_AND_B);
        getGraph().flush();

        Map<Object, Long> vertexPropertyCountByValue = getGraph().getVertexPropertyCountByValue("name", AUTHORIZATIONS_EMPTY);
        assertEquals(2, vertexPropertyCountByValue.size());
        assertEquals(2L, (long) vertexPropertyCountByValue.get("joe"));
        assertEquals(searchIndexFieldLevelSecurity ? 1L : 2L, (long) vertexPropertyCountByValue.get("joseph"));

        vertexPropertyCountByValue = getGraph().getVertexPropertyCountByValue("name", AUTHORIZATIONS_A_AND_B);
        assertEquals(2, vertexPropertyCountByValue.size());
        assertEquals(2L, (long) vertexPropertyCountByValue.get("joe"));
        assertEquals(2L, (long) vertexPropertyCountByValue.get("joseph"));
    }

    @Test
    public void testGetCounts() {
        Vertex v1 = getGraph().addVertex("v1", VISIBILITY_A, AUTHORIZATIONS_A, CONCEPT_TYPE_THING);
        Vertex v2 = getGraph().addVertex("v2", VISIBILITY_A, AUTHORIZATIONS_A, CONCEPT_TYPE_THING);
        getGraph().addEdge("e1", v1, v2, LABEL_LABEL1, VISIBILITY_A, AUTHORIZATIONS_A);
        getGraph().flush();

        assertEquals(2, getGraph().getVertexCount(AUTHORIZATIONS_A));
        assertEquals(1, getGraph().getEdgeCount(AUTHORIZATIONS_A));
    }

    protected boolean isFieldNamesInQuerySupported() {
        return true;
    }

    protected boolean isLuceneAndQueriesSupported() {
        return !(getGraph().query(AUTHORIZATIONS_A) instanceof DefaultGraphQuery);
    }

    protected boolean isIterableWithTotalHitsSupported(Iterable<Vertex> vertices) {
        return vertices instanceof IterableWithTotalHits;
    }

    protected boolean isFetchHintNoneVertexQuerySupported() {
        return true;
    }

    private Map<String, Long> geoHashBucketToMap(Iterable<GeohashBucket> buckets) {
        Map<String, Long> results = new HashMap<>();
        for (GeohashBucket b : buckets) {
            results.put(b.getKey(), b.getCount());
        }
        return results;
    }

    private Map<Object, Map<Object, Long>> nestedTermsBucketToMap(Iterable<TermsBucket> buckets, String nestedAggName) {
        Map<Object, Map<Object, Long>> results = new HashMap<>();
        for (TermsBucket entry : buckets) {
            TermsResult nestedResults = (TermsResult) entry.getNestedResults().get(nestedAggName);
            if (nestedResults == null) {
                throw new GeException("Could not find nested: " + nestedAggName);
            }
            results.put(entry.getKey(), termsBucketToMap(nestedResults.getBuckets()));
        }
        return results;
    }

    private Map<Object, Long> histogramBucketToMap(Iterable<HistogramBucket> buckets) {
        Map<Object, Long> results = new HashMap<>();
        for (HistogramBucket b : buckets) {
            results.put(b.getKey(), b.getCount());
        }
        return results;
    }

    private boolean isSearchIndexFieldLevelSecuritySupported() {
        if (getGraph() instanceof GraphWithSearchIndex) {
            return ((GraphWithSearchIndex) getGraph()).getSearchIndex().isFieldLevelSecuritySupported();
        }
        return true;
    }

    protected boolean isParitalUpdateOfVertexPropertyKeySupported() {
        return true;
    }

    private CardinalityResult queryGraphQueryWithCardinalityAggregation(String propertyName, Authorizations authorizations) {
        Query q = getGraph().query(searchAll().limit(0), authorizations);
        CardinalityAggregation agg = new CardinalityAggregation("card", propertyName);
        if (!q.isAggregationSupported(agg)) {
            LOGGER.warn("%s unsupported", CardinalityAggregation.class.getName());
            return null;
        }
        q.addAggregation(agg);
        return q.vertices().getAggregationResult("card", CardinalityResult.class);
    }

    private Map<String, Long> queryGraphQueryWithGeohashAggregation(String propertyName, int precision, Authorizations authorizations) {
        Query q = getGraph().query(searchAll().limit(0), authorizations);
        GeohashAggregation agg = new GeohashAggregation("geo-count", propertyName, precision);
        if (!q.isAggregationSupported(agg)) {
            LOGGER.warn("%s unsupported", GeohashAggregation.class.getName());
            return null;
        }
        q.addAggregation(agg);
        return geoHashBucketToMap(q.vertices().getAggregationResult("geo-count", GeohashResult.class).getBuckets());
    }

}
