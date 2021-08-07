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
import com.mware.ge.query.Compare;
import com.mware.ge.query.IterableWithScores;
import com.mware.ge.query.QueryResultsIterable;
import com.mware.ge.query.SortDirection;
import com.mware.ge.query.builder.GeQueryBuilders;
import com.mware.ge.scoring.FieldValueScoringStrategy;
import com.mware.ge.scoring.HammingDistanceScoringStrategy;
import com.mware.ge.scoring.ScoringStrategy;
import com.mware.ge.sorting.LengthOfStringSortingStrategy;
import com.mware.ge.sorting.SortingStrategy;
import com.mware.ge.values.storable.IntValue;
import com.mware.ge.values.storable.TextValue;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import java.util.List;

import static com.mware.core.model.schema.SchemaConstants.CONCEPT_TYPE_THING;
import static com.mware.ge.query.builder.GeQueryBuilders.*;
import static com.mware.ge.util.GeAssert.*;
import static com.mware.ge.util.GeAssert.addGraphEvent;
import static com.mware.ge.util.IterableUtils.count;
import static com.mware.ge.util.IterableUtils.toList;
import static com.mware.ge.values.storable.Values.intValue;
import static com.mware.ge.values.storable.Values.stringValue;
import static org.junit.Assume.assumeTrue;

@RunWith(JUnit4.class)
public abstract class GraphSortingScoringTests implements GraphTestSetup {
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
    public void testHammingDistanceScoringStrategy() {
        getGraph().defineProperty("prop1")
                .dataType(TextValue.class)
                .sortable(true)
                .textIndexHint(TextIndexHint.NONE)
                .define();
        getGraph().prepareVertex("v1", VISIBILITY_A, CONCEPT_TYPE_THING)
                .setProperty("prop1", stringValue("0000000000000000"), VISIBILITY_A)
                .save(AUTHORIZATIONS_A_AND_B);
        getGraph().prepareVertex("v2", VISIBILITY_A, CONCEPT_TYPE_THING)
                .setProperty("prop1", stringValue("ffffffffffffffff"), VISIBILITY_A)
                .save(AUTHORIZATIONS_A_AND_B);
        getGraph().prepareVertex("v3", VISIBILITY_A, CONCEPT_TYPE_THING)
                .setProperty("prop1", stringValue("0000000000000001"), VISIBILITY_A)
                .save(AUTHORIZATIONS_A_AND_B);
        getGraph().prepareVertex("v4", VISIBILITY_A, CONCEPT_TYPE_THING)
                .setProperty("prop1", stringValue("3000000000000000"), VISIBILITY_A)
                .save(AUTHORIZATIONS_A_AND_B);
        getGraph().flush();

        QueryResultsIterable<Vertex> vertices = getGraph().query(
                searchAll()
                        .scoringStrategy(getHammingDistanceScoringStrategy("prop1", "0000000000000000")), AUTHORIZATIONS_A
        ).vertices();
        assumeTrue("IterableWithScores", vertices instanceof IterableWithScores);
        IterableWithScores<Vertex> scores = (IterableWithScores<Vertex>) vertices;
        List<Vertex> verticesList = toList(vertices);
        assertEquals(4, verticesList.size());
        assertEquals("v1", verticesList.get(0).getId());
        assertEquals(64, scores.getScore("v1"), 0.0001);
        assertEquals("v3", verticesList.get(1).getId());
        assertEquals(63, scores.getScore("v3"), 0.0001);
        assertEquals("v4", verticesList.get(2).getId());
        assertEquals(62, scores.getScore("v4"), 0.0001);
        assertEquals("v2", verticesList.get(3).getId());
        assertEquals(0, scores.getScore("v2"), 0.0001);
    }

    @Test
    public void testGetLengthOfStringSortingStrategy() {
        getGraph().defineProperty("prop1")
                .dataType(TextValue.class)
                .sortable(true)
                .textIndexHint(TextIndexHint.ALL)
                .define();

        getGraph().prepareVertex("v1", VISIBILITY_A, CONCEPT_TYPE_THING)
                .setProperty("prop1", stringValue("aaaaa"), VISIBILITY_A)
                .save(AUTHORIZATIONS_A_AND_B);
        getGraph().prepareVertex("v2", VISIBILITY_A, CONCEPT_TYPE_THING)
                .setProperty("prop1", stringValue("bbb"), VISIBILITY_A)
                .save(AUTHORIZATIONS_A_AND_B);
        getGraph().prepareVertex("v3", VISIBILITY_A, CONCEPT_TYPE_THING)
                .addPropertyValue("k1", "prop1", stringValue("zzzzzzz"), VISIBILITY_A)
                .addPropertyValue("k2", "prop1", stringValue("z"), VISIBILITY_A)
                .save(AUTHORIZATIONS_A_AND_B);
        getGraph().prepareVertex("v4", VISIBILITY_A, CONCEPT_TYPE_THING)
                .setProperty("prop1", stringValue("cccc"), VISIBILITY_A)
                .save(AUTHORIZATIONS_A_AND_B);
        getGraph().prepareVertex("v5", VISIBILITY_A, CONCEPT_TYPE_THING)
                .save(AUTHORIZATIONS_A_AND_B);
        getGraph().prepareVertex("v6", VISIBILITY_A, CONCEPT_TYPE_THING)
                .addPropertyValue("k1", "prop1", stringValue("aaaaaaaaaaaaaaaaaaaaaa"), VISIBILITY_B)
                .addPropertyValue("k2", "prop1", stringValue("a"), VISIBILITY_B)
                .addPropertyValue("k3", "prop1", stringValue("ddddd"), VISIBILITY_A)
                .save(AUTHORIZATIONS_A_AND_B);
        getGraph().flush();

        QueryResultsIterable<Vertex> vertices = getGraph().query(searchAll().sort(getLengthOfStringSortingStrategy("prop1"), SortDirection.ASCENDING), AUTHORIZATIONS_A)
                .vertices();
        assertVertexIds(vertices, "v3", "v2", "v4", "v1", "v6", "v5");

        vertices = getGraph().query(searchAll().sort(getLengthOfStringSortingStrategy("prop1"), SortDirection.DESCENDING), AUTHORIZATIONS_A)
                .vertices();
        assertVertexIds(vertices, "v3", "v1", "v6", "v4", "v2", "v5");
    }


    @Test
    public void testMinimumScoreQueryParameter() {
        getGraph().prepareVertex("v1", VISIBILITY_A, CONCEPT_TYPE_THING)
                .setProperty("prop1", intValue(1), VISIBILITY_A)
                .save(AUTHORIZATIONS_A_AND_B);
        getGraph().prepareVertex("v2", VISIBILITY_A, CONCEPT_TYPE_THING)
                .setProperty("prop1", intValue(2), VISIBILITY_A)
                .save(AUTHORIZATIONS_A_AND_B);
        getGraph().prepareVertex("v3", VISIBILITY_A, CONCEPT_TYPE_THING)
                .setProperty("prop1", intValue(3), VISIBILITY_A)
                .save(AUTHORIZATIONS_A_AND_B);
        getGraph().flush();

        QueryResultsIterable<Vertex> vertices = getGraph().query(searchAll().scoringStrategy(getFieldValueScoringStrategy("prop1")).minScore(2), AUTHORIZATIONS_A)
                .vertices();
        assumeTrue("IterableWithScores", vertices instanceof IterableWithScores);
        assertEquals(2, Lists.newArrayList(vertices).size());
        IterableWithScores<Vertex> scores = (IterableWithScores<Vertex>) vertices;
        assertEquals(2, scores.getScore("v2"), 0.001);
        assertEquals(3, scores.getScore("v3"), 0.001);
        vertices = getGraph().query(searchAll().scoringStrategy(getFieldValueScoringStrategy("prop1")).minScore(4), AUTHORIZATIONS_A)
                .vertices();
        assertEquals(0, Lists.newArrayList(vertices).size());
    }

    @Test
    public void testGraphQuerySort() {
        String namePropertyName = "first.name";
        String agePropertyName = "age";
        String genderPropertyName = "gender";
        getGraph().defineProperty(agePropertyName).dataType(IntValue.class).sortable(true).define();
        getGraph().defineProperty(namePropertyName).dataType(TextValue.class).sortable(true).textIndexHint(TextIndexHint.EXACT_MATCH).define();
        getGraph().defineProperty(genderPropertyName).dataType(TextValue.class).sortable(true).textIndexHint(TextIndexHint.FULL_TEXT, TextIndexHint.EXACT_MATCH).define();

        getGraph().prepareVertex("v1", VISIBILITY_A, CONCEPT_TYPE_THING)
                .setProperty(namePropertyName, stringValue("joe"), VISIBILITY_A)
                .save(AUTHORIZATIONS_A_AND_B);
        getGraph().prepareVertex("v2", VISIBILITY_A, CONCEPT_TYPE_THING)
                .setProperty(namePropertyName, stringValue("bob"), VISIBILITY_B)
                .setProperty(agePropertyName, intValue(25), VISIBILITY_A)
                .save(AUTHORIZATIONS_A_AND_B);
        getGraph().prepareVertex("v3", VISIBILITY_A, CONCEPT_TYPE_THING)
                .setProperty(namePropertyName, stringValue("tom"), VISIBILITY_A)
                .setProperty(agePropertyName, intValue(30), VISIBILITY_B)
                .save(AUTHORIZATIONS_A_AND_B);
        getGraph().prepareVertex("v4", VISIBILITY_A, CONCEPT_TYPE_THING)
                .setProperty(namePropertyName, stringValue("tom"), VISIBILITY_A)
                .setProperty(agePropertyName, intValue(35), VISIBILITY_B)
                .save(AUTHORIZATIONS_A_AND_B);
        getGraph().prepareEdge("e1", "v1", "v2", LABEL_LABEL2, VISIBILITY_A).save(AUTHORIZATIONS_A_AND_B);
        getGraph().prepareEdge("e2", "v1", "v2", LABEL_LABEL1, VISIBILITY_A).save(AUTHORIZATIONS_A_AND_B);
        getGraph().prepareEdge("e3", "v1", "v2", LABEL_LABEL3, VISIBILITY_A).save(AUTHORIZATIONS_A_AND_B);
        getGraph().flush();

        List<Vertex> vertices = toList(getGraph().query(searchAll().sort(agePropertyName, SortDirection.ASCENDING), AUTHORIZATIONS_A_AND_B)
                .vertices());
        assertVertexIds(vertices, "v2", "v3", "v4", "v1");

        vertices = toList(getGraph().query(searchAll().sort(agePropertyName, SortDirection.DESCENDING), AUTHORIZATIONS_A_AND_B)
                .vertices());
        assertVertexIds(vertices, "v4", "v3", "v2", "v1");

        vertices = toList(getGraph().query(searchAll().sort(namePropertyName, SortDirection.ASCENDING), AUTHORIZATIONS_A_AND_B)
                .vertices());
        assertEquals(4, count(vertices));
        assertEquals("v2", vertices.get(0).getId());
        assertEquals("v1", vertices.get(1).getId());
        assertTrue(vertices.get(2).getId().equals("v3") || vertices.get(2).getId().equals("v4"));
        assertTrue(vertices.get(3).getId().equals("v3") || vertices.get(3).getId().equals("v4"));

        vertices = toList(getGraph().query(searchAll().sort(namePropertyName, SortDirection.DESCENDING), AUTHORIZATIONS_A_AND_B)
                .vertices());
        assertEquals(4, count(vertices));
        assertTrue(vertices.get(0).getId().equals("v3") || vertices.get(0).getId().equals("v4"));
        assertTrue(vertices.get(1).getId().equals("v3") || vertices.get(1).getId().equals("v4"));
        assertEquals("v1", vertices.get(2).getId());
        assertEquals("v2", vertices.get(3).getId());

        vertices = toList(getGraph().query(
                searchAll()
                        .sort(namePropertyName, SortDirection.ASCENDING)
                        .sort(agePropertyName, SortDirection.ASCENDING),
                AUTHORIZATIONS_A_AND_B
        ).vertices());
        assertVertexIds(vertices, "v2", "v1", "v3", "v4");

        vertices = toList(getGraph().query(
                searchAll()
                        .sort(namePropertyName, SortDirection.ASCENDING)
                        .sort(agePropertyName, SortDirection.DESCENDING),
                AUTHORIZATIONS_A_AND_B
        ).vertices());
        assertVertexIds(vertices, "v2", "v1", "v4", "v3");

        vertices = toList(getGraph().query(searchAll().sort(Element.ID_PROPERTY_NAME, SortDirection.ASCENDING), AUTHORIZATIONS_A_AND_B)
                .vertices());
        assertVertexIds(vertices, "v1", "v2", "v3", "v4");

        vertices = toList(getGraph().query(searchAll().sort(Element.ID_PROPERTY_NAME, SortDirection.DESCENDING), AUTHORIZATIONS_A_AND_B)
                .vertices());
        assertVertexIds(vertices, "v4", "v3", "v2", "v1");

        vertices = toList(getGraph().query(searchAll().sort("otherfield", SortDirection.ASCENDING), AUTHORIZATIONS_A_AND_B)
                .vertices());
        assertEquals(4, count(vertices));

        List<Edge> edges = toList(getGraph().query(searchAll().sort(Edge.LABEL_PROPERTY_NAME, SortDirection.ASCENDING), AUTHORIZATIONS_A_AND_B)
                .edges());
        assertEdgeIds(edges, "e2", "e1", "e3");

        edges = toList(getGraph().query(searchAll().sort(Edge.LABEL_PROPERTY_NAME, SortDirection.DESCENDING), AUTHORIZATIONS_A_AND_B)
                .edges());
        assertEdgeIds(edges, "e3", "e1", "e2");

        edges = toList(getGraph().query(
                searchAll()
                        .sort(Edge.OUT_VERTEX_ID_PROPERTY_NAME, SortDirection.ASCENDING)
                        .sort(Element.ID_PROPERTY_NAME, SortDirection.ASCENDING),
                AUTHORIZATIONS_A_AND_B
        ).edges());
        assertEdgeIds(edges, "e1", "e2", "e3");
        edges = toList(getGraph().query(
                searchAll()
                        .sort(Edge.IN_VERTEX_ID_PROPERTY_NAME, SortDirection.ASCENDING)
                        .sort(Element.ID_PROPERTY_NAME, SortDirection.ASCENDING),
                AUTHORIZATIONS_A_AND_B
        ).edges());
        assertEdgeIds(edges, "e1", "e2", "e3");

        getGraph().prepareVertex("v5", VISIBILITY_A, CONCEPT_TYPE_THING)
                .setProperty(genderPropertyName, stringValue("female"), VISIBILITY_A)
                .addExtendedData("table1", "row1", "column1", stringValue("value1"), VISIBILITY_A)
                .save(AUTHORIZATIONS_A_AND_B);
        getGraph().prepareVertex("v6", VISIBILITY_A, CONCEPT_TYPE_THING)
                .setProperty(genderPropertyName, stringValue("male"), VISIBILITY_A)
                .save(AUTHORIZATIONS_A_AND_B);
        getGraph().flush();

        vertices = toList(getGraph().query(searchAll().sort(genderPropertyName, SortDirection.ASCENDING), AUTHORIZATIONS_A_AND_B)
                .vertices());
        assertEquals(6, count(vertices));
        assertEquals("v5", vertices.get(0).getId());
        assertEquals("v6", vertices.get(1).getId());
        assertTrue(vertices.get(2).getId().equals("v2") || vertices.get(2).getId().equals("v1"));
        assertTrue(vertices.get(3).getId().equals("v2") || vertices.get(3).getId().equals("v1"));
        assertTrue(vertices.get(4).getId().equals("v3") || vertices.get(4).getId().equals("v4"));
        assertTrue(vertices.get(5).getId().equals("v3") || vertices.get(5).getId().equals("v4"));
    }

    @Test
    public void testGraphQuerySortOnPropertyThatHasNoValuesInTheIndex() {
        getGraph().defineProperty("age").dataType(IntValue.class).sortable(true).define();

        getGraph().prepareVertex("v1", VISIBILITY_A, CONCEPT_TYPE_THING)
                .setProperty("name", stringValue("joe"), VISIBILITY_A)
                .save(AUTHORIZATIONS_A_AND_B);
        getGraph().prepareVertex("v2", VISIBILITY_A, CONCEPT_TYPE_THING)
                .setProperty("name", stringValue("bob"), VISIBILITY_B)
                .save(AUTHORIZATIONS_A_AND_B);
        getGraph().flush();

        QueryResultsIterable<Vertex> vertices = getGraph().query(searchAll().sort("age", SortDirection.ASCENDING), AUTHORIZATIONS_A).vertices();
        assertEquals(2, count(vertices));
    }

    @Test
    public void testGraphQuerySortOnPropertyWhichIsFullTextAndExactMatchIndexed() {
        getGraph().defineProperty("name")
                .dataType(TextValue.class)
                .sortable(true)
                .textIndexHint(TextIndexHint.EXACT_MATCH, TextIndexHint.FULL_TEXT)
                .define();

        getGraph().prepareVertex("v1", VISIBILITY_A, CONCEPT_TYPE_THING)
                .setProperty("name", stringValue("1-2"), VISIBILITY_A)
                .save(AUTHORIZATIONS_A_AND_B);
        getGraph().prepareVertex("v2", VISIBILITY_A, CONCEPT_TYPE_THING)
                .setProperty("name", stringValue("1-1"), VISIBILITY_B)
                .save(AUTHORIZATIONS_A_AND_B);
        getGraph().prepareVertex("v3", VISIBILITY_A, CONCEPT_TYPE_THING)
                .setProperty("name", stringValue("3-1"), VISIBILITY_B)
                .save(AUTHORIZATIONS_A_AND_B);
        getGraph().flush();

        QueryResultsIterable<Vertex> vertices
                = getGraph().query(searchAll().sort("name", SortDirection.ASCENDING), AUTHORIZATIONS_A_AND_B).vertices();
        assertVertexIds(vertices, "v2", "v1", "v3");

        vertices = getGraph().query("3", AUTHORIZATIONS_A_AND_B).vertices();
        assertVertexIds(vertices, "v3");

        vertices = getGraph().query(
                boolQuery()
                        .and(searchAll())
                        .and(hasFilter("name", Compare.EQUAL, stringValue("3-1"))),
                AUTHORIZATIONS_A_AND_B
        ).vertices();
        assertVertexIds(vertices, "v3");
    }

    protected ScoringStrategy getHammingDistanceScoringStrategy(String field, String hash) {
        return new HammingDistanceScoringStrategy(field, hash);
    }

    protected SortingStrategy getLengthOfStringSortingStrategy(String propertyName) {
        return new LengthOfStringSortingStrategy(propertyName);
    }

    protected ScoringStrategy getFieldValueScoringStrategy(String field) {
        return new FieldValueScoringStrategy(field);
    }
}
