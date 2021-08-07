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
import com.mware.ge.event.*;
import com.mware.ge.metric.StackTraceTracker;
import com.mware.ge.mutation.ElementMutation;
import com.mware.ge.mutation.ExistingElementMutation;
import com.mware.ge.query.Compare;
import com.mware.ge.query.QueryResultsIterable;
import com.mware.ge.query.SortDirection;
import com.mware.ge.query.builder.GeQueryBuilders;
import com.mware.ge.search.DefaultSearchIndex;
import com.mware.ge.search.IndexHint;
import com.mware.ge.search.SearchIndex;
import com.mware.ge.util.GeLogger;
import com.mware.ge.util.GeLoggerFactory;
import com.mware.ge.util.IncreasingTime;
import com.mware.ge.util.IterableUtils;
import com.mware.ge.values.storable.*;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import java.time.*;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import static com.mware.core.model.schema.SchemaConstants.*;
import static com.mware.core.model.schema.SchemaConstants.CONCEPT_TYPE_AUDIO;
import static com.mware.ge.query.Compare.EQUAL;
import static com.mware.ge.query.builder.GeQueryBuilders.boolQuery;
import static com.mware.ge.query.builder.GeQueryBuilders.hasFilter;
import static com.mware.ge.util.GeAssert.*;
import static com.mware.ge.util.IterableUtils.count;
import static com.mware.ge.util.IterableUtils.toList;
import static com.mware.ge.values.storable.Values.*;
import static org.junit.Assume.assumeTrue;

@RunWith(JUnit4.class)
public abstract class GraphBaseTests implements GraphTestSetup {
    private static final GeLogger LOGGER = GeLoggerFactory.getLogger(GraphBaseTests.class);
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
    public void testElementId() {
        getGraph().prepareVertex("v1", VISIBILITY_A, CONCEPT_TYPE_THING).save(AUTHORIZATIONS_A);
        getGraph().prepareVertex("v2", VISIBILITY_A, CONCEPT_TYPE_THING).save(AUTHORIZATIONS_A);
        getGraph().prepareEdge("e1", "v1", "v2", "label", VISIBILITY_A).save(AUTHORIZATIONS_A);
        getGraph().flush();

        Vertex v1 = getGraph().getVertex("v1", AUTHORIZATIONS_A);
        assertTrue(v1.equals(ElementId.vertex("v1")));
        assertTrue(ElementId.vertex("v1").equals(v1));
        assertEquals(v1.hashCode(), ElementId.vertex("v1").hashCode());

        Edge e1 = getGraph().getEdge("e1", AUTHORIZATIONS_A);
        assertTrue(e1.equals(ElementId.edge("e1")));
        assertTrue(ElementId.edge("e1").equals(e1));
        assertEquals(e1.hashCode(), ElementId.vertex("e1").hashCode());
    }

    @Test
    public void testAddVertexWithId() {
        Vertex vertexAdded = getGraph().addVertex("v1", VISIBILITY_A, AUTHORIZATIONS_A, CONCEPT_TYPE_THING);
        assertNotNull(vertexAdded);
        assertEquals("v1", vertexAdded.getId());
        getGraph().flush();

        Vertex v = getGraph().getVertex("v1", AUTHORIZATIONS_A);
        assertNotNull(v);
        assertEquals("v1", v.getId());
        assertEquals(VISIBILITY_A, v.getVisibility());

        v = getGraph().getVertex("", AUTHORIZATIONS_A);
        assertNull(v);

        v = getGraph().getVertex(null, AUTHORIZATIONS_A);
        assertNull(v);

        assertEvents(
                new AddVertexEvent(getGraph(), vertexAdded)
        );
    }

    @Test
    public void testAddVertexWithoutId() {
        Vertex vertexAdded = getGraph().addVertex(VISIBILITY_A, AUTHORIZATIONS_A, CONCEPT_TYPE_THING);
        assertNotNull(vertexAdded);
        String vertexId = vertexAdded.getId();
        assertNotNull(vertexId);
        getGraph().flush();

        Vertex v = getGraph().getVertex(vertexId, AUTHORIZATIONS_A);
        assertNotNull(v);
        assertNotNull(vertexId);

        assertEvents(
                new AddVertexEvent(getGraph(), vertexAdded)
        );
    }

    @Test
    public void testGetSingleVertexWithSameRowPrefix() {
        getGraph().addVertex("prefix", VISIBILITY_EMPTY, AUTHORIZATIONS_EMPTY, CONCEPT_TYPE_THING);
        getGraph().addVertex("prefixA", VISIBILITY_EMPTY, AUTHORIZATIONS_EMPTY, CONCEPT_TYPE_THING);
        getGraph().flush();

        Vertex v = getGraph().getVertex("prefix", AUTHORIZATIONS_EMPTY);
        assertEquals("prefix", v.getId());

        v = getGraph().getVertex("prefixA", AUTHORIZATIONS_EMPTY);
        assertEquals("prefixA", v.getId());
    }

    @Test
    public void testAddVertexPropertyWithMetadata() {
        Metadata prop1Metadata = Metadata.create();
        prop1Metadata.add("metadata1", stringValue("metadata1Value"), VISIBILITY_A);

        getGraph().prepareVertex("v1", VISIBILITY_A, CONCEPT_TYPE_THING)
                .setProperty("prop1", stringValue("value1"), prop1Metadata, VISIBILITY_A)
                .save(AUTHORIZATIONS_A_AND_B);
        getGraph().flush();

        AtomicInteger vertexCount = new AtomicInteger();
        AtomicInteger vertexPropertyCount = new AtomicInteger();
        getGraph().visitElements(new DefaultGraphVisitor() {
            @Override
            public void visitVertex(Vertex vertex) {
                vertexCount.incrementAndGet();
            }

            @Override
            public void visitProperty(Element element, Property property) {
                vertexPropertyCount.incrementAndGet();
            }
        }, AUTHORIZATIONS_A);
        assertEquals(1, vertexCount.get());
        assertEquals(1, vertexPropertyCount.get());

        Vertex v = getGraph().getVertex("v1", FetchHints.ALL, AUTHORIZATIONS_A);
        if (v instanceof HasTimestamp) {
            assertTrue("timestamp should be more than 0", v.getTimestamp() > 0);
        }

        assertEquals(1, count(v.getProperties("prop1")));
        Property prop1 = v.getProperties("prop1").iterator().next();
        if (prop1 instanceof HasTimestamp) {
            assertTrue("timestamp should be more than 0", prop1.getTimestamp() > 0);
        }

        prop1Metadata = prop1.getMetadata();
        assertNotNull(prop1Metadata);
        assertEquals(1, prop1Metadata.entrySet().size());
        assertEquals(stringValue("metadata1Value"), prop1Metadata.getEntry("metadata1", VISIBILITY_A).getValue());

        prop1Metadata.add("metadata2", stringValue("metadata2Value"), VISIBILITY_A);
        v.prepareMutation()
                .setProperty("prop1", stringValue("value1"), prop1Metadata, VISIBILITY_A)
                .save(AUTHORIZATIONS_A_AND_B);
        getGraph().flush();

        v = getGraph().getVertex("v1", FetchHints.ALL, AUTHORIZATIONS_A);
        assertEquals(1, count(v.getProperties("prop1")));
        prop1 = v.getProperties("prop1").iterator().next();
        prop1Metadata = prop1.getMetadata();
        assertEquals(2, prop1Metadata.entrySet().size());
        assertEquals(stringValue("metadata1Value"), prop1Metadata.getEntry("metadata1", VISIBILITY_A).getValue());
        assertEquals(stringValue("metadata2Value"), prop1Metadata.getEntry("metadata2", VISIBILITY_A).getValue());

        // make sure that when we update the value the metadata is not carried over
        prop1Metadata = Metadata.create();
        v.setProperty("prop1", stringValue("value2"), prop1Metadata, VISIBILITY_A, AUTHORIZATIONS_A_AND_B);
        getGraph().flush();

        v = getGraph().getVertex("v1", FetchHints.ALL, AUTHORIZATIONS_A);
        assertEquals(1, count(v.getProperties("prop1")));
        prop1 = v.getProperties("prop1").iterator().next();
        assertEquals(stringValue("value2"), prop1.getValue());
        prop1Metadata = prop1.getMetadata();
        assertEquals(0, prop1Metadata.entrySet().size());
    }

    @Test
    public void testAddVertexWithProperties() {
        Vertex vertexAdded = getGraph().prepareVertex("v1", VISIBILITY_A, CONCEPT_TYPE_THING)
                .setProperty("prop1", stringValue("value1"), VISIBILITY_A)
                .setProperty("prop2", stringValue("value2"), VISIBILITY_B)
                .save(AUTHORIZATIONS_A_AND_B);

        Iterable<Property> p = vertexAdded.getProperties("prop1");

        assertEquals(1, count(vertexAdded.getProperties("prop1")));
        assertEquals(stringValue("value1"), vertexAdded.getPropertyValues("prop1").iterator().next());
        assertEquals(1, count(vertexAdded.getProperties("prop2")));
        assertEquals(stringValue("value2"), vertexAdded.getPropertyValues("prop2").iterator().next());
        getGraph().flush();

        Vertex v = getGraph().getVertex("v1", AUTHORIZATIONS_A_AND_B);
        assertEquals(1, count(v.getProperties("prop1")));
        assertEquals(stringValue("value1"), v.getPropertyValues("prop1").iterator().next());
        assertEquals(1, count(v.getProperties("prop2")));
        assertEquals(stringValue("value2"), v.getPropertyValues("prop2").iterator().next());

        assertEvents(
                new AddVertexEvent(getGraph(), vertexAdded),
                new AddPropertyEvent(getGraph(), vertexAdded, vertexAdded.getProperty("prop1")),
                new AddPropertyEvent(getGraph(), vertexAdded, vertexAdded.getProperty("prop2"))
        );
        clearGraphEvents();

        v = getGraph().getVertex("v1", AUTHORIZATIONS_A_AND_B);
        vertexAdded = v.prepareMutation()
                .addPropertyValue("key1", "prop1Mutation", stringValue("value1Mutation"), VISIBILITY_A)
                .save(AUTHORIZATIONS_A_AND_B);
        getGraph().flush();
        v = getGraph().getVertex("v1", AUTHORIZATIONS_A_AND_B);
        assertEquals(1, count(v.getProperties("prop1Mutation")));
        assertEquals(stringValue("value1Mutation"), v.getPropertyValues("prop1Mutation").iterator().next());
        assertEvents(
                new AddPropertyEvent(getGraph(), vertexAdded, vertexAdded.getProperty("prop1Mutation"))
        );
    }

    @Test
    public void testNullPropertyValue() {
        try {
            getGraph().prepareVertex("v1", VISIBILITY_EMPTY, CONCEPT_TYPE_THING)
                    .setProperty("prop1", null, VISIBILITY_A)
                    .save(AUTHORIZATIONS_A_AND_B);
            throw new GeException("expected null check");
        } catch (NullPointerException ex) {
            assertTrue(ex.getMessage().contains("prop1"));
        }
    }

    @Test
    public void testConcurrentModificationOfProperties() {
        Vertex v = getGraph().prepareVertex("v1", VISIBILITY_EMPTY, CONCEPT_TYPE_THING)
                .setProperty("prop1", stringValue("value1"), VISIBILITY_A)
                .setProperty("prop2", stringValue("value2"), VISIBILITY_A)
                .save(AUTHORIZATIONS_A_AND_B);

        int i = 0;
        for (Property p : v.getProperties()) {
            assertNotNull(p.toString());
            if (i == 0) {
                v.setProperty("prop3", stringValue("value3"), VISIBILITY_A, AUTHORIZATIONS_A_AND_B);
            }
            i++;
        }
    }

    @Test
    public void testAddVertexWithPropertiesWithTwoDifferentVisibilities() {
        Vertex v = getGraph().prepareVertex("v1", VISIBILITY_EMPTY, CONCEPT_TYPE_THING)
                .setProperty("prop1", stringValue("value1a"), VISIBILITY_A)
                .setProperty("prop1", stringValue("value1b"), VISIBILITY_B)
                .save(AUTHORIZATIONS_A_AND_B);
        assertEquals(2, count(v.getProperties("prop1")));
        getGraph().flush();

        v = getGraph().getVertex("v1", AUTHORIZATIONS_A_AND_B);
        assertEquals(2, count(v.getProperties("prop1")));

        v = getGraph().getVertex("v1", AUTHORIZATIONS_A);
        assertEquals(1, count(v.getProperties("prop1")));
        assertEquals(stringValue("value1a"), v.getPropertyValue("prop1"));

        v = getGraph().getVertex("v1", AUTHORIZATIONS_B);
        assertEquals(1, count(v.getProperties("prop1")));
        assertEquals(stringValue("value1b"), v.getPropertyValue("prop1"));
    }

    @Test
    public void testMultivaluedProperties() {
        Vertex v = getGraph().addVertex("v1", VISIBILITY_A, AUTHORIZATIONS_ALL, CONCEPT_TYPE_THING);

        v.prepareMutation()
                .addPropertyValue("propid1a", "prop1", stringValue("value1a"), VISIBILITY_A)
                .addPropertyValue("propid2a", "prop2", stringValue("value2a"), VISIBILITY_A)
                .addPropertyValue("propid3a", "prop3", stringValue("value3a"), VISIBILITY_A)
                .save(AUTHORIZATIONS_ALL);
        getGraph().flush();
        v = getGraph().getVertex("v1", AUTHORIZATIONS_A);
        assertEquals(stringValue("value1a"), v.getPropertyValues("prop1").iterator().next());
        assertEquals(stringValue("value2a"), v.getPropertyValues("prop2").iterator().next());
        assertEquals(stringValue("value3a"), v.getPropertyValues("prop3").iterator().next());
        assertEquals(3, count(v.getProperties()));

        v.prepareMutation()
                .addPropertyValue("propid1a", "prop1", stringValue("value1b"), VISIBILITY_A)
                .addPropertyValue("propid2a", "prop2", stringValue("value2b"), VISIBILITY_A)
                .save(AUTHORIZATIONS_A_AND_B);
        getGraph().flush();
        v = getGraph().getVertex("v1", AUTHORIZATIONS_A);
        assertEquals(1, count(v.getPropertyValues("prop1")));
        assertEquals(stringValue("value1b"), v.getPropertyValues("prop1").iterator().next());
        assertEquals(1, count(v.getPropertyValues("prop2")));
        assertEquals(stringValue("value2b"), v.getPropertyValues("prop2").iterator().next());
        assertEquals(1, count(v.getPropertyValues("prop3")));
        assertEquals(stringValue("value3a"), v.getPropertyValues("prop3").iterator().next());
        assertEquals(3, count(v.getProperties()));

        v.addPropertyValue("propid1b", "prop1", stringValue("value1a-new"), VISIBILITY_A, AUTHORIZATIONS_A_AND_B);
        getGraph().flush();
        v = getGraph().getVertex("v1", AUTHORIZATIONS_A);
        IterableUtils.assertContains(stringValue("value1b"), v.getPropertyValues("prop1"));
        IterableUtils.assertContains(stringValue("value1a-new"), v.getPropertyValues("prop1"));
        assertEquals(4, count(v.getProperties()));
    }

    @Test
    public void testMultivaluedPropertyOrder() {
        getGraph().prepareVertex("v1", VISIBILITY_A, CONCEPT_TYPE_THING)
                .addPropertyValue("a", "prop", stringValue("a"), VISIBILITY_A)
                .addPropertyValue("aa", "prop", stringValue("aa"), VISIBILITY_A)
                .addPropertyValue("b", "prop", stringValue("b"), VISIBILITY_A)
                .addPropertyValue("0", "prop", stringValue("0"), VISIBILITY_A)
                .addPropertyValue("A", "prop", stringValue("A"), VISIBILITY_A)
                .addPropertyValue("Z", "prop", stringValue("Z"), VISIBILITY_A)
                .save(AUTHORIZATIONS_A_AND_B);
        getGraph().flush();

        Vertex v1 = getGraph().getVertex("v1", AUTHORIZATIONS_A);
        assertEquals(stringValue("0"), v1.getPropertyValue("prop", 0));
        assertEquals(stringValue("A"), v1.getPropertyValue("prop", 1));
        assertEquals(stringValue("Z"), v1.getPropertyValue("prop", 2));
        assertEquals(stringValue("a"), v1.getPropertyValue("prop", 3));
        assertEquals(stringValue("aa"), v1.getPropertyValue("prop", 4));
        assertEquals(stringValue("b"), v1.getPropertyValue("prop", 5));
    }

    @Test
    public void testDeleteProperty() {
        Vertex v = getGraph().addVertex("v1", VISIBILITY_A, AUTHORIZATIONS_ALL, CONCEPT_TYPE_THING);

        v.prepareMutation()
                .addPropertyValue("propid1a", "prop1", stringValue("value1a"), VISIBILITY_A)
                .addPropertyValue("propid1b", "prop1", stringValue("value1b"), VISIBILITY_A)
                .addPropertyValue("propid2a", "prop2", stringValue("value2a"), VISIBILITY_A)
                .save(AUTHORIZATIONS_ALL);
        getGraph().flush();
        clearGraphEvents();

        v = getGraph().getVertex("v1", FetchHints.ALL, AUTHORIZATIONS_A);
        Property prop1_propid1a = v.getProperty("propid1a", "prop1");
        Property prop1_propid1b = v.getProperty("propid1b", "prop1");
        v.deleteProperties("prop1", AUTHORIZATIONS_A_AND_B);
        getGraph().flush();
        getGraph().dumpGraph();
        assertEquals(1, count(v.getProperties()));
        v = getGraph().getVertex("v1", FetchHints.ALL, AUTHORIZATIONS_A);
        assertEquals(1, count(v.getProperties()));

        assertEquals(1, count(getGraph().query(hasFilter("prop2", EQUAL, stringValue("value2a")), AUTHORIZATIONS_A_AND_B).vertices()));
        assertEquals(0, count(getGraph().query(hasFilter("prop1", EQUAL, stringValue("value1a")), AUTHORIZATIONS_A_AND_B).vertices()));
        assertEvents(
                new DeletePropertyEvent(getGraph(), v, prop1_propid1a),
                new DeletePropertyEvent(getGraph(), v, prop1_propid1b)
        );
        clearGraphEvents();

        Property prop2_propid2a = v.getProperty("propid2a", "prop2");
        v.deleteProperty("propid2a", "prop2", AUTHORIZATIONS_A_AND_B);
        getGraph().flush();
        assertEquals(0, count(v.getProperties()));
        v = getGraph().getVertex("v1", AUTHORIZATIONS_A);
        assertEquals(0, count(v.getProperties()));

        assertEvents(
                new DeletePropertyEvent(getGraph(), v, prop2_propid2a)
        );
    }

    @Test
    public void testDeletePropertyWithMutation() {
        Vertex v1 = getGraph().prepareVertex("v1", VISIBILITY_A, CONCEPT_TYPE_THING)
                .addPropertyValue("propid1a", "prop1", stringValue("value1a"), VISIBILITY_A)
                .addPropertyValue("propid1b", "prop1", stringValue("value1b"), VISIBILITY_A)
                .addPropertyValue("propid2a", "prop2", stringValue("value2a"), VISIBILITY_A)
                .save(AUTHORIZATIONS_A_AND_B);
        Vertex v2 = getGraph().addVertex("v2", VISIBILITY_A, AUTHORIZATIONS_A, CONCEPT_TYPE_THING);
        getGraph().prepareEdge("e1", v1, v2, LABEL_LABEL1, VISIBILITY_A)
                .addPropertyValue("key1", "prop1", stringValue("value1"), VISIBILITY_A)
                .save(AUTHORIZATIONS_A);
        getGraph().flush();
        clearGraphEvents();

        // delete multiple properties
        v1 = getGraph().getVertex("v1", FetchHints.ALL, AUTHORIZATIONS_A);
        Property prop1_propid1a = v1.getProperty("propid1a", "prop1");
        Property prop1_propid1b = v1.getProperty("propid1b", "prop1");
        v1.prepareMutation()
                .deleteProperties("prop1")
                .save(AUTHORIZATIONS_A_AND_B);
        getGraph().flush();
        assertEquals(1, count(v1.getProperties()));
        v1 = getGraph().getVertex("v1", FetchHints.ALL, AUTHORIZATIONS_A);
        assertEquals(1, count(v1.getProperties()));

        assertEquals(1, count(getGraph().query(hasFilter("prop2", EQUAL, stringValue("value2a")), AUTHORIZATIONS_A_AND_B).vertices()));
        assertEquals(0, count(getGraph().query(hasFilter("prop1", EQUAL, stringValue("value1a")), AUTHORIZATIONS_A_AND_B).vertices()));
        assertEvents(
                new DeletePropertyEvent(getGraph(), v1, prop1_propid1a),
                new DeletePropertyEvent(getGraph(), v1, prop1_propid1b)
        );
        clearGraphEvents();

        // delete property with key and name
        Property prop2_propid2a = v1.getProperty("propid2a", "prop2");
        v1.prepareMutation()
                .deleteProperties("propid2a", "prop2")
                .save(AUTHORIZATIONS_A_AND_B);
        getGraph().flush();
        assertEquals(0, count(v1.getProperties()));
        v1 = getGraph().getVertex("v1", AUTHORIZATIONS_A);
        assertEquals(0, count(v1.getProperties()));
        assertEvents(
                new DeletePropertyEvent(getGraph(), v1, prop2_propid2a)
        );
        clearGraphEvents();

        // delete property from edge
        Edge e1 = getGraph().getEdge("e1", FetchHints.ALL, AUTHORIZATIONS_A);
        Property edgeProperty = e1.getProperty("key1", "prop1");
        e1.prepareMutation()
                .deleteProperties("key1", "prop1")
                .save(AUTHORIZATIONS_A_AND_B);
        getGraph().flush();
        assertEquals(0, count(e1.getProperties()));
        e1 = getGraph().getEdge("e1", AUTHORIZATIONS_A);
        assertEquals(0, count(e1.getProperties()));
        assertEvents(
                new DeletePropertyEvent(getGraph(), e1, edgeProperty)
        );
    }

    @Test
    public void testDeleteElement() {
        Vertex v = getGraph().addVertex("v1", VISIBILITY_A, AUTHORIZATIONS_ALL, CONCEPT_TYPE_THING);

        v.prepareMutation()
                .setProperty("prop1", stringValue("value1"), VISIBILITY_A)
                .save(AUTHORIZATIONS_ALL);
        getGraph().flush();

        v = getGraph().getVertex("v1", AUTHORIZATIONS_A);
        assertNotNull(v);
        assertEquals(1, count(getGraph().query(hasFilter("prop1", EQUAL, stringValue("value1")), AUTHORIZATIONS_A_AND_B).vertices()));

        getGraph().deleteVertex(v.getId(), AUTHORIZATIONS_A_AND_B);
        getGraph().flush();

        v = getGraph().getVertex("v1", AUTHORIZATIONS_A);
        assertNull(v);
        assertEquals(0, count(getGraph().query(hasFilter("prop1", EQUAL, stringValue("value1")), AUTHORIZATIONS_A_AND_B).vertices()));
    }

    @Test
    public void testDeleteElements() {
        getGraph().prepareVertex("v1", VISIBILITY_A, CONCEPT_TYPE_THING)
                .setProperty("prop1", stringValue("value1"), VISIBILITY_A)
                .addExtendedData("table1", "row1", "column1", stringValue("value1"), VISIBILITY_A)
                .save(AUTHORIZATIONS_ALL);
        getGraph().prepareVertex("v2", VISIBILITY_A, CONCEPT_TYPE_THING)
                .setProperty("prop1", stringValue("value1"), VISIBILITY_A)
                .save(AUTHORIZATIONS_ALL);
        getGraph().prepareEdge("e1", "v1", "v2", "label1", VISIBILITY_A)
                .addExtendedData("table1", "row1", "column1", stringValue("value1"), VISIBILITY_A)
                .save(AUTHORIZATIONS_ALL);
        getGraph().flush();

        List<ElementId> elements = new ArrayList<>();
        FetchHints fetchHints = new FetchHintsBuilder(FetchHints.EDGE_REFS)
                .setIncludeExtendedDataTableNames(true)
                .build();
        elements.add(getGraph().getVertex("v1", fetchHints, AUTHORIZATIONS_A));
        elements.add(ElementId.vertex("v2"));
        getGraph().deleteElements(elements.stream(), AUTHORIZATIONS_A_AND_B);
        getGraph().flush();

        assertNull(getGraph().getVertex("v1", AUTHORIZATIONS_A));
        assertNull(getGraph().getVertex("v2", AUTHORIZATIONS_A));
        assertNull(getGraph().getEdge("e1", AUTHORIZATIONS_A));
        assertResultsCount(0, 0, getGraph().query(hasFilter("prop1", EQUAL, stringValue("value1")), AUTHORIZATIONS_A_AND_B).vertices());
        assertResultsCount(0, 0, getGraph().query(AUTHORIZATIONS_A_AND_B).vertices());
        assertResultsCount(0, 0, getGraph().query(AUTHORIZATIONS_A_AND_B).edges());
        assertResultsCount(0, 0, getGraph().query(AUTHORIZATIONS_A_AND_B).extendedDataRows());
    }

    @Test
    public void testDeleteVertex() {
        getGraph().addVertex("v1", VISIBILITY_A, AUTHORIZATIONS_A, CONCEPT_TYPE_THING);
        getGraph().addVertex("v2", VISIBILITY_A, AUTHORIZATIONS_A, CONCEPT_TYPE_THING);
        getGraph().addEdge("e1", "v1", "v2", "label1", VISIBILITY_A, AUTHORIZATIONS_A);
        getGraph().flush();
        assertVertexIds(getGraph().getVertices(AUTHORIZATIONS_A), "v1", "v2");
        assertEdgeIds(getGraph().getEdges(AUTHORIZATIONS_A), "e1");

        getGraph().deleteVertex("v1", AUTHORIZATIONS_A);
        getGraph().flush();
        assertVertexIds(getGraph().getVertices(AUTHORIZATIONS_A), "v2");
        assertEdgeIds(getGraph().getEdges(AUTHORIZATIONS_A));
    }

    @Test
    public void testAddEdge() {
        Vertex v1 = getGraph().addVertex("v1", VISIBILITY_A, AUTHORIZATIONS_A, CONCEPT_TYPE_THING);
        Vertex v2 = getGraph().addVertex("v2", VISIBILITY_A, AUTHORIZATIONS_A, CONCEPT_TYPE_THING);
        Edge addedEdge = getGraph().addEdge("e1", v1, v2, LABEL_LABEL1, VISIBILITY_A, AUTHORIZATIONS_A);
        getGraph().flush();
        assertNotNull(addedEdge);
        assertEquals("e1", addedEdge.getId());
        assertEquals(LABEL_LABEL1, addedEdge.getLabel());
        assertEquals("v1", addedEdge.getVertexId(Direction.OUT));
        assertEquals(v1, addedEdge.getVertex(Direction.OUT, AUTHORIZATIONS_A));
        assertEquals("v2", addedEdge.getVertexId(Direction.IN));
        assertEquals(v2, addedEdge.getVertex(Direction.IN, AUTHORIZATIONS_A));
        assertEquals(VISIBILITY_A, addedEdge.getVisibility());

        EdgeVertices addedEdgeVertices = addedEdge.getVertices(AUTHORIZATIONS_A);
        assertEquals(v1, addedEdgeVertices.getOutVertex());
        assertEquals(v2, addedEdgeVertices.getInVertex());

        FetchHints propertiesFetchHints = FetchHints.builder()
                .setIncludeAllProperties(true)
                .build();
        FetchHints inEdgeRefsFetchHints = FetchHints.builder()
                .setIncludeInEdgeRefs(true)
                .build();
        FetchHints outEdgeRefsFetchHints = FetchHints.builder()
                .setIncludeOutEdgeRefs(true)
                .build();

        getGraph().getVertex("v1", FetchHints.NONE, AUTHORIZATIONS_A);
        getGraph().getVertex("v1", getGraph().getDefaultFetchHints(), AUTHORIZATIONS_A);
        getGraph().getVertex("v1", propertiesFetchHints, AUTHORIZATIONS_A);
        getGraph().getVertex("v1", FetchHints.EDGE_REFS, AUTHORIZATIONS_A);
        getGraph().getVertex("v1", inEdgeRefsFetchHints, AUTHORIZATIONS_A);
        getGraph().getVertex("v1", outEdgeRefsFetchHints, AUTHORIZATIONS_A);

        getGraph().getEdge("e1", FetchHints.NONE, AUTHORIZATIONS_A);
        getGraph().getEdge("e1", getGraph().getDefaultFetchHints(), AUTHORIZATIONS_A);
        getGraph().getEdge("e1", propertiesFetchHints, AUTHORIZATIONS_A);

        Edge e = getGraph().getEdge("e1", AUTHORIZATIONS_B);
        assertNull(e);

        e = getGraph().getEdge("e1", AUTHORIZATIONS_A);
        assertNotNull(e);
        assertEquals("e1", e.getId());
        assertEquals(LABEL_LABEL1, e.getLabel());
        assertEquals("v1", e.getVertexId(Direction.OUT));
        assertEquals(v1, e.getVertex(Direction.OUT, AUTHORIZATIONS_A));
        assertEquals("v2", e.getVertexId(Direction.IN));
        assertEquals(v2, e.getVertex(Direction.IN, AUTHORIZATIONS_A));
        assertEquals(VISIBILITY_A, e.getVisibility());

        getGraph().flush();
        assertEvents(
                new AddVertexEvent(getGraph(), v1),
                new AddVertexEvent(getGraph(), v2),
                new AddEdgeEvent(getGraph(), addedEdge)
        );
    }

    @Test
    public void testGetEdge() {
        Vertex v1 = getGraph().addVertex("v1", VISIBILITY_A, AUTHORIZATIONS_A, CONCEPT_TYPE_THING);
        Vertex v2 = getGraph().addVertex("v2", VISIBILITY_A, AUTHORIZATIONS_A, CONCEPT_TYPE_THING);
        getGraph().addEdge("e1to2label1", v1, v2, LABEL_LABEL1, VISIBILITY_A, AUTHORIZATIONS_A);
        getGraph().addEdge("e1to2label2", v1, v2, LABEL_LABEL2, VISIBILITY_A, AUTHORIZATIONS_A);
        getGraph().addEdge("e2to1", v2.getId(), v1.getId(), LABEL_LABEL1, VISIBILITY_A, AUTHORIZATIONS_A);
        getGraph().flush();

        v1 = getGraph().getVertex("v1", AUTHORIZATIONS_A);

        assertEquals(3, count(v1.getEdges(Direction.BOTH, AUTHORIZATIONS_A)));
        assertEquals(2, count(v1.getEdges(Direction.OUT, AUTHORIZATIONS_A)));
        assertEquals(1, count(v1.getEdges(Direction.IN, AUTHORIZATIONS_A)));
        assertEquals(3, count(v1.getEdges(v2, Direction.BOTH, AUTHORIZATIONS_A)));
        assertEquals(2, count(v1.getEdges(v2, Direction.OUT, AUTHORIZATIONS_A)));
        assertEquals(1, count(v1.getEdges(v2, Direction.IN, AUTHORIZATIONS_A)));
        assertEquals(2, count(v1.getEdges(v2, Direction.BOTH, LABEL_LABEL1, AUTHORIZATIONS_A)));
        assertEquals(1, count(v1.getEdges(v2, Direction.OUT, LABEL_LABEL1, AUTHORIZATIONS_A)));
        assertEquals(1, count(v1.getEdges(v2, Direction.IN, LABEL_LABEL1, AUTHORIZATIONS_A)));
        assertEquals(3, count(v1.getEdges(v2, Direction.BOTH, new String[]{LABEL_LABEL1, LABEL_LABEL2}, AUTHORIZATIONS_A)));
        assertEquals(2, count(v1.getEdges(v2, Direction.OUT, new String[]{LABEL_LABEL1, LABEL_LABEL2}, AUTHORIZATIONS_A)));
        assertEquals(1, count(v1.getEdges(v2, Direction.IN, new String[]{LABEL_LABEL1, LABEL_LABEL2}, AUTHORIZATIONS_A)));

        assertArrayEquals(new String[]{LABEL_LABEL1, LABEL_LABEL2}, IterableUtils.toArray(v1.getEdgesSummary(AUTHORIZATIONS_A).getOutEdgeLabels(), String.class));
        assertArrayEquals(new String[]{LABEL_LABEL1}, IterableUtils.toArray(v1.getEdgesSummary(AUTHORIZATIONS_A).getInEdgeLabels(), String.class));
        assertArrayEquals(new String[]{LABEL_LABEL1, LABEL_LABEL2}, IterableUtils.toArray(v1.getEdgesSummary(AUTHORIZATIONS_A).getEdgeLabels(), String.class));
    }

    @Test
    public void testGetEdgeVertexPairs() {
        Vertex v1 = getGraph().addVertex("v1", VISIBILITY_A, AUTHORIZATIONS_A, CONCEPT_TYPE_THING);
        Vertex v2 = getGraph().addVertex("v2", VISIBILITY_A, AUTHORIZATIONS_A, CONCEPT_TYPE_THING);
        Vertex v3 = getGraph().addVertex("v3", VISIBILITY_A, AUTHORIZATIONS_A, CONCEPT_TYPE_THING);
        Edge v1_to_v2_label1 = getGraph().addEdge("v1_to_v2_label1", v1, v2, LABEL_LABEL1, VISIBILITY_A, AUTHORIZATIONS_A);
        Edge v1_to_v2_label2 = getGraph().addEdge("v1_to_v2_label2", v1, v2, LABEL_LABEL2, VISIBILITY_A, AUTHORIZATIONS_A);
        Edge v1_to_v3_label2 = getGraph().addEdge("v1_to_v3_label2", v1, v3, LABEL_LABEL2, VISIBILITY_A, AUTHORIZATIONS_A);
        getGraph().flush();

        v1 = getGraph().getVertex("v1", AUTHORIZATIONS_A);

        List<EdgeVertexPair> pairs = toList(v1.getEdgeVertexPairs(Direction.BOTH, AUTHORIZATIONS_A));
        assertEquals(3, pairs.size());
        assertTrue(pairs.contains(new EdgeVertexPair(v1_to_v2_label1, v2)));
        assertTrue(pairs.contains(new EdgeVertexPair(v1_to_v2_label2, v2)));
        assertTrue(pairs.contains(new EdgeVertexPair(v1_to_v3_label2, v3)));

        pairs = toList(v1.getEdgeVertexPairs(Direction.BOTH, LABEL_LABEL2, AUTHORIZATIONS_A));
        assertEquals(2, pairs.size());
        assertTrue(pairs.contains(new EdgeVertexPair(v1_to_v2_label2, v2)));
        assertTrue(pairs.contains(new EdgeVertexPair(v1_to_v3_label2, v3)));
    }

    @Test
    public void testAddEdgeWithProperties() {
        Vertex v1 = getGraph().addVertex("v1", VISIBILITY_A, AUTHORIZATIONS_A, CONCEPT_TYPE_THING);
        Vertex v2 = getGraph().addVertex("v2", VISIBILITY_A, AUTHORIZATIONS_A, CONCEPT_TYPE_THING);
        Edge addedEdge = getGraph().prepareEdge("e1", v1, v2, LABEL_LABEL1, VISIBILITY_A)
                .setProperty("propA", stringValue("valueA"), VISIBILITY_A)
                .setProperty("propB", stringValue("valueB"), VISIBILITY_B)
                .save(AUTHORIZATIONS_A_AND_B);
        getGraph().flush();

        Edge e = getGraph().getEdge("e1", AUTHORIZATIONS_A);
        assertEquals(1, count(e.getProperties()));
        assertEquals(stringValue("valueA"), e.getPropertyValues("propA").iterator().next());
        assertEquals(0, count(e.getPropertyValues("propB")));

        e = getGraph().getEdge("e1", AUTHORIZATIONS_A_AND_B);
        assertEquals(2, count(e.getProperties()));
        assertEquals(stringValue("valueA"), e.getPropertyValues("propA").iterator().next());
        assertEquals(stringValue("valueB"), e.getPropertyValues("propB").iterator().next());
        assertEquals(stringValue("valueA"), e.getPropertyValue("propA"));
        assertEquals(stringValue("valueB"), e.getPropertyValue("propB"));

        getGraph().flush();
        assertEvents(
                new AddVertexEvent(getGraph(), v1),
                new AddVertexEvent(getGraph(), v2),
                new AddEdgeEvent(getGraph(), addedEdge),
                new AddPropertyEvent(getGraph(), addedEdge, addedEdge.getProperty("propA")),
                new AddPropertyEvent(getGraph(), addedEdge, addedEdge.getProperty("propB"))
        );
    }

    @Test
    public void testAddEdgeWithNullInOutVertices() {
        try {
            String outVertexId = null;
            String inVertexId = null;
            getGraph().prepareEdge("e1", outVertexId, inVertexId, LABEL_LABEL1, VISIBILITY_EMPTY)
                    .save(AUTHORIZATIONS_ALL);
            fail("should throw an exception");
        } catch (Exception ex) {
            assertNotNull(ex);
        }

        try {
            Vertex outVertex = null;
            Vertex inVertex = null;
            getGraph().prepareEdge("e1", outVertex, inVertex, LABEL_LABEL1, VISIBILITY_EMPTY)
                    .save(AUTHORIZATIONS_ALL);
            fail("should throw an exception");
        } catch (Exception ex) {
            assertNotNull(ex);
        }
    }

    @Test
    public void testAddEdgeWithNullLabels() {
        try {
            String label = null;
            getGraph().prepareEdge("e1", "v1", "v2", label, VISIBILITY_EMPTY)
                    .save(AUTHORIZATIONS_ALL);
            fail("should throw an exception");
        } catch (Exception ex) {
            assertNotNull(ex);
        }

        try {
            String label = null;
            Vertex outVertex = getGraph().addVertex("v1", VISIBILITY_EMPTY, AUTHORIZATIONS_ALL, CONCEPT_TYPE_THING);
            Vertex inVertex = getGraph().addVertex("v2", VISIBILITY_EMPTY, AUTHORIZATIONS_ALL, CONCEPT_TYPE_THING);
            getGraph().prepareEdge("e1", outVertex, inVertex, label, VISIBILITY_EMPTY)
                    .save(AUTHORIZATIONS_ALL);
            fail("should throw an exception");
        } catch (Exception ex) {
            assertNotNull(ex);
        }
    }

    @Test
    public void testChangingPropertyOnEdge() {
        Vertex v1 = getGraph().addVertex("v1", VISIBILITY_A, AUTHORIZATIONS_A, CONCEPT_TYPE_THING);
        Vertex v2 = getGraph().addVertex("v2", VISIBILITY_A, AUTHORIZATIONS_A, CONCEPT_TYPE_THING);
        getGraph().prepareEdge("e1", v1, v2, LABEL_LABEL1, VISIBILITY_A)
                .setProperty("propA", stringValue("valueA"), VISIBILITY_A)
                .save(AUTHORIZATIONS_A_AND_B);
        getGraph().flush();

        Edge e = getGraph().getEdge("e1", AUTHORIZATIONS_A_AND_B);
        assertEquals(1, count(e.getProperties()));
        assertEquals(stringValue("valueA"), e.getPropertyValues("propA").iterator().next());

        Property propA = e.getProperty("", "propA");
        assertNotNull(propA);

        e.markPropertyHidden(propA, VISIBILITY_A, AUTHORIZATIONS_A_AND_B);
        getGraph().flush();

        e = getGraph().getEdge("e1", AUTHORIZATIONS_A_AND_B);
        assertEquals(0, count(e.getProperties()));
        assertEquals(0, count(e.getPropertyValues("propA")));

        e.setProperty(propA.getName(), stringValue("valueA_changed"), VISIBILITY_B, AUTHORIZATIONS_A_AND_B);
        getGraph().flush();

        e = getGraph().getEdge("e1", AUTHORIZATIONS_A_AND_B);
        assertEquals(1, count(e.getProperties()));
        assertEquals(stringValue("valueA_changed"), e.getPropertyValues("propA").iterator().next());

        e.markPropertyVisible(propA, VISIBILITY_A, AUTHORIZATIONS_A_AND_B);
        getGraph().flush();

        e = getGraph().getEdge("e1", AUTHORIZATIONS_A_AND_B);
        assertEquals(2, count(e.getProperties()));
        assertEquals(2, count(e.getPropertyValues("propA")));

        List<Value> propertyValues = toList(e.getPropertyValues("propA"));
        assertTrue(propertyValues.contains(stringValue("valueA")));
        assertTrue(propertyValues.contains(stringValue("valueA_changed")));
    }

    @Test
    public void testsAlterVertexConceptType() {
        Vertex v = getGraph().addVertex("v1", VISIBILITY_A, AUTHORIZATIONS_A, CONCEPT_TYPE_THING);
        getGraph().flush();

        v = getGraph().getVertex("v1", AUTHORIZATIONS_A);
        assertEquals(CONCEPT_TYPE_THING, v.getConceptType());

        v.prepareMutation()
                .alterConceptType(CONCEPT_TYPE_DOCUMENT)
                .save(AUTHORIZATIONS_A);
        getGraph().flush();

        v = getGraph().getVertex("v1", AUTHORIZATIONS_A);
        assertEquals(CONCEPT_TYPE_DOCUMENT, v.getConceptType());

        v.prepareMutation()
                .alterConceptType(CONCEPT_TYPE_AUDIO)
                .save(AUTHORIZATIONS_A);
        getGraph().flush();

        v = getGraph().getVertex("v1", AUTHORIZATIONS_A);
        assertEquals(CONCEPT_TYPE_AUDIO, v.getConceptType());
    }

    @Test
    public void testAlterEdgeLabel() {
        Vertex v1 = getGraph().addVertex("v1", VISIBILITY_A, AUTHORIZATIONS_A, CONCEPT_TYPE_THING);
        Vertex v2 = getGraph().addVertex("v2", VISIBILITY_A, AUTHORIZATIONS_A, CONCEPT_TYPE_THING);
        getGraph().prepareEdge("e1", v1, v2, LABEL_LABEL1, VISIBILITY_A)
                .setProperty("propA", stringValue("valueA"), VISIBILITY_A)
                .save(AUTHORIZATIONS_A);
        getGraph().flush();

        Edge e = getGraph().getEdge("e1", AUTHORIZATIONS_A);
        assertEquals(LABEL_LABEL1, e.getLabel());
        assertEquals(1, count(e.getProperties()));
        assertEquals(stringValue("valueA"), e.getPropertyValues("propA").iterator().next());
        assertEquals(1, count(v1.getEdges(Direction.OUT, AUTHORIZATIONS_A)));
        assertEquals(LABEL_LABEL1, IterableUtils.single(v1.getEdgesSummary(AUTHORIZATIONS_A).getOutEdgeLabels()));
        assertEquals(1, count(v2.getEdges(Direction.IN, AUTHORIZATIONS_A)));
        assertEquals(LABEL_LABEL1, IterableUtils.single(v2.getEdgesSummary(AUTHORIZATIONS_A).getInEdgeLabels()));

        e.prepareMutation()
                .alterEdgeLabel(LABEL_LABEL2)
                .save(AUTHORIZATIONS_A);
        getGraph().flush();

        e = getGraph().getEdge("e1", AUTHORIZATIONS_A);
        assertEquals(LABEL_LABEL2, e.getLabel());
        assertEquals(1, count(e.getProperties()));
        assertEquals(stringValue("valueA"), e.getPropertyValues("propA").iterator().next());
        v1 = getGraph().getVertex("v1", AUTHORIZATIONS_A);
        assertEquals(1, count(v1.getEdges(Direction.OUT, AUTHORIZATIONS_A)));
        assertEquals(LABEL_LABEL2, IterableUtils.single(v1.getEdgesSummary(AUTHORIZATIONS_A).getOutEdgeLabels()));
        v2 = getGraph().getVertex("v2", AUTHORIZATIONS_A);
        assertEquals(1, count(v2.getEdges(Direction.IN, AUTHORIZATIONS_A)));
        assertEquals(LABEL_LABEL2, IterableUtils.single(v2.getEdgesSummary(AUTHORIZATIONS_A).getInEdgeLabels()));

        getGraph().prepareEdge(e.getId(), e.getVertexId(Direction.OUT), e.getVertexId(Direction.IN), e.getLabel(), e.getVisibility())
                .alterEdgeLabel("label3")
                .save(AUTHORIZATIONS_A);
        getGraph().flush();
        e = getGraph().getEdge("e1", AUTHORIZATIONS_A);
        assertEquals("label3", e.getLabel());
        assertEquals(1, count(e.getProperties()));
        assertEquals(stringValue("valueA"), e.getPropertyValues("propA").iterator().next());
        v1 = getGraph().getVertex("v1", AUTHORIZATIONS_A);
        assertEquals(1, count(v1.getEdges(Direction.OUT, AUTHORIZATIONS_A)));
        assertEquals("label3", IterableUtils.single(v1.getEdgesSummary(AUTHORIZATIONS_A).getOutEdgeLabels()));
        v2 = getGraph().getVertex("v2", AUTHORIZATIONS_A);
        assertEquals(1, count(v2.getEdges(Direction.IN, AUTHORIZATIONS_A)));
        assertEquals("label3", IterableUtils.single(v2.getEdgesSummary(AUTHORIZATIONS_A).getInEdgeLabels()));
    }

    @Test
    public void testDeleteEdge() {
        Vertex v1 = getGraph().addVertex("v1", VISIBILITY_A, AUTHORIZATIONS_A, CONCEPT_TYPE_THING);
        Vertex v2 = getGraph().addVertex("v2", VISIBILITY_A, AUTHORIZATIONS_A, CONCEPT_TYPE_THING);
        Edge addedEdge = getGraph().addEdge("e1", v1, v2, LABEL_LABEL1, VISIBILITY_A, AUTHORIZATIONS_A);
        getGraph().flush();
        getGraph().dumpGraph();

        assertEquals(1, count(getGraph().getEdges(AUTHORIZATIONS_A)));


        try {
            getGraph().deleteEdge("e1", AUTHORIZATIONS_B);
            getGraph().dumpGraph();
        } catch (NullPointerException e) {
            // expected
        }
        assertEquals(1, count(getGraph().getEdges(AUTHORIZATIONS_A)));

        getGraph().deleteEdge("e1", AUTHORIZATIONS_A);
        getGraph().flush();
        getGraph().dumpGraph();
        assertEquals(0, count(getGraph().getEdges(AUTHORIZATIONS_A)));

        v1 = getGraph().getVertex("v1", AUTHORIZATIONS_A);
        assertEquals(0, count(v1.getVertices(Direction.BOTH, AUTHORIZATIONS_A)));
        v2 = getGraph().getVertex("v2", AUTHORIZATIONS_A);
        assertEquals(0, count(v2.getVertices(Direction.BOTH, AUTHORIZATIONS_A)));

        getGraph().flush();
        assertEvents(
                new AddVertexEvent(getGraph(), v1),
                new AddVertexEvent(getGraph(), v2),
                new AddEdgeEvent(getGraph(), addedEdge),
                new DeleteEdgeEvent(getGraph(), addedEdge)
        );
    }

    @Test
    public void testDeleteElementEdge() {
        getGraph().prepareVertex("v1", VISIBILITY_A, CONCEPT_TYPE_THING)
                .save(AUTHORIZATIONS_ALL);
        getGraph().prepareVertex("v2", VISIBILITY_A, CONCEPT_TYPE_THING)
                .save(AUTHORIZATIONS_ALL);
        getGraph().prepareEdge("e1", "v1", "v2", "label1", VISIBILITY_A)
                .addExtendedData("table1", "row1", "column1", stringValue("value1"), VISIBILITY_A)
                .save(AUTHORIZATIONS_ALL);
        getGraph().flush();

        getGraph().deleteElement(ElementId.edge("e1"), AUTHORIZATIONS_A_AND_B);
        getGraph().flush();

        assertNull(getGraph().getEdge("e1", AUTHORIZATIONS_A));
        assertResultsCount(0, 0, getGraph().query(AUTHORIZATIONS_A_AND_B).edges());
        assertResultsCount(0, 0, getGraph().query(AUTHORIZATIONS_A_AND_B).extendedDataRows());
    }

    @Test
    public void testAddEdgeWithVisibility() {
        Vertex v1 = getGraph().addVertex("v1", VISIBILITY_A, AUTHORIZATIONS_A, CONCEPT_TYPE_THING);
        Vertex v2 = getGraph().addVertex("v2", VISIBILITY_A, AUTHORIZATIONS_A, CONCEPT_TYPE_THING);
        getGraph().addEdge("e1", v1, v2, LABEL_LABEL1, VISIBILITY_A, AUTHORIZATIONS_A);
        getGraph().addEdge("e2", v1, v2, LABEL_LABEL2, VISIBILITY_B, AUTHORIZATIONS_A_AND_B);
        getGraph().flush();

        Iterable<Edge> aEdges = getGraph().getVertex("v1", AUTHORIZATIONS_A_AND_B).getEdges(Direction.BOTH, AUTHORIZATIONS_A);
        assertEquals(1, count(aEdges));
        assertEquals(LABEL_LABEL1, IterableUtils.single(aEdges).getLabel());

        Iterable<Edge> bEdges = getGraph().getVertex("v1", AUTHORIZATIONS_A_AND_B).getEdges(Direction.BOTH, AUTHORIZATIONS_B);
        assertEquals(1, count(bEdges));
        assertEquals(LABEL_LABEL2, IterableUtils.single(bEdges).getLabel());

        Iterable<Edge> allEdges = getGraph().getVertex("v1", AUTHORIZATIONS_A_AND_B).getEdges(Direction.BOTH, AUTHORIZATIONS_A_AND_B);
        assertEquals(2, count(allEdges));
    }

    @Test
    public void testSoftDeleteVertex() {
        getGraph().prepareVertex("v1", VISIBILITY_A, CONCEPT_TYPE_THING)
                .addPropertyValue("key1", "name1", stringValue("value1"), VISIBILITY_A)
                .save(AUTHORIZATIONS_A);
        getGraph().prepareVertex("v2", VISIBILITY_A, CONCEPT_TYPE_THING)
                .save(AUTHORIZATIONS_A);
        getGraph().prepareEdge("e1", "v1", "v2", LABEL_LABEL1, VISIBILITY_A)
                .save(AUTHORIZATIONS_A);
        getGraph().flush();
        assertEquals(2, count(getGraph().getVertices(AUTHORIZATIONS_A)));
        assertEquals(1, count(getGraph().query(hasFilter("name1", EQUAL, stringValue("value1")), AUTHORIZATIONS_A).vertices()));

        Vertex v2 = getGraph().getVertex("v2", AUTHORIZATIONS_A);
        assertEquals(1, v2.getEdgesSummary(AUTHORIZATIONS_A).getCountOfEdges());

        getGraph().softDeleteVertex("v1", AUTHORIZATIONS_A);
        getGraph().flush();
        assertEquals(1, count(getGraph().getVertices(AUTHORIZATIONS_A)));
        assertEquals(0, count(getGraph().getEdges(AUTHORIZATIONS_A)));
        assertEquals(0, count(getGraph().query(hasFilter("name1", EQUAL, stringValue("value1")), AUTHORIZATIONS_A).vertices()));

        v2 = getGraph().getVertex("v2", AUTHORIZATIONS_A);
        assertEquals(0, v2.getEdgesSummary(AUTHORIZATIONS_A).getCountOfEdges());

        getGraph().prepareVertex("v1", VISIBILITY_A, CONCEPT_TYPE_THING)
                .addPropertyValue("key1", "name1", stringValue("value1"), VISIBILITY_A)
                .save(AUTHORIZATIONS_A);
        getGraph().prepareVertex("v3", VISIBILITY_A, CONCEPT_TYPE_THING)
                .addPropertyValue("key1", "name1", stringValue("value1"), VISIBILITY_A)
                .save(AUTHORIZATIONS_A);
        getGraph().prepareVertex("v4", VISIBILITY_A, CONCEPT_TYPE_THING)
                .addPropertyValue("key1", "name1", stringValue("value1"), VISIBILITY_A)
                .save(AUTHORIZATIONS_A);
        getGraph().flush();
        assertEquals(4, count(getGraph().getVertices(AUTHORIZATIONS_A)));
        assertResultsCount(3, getGraph().query(hasFilter("name1", EQUAL, stringValue("value1")), AUTHORIZATIONS_A).vertices());

        getGraph().softDeleteVertex("v3", AUTHORIZATIONS_A);
        getGraph().flush();
        assertEquals(3, count(getGraph().getVertices(AUTHORIZATIONS_A)));
        assertResultsCount(2, getGraph().query(hasFilter("name1", EQUAL, stringValue("value1")), AUTHORIZATIONS_A).vertices());
    }

    @Test
    public void testReAddingSoftDeletedVertex() {
        Vertex v1 = getGraph().prepareVertex("v1", VISIBILITY_A, CONCEPT_TYPE_THING)
                .addPropertyValue("k1", "p1", stringValue("value1"), VISIBILITY_A)
                .save(AUTHORIZATIONS_A);
        getGraph().flush();

        assertEquals(VISIBILITY_A.getVisibilityString(), v1.getVisibility().getVisibilityString());

        getGraph().softDeleteVertex(v1, AUTHORIZATIONS_A);
        getGraph().flush();

        getGraph().prepareVertex("v1", VISIBILITY_A, CONCEPT_TYPE_THING)
                .save(AUTHORIZATIONS_A);
        getGraph().flush();

        v1 = getGraph().getVertex("v1", AUTHORIZATIONS_A);
        assertNotNull(v1);
        assertEquals(VISIBILITY_A.getVisibilityString(), v1.getVisibility().getVisibilityString());
        assertEquals(0, count(v1.getProperties()));

        getGraph().softDeleteVertex(v1, AUTHORIZATIONS_A);
        getGraph().flush();

        getGraph().prepareVertex("v1", VISIBILITY_A_AND_B, CONCEPT_TYPE_THING)
                .save(AUTHORIZATIONS_A_AND_B);
        getGraph().flush();

        v1 = getGraph().getVertex("v1", AUTHORIZATIONS_A_AND_B);
        assertNotNull(v1);
        assertEquals(VISIBILITY_A_AND_B.getVisibilityString(), v1.getVisibility().getVisibilityString());

        getGraph().softDeleteVertex(v1, AUTHORIZATIONS_A_AND_B);
        getGraph().flush();

        getGraph().addVertex("v1", VISIBILITY_EMPTY, AUTHORIZATIONS_EMPTY, CONCEPT_TYPE_THING);
        getGraph().flush();

        v1 = getGraph().getVertex("v1", AUTHORIZATIONS_A_AND_B);
        assertNotNull(v1);
        assertEquals(VISIBILITY_EMPTY.getVisibilityString(), v1.getVisibility().getVisibilityString());
    }

    @Test
    public void testGetSoftDeletedElementWithFetchHintsAndTimestamp() {
        Vertex v1 = getGraph().addVertex("v1", VISIBILITY_A, AUTHORIZATIONS_A, CONCEPT_TYPE_THING);
        Vertex v2 = getGraph().addVertex("v2", VISIBILITY_A, AUTHORIZATIONS_A, CONCEPT_TYPE_THING);
        Edge e1 = getGraph().addEdge("e1", v1, v2, LABEL_LABEL1, VISIBILITY_A, AUTHORIZATIONS_A);
        getGraph().flush();

        long beforeDeleteTime = IncreasingTime.currentTimeMillis();
        getGraph().softDeleteEdge(e1, AUTHORIZATIONS_A);
        getGraph().softDeleteVertex(v1, AUTHORIZATIONS_A);
        getGraph().flush();

        getGraph().dumpGraph();

        assertNull(getGraph().getEdge(e1.getId(), AUTHORIZATIONS_A));
        assertNull(getGraph().getEdge(e1.getId(), getGraph().getDefaultFetchHints(), AUTHORIZATIONS_A));
        assertNull(getGraph().getEdge(e1.getId(), FetchHints.ALL_INCLUDING_HIDDEN, AUTHORIZATIONS_A));
        assertNull(getGraph().getVertex(v1.getId(), AUTHORIZATIONS_A));
        assertNull(getGraph().getVertex(v1.getId(), getGraph().getDefaultFetchHints(), AUTHORIZATIONS_A));
        assertNull(getGraph().getVertex(v1.getId(), FetchHints.ALL_INCLUDING_HIDDEN, AUTHORIZATIONS_A));

        assertNotNull(getGraph().getEdge(e1.getId(), getGraph().getDefaultFetchHints(), beforeDeleteTime, AUTHORIZATIONS_A));
        assertNotNull(getGraph().getEdge(e1.getId(), FetchHints.ALL_INCLUDING_HIDDEN, beforeDeleteTime, AUTHORIZATIONS_A));
        assertNotNull(getGraph().getVertex(v1.getId(), getGraph().getDefaultFetchHints(), beforeDeleteTime, AUTHORIZATIONS_A));
        assertNotNull(getGraph().getVertex(v1.getId(), FetchHints.ALL_INCLUDING_HIDDEN, beforeDeleteTime, AUTHORIZATIONS_A));
    }

    @Test
    public void testSoftDeleteEdge() {
        Vertex v1 = getGraph().addVertex("v1", VISIBILITY_A, AUTHORIZATIONS_A_AND_B, CONCEPT_TYPE_THING);
        Vertex v2 = getGraph().addVertex("v2", VISIBILITY_B, AUTHORIZATIONS_A_AND_B, CONCEPT_TYPE_THING);
        Vertex v3 = getGraph().addVertex("v3", VISIBILITY_B, AUTHORIZATIONS_A_AND_B, CONCEPT_TYPE_THING);
        getGraph().addEdge("e1", v1, v2, LABEL_LABEL1, VISIBILITY_B, AUTHORIZATIONS_A_AND_B);
        getGraph().addEdge("e2", v1, v3, LABEL_LABEL1, VISIBILITY_B, AUTHORIZATIONS_A_AND_B);
        getGraph().flush();

        Edge e1 = getGraph().getEdge("e1", AUTHORIZATIONS_A_AND_B);
        getGraph().softDeleteEdge(e1, AUTHORIZATIONS_A_AND_B);
        getGraph().flush();

        v1 = getGraph().getVertex("v1", AUTHORIZATIONS_A_AND_B);
        assertEquals(1, count(v1.getEdgeIds(Direction.BOTH, AUTHORIZATIONS_A_AND_B)));
        assertEquals(1, count(v1.getEdges(Direction.BOTH, AUTHORIZATIONS_A_AND_B)));
        assertEquals(1, count(v1.getVertexIds(Direction.BOTH, AUTHORIZATIONS_A_AND_B)));

        v1 = getGraph().getVertex("v1", FetchHints.ALL_INCLUDING_HIDDEN, AUTHORIZATIONS_A_AND_B);
        assertEquals(1, count(v1.getEdgeIds(Direction.BOTH, AUTHORIZATIONS_A_AND_B)));
        assertEquals(1, count(v1.getEdges(Direction.BOTH, AUTHORIZATIONS_A_AND_B)));
        assertEquals(1, count(v1.getVertexIds(Direction.BOTH, AUTHORIZATIONS_A_AND_B)));

        v2 = getGraph().getVertex("v2", AUTHORIZATIONS_A_AND_B);
        assertEquals(0, count(v2.getEdgeIds(Direction.BOTH, AUTHORIZATIONS_A_AND_B)));
        assertEquals(0, count(v2.getEdges(Direction.BOTH, AUTHORIZATIONS_A_AND_B)));
        assertEquals(0, count(v2.getVertexIds(Direction.BOTH, AUTHORIZATIONS_A_AND_B)));

        v2 = getGraph().getVertex("v2", FetchHints.ALL_INCLUDING_HIDDEN, AUTHORIZATIONS_A_AND_B);
        assertEquals(0, count(v2.getEdgeIds(Direction.BOTH, AUTHORIZATIONS_A_AND_B)));
        assertEquals(0, count(v2.getEdges(Direction.BOTH, AUTHORIZATIONS_A_AND_B)));
        assertEquals(0, count(v2.getVertexIds(Direction.BOTH, AUTHORIZATIONS_A_AND_B)));

        v3 = getGraph().getVertex("v3", AUTHORIZATIONS_A_AND_B);
        assertEquals(1, count(v3.getEdgeIds(Direction.BOTH, AUTHORIZATIONS_A_AND_B)));
        assertEquals(1, count(v3.getEdges(Direction.BOTH, AUTHORIZATIONS_A_AND_B)));
        assertEquals(1, count(v3.getVertexIds(Direction.BOTH, AUTHORIZATIONS_A_AND_B)));

        v3 = getGraph().getVertex("v3", FetchHints.ALL_INCLUDING_HIDDEN, AUTHORIZATIONS_A_AND_B);
        assertEquals(1, count(v3.getEdgeIds(Direction.BOTH, AUTHORIZATIONS_A_AND_B)));
        assertEquals(1, count(v3.getEdges(Direction.BOTH, AUTHORIZATIONS_A_AND_B)));
        assertEquals(1, count(v3.getVertexIds(Direction.BOTH, AUTHORIZATIONS_A_AND_B)));
    }

    @Test
    public void testBlindWriteEdgeBothDirections() {
        getGraph().prepareVertex("v1", VISIBILITY_A, CONCEPT_TYPE_THING).save(AUTHORIZATIONS_A);
        getGraph().prepareVertex("v2", VISIBILITY_A, CONCEPT_TYPE_THING).save(AUTHORIZATIONS_A);
        getGraph().prepareEdge("e1", "v1", "v2", LABEL_LABEL1, VISIBILITY_A)
                .addPropertyValue("k1", "name1", stringValue("value1"), VISIBILITY_A)
                .save(AUTHORIZATIONS_A);
        getGraph().flush();
        getGraph().prepareEdge("e1", "v2", "v1", LABEL_LABEL1, VISIBILITY_A)
                .save(AUTHORIZATIONS_A);
        getGraph().flush();

        Edge e1 = getGraph().getEdge("e1", AUTHORIZATIONS_A);
        assertEquals("v2", e1.getVertexId(Direction.OUT));
        assertEquals("v1", e1.getVertexId(Direction.IN));

        assertEdgeIdsAnyOrder(getGraph().query(hasFilter(Edge.OUT_VERTEX_ID_PROPERTY_NAME, EQUAL, stringValue("v2")), AUTHORIZATIONS_A).edges(), "e1");
        assertEdgeIdsAnyOrder(getGraph().query(hasFilter(Edge.OUT_VERTEX_ID_PROPERTY_NAME, EQUAL, stringValue("v1")), AUTHORIZATIONS_A).edges());
        assertEdgeIdsAnyOrder(getGraph().query(hasFilter(Edge.IN_VERTEX_ID_PROPERTY_NAME, EQUAL, stringValue("v1")), AUTHORIZATIONS_A).edges(), "e1");
        assertEdgeIdsAnyOrder(getGraph().query(hasFilter(Edge.IN_VERTEX_ID_PROPERTY_NAME, EQUAL, stringValue("v2")), AUTHORIZATIONS_A).edges());
    }

    @Test
    public void testReAddingSoftDeletedEdge() {
        Vertex v1 = getGraph().addVertex("v1", VISIBILITY_A, AUTHORIZATIONS_A, CONCEPT_TYPE_THING);
        Vertex v2 = getGraph().addVertex("v2", VISIBILITY_A, AUTHORIZATIONS_A, CONCEPT_TYPE_THING);
        getGraph().addEdge("e1", v1, v2, LABEL_LABEL1, VISIBILITY_A, AUTHORIZATIONS_A);
        getGraph().flush();

        Edge e1 = getGraph().getEdge("e1", AUTHORIZATIONS_A);
        getGraph().softDeleteEdge(e1, AUTHORIZATIONS_A);
        getGraph().flush();

        getGraph().prepareEdge("e1", v1, v2, LABEL_LABEL1, VISIBILITY_A)
                .save(AUTHORIZATIONS_A);
        getGraph().flush();

        e1 = getGraph().getEdge("e1", AUTHORIZATIONS_A);
        assertNotNull(e1);
        assertEquals(VISIBILITY_A.getVisibilityString(), e1.getVisibility().getVisibilityString());
        getGraph().dumpGraph();

        v1 = getGraph().getVertex("v1", AUTHORIZATIONS_A);
        assertEquals(1, count(v1.getEdgeIds(Direction.BOTH, AUTHORIZATIONS_A)));
        assertEquals(1, count(v1.getEdges(Direction.BOTH, AUTHORIZATIONS_A)));
        assertEquals(1, count(v1.getVertexIds(Direction.BOTH, AUTHORIZATIONS_A)));

        v2 = getGraph().getVertex("v2", AUTHORIZATIONS_A);
        assertEquals(1, count(v2.getEdgeIds(Direction.BOTH, AUTHORIZATIONS_A)));
        assertEquals(1, count(v2.getEdges(Direction.BOTH, AUTHORIZATIONS_A)));
        assertEquals(1, count(v2.getVertexIds(Direction.BOTH, AUTHORIZATIONS_A)));
    }

    @Test
    public void testSoftDeleteProperty() throws InterruptedException {
        getGraph().prepareVertex("v1", VISIBILITY_A, CONCEPT_TYPE_THING)
                .addPropertyValue("key1", "name1", stringValue("value1"), VISIBILITY_A)
                .save(AUTHORIZATIONS_A);
        getGraph().flush();
        getGraph().dumpGraph();
        assertEquals(1, count(getGraph().getVertex("v1", AUTHORIZATIONS_A).getProperties()));
        assertResultsCount(1, getGraph().query(hasFilter("name1", EQUAL, stringValue("value1")), AUTHORIZATIONS_A).vertices());

        getGraph().getVertex("v1", AUTHORIZATIONS_A).softDeleteProperties("name1", AUTHORIZATIONS_A);
        getGraph().flush();
        getGraph().dumpGraph();
        assertEquals(0, count(getGraph().getVertex("v1", AUTHORIZATIONS_A).getProperties()));
        assertResultsCount(0, getGraph().query(hasFilter("name1", EQUAL, stringValue("value1")), AUTHORIZATIONS_A).vertices());

        getGraph().prepareVertex("v1", VISIBILITY_A, CONCEPT_TYPE_THING)
                .addPropertyValue("key1", "name1", stringValue("value1"), VISIBILITY_A)
                .save(AUTHORIZATIONS_A);
        getGraph().flush();
        getGraph().dumpGraph();
        assertEquals(1, count(getGraph().getVertex("v1", AUTHORIZATIONS_A).getProperties()));
        assertResultsCount(1, getGraph().query(hasFilter("name1", EQUAL, stringValue("value1")), AUTHORIZATIONS_A).vertices());

        getGraph().getVertex("v1", AUTHORIZATIONS_A).softDeleteProperties("name1", AUTHORIZATIONS_A);
        getGraph().flush();
        getGraph().dumpGraph();
        assertEquals(0, count(getGraph().getVertex("v1", AUTHORIZATIONS_A).getProperties()));
        assertResultsCount(0, getGraph().query(hasFilter("name1", EQUAL, stringValue("value1")), AUTHORIZATIONS_A).vertices());
    }

    @Test
    public void testFindRelatedEdgeSummaryAfterSoftDeleteAndReAdd() {
        Vertex v1 = getGraph().addVertex("v1", VISIBILITY_A, AUTHORIZATIONS_A, CONCEPT_TYPE_THING);
        Vertex v2 = getGraph().addVertex("v2", VISIBILITY_A, AUTHORIZATIONS_A, CONCEPT_TYPE_THING);
        Edge e1 = getGraph().addEdge("e v1->v2", v1, v2, LABEL_LABEL1, VISIBILITY_A, AUTHORIZATIONS_A);
        getGraph().flush();
        List<String> vertexIds = new ArrayList<>();
        vertexIds.add("v1");
        vertexIds.add("v2");
        List<RelatedEdge> relatedEdges = toList(getGraph().findRelatedEdgeSummary(vertexIds, AUTHORIZATIONS_A));
        assertEquals(1, relatedEdges.size());
        IterableUtils.assertContains(new RelatedEdgeImpl("e v1->v2", LABEL_LABEL1, v1.getId(), v2.getId()), relatedEdges);
        getGraph().softDeleteEdge(e1, AUTHORIZATIONS_A);
        getGraph().flush();
        getGraph().prepareEdge("e v1->v2", v1, v2, LABEL_LABEL1, VISIBILITY_A)
                .save(AUTHORIZATIONS_A);
        getGraph().flush();
        relatedEdges = toList(getGraph().findRelatedEdgeSummary(vertexIds, AUTHORIZATIONS_A));
        assertEquals(1, relatedEdges.size());
        IterableUtils.assertContains(new RelatedEdgeImpl("e v1->v2", LABEL_LABEL1, v1.getId(), v2.getId()), relatedEdges);
    }

    @Test
    public void testSoftDeletePropertyThroughMutation() {
        getGraph().prepareVertex("v1", VISIBILITY_A, CONCEPT_TYPE_THING)
                .addPropertyValue("key1", "name1", stringValue("value1"), VISIBILITY_A)
                .save(AUTHORIZATIONS_A);
        getGraph().flush();
        assertEquals(1, count(getGraph().getVertex("v1", AUTHORIZATIONS_A).getProperties()));
        assertEquals(1, count(getGraph().query(hasFilter("name1", EQUAL, stringValue("value1")), AUTHORIZATIONS_A).vertices()));

        getGraph().getVertex("v1", AUTHORIZATIONS_A)
                .prepareMutation()
                .softDeleteProperties("name1")
                .save(AUTHORIZATIONS_A);
        getGraph().flush();
        assertEquals(0, count(getGraph().getVertex("v1", AUTHORIZATIONS_A).getProperties()));
        assertEquals(0, count(getGraph().query(hasFilter("name1", EQUAL, stringValue("value1")), AUTHORIZATIONS_A).vertices()));

        getGraph().prepareVertex("v1", VISIBILITY_A, CONCEPT_TYPE_THING)
                .addPropertyValue("key1", "name1", stringValue("value1"), VISIBILITY_A)
                .save(AUTHORIZATIONS_A);
        getGraph().flush();
        assertEquals(1, count(getGraph().getVertex("v1", AUTHORIZATIONS_A).getProperties()));
        assertResultsCount(1, getGraph().query(hasFilter("name1", EQUAL, stringValue("value1")), AUTHORIZATIONS_A).vertices());

        getGraph().getVertex("v1", AUTHORIZATIONS_A)
                .prepareMutation()
                .softDeleteProperties("name1")
                .save(AUTHORIZATIONS_A);
        getGraph().flush();
        assertEquals(0, count(getGraph().getVertex("v1", AUTHORIZATIONS_A).getProperties()));
        assertResultsCount(0, getGraph().query(hasFilter("name1", EQUAL, stringValue("value1")), AUTHORIZATIONS_A).vertices());
    }

    @Test
    public void testSoftDeletePropertyOnEdgeNotIndexed() {
        Vertex v1 = getGraph().addVertex("v1", VISIBILITY_A, AUTHORIZATIONS_A_AND_B, CONCEPT_TYPE_THING);
        Vertex v2 = getGraph().addVertex("v2", VISIBILITY_B, AUTHORIZATIONS_A_AND_B, CONCEPT_TYPE_THING);
        ElementBuilder<Edge> elementBuilder = getGraph().prepareEdge("e1", v1, v2, LABEL_LABEL1, VISIBILITY_B)
                .setProperty("prop1", stringValue("value1"), VISIBILITY_B);
        elementBuilder.setIndexHint(IndexHint.DO_NOT_INDEX);
        Edge e1 = elementBuilder.save(AUTHORIZATIONS_A_AND_B);
        getGraph().flush();

        ExistingElementMutation<Edge> m = e1.prepareMutation();
        m.softDeleteProperty("prop1", VISIBILITY_B);
        m.setIndexHint(IndexHint.DO_NOT_INDEX);
        m.save(AUTHORIZATIONS_A_AND_B);
        getGraph().flush();

        e1 = getGraph().getEdge("e1", AUTHORIZATIONS_A_AND_B);
        assertEquals(0, IterableUtils.count(e1.getProperties()));
    }

    @Test
    public void testSoftDeletePropertyWithVisibility() {
        Vertex v1 = getGraph().prepareVertex("v1", VISIBILITY_A, CONCEPT_TYPE_THING)
                .addPropertyValue("key1", "name1", stringValue("value1"), VISIBILITY_A)
                .addPropertyValue("key1", "name1", stringValue("value2"), VISIBILITY_B)
                .save(AUTHORIZATIONS_A_AND_B);
        getGraph().flush();
        assertEquals(2, count(getGraph().getVertex("v1", AUTHORIZATIONS_A_AND_B).getProperties()));
        IterableUtils.assertContains(stringValue("value1"), v1.getPropertyValues("name1"));
        IterableUtils.assertContains(stringValue("value2"), v1.getPropertyValues("name1"));

        getGraph().getVertex("v1", AUTHORIZATIONS_A_AND_B).softDeleteProperty("key1", "name1", VISIBILITY_A, AUTHORIZATIONS_A_AND_B);
        getGraph().flush();
        assertEquals(1, count(getGraph().getVertex("v1", AUTHORIZATIONS_A_AND_B).getProperties()));
        assertEquals(1, count(getGraph().getVertex("v1", AUTHORIZATIONS_A_AND_B).getPropertyValues("key1", "name1")));
        IterableUtils.assertContains(stringValue("value2"), getGraph().getVertex("v1", AUTHORIZATIONS_A_AND_B).getPropertyValues("name1"));
    }

    @Test
    public void testSoftDeletePropertyThroughMutationWithVisibility() {
        Vertex v1 = getGraph().prepareVertex("v1", VISIBILITY_A, CONCEPT_TYPE_THING)
                .addPropertyValue("key1", "name1", stringValue("value1"), VISIBILITY_A)
                .addPropertyValue("key1", "name1", stringValue("value2"), VISIBILITY_B)
                .save(AUTHORIZATIONS_A_AND_B);
        getGraph().flush();
        assertEquals(2, count(getGraph().getVertex("v1", AUTHORIZATIONS_A_AND_B).getProperties()));
        IterableUtils.assertContains(stringValue("value1"), v1.getPropertyValues("name1"));
        IterableUtils.assertContains(stringValue("value2"), v1.getPropertyValues("name1"));

        v1 = getGraph().getVertex("v1", AUTHORIZATIONS_A_AND_B)
                .prepareMutation()
                .softDeleteProperty("key1", "name1", VISIBILITY_A)
                .save(AUTHORIZATIONS_A_AND_B);
        getGraph().flush();
        assertEquals(1, count(v1.getProperties()));
        assertEquals(1, count(v1.getPropertyValues("key1", "name1")));
        IterableUtils.assertContains(stringValue("value2"), v1.getPropertyValues("name1"));
    }

    @Test
    public void testSoftDeletePropertyOnAHiddenVertex() {
        Vertex v1 = getGraph().prepareVertex("v1", VISIBILITY_EMPTY, CONCEPT_TYPE_THING)
                .addPropertyValue("key1", "name1", stringValue("value1"), VISIBILITY_EMPTY)
                .save(AUTHORIZATIONS_A);
        getGraph().flush();

        getGraph().markVertexHidden(v1, VISIBILITY_A, AUTHORIZATIONS_A);
        getGraph().flush();

        v1 = getGraph().getVertex("v1", FetchHints.ALL_INCLUDING_HIDDEN, AUTHORIZATIONS_A);
        v1.softDeleteProperty("key1", "name1", AUTHORIZATIONS_A);
        getGraph().flush();

        v1 = getGraph().getVertex("v1", FetchHints.ALL_INCLUDING_HIDDEN, AUTHORIZATIONS_A);
        assertNull(v1.getProperty("key1", "name1", VISIBILITY_EMPTY));
    }

    @Test
    public void testMarkHiddenWithVisibilityChange() {
        Vertex v1 = getGraph().prepareVertex("v1", VISIBILITY_A, CONCEPT_TYPE_THING)
                .addPropertyValue("key1", "firstName", stringValue("Joe"), VISIBILITY_A)
                .save(AUTHORIZATIONS_A_AND_B);
        getGraph().flush();
        assertEquals(1, count(v1.getProperties()));
        IterableUtils.assertContains(stringValue("Joe"), v1.getPropertyValues("firstName"));

        v1 = getGraph().getVertex("v1", AUTHORIZATIONS_A_AND_B);
        v1.markPropertyHidden("key1", "firstName", VISIBILITY_A, VISIBILITY_B, AUTHORIZATIONS_A_AND_B);
        v1.addPropertyValue("key1", "firstName", stringValue("Joseph"), VISIBILITY_B, AUTHORIZATIONS_A_AND_B);
        getGraph().flush();

        v1 = getGraph().getVertex("v1", FetchHints.ALL_INCLUDING_HIDDEN, AUTHORIZATIONS_A_AND_B);
        List<Property> properties = toList(v1.getProperties());
        assertEquals(2, count(properties));

        boolean foundJoeProp = false;
        boolean foundJosephProp = false;
        for (Property property : properties) {
            if (property.getName().equals("firstName")) {
                if (property.getKey().equals("key1") && property.getValue().eq(stringValue("Joe"))) {
                    foundJoeProp = true;
                    assertTrue("should be hidden", property.isHidden(AUTHORIZATIONS_A_AND_B));
                    assertFalse("should not be hidden", property.isHidden(AUTHORIZATIONS_A));
                } else if (property.getKey().equals("key1") && property.getValue().eq(stringValue("Joseph"))) {
                    if (property.getVisibility().equals(VISIBILITY_B)) {
                        foundJosephProp = true;
                        assertFalse("should not be hidden", property.isHidden(AUTHORIZATIONS_A_AND_B));
                    } else {
                        throw new RuntimeException("Unexpected visibility " + property.getVisibility());
                    }
                } else {
                    throw new RuntimeException("Unexpected property key " + property.getKey());
                }
            } else {
                throw new RuntimeException("Unexpected property name " + property.getName());
            }
        }
        assertTrue("Joseph property value not found", foundJosephProp);
        assertTrue("Joe property value not found", foundJoeProp);
    }

    @Test
    public void testSoftDeleteWithVisibilityChanges() {
        Vertex v1 = getGraph().prepareVertex("v1", VISIBILITY_A, CONCEPT_TYPE_THING)
                .addPropertyValue("key1", "firstName", stringValue("Joe"), VISIBILITY_A)
                .save(AUTHORIZATIONS_A_AND_B);
        getGraph().flush();
        assertEquals(1, count(v1.getProperties()));
        IterableUtils.assertContains(stringValue("Joe"), v1.getPropertyValues("firstName"));

        v1 = getGraph().getVertex("v1", AUTHORIZATIONS_A_AND_B);
        v1.markPropertyHidden("key1", "firstName", VISIBILITY_A, VISIBILITY_B, AUTHORIZATIONS_A_AND_B);
        v1.addPropertyValue("key1", "firstName", stringValue("Joseph"), VISIBILITY_B, AUTHORIZATIONS_A_AND_B);
        getGraph().flush();
        v1.softDeleteProperty("key1", "firstName", VISIBILITY_B, AUTHORIZATIONS_A_AND_B);
        getGraph().flush();
        v1.markPropertyVisible("key1", "firstName", VISIBILITY_A, VISIBILITY_B, AUTHORIZATIONS_A_AND_B);
        v1.addPropertyValue("key1", "firstName", stringValue("Joseph"), VISIBILITY_A, AUTHORIZATIONS_A_AND_B);
        getGraph().flush();

        List<Property> properties = toList(getGraph().getVertex("v1", AUTHORIZATIONS_A_AND_B).getProperties());
        assertEquals(1, count(properties));
        Property property = properties.iterator().next();
        assertEquals(VISIBILITY_A, property.getVisibility());
        assertEquals(stringValue("Joseph"), property.getValue());

        Vertex v2 = getGraph().prepareVertex("v2", VISIBILITY_A, CONCEPT_TYPE_THING)
                .addPropertyValue("key1", "firstName", stringValue("Joe"), VISIBILITY_A)
                .save(AUTHORIZATIONS_A_AND_B);
        getGraph().flush();
        assertEquals(1, count(v2.getProperties()));
        IterableUtils.assertContains(stringValue("Joe"), v2.getPropertyValues("firstName"));

        v2 = getGraph().getVertex("v2", AUTHORIZATIONS_A_AND_B);
        v2.markPropertyHidden("key1", "firstName", VISIBILITY_A, VISIBILITY_B, AUTHORIZATIONS_A_AND_B);
        v2.addPropertyValue("key1", "firstName", stringValue("Joseph"), VISIBILITY_B, AUTHORIZATIONS_A_AND_B);
        getGraph().flush();
        v2.softDeleteProperty("key1", "firstName", VISIBILITY_B, AUTHORIZATIONS_A_AND_B);
        getGraph().flush();
        v2.markPropertyVisible("key1", "firstName", VISIBILITY_A, VISIBILITY_B, AUTHORIZATIONS_A_AND_B);
        v2.addPropertyValue("key1", "firstName", stringValue("Joe"), VISIBILITY_A, AUTHORIZATIONS_A_AND_B);
        getGraph().flush();

        properties = toList(getGraph().getVertex("v2", AUTHORIZATIONS_A_AND_B).getProperties());
        assertEquals(1, count(properties));
        property = properties.iterator().next();
        assertEquals(VISIBILITY_A, property.getVisibility());
        assertEquals(stringValue("Joe"), property.getValue());
    }

    @Test
    public void testMarkPropertyVisible() {
        Vertex v1 = getGraph().prepareVertex("v1", VISIBILITY_A, CONCEPT_TYPE_THING)
                .addPropertyValue("key1", "firstName", stringValue("Joe"), VISIBILITY_A)
                .save(AUTHORIZATIONS_A_AND_B);
        getGraph().flush();
        assertEquals(1, count(v1.getProperties()));
        IterableUtils.assertContains(stringValue("Joe"), v1.getPropertyValues("firstName"));

        long t = IncreasingTime.currentTimeMillis();

        v1 = getGraph().getVertex("v1", AUTHORIZATIONS_A_AND_B);
        v1.markPropertyHidden("key1", "firstName", VISIBILITY_A, t, VISIBILITY_B, AUTHORIZATIONS_A_AND_B);
        t += 1000;
        List<Property> properties = toList(getGraph().getVertex("v1", FetchHints.ALL_INCLUDING_HIDDEN, AUTHORIZATIONS_A_AND_B).getProperties());
        assertEquals(1, count(properties));

        long beforeMarkPropertyVisibleTimestamp = t;
        t += 1000;

        v1.markPropertyVisible("key1", "firstName", VISIBILITY_A, t, VISIBILITY_B, AUTHORIZATIONS_A_AND_B);
        t += 1000;
        properties = toList(getGraph().getVertex("v1", AUTHORIZATIONS_A_AND_B).getProperties());
        assertEquals(1, count(properties));
        getGraph().flush();

        v1 = getGraph().getVertex("v1", getGraph().getDefaultFetchHints(), beforeMarkPropertyVisibleTimestamp, AUTHORIZATIONS_A_AND_B);
        assertNotNull("could not find v1 before timestamp " + beforeMarkPropertyVisibleTimestamp + " current time " + t, v1);
        properties = toList(v1.getProperties());
        assertEquals(0, count(properties));
    }

    @Test
    public void testAddVertexWithVisibility() {
        getGraph().addVertex("v1", VISIBILITY_A, AUTHORIZATIONS_ALL, CONCEPT_TYPE_THING);
        getGraph().addVertex("v2", VISIBILITY_B, AUTHORIZATIONS_ALL, CONCEPT_TYPE_THING);
        getGraph().flush();

        Iterable<Vertex> cVertices = getGraph().getVertices(AUTHORIZATIONS_C);
        assertEquals(0, count(cVertices));

        Iterable<Vertex> aVertices = getGraph().getVertices(AUTHORIZATIONS_A);
        assertEquals("v1", IterableUtils.single(aVertices).getId());

        Iterable<Vertex> bVertices = getGraph().getVertices(AUTHORIZATIONS_B);
        assertEquals("v2", IterableUtils.single(bVertices).getId());

        Iterable<Vertex> allVertices = getGraph().getVertices(AUTHORIZATIONS_A_AND_B);
        assertEquals(2, count(allVertices));
    }

    @Test
    public void testAddMultipleVertices() {
        List<ElementBuilder<Vertex>> elements = new ArrayList<>();
        elements.add(getGraph().prepareVertex("v1", VISIBILITY_A, CONCEPT_TYPE_THING)
                .setProperty("prop1", stringValue("v1"), VISIBILITY_A));
        elements.add(getGraph().prepareVertex("v2", VISIBILITY_A, CONCEPT_TYPE_THING)
                .setProperty("prop1", stringValue("v2"), VISIBILITY_A));
        Iterable<Vertex> vertices = getGraph().addVertices(elements, AUTHORIZATIONS_A_AND_B);
        assertVertexIds(vertices, "v1", "v2");
        getGraph().flush();

        if (getGraph() instanceof GraphWithSearchIndex) {
            ((GraphWithSearchIndex) getGraph()).getSearchIndex().addElements(getGraph(), vertices, AUTHORIZATIONS_A_AND_B);
            assertVertexIds(getGraph().query(hasFilter("prop1", EQUAL, stringValue("v1")), AUTHORIZATIONS_A_AND_B).vertices(), "v1");
            assertVertexIds(getGraph().query(hasFilter("prop1", EQUAL, stringValue("v2")), AUTHORIZATIONS_A_AND_B).vertices(), "v2");
        }
    }

    @Test
    public void testGetVerticesWithIds() {
        getGraph().prepareVertex("v1", VISIBILITY_A, CONCEPT_TYPE_THING)
                .setProperty("prop1", stringValue("v1"), VISIBILITY_A)
                .save(AUTHORIZATIONS_A_AND_B);
        getGraph().prepareVertex("v1b", VISIBILITY_A, CONCEPT_TYPE_THING)
                .setProperty("prop1", stringValue("v1b"), VISIBILITY_A)
                .save(AUTHORIZATIONS_A_AND_B);
        getGraph().prepareVertex("v2", VISIBILITY_A, CONCEPT_TYPE_THING)
                .setProperty("prop1", stringValue("v2"), VISIBILITY_A)
                .save(AUTHORIZATIONS_A_AND_B);
        getGraph().prepareVertex("v3", VISIBILITY_A, CONCEPT_TYPE_THING)
                .setProperty("prop1", stringValue("v3"), VISIBILITY_A)
                .save(AUTHORIZATIONS_A_AND_B);
        getGraph().flush();

        List<String> ids = new ArrayList<>();
        ids.add("v2");
        ids.add("v1");

        Iterable<Vertex> vertices = getGraph().getVertices(ids, AUTHORIZATIONS_A);
        boolean foundV1 = false, foundV2 = false;
        for (Vertex v : vertices) {
            if (v.getId().equals("v1")) {
                assertEquals(stringValue("v1"), v.getPropertyValue("prop1"));
                foundV1 = true;
            } else if (v.getId().equals("v2")) {
                assertEquals(stringValue("v2"), v.getPropertyValue("prop1"));
                foundV2 = true;
            } else {
                assertTrue("Unexpected vertex id: " + v.getId(), false);
            }
        }
        assertTrue("v1 not found", foundV1);
        assertTrue("v2 not found", foundV2);

        List<Vertex> verticesInOrder = getGraph().getVerticesInOrder(ids, AUTHORIZATIONS_A);
        assertEquals(2, verticesInOrder.size());
        assertEquals("v2", verticesInOrder.get(0).getId());
        assertEquals("v1", verticesInOrder.get(1).getId());
    }

    @Test
    public void testGetVerticesWithPrefix() {
        getGraph().addVertex("a", VISIBILITY_EMPTY, AUTHORIZATIONS_ALL, CONCEPT_TYPE_THING);
        getGraph().addVertex("aa", VISIBILITY_EMPTY, AUTHORIZATIONS_ALL, CONCEPT_TYPE_THING);
        getGraph().addVertex("az", VISIBILITY_EMPTY, AUTHORIZATIONS_ALL, CONCEPT_TYPE_THING);
        getGraph().addVertex("b", VISIBILITY_EMPTY, AUTHORIZATIONS_ALL, CONCEPT_TYPE_THING);
        getGraph().flush();

        List<Vertex> vertices = sortById(toList(getGraph().getVerticesWithPrefix("a", AUTHORIZATIONS_ALL)));
        assertVertexIds(vertices, "a", "aa", "az");

        vertices = sortById(toList(getGraph().getVerticesWithPrefix("b", AUTHORIZATIONS_ALL)));
        assertVertexIds(vertices, "b");

        vertices = sortById(toList(getGraph().getVerticesWithPrefix("c", AUTHORIZATIONS_ALL)));
        assertVertexIds(vertices);
    }

    @Test
    public void testGetVerticesInRange() {
        getGraph().addVertex("a", VISIBILITY_EMPTY, AUTHORIZATIONS_ALL, CONCEPT_TYPE_THING);
        getGraph().addVertex("aa", VISIBILITY_EMPTY, AUTHORIZATIONS_ALL, CONCEPT_TYPE_THING);
        getGraph().addVertex("az", VISIBILITY_EMPTY, AUTHORIZATIONS_ALL, CONCEPT_TYPE_THING);
        getGraph().addVertex("b", VISIBILITY_EMPTY, AUTHORIZATIONS_ALL, CONCEPT_TYPE_THING);
        getGraph().flush();

        List<Vertex> vertices = toList(getGraph().getVerticesInRange(new IdRange(null, "a"), AUTHORIZATIONS_ALL));
        assertVertexIds(vertices);

        vertices = toList(getGraph().getVerticesInRange(new IdRange(null, "b"), AUTHORIZATIONS_ALL));
        assertVertexIds(vertices, "a", "aa", "az");

        vertices = toList(getGraph().getVerticesInRange(new IdRange(null, "bb"), AUTHORIZATIONS_ALL));
        assertVertexIds(vertices, "a", "aa", "az", "b");

        vertices = toList(getGraph().getVerticesInRange(new IdRange(null, null), AUTHORIZATIONS_ALL));
        assertVertexIds(vertices, "a", "aa", "az", "b");
    }

    @Test
    public void testGetEdgesInRange() {
        getGraph().addVertex("v1", VISIBILITY_A, AUTHORIZATIONS_A, CONCEPT_TYPE_THING);
        getGraph().addVertex("v2", VISIBILITY_A, AUTHORIZATIONS_A, CONCEPT_TYPE_THING);
        getGraph().addEdge("a", "v1", "v2", LABEL_LABEL1, VISIBILITY_EMPTY, AUTHORIZATIONS_ALL);
        getGraph().addEdge("aa", "v1", "v2", LABEL_LABEL1, VISIBILITY_EMPTY, AUTHORIZATIONS_ALL);
        getGraph().addEdge("az", "v1", "v2", LABEL_LABEL1, VISIBILITY_EMPTY, AUTHORIZATIONS_ALL);
        getGraph().addEdge("b", "v1", "v2", LABEL_LABEL1, VISIBILITY_EMPTY, AUTHORIZATIONS_ALL);
        getGraph().flush();

        List<Edge> edges = toList(getGraph().getEdgesInRange(new IdRange(null, "a"), AUTHORIZATIONS_ALL));
        assertEdgeIds(edges);

        edges = toList(getGraph().getEdgesInRange(new IdRange(null, "b"), AUTHORIZATIONS_ALL));
        assertEdgeIds(edges, "a", "aa", "az");

        edges = toList(getGraph().getEdgesInRange(new IdRange(null, "bb"), AUTHORIZATIONS_ALL));
        assertEdgeIds(edges, "a", "aa", "az", "b");

        edges = toList(getGraph().getEdgesInRange(new IdRange(null, null), AUTHORIZATIONS_ALL));
        assertEdgeIds(edges, "a", "aa", "az", "b");
    }


    @Test
    public void testGetEdgesWithIds() {
        Vertex v1 = getGraph().addVertex("v1", VISIBILITY_A, AUTHORIZATIONS_A, CONCEPT_TYPE_THING);
        Vertex v2 = getGraph().addVertex("v2", VISIBILITY_A, AUTHORIZATIONS_A, CONCEPT_TYPE_THING);
        Vertex v3 = getGraph().addVertex("v3", VISIBILITY_A, AUTHORIZATIONS_A, CONCEPT_TYPE_THING);
        getGraph().prepareEdge("e1", v1, v2, LABEL_LABEL1, VISIBILITY_A)
                .setProperty("prop1", stringValue("e1"), VISIBILITY_A)
                .save(AUTHORIZATIONS_A_AND_B);
        getGraph().prepareEdge("e1a", v1, v2, LABEL_LABEL1, VISIBILITY_A)
                .setProperty("prop1", stringValue("e1a"), VISIBILITY_A)
                .save(AUTHORIZATIONS_A_AND_B);
        getGraph().prepareEdge("e2", v1, v3, LABEL_LABEL1, VISIBILITY_A)
                .setProperty("prop1", stringValue("e2"), VISIBILITY_A)
                .save(AUTHORIZATIONS_A_AND_B);
        getGraph().prepareEdge("e3", v2, v3, LABEL_LABEL1, VISIBILITY_A)
                .setProperty("prop1", stringValue("e3"), VISIBILITY_A)
                .save(AUTHORIZATIONS_A_AND_B);
        getGraph().flush();

        List<String> ids = new ArrayList<>();
        ids.add("e1");
        ids.add("e2");
        Iterable<Edge> edges = getGraph().getEdges(ids, AUTHORIZATIONS_A);
        boolean foundE1 = false, foundE2 = false;
        for (Edge e : edges) {
            if (e.getId().equals("e1")) {
                assertEquals(stringValue("e1"), e.getPropertyValue("prop1"));
                foundE1 = true;
            } else if (e.getId().equals("e2")) {
                assertEquals(stringValue("e2"), e.getPropertyValue("prop1"));
                foundE2 = true;
            } else {
                assertTrue("Unexpected vertex id: " + e.getId(), false);
            }
        }
        assertTrue("e1 not found", foundE1);
        assertTrue("e2 not found", foundE2);
    }

    @Test
    public void testMarkVertexAndPropertiesHidden() {
        getGraph().prepareVertex("v1", VISIBILITY_EMPTY, CONCEPT_TYPE_THING)
                .addPropertyValue("k1", "age", intValue(25), VISIBILITY_EMPTY)
                .addPropertyValue("k2", "age", intValue(30), VISIBILITY_EMPTY)
                .save(AUTHORIZATIONS_ALL);
        getGraph().flush();

        Vertex v1 = getGraph().getVertex("v1", AUTHORIZATIONS_ALL);
        getGraph().markVertexHidden(v1, VISIBILITY_A, AUTHORIZATIONS_ALL);
        for (Property property : v1.getProperties()) {
            v1.markPropertyHidden(property, VISIBILITY_A, AUTHORIZATIONS_ALL);
        }
        getGraph().flush();

        v1 = getGraph().getVertex("v1", AUTHORIZATIONS_A);
        assertNull("v1 was found", v1);

        v1 = getGraph().getVertex("v1", FetchHints.ALL_INCLUDING_HIDDEN, AUTHORIZATIONS_ALL);
        assertNotNull("could not find v1", v1);
        assertEquals(2, count(v1.getProperties()));
        assertEquals(intValue(25), v1.getPropertyValue("k1", "age"));
        assertEquals(intValue(30), v1.getPropertyValue("k2", "age"));

        v1 = getGraph().getVertex("v1", FetchHints.ALL_INCLUDING_HIDDEN, AUTHORIZATIONS_ALL);
        getGraph().markVertexVisible(v1, VISIBILITY_A, AUTHORIZATIONS_ALL);
        getGraph().flush();

        Vertex v1AfterVisible = getGraph().getVertex("v1", AUTHORIZATIONS_A);
        assertNotNull("could not find v1", v1AfterVisible);
        assertEquals(0, count(v1AfterVisible.getProperties()));

        for (Property property : v1.getProperties()) {
            v1.markPropertyVisible(property, VISIBILITY_A, AUTHORIZATIONS_ALL);
        }
        getGraph().flush();

        v1 = getGraph().getVertex("v1", AUTHORIZATIONS_A);
        assertNotNull("could not find v1", v1);
        assertEquals(2, count(v1.getProperties()));
        assertEquals(intValue(25), v1.getPropertyValue("k1", "age"));
        assertEquals(intValue(30), v1.getPropertyValue("k2", "age"));
    }

    //
    // This test is to verify a bug found after rebuilding the search index. Since the visibility of the property was
    //  changed before the index was rebuilt, both the new and old visibility were in the metadata table. With no
    //  data left in the system using the old visibility, the rebuilt search index didn't have a mapping for it.
    //  This resulted in an error when sorting by that property because the Painless script for sorting was trying to sort on a field
    //  that didn't have a mapping.
    //
    @Test
    public void testRebuildIndexAfterPropertyVisibilityChange() {
        String propertyName = "first.name";
        getGraph().defineProperty(propertyName).dataType(TextValue.class).sortable(true).textIndexHint(TextIndexHint.ALL).define();

        String vertexId = "v1";
        String propertyKey = "k1";
        getGraph().prepareVertex(vertexId, VISIBILITY_EMPTY, CONCEPT_TYPE_THING)
                .addPropertyValue(propertyKey, propertyName, stringValue("Joe"), VISIBILITY_A)
                .save(AUTHORIZATIONS_ALL);
        getGraph().flush();

        QueryResultsIterable<String> results = getGraph().query(
                        hasFilter(propertyName, EQUAL, stringValue("joe"))
                                .sort(propertyName, SortDirection.ASCENDING)
                        , AUTHORIZATIONS_ALL)
                .vertexIds();
        assertIdsAnyOrder(results, vertexId);

        getGraph().getVertex(vertexId, AUTHORIZATIONS_ALL).prepareMutation()
                .alterPropertyVisibility(propertyKey, propertyName, VISIBILITY_EMPTY)
                .save(AUTHORIZATIONS_ALL);
        getGraph().flush();

        results = getGraph().query(
                hasFilter(propertyName, EQUAL, stringValue("joe"))
                        .sort(propertyName, SortDirection.ASCENDING)
                , AUTHORIZATIONS_ALL).vertexIds();
        assertIdsAnyOrder(results, vertexId);

        SearchIndex searchIndex = ((GraphWithSearchIndex) getGraph()).getSearchIndex();
        searchIndex.drop(getGraph());

        searchIndex.addElements(getGraph(), Collections.singletonList(getGraph().getVertex(vertexId, AUTHORIZATIONS_ALL)), AUTHORIZATIONS_ALL);
        getGraph().flush();

        results = getGraph().query(
                hasFilter(propertyName, EQUAL, stringValue("joe"))
                        .sort(propertyName, SortDirection.ASCENDING)
                , AUTHORIZATIONS_ALL
        ).vertexIds();
        assertIdsAnyOrder(results, vertexId);
    }


    @Test
    public void testMarkVertexHidden() {
        Vertex v1 = getGraph().addVertex("v1", VISIBILITY_A, AUTHORIZATIONS_ALL, CONCEPT_TYPE_THING);
        Vertex v2 = getGraph().addVertex("v2", VISIBILITY_A, AUTHORIZATIONS_ALL, CONCEPT_TYPE_THING);
        getGraph().addEdge("v1tov2", v1, v2, LABEL_LABEL1, VISIBILITY_A, AUTHORIZATIONS_ALL);
        getGraph().flush();

        List<String> vertexIdList = new ArrayList<>();
        vertexIdList.add("v1");
        vertexIdList.add("v2");
        vertexIdList.add("bad"); // add "bad" to the end of the list to test ordering of results
        Map<String, Boolean> verticesExist = getGraph().doVerticesExist(vertexIdList, AUTHORIZATIONS_A);
        assertEquals(3, vertexIdList.size());
        assertTrue("v1 exist", verticesExist.get("v1"));
        assertTrue("v2 exist", verticesExist.get("v2"));
        assertFalse("bad exist", verticesExist.get("bad"));

        assertTrue("v1 exists (auth A)", getGraph().doesVertexExist("v1", AUTHORIZATIONS_A));
        assertFalse("v1 exists (auth B)", getGraph().doesVertexExist("v1", AUTHORIZATIONS_B));
        assertTrue("v1 exists (auth A&B)", getGraph().doesVertexExist("v1", AUTHORIZATIONS_A_AND_B));
        assertEquals(2, count(getGraph().getVertices(AUTHORIZATIONS_A)));
        assertEquals(1, count(getGraph().getEdges(AUTHORIZATIONS_A)));

        getGraph().markVertexHidden(v1, VISIBILITY_A_AND_B, AUTHORIZATIONS_A);
        getGraph().flush();

        assertTrue("v1 exists (auth A)", getGraph().doesVertexExist("v1", AUTHORIZATIONS_A));
        assertFalse("v1 exists (auth B)", getGraph().doesVertexExist("v1", AUTHORIZATIONS_B));
        assertFalse("v1 exists (auth A&B)", getGraph().doesVertexExist("v1", AUTHORIZATIONS_A_AND_B));
        assertEquals(1, count(getGraph().getVertices(AUTHORIZATIONS_A_AND_B)));
        assertEquals(2, count(getGraph().getVertices(AUTHORIZATIONS_A)));
        assertEquals(0, count(getGraph().getVertices(AUTHORIZATIONS_B)));
        assertEquals(1, count(getGraph().getEdges(AUTHORIZATIONS_A)));

        getGraph().markVertexHidden(v1, VISIBILITY_A, AUTHORIZATIONS_A);
        getGraph().flush();

        assertFalse("v1 exists (auth A)", getGraph().doesVertexExist("v1", AUTHORIZATIONS_A));
        assertFalse("v1 exists (auth B)", getGraph().doesVertexExist("v1", AUTHORIZATIONS_B));
        assertFalse("v1 exists (auth A&B)", getGraph().doesVertexExist("v1", AUTHORIZATIONS_A_AND_B));
        assertEquals(1, count(getGraph().getVertices(AUTHORIZATIONS_A_AND_B)));
        assertEquals(1, count(getGraph().getVertices(AUTHORIZATIONS_A)));
        assertEquals(0, count(getGraph().getVertices(AUTHORIZATIONS_B)));
        assertEquals(0, count(getGraph().getEdges(AUTHORIZATIONS_A)));
        assertNull("found v1 but shouldn't have", getGraph().getVertex("v1", getGraph().getDefaultFetchHints(), AUTHORIZATIONS_A));
        Vertex v1Hidden = getGraph().getVertex("v1", FetchHints.ALL_INCLUDING_HIDDEN, AUTHORIZATIONS_A);
        assertNotNull("did not find v1 but should have", v1Hidden);
        assertTrue("v1 should be hidden", v1Hidden.isHidden(AUTHORIZATIONS_A));

        getGraph().markVertexVisible(v1, VISIBILITY_A, AUTHORIZATIONS_A);
        getGraph().flush();

        assertTrue("v1 exists (auth A)", getGraph().doesVertexExist("v1", AUTHORIZATIONS_A));
        assertFalse("v1 exists (auth B)", getGraph().doesVertexExist("v1", AUTHORIZATIONS_B));
        assertFalse("v1 exists (auth A&B)", getGraph().doesVertexExist("v1", AUTHORIZATIONS_A_AND_B));
        assertEquals(1, count(getGraph().getVertices(AUTHORIZATIONS_A_AND_B)));
        assertEquals(2, count(getGraph().getVertices(AUTHORIZATIONS_A)));
        assertEquals(0, count(getGraph().getVertices(AUTHORIZATIONS_B)));
        assertEquals(1, count(getGraph().getEdges(AUTHORIZATIONS_A)));

        getGraph().markVertexVisible(v1, VISIBILITY_A_AND_B, AUTHORIZATIONS_A);
        getGraph().flush();

        assertTrue("v1 exists (auth A)", getGraph().doesVertexExist("v1", AUTHORIZATIONS_A));
        assertFalse("v1 exists (auth B)", getGraph().doesVertexExist("v1", AUTHORIZATIONS_B));
        assertTrue("v1 exists (auth A&B)", getGraph().doesVertexExist("v1", AUTHORIZATIONS_A_AND_B));
        assertEquals(2, count(getGraph().getVertices(AUTHORIZATIONS_A)));
        assertEquals(1, count(getGraph().getEdges(AUTHORIZATIONS_A)));
    }

    @Test
    public void testMarkEdgeHidden() {
        Vertex v1 = getGraph().addVertex("v1", VISIBILITY_A, AUTHORIZATIONS_ALL, CONCEPT_TYPE_THING);
        Vertex v2 = getGraph().addVertex("v2", VISIBILITY_A, AUTHORIZATIONS_ALL, CONCEPT_TYPE_THING);
        Vertex v3 = getGraph().addVertex("v3", VISIBILITY_A, AUTHORIZATIONS_ALL, CONCEPT_TYPE_THING);
        Edge e1 = getGraph().addEdge("v1tov2", v1, v2, LABEL_LABEL1, VISIBILITY_A, AUTHORIZATIONS_ALL);
        getGraph().addEdge("v2tov3", v2, v3, LABEL_LABEL1, VISIBILITY_A, AUTHORIZATIONS_ALL);
        getGraph().flush();

        List<String> edgeIdList = new ArrayList<>();
        edgeIdList.add("v1tov2");
        edgeIdList.add("v2tov3");
        edgeIdList.add("bad");
        Map<String, Boolean> edgesExist = getGraph().doEdgesExist(edgeIdList, AUTHORIZATIONS_A);
        assertEquals(3, edgeIdList.size());
        assertTrue("v1tov2 exist", edgesExist.get("v1tov2"));
        assertTrue("v2tov3 exist", edgesExist.get("v2tov3"));
        assertFalse("bad exist", edgesExist.get("bad"));

        assertTrue("v1tov2 exists (auth A)", getGraph().doesEdgeExist("v1tov2", AUTHORIZATIONS_A));
        assertFalse("v1tov2 exists (auth B)", getGraph().doesEdgeExist("v1tov2", AUTHORIZATIONS_B));
        assertTrue("v1tov2 exists (auth A&B)", getGraph().doesEdgeExist("v1tov2", AUTHORIZATIONS_A_AND_B));
        assertEquals(3, count(getGraph().getVertices(AUTHORIZATIONS_A)));
        assertEquals(2, count(getGraph().getEdges(AUTHORIZATIONS_A)));
        assertEquals(1, count(getGraph().getVertex("v1", AUTHORIZATIONS_A).getEdges(Direction.BOTH, AUTHORIZATIONS_A)));
        assertEquals(1, count(getGraph().findPaths(new FindPathOptions("v1", "v3", 2), AUTHORIZATIONS_A_AND_B)));
        assertEquals(1, count(getGraph().findPaths(new FindPathOptions("v1", "v3", 10), AUTHORIZATIONS_A_AND_B)));

        getGraph().markEdgeHidden(e1, VISIBILITY_A_AND_B, AUTHORIZATIONS_A);
        getGraph().flush();

        assertTrue("v1tov2 exists (auth A)", getGraph().doesEdgeExist("v1tov2", AUTHORIZATIONS_A));
        assertFalse("v1tov2 exists (auth B)", getGraph().doesEdgeExist("v1tov2", AUTHORIZATIONS_B));
        assertFalse("v1tov2 exists (auth A&B)", getGraph().doesEdgeExist("v1tov2", AUTHORIZATIONS_A_AND_B));
        assertEquals(2, count(getGraph().getEdges(AUTHORIZATIONS_A)));
        assertEquals(0, count(getGraph().getEdges(AUTHORIZATIONS_B)));
        assertEquals(1, count(getGraph().getEdges(AUTHORIZATIONS_A_AND_B)));
        assertEquals(1, count(getGraph().getVertex("v1", AUTHORIZATIONS_A).getEdges(Direction.BOTH, AUTHORIZATIONS_A)));
        assertEquals(1, count(getGraph().getVertex("v1", AUTHORIZATIONS_A).getEdgeIds(Direction.BOTH, AUTHORIZATIONS_A)));
        assertEquals(0, count(getGraph().getVertex("v1", AUTHORIZATIONS_A_AND_B).getEdges(Direction.BOTH, AUTHORIZATIONS_A_AND_B)));
        assertEquals(0, count(getGraph().getVertex("v1", AUTHORIZATIONS_A_AND_B).getEdgeIds(Direction.BOTH, AUTHORIZATIONS_A_AND_B)));
        assertEquals(0, count(getGraph().getVertex("v1", AUTHORIZATIONS_A_AND_B).getEdgeInfos(Direction.BOTH, AUTHORIZATIONS_A_AND_B)));
        assertEquals(0, count(getGraph().getVertex("v1", FetchHints.ALL_INCLUDING_HIDDEN, AUTHORIZATIONS_A_AND_B).getEdges(Direction.BOTH, AUTHORIZATIONS_A_AND_B)));
        assertEquals(1, count(getGraph().getVertex("v1", FetchHints.ALL_INCLUDING_HIDDEN, AUTHORIZATIONS_A_AND_B).getEdges(Direction.BOTH, FetchHints.ALL_INCLUDING_HIDDEN, AUTHORIZATIONS_A_AND_B)));
        assertEquals(1, count(getGraph().getVertex("v1", FetchHints.ALL_INCLUDING_HIDDEN, AUTHORIZATIONS_A_AND_B).getEdgeIds(Direction.BOTH, AUTHORIZATIONS_A_AND_B)));
        assertEquals(1, count(getGraph().getVertex("v1", FetchHints.ALL_INCLUDING_HIDDEN, AUTHORIZATIONS_A_AND_B).getEdgeInfos(Direction.BOTH, AUTHORIZATIONS_A_AND_B)));
        assertEquals(0, count(getGraph().findPaths(new FindPathOptions("v1", "v3", 2), AUTHORIZATIONS_A_AND_B)));
        assertEquals(0, count(getGraph().findPaths(new FindPathOptions("v1", "v3", 10), AUTHORIZATIONS_A_AND_B)));
        assertEquals(1, count(getGraph().findPaths(new FindPathOptions("v1", "v3", 10), AUTHORIZATIONS_A)));
        assertNull("found e1 but shouldn't have", getGraph().getEdge("v1tov2", getGraph().getDefaultFetchHints(), AUTHORIZATIONS_A_AND_B));
        Edge e1Hidden = getGraph().getEdge("v1tov2", FetchHints.ALL_INCLUDING_HIDDEN, AUTHORIZATIONS_A_AND_B);
        assertNotNull("did not find e1 but should have", e1Hidden);
        assertTrue("e1 should be hidden", e1Hidden.isHidden(AUTHORIZATIONS_A_AND_B));

        getGraph().markEdgeVisible(e1, VISIBILITY_A_AND_B, AUTHORIZATIONS_A);
        getGraph().flush();

        assertTrue("v1tov2 exists (auth A)", getGraph().doesEdgeExist("v1tov2", AUTHORIZATIONS_A));
        assertFalse("v1tov2 exists (auth B)", getGraph().doesEdgeExist("v1tov2", AUTHORIZATIONS_B));
        assertTrue("v1tov2 exists (auth A&B)", getGraph().doesEdgeExist("v1tov2", AUTHORIZATIONS_A_AND_B));
        assertEquals(3, count(getGraph().getVertices(AUTHORIZATIONS_A)));
        assertEquals(2, count(getGraph().getEdges(AUTHORIZATIONS_A)));
        assertEquals(1, count(getGraph().getVertex("v1", AUTHORIZATIONS_A).getEdges(Direction.BOTH, AUTHORIZATIONS_A)));
        assertEquals(1, count(getGraph().findPaths(new FindPathOptions("v1", "v3", 2), AUTHORIZATIONS_A_AND_B)));
        assertEquals(1, count(getGraph().findPaths(new FindPathOptions("v1", "v3", 10), AUTHORIZATIONS_A_AND_B)));
    }

    @Test
    public void testSearchingForHiddenEdges() {
        Vertex v1 = getGraph().addVertex("v1", VISIBILITY_A, AUTHORIZATIONS_ALL, CONCEPT_TYPE_THING);
        Vertex v2 = getGraph().addVertex("v2", VISIBILITY_A, AUTHORIZATIONS_ALL, CONCEPT_TYPE_THING);
        Vertex v3 = getGraph().addVertex("v3", VISIBILITY_A, AUTHORIZATIONS_ALL, CONCEPT_TYPE_THING);
        Edge e1 = getGraph().addEdge("v1tov2", v1, v2, LABEL_LABEL1, VISIBILITY_A, AUTHORIZATIONS_ALL);
        Edge e2 = getGraph().addEdge("v2tov3", v2, v3, LABEL_LABEL1, VISIBILITY_A, AUTHORIZATIONS_ALL);
        getGraph().flush();

        getGraph().markEdgeHidden(e1, VISIBILITY_B, AUTHORIZATIONS_ALL);
        getGraph().flush();

        FetchHints propertiesFetchHints = FetchHints.builder()
                .setIncludeAllProperties(true)
                .build();
        QueryResultsIterable<Edge> edges = getGraph().query(AUTHORIZATIONS_A)
                .edges(propertiesFetchHints);
        assertResultsCount(2, edges);
        assertEdgeIdsAnyOrder(edges, e1.getId(), e2.getId());

        edges = getGraph().query(AUTHORIZATIONS_A_AND_B)
                .edges(propertiesFetchHints);
        assertResultsCount(1, edges);
        assertEdgeIdsAnyOrder(edges, e2.getId());

        getGraph().markEdgeVisible(e1, VISIBILITY_B, AUTHORIZATIONS_ALL);
        getGraph().flush();

        edges = getGraph().query(AUTHORIZATIONS_A_AND_B)
                .edges(propertiesFetchHints);
        assertResultsCount(2, edges);
        assertEdgeIdsAnyOrder(edges, e1.getId(), e2.getId());
    }

    @Test
    public void testMarkPropertyHidden() {
        Vertex v1 = getGraph().prepareVertex("v1", VISIBILITY_A, CONCEPT_TYPE_THING)
                .addPropertyValue("key1", "prop1", stringValue("value1"), VISIBILITY_A)
                .addPropertyValue("key1", "prop1", stringValue("value1"), VISIBILITY_B)
                .addPropertyValue("key2", "prop1", stringValue("value1"), VISIBILITY_A)
                .save(AUTHORIZATIONS_ALL);
        getGraph().flush();

        Vertex vvv = getGraph().getVertex("v1", AUTHORIZATIONS_A_AND_B);

        assertEquals(3, count(vvv.getProperties("prop1")));

        v1.markPropertyHidden("key1", "prop1", VISIBILITY_A, VISIBILITY_A_AND_B, AUTHORIZATIONS_A_AND_B);
        getGraph().flush();

        List<Property> properties = toList(getGraph().getVertex("v1", AUTHORIZATIONS_A_AND_B).getProperties("prop1"));
        assertEquals(2, count(properties));
        boolean foundProp1Key2 = false;
        boolean foundProp1Key1VisB = false;
        for (Property property : properties) {
            if (property.getName().equals("prop1")) {
                if (property.getKey().equals("key2")) {
                    foundProp1Key2 = true;
                } else if (property.getKey().equals("key1")) {
                    if (property.getVisibility().equals(VISIBILITY_B)) {
                        foundProp1Key1VisB = true;
                    } else {
                        throw new RuntimeException("Unexpected visibility " + property.getVisibility());
                    }
                } else {
                    throw new RuntimeException("Unexpected property key " + property.getKey());
                }
            } else {
                throw new RuntimeException("Unexpected property name " + property.getName());
            }
        }
        assertTrue("Prop1Key2 not found", foundProp1Key2);
        assertTrue("Prop1Key1VisB not found", foundProp1Key1VisB);

        List<Property> hiddenProperties = toList(getGraph().getVertex("v1", FetchHints.ALL_INCLUDING_HIDDEN, AUTHORIZATIONS_A_AND_B).getProperties());
        assertEquals(3, hiddenProperties.size());
        boolean foundProp1Key1VisA = false;
        foundProp1Key2 = false;
        foundProp1Key1VisB = false;
        for (Property property : hiddenProperties) {
            if (property.getName().equals("prop1")) {
                if (property.getKey().equals("key2")) {
                    foundProp1Key2 = true;
                    assertFalse("should not be hidden", property.isHidden(AUTHORIZATIONS_A_AND_B));
                } else if (property.getKey().equals("key1")) {
                    if (property.getVisibility().equals(VISIBILITY_A)) {
                        foundProp1Key1VisA = true;
                        assertFalse("should not be hidden", property.isHidden(AUTHORIZATIONS_A));
                        assertTrue("should be hidden", property.isHidden(AUTHORIZATIONS_A_AND_B));
                    } else if (property.getVisibility().equals(VISIBILITY_B)) {
                        foundProp1Key1VisB = true;
                        assertFalse("should not be hidden", property.isHidden(AUTHORIZATIONS_A_AND_B));
                    } else {
                        throw new RuntimeException("Unexpected visibility " + property.getVisibility());
                    }
                } else {
                    throw new RuntimeException("Unexpected property key " + property.getKey());
                }
            } else {
                throw new RuntimeException("Unexpected property name " + property.getName());
            }
        }
        assertTrue("Prop1Key2 not found", foundProp1Key2);
        assertTrue("Prop1Key1VisB not found", foundProp1Key1VisB);
        assertTrue("Prop1Key1VisA not found", foundProp1Key1VisA);

        v1.markPropertyVisible("key1", "prop1", VISIBILITY_A, VISIBILITY_A_AND_B, AUTHORIZATIONS_A_AND_B);
        getGraph().flush();

        assertEquals(3, count(getGraph().getVertex("v1", AUTHORIZATIONS_A_AND_B).getProperties("prop1")));
    }

    @Test
    public void testSearchingForHiddenVertices() {
        Vertex v1 = getGraph().prepareVertex("v1", VISIBILITY_A, CONCEPT_TYPE_THING)
                .addPropertyValue("key1", "prop1", stringValue("value1"), VISIBILITY_A)
                .save(AUTHORIZATIONS_ALL);
        Vertex v2 = getGraph().prepareVertex("v2", VISIBILITY_A, CONCEPT_TYPE_THING)
                .addPropertyValue("key1", "prop1", stringValue("value1"), VISIBILITY_A)
                .save(AUTHORIZATIONS_ALL);
        getGraph().flush();

        getGraph().markVertexHidden(v1, VISIBILITY_B, AUTHORIZATIONS_ALL);
        getGraph().flush();

        FetchHints propertiesFetchHints = FetchHints.builder()
                .setIncludeAllProperties(true)
                .build();
        QueryResultsIterable<Vertex> vertices = getGraph().query(hasFilter("prop1", EQUAL, stringValue("value1")), AUTHORIZATIONS_A)
                .vertices(propertiesFetchHints);
        assertResultsCount(2, vertices);
        assertVertexIdsAnyOrder(vertices, v1.getId(), v2.getId());

        vertices = getGraph().query(hasFilter("prop1", EQUAL, stringValue("value1")), AUTHORIZATIONS_A_AND_B)
                .vertices(propertiesFetchHints);
        assertResultsCount(1, vertices);
        assertVertexIdsAnyOrder(vertices, v2.getId());

        getGraph().markVertexVisible(v1, VISIBILITY_B, AUTHORIZATIONS_ALL);
        getGraph().flush();

        vertices = getGraph().query(hasFilter("prop1", EQUAL, stringValue("value1")), AUTHORIZATIONS_A_AND_B)
                .vertices(propertiesFetchHints);
        assertResultsCount(2, vertices);
        assertVertexIdsAnyOrder(vertices, v1.getId(), v2.getId());
    }

    // This tests simulates two workspaces w1 (via A) and w1 (vis B).
    // Both w1 and w2 has e1 on it.
    // e1 is linked to e2.
    // What happens if w1 (vis A) marks e1 hidden, then deletes itself?
    @Test
    public void testMarkVertexHiddenAndDeleteEdges() {
        Vertex w1 = getGraph().addVertex("w1", VISIBILITY_A, AUTHORIZATIONS_A, CONCEPT_TYPE_THING);
        Vertex w2 = getGraph().addVertex("w2", VISIBILITY_B, AUTHORIZATIONS_B, CONCEPT_TYPE_THING);
        Vertex e1 = getGraph().addVertex("e1", VISIBILITY_EMPTY, AUTHORIZATIONS_A, CONCEPT_TYPE_THING);
        Vertex e2 = getGraph().addVertex("e2", VISIBILITY_EMPTY, AUTHORIZATIONS_A, CONCEPT_TYPE_THING);
        getGraph().addEdge("w1-e1", w1, e1, LABEL_LABEL1, VISIBILITY_A, AUTHORIZATIONS_A);
        getGraph().addEdge("w2-e1", w2, e1, LABEL_LABEL1, VISIBILITY_B, AUTHORIZATIONS_B);
        getGraph().addEdge("e1-e2", e1, e2, LABEL_LABEL1, VISIBILITY_EMPTY, AUTHORIZATIONS_A);
        getGraph().flush();

        e1 = getGraph().getVertex("e1", AUTHORIZATIONS_EMPTY);
        getGraph().markVertexHidden(e1, VISIBILITY_A, AUTHORIZATIONS_EMPTY);
        getGraph().flush();

        w1 = getGraph().getVertex("w1", AUTHORIZATIONS_A);
        getGraph().deleteVertex("w1", AUTHORIZATIONS_A);
        getGraph().flush();

        assertEquals(1, count(getGraph().getVertices(AUTHORIZATIONS_A)));
        assertEquals("e2", toList(getGraph().getVertices(AUTHORIZATIONS_A)).get(0).getId());

        assertEquals(3, count(getGraph().getVertices(AUTHORIZATIONS_B)));
        boolean foundW2 = false;
        boolean foundE1 = false;
        boolean foundE2 = false;
        for (Vertex v : getGraph().getVertices(AUTHORIZATIONS_B)) {
            if (v.getId().equals("w2")) {
                foundW2 = true;
            } else if (v.getId().equals("e1")) {
                foundE1 = true;
            } else if (v.getId().equals("e2")) {
                foundE2 = true;
            } else {
                throw new GeException("Unexpected id: " + v.getId());
            }
        }
        assertTrue("w2", foundW2);
        assertTrue("e1", foundE1);
        assertTrue("e2", foundE2);
    }

    @Test
    public void testDeleteVertexWithProperties() {
        Vertex v1 = getGraph().prepareVertex("v1", VISIBILITY_A, CONCEPT_TYPE_THING)
                .setProperty("prop1", stringValue("value1"), VISIBILITY_B)
                .save(AUTHORIZATIONS_A_AND_B);
        getGraph().flush();
        Property prop1 = v1.getProperty("prop1");

        assertEquals(1, count(getGraph().getVertices(AUTHORIZATIONS_A)));

        getGraph().deleteVertex("v1", AUTHORIZATIONS_A);
        getGraph().flush();
        assertEquals(0, count(getGraph().getVertices(AUTHORIZATIONS_A_AND_B)));

        assertEvents(
                new AddVertexEvent(getGraph(), v1),
                new AddPropertyEvent(getGraph(), v1, prop1),
                new DeleteVertexEvent(getGraph(), v1)
        );
    }

    @Test
    public void testSaveElementMutations() {
        List<ElementMutation<? extends Element>> mutations = new ArrayList<>();
        for (int i = 0; i < 2; i++) {
            ElementBuilder<Vertex> m = getGraph().prepareVertex("v" + i, VISIBILITY_A, CONCEPT_TYPE_THING)
                    .addPropertyValue("k1", "name", stringValue("joe"), VISIBILITY_A)
                    .addExtendedData("table1", "row1", "col1", stringValue("extended"), VISIBILITY_A);
            mutations.add(m);
        }
        List<Element> saveVertices = toList(getGraph().saveElementMutations(mutations, AUTHORIZATIONS_ALL));
        getGraph().flush();

        assertEvents(
                new AddVertexEvent(getGraph(), (Vertex) saveVertices.get(0)),
                new AddPropertyEvent(getGraph(), saveVertices.get(0), saveVertices.get(0).getProperty("k1", "name")),
                new AddExtendedDataEvent(getGraph(), saveVertices.get(0), "table1", "row1", "col1", null, stringValue("extended"), VISIBILITY_A),
                new AddVertexEvent(getGraph(), (Vertex) saveVertices.get(1)),
                new AddPropertyEvent(getGraph(), saveVertices.get(1), saveVertices.get(1).getProperty("k1", "name")),
                new AddExtendedDataEvent(getGraph(), saveVertices.get(1), "table1", "row1", "col1", null, stringValue("extended"), VISIBILITY_A)
        );
        clearGraphEvents();

        QueryResultsIterable<Vertex> vertices = getGraph().query(AUTHORIZATIONS_ALL).vertices();
        assertResultsCount(2, 2, vertices);

        QueryResultsIterable<? extends GeObject> items = getGraph().query(
                hasFilter("col1", EQUAL, stringValue("extended")),
                AUTHORIZATIONS_ALL
        ).search();
        assertResultsCount(2, 2, items);

        mutations.clear();
        mutations.add(((Vertex) saveVertices.get(0)).prepareMutation());
        getGraph().saveElementMutations(mutations, AUTHORIZATIONS_ALL);
        getGraph().flush();

        assertEvents();
    }

    @Test
    public void testSaveCombinedElementMutations() {
        List<ElementMutation<? extends Element>> mutations = new ArrayList<>();

        for (int i = 0; i < 2; i++) {
            ElementBuilder<Edge> m = getGraph().prepareEdge("v" + i, "v" + (i + 1), LABEL_LABEL1, VISIBILITY_A);
            mutations.add(m);
        }

        for (int i = 0; i < 3; i++) {
            ElementBuilder<Vertex> m = getGraph().prepareVertex("v" + i, VISIBILITY_A, CONCEPT_TYPE_THING);
            mutations.add(m);
        }

        Iterable<Element> elements = getGraph().saveElementMutations(mutations, AUTHORIZATIONS_ALL);
        assertEquals(5, count(elements));
    }

    @Test
    public void testAddValuesToExistingProperties() {
        Vertex v1 = getGraph().addVertex("v1", VISIBILITY_A, AUTHORIZATIONS_ALL, CONCEPT_TYPE_THING);
        getGraph().flush();

        getGraph().defineProperty("p1").dataType(TextValue.class).sortable(true).textIndexHint(TextIndexHint.ALL).define();

        v1.addPropertyValue("k1", "p1", stringValue("val1"), VISIBILITY_A, AUTHORIZATIONS_ALL);
        getGraph().flush();

        assertIdsAnyOrder(getGraph().query(hasFilter("p1", EQUAL, stringValue("val1")), AUTHORIZATIONS_ALL).vertexIds(), "v1");

        v1.addPropertyValue("k2", "p1", stringValue("val2"), VISIBILITY_A, AUTHORIZATIONS_ALL);
        getGraph().flush();

        assertIdsAnyOrder(getGraph().query(hasFilter("p1", EQUAL, stringValue("val1")), AUTHORIZATIONS_ALL).vertexIds(), "v1");
        assertIdsAnyOrder(getGraph().query(hasFilter("p1", EQUAL, stringValue("val2")), AUTHORIZATIONS_ALL).vertexIds(), "v1");
        assertResultsCount(0, 0, getGraph().query(hasFilter("p1", EQUAL, stringValue("val3")), AUTHORIZATIONS_ALL).vertexIds());

        v1.addPropertyValue("k1", "p1", stringValue("val3"), VISIBILITY_A, AUTHORIZATIONS_ALL);
        getGraph().flush();

        assertIdsAnyOrder(getGraph().query(hasFilter("p1", EQUAL, stringValue("val3")), AUTHORIZATIONS_ALL).vertexIds(), "v1");
        assertIdsAnyOrder(getGraph().query(hasFilter("p1", EQUAL, stringValue("val2")), AUTHORIZATIONS_ALL).vertexIds(), "v1");
        assertResultsCount(0, 0, getGraph().query(hasFilter("p1", EQUAL, stringValue("val1")), AUTHORIZATIONS_ALL).vertexIds());
    }

    @Test
    public void testRemoveValuesFromMultivalueProperties() {
        getGraph().defineProperty("p1").dataType(TextValue.class).sortable(true).textIndexHint(TextIndexHint.ALL).define();

        Vertex v1 = getGraph().prepareVertex("v1", VISIBILITY_A, CONCEPT_TYPE_THING)
                .addPropertyValue("k1", "p1", stringValue("v1"), VISIBILITY_A)
                .addPropertyValue("k2", "p1", stringValue("v2"), VISIBILITY_A)
                .save(AUTHORIZATIONS_ALL);
        getGraph().flush();

        assertIdsAnyOrder(getGraph().query(hasFilter("p1", EQUAL, stringValue("v1")), AUTHORIZATIONS_ALL).vertexIds(), "v1");
        assertIdsAnyOrder(getGraph().query(hasFilter("p1", EQUAL, stringValue("v2")), AUTHORIZATIONS_ALL).vertexIds(), "v1");
        assertResultsCount(0, 0, getGraph().query(hasFilter("p1", EQUAL, stringValue("v3")), AUTHORIZATIONS_ALL).vertexIds());

        v1.prepareMutation()
                .addPropertyValue("k3", "p1", stringValue("v3"), VISIBILITY_A)
                .deleteProperty("k1", "p1", VISIBILITY_A)
                .save(AUTHORIZATIONS_ALL);
        getGraph().flush();

        assertIdsAnyOrder(getGraph().query(hasFilter("p1", EQUAL, stringValue("v2")), AUTHORIZATIONS_ALL).vertexIds(), "v1");
        assertIdsAnyOrder(getGraph().query(hasFilter("p1", EQUAL, stringValue("v3")), AUTHORIZATIONS_ALL).vertexIds(), "v1");
        assertResultsCount(0, 0, getGraph().query(hasFilter("p1", EQUAL, stringValue("v1")), AUTHORIZATIONS_ALL).vertexIds());

        v1.deleteProperty("k2", "p1", VISIBILITY_A, AUTHORIZATIONS_ALL);
        getGraph().flush();

        assertIdsAnyOrder(getGraph().query(hasFilter("p1", EQUAL, stringValue("v3")), AUTHORIZATIONS_ALL).vertexIds(), "v1");
        assertResultsCount(0, 0, getGraph().query(hasFilter("p1", EQUAL, stringValue("v1")), AUTHORIZATIONS_ALL).vertexIds());
        assertResultsCount(0, 0, getGraph().query(hasFilter("p1", EQUAL, stringValue("v2")), AUTHORIZATIONS_ALL).vertexIds());
    }

    @Test
    public void testGetVertexWithBadAuthorizations() {
        getGraph().addVertex("v1", VISIBILITY_EMPTY, AUTHORIZATIONS_A, CONCEPT_TYPE_THING);
        getGraph().flush();

        try {
            getGraph().getVertex("v1", AUTHORIZATIONS_BAD);
            throw new RuntimeException("Should throw " + SecurityGeException.class.getSimpleName());
        } catch (SecurityGeException ex) {
            // ok
        }
    }

    @Test
    public void testGetVerticesFromVertex() {
        Vertex v1 = getGraph().addVertex("v1", VISIBILITY_A, AUTHORIZATIONS_A, CONCEPT_TYPE_THING);
        Vertex v2 = getGraph().addVertex("v2", VISIBILITY_A, AUTHORIZATIONS_A, CONCEPT_TYPE_THING);
        Vertex v3 = getGraph().addVertex("v3", VISIBILITY_A, AUTHORIZATIONS_A, CONCEPT_TYPE_THING);
        Vertex v4 = getGraph().addVertex("v4", VISIBILITY_A, AUTHORIZATIONS_A, CONCEPT_TYPE_THING);
        Vertex v5 = getGraph().addVertex("v5", VISIBILITY_B, AUTHORIZATIONS_B, CONCEPT_TYPE_THING);
        getGraph().addEdge(v1, v2, LABEL_LABEL1, VISIBILITY_A, AUTHORIZATIONS_A);
        getGraph().addEdge(v1, v3, LABEL_LABEL1, VISIBILITY_A, AUTHORIZATIONS_A);
        getGraph().addEdge(v1, v4, LABEL_LABEL1, VISIBILITY_A, AUTHORIZATIONS_A);
        getGraph().addEdge(v2, v3, LABEL_LABEL1, VISIBILITY_A, AUTHORIZATIONS_A);
        getGraph().addEdge(v2, v5, LABEL_LABEL1, VISIBILITY_A, AUTHORIZATIONS_A_AND_B);
        getGraph().flush();

        v1 = getGraph().getVertex("v1", AUTHORIZATIONS_A);
        assertEquals(3, count(v1.getVertices(Direction.BOTH, AUTHORIZATIONS_A)));
        assertEquals(3, count(v1.getVertices(Direction.OUT, AUTHORIZATIONS_A)));
        assertEquals(0, count(v1.getVertices(Direction.IN, AUTHORIZATIONS_A)));

        v2 = getGraph().getVertex("v2", AUTHORIZATIONS_A);
        assertEquals(2, count(v2.getVertices(Direction.BOTH, AUTHORIZATIONS_A)));
        assertEquals(1, count(v2.getVertices(Direction.OUT, AUTHORIZATIONS_A)));
        assertEquals(1, count(v2.getVertices(Direction.IN, AUTHORIZATIONS_A)));

        v3 = getGraph().getVertex("v3", AUTHORIZATIONS_A);
        assertEquals(2, count(v3.getVertices(Direction.BOTH, AUTHORIZATIONS_A)));
        assertEquals(0, count(v3.getVertices(Direction.OUT, AUTHORIZATIONS_A)));
        assertEquals(2, count(v3.getVertices(Direction.IN, AUTHORIZATIONS_A)));

        v4 = getGraph().getVertex("v4", AUTHORIZATIONS_A);
        assertEquals(1, count(v4.getVertices(Direction.BOTH, AUTHORIZATIONS_A)));
        assertEquals(0, count(v4.getVertices(Direction.OUT, AUTHORIZATIONS_A)));
        assertEquals(1, count(v4.getVertices(Direction.IN, AUTHORIZATIONS_A)));
    }

    @Test
    public void testGetVertexIdsFromVertex() {
        Vertex v1 = getGraph().addVertex("v1", VISIBILITY_A, AUTHORIZATIONS_A, CONCEPT_TYPE_THING);
        Vertex v2 = getGraph().addVertex("v2", VISIBILITY_A, AUTHORIZATIONS_A, CONCEPT_TYPE_THING);
        Vertex v3 = getGraph().addVertex("v3", VISIBILITY_A, AUTHORIZATIONS_A, CONCEPT_TYPE_THING);
        Vertex v4 = getGraph().addVertex("v4", VISIBILITY_A, AUTHORIZATIONS_A, CONCEPT_TYPE_THING);
        Vertex v5 = getGraph().addVertex("v5", VISIBILITY_B, AUTHORIZATIONS_B, CONCEPT_TYPE_THING);
        getGraph().addEdge(v1, v2, LABEL_LABEL1, VISIBILITY_A, AUTHORIZATIONS_A);
        getGraph().addEdge(v1, v3, LABEL_LABEL1, VISIBILITY_A, AUTHORIZATIONS_A);
        getGraph().addEdge(v1, v4, LABEL_LABEL1, VISIBILITY_A, AUTHORIZATIONS_A);
        getGraph().addEdge(v2, v3, LABEL_LABEL1, VISIBILITY_A, AUTHORIZATIONS_A);
        getGraph().addEdge(v2, v5, LABEL_LABEL1, VISIBILITY_A, AUTHORIZATIONS_A_AND_B);
        getGraph().flush();

        v1 = getGraph().getVertex("v1", AUTHORIZATIONS_A);
        assertEquals(3, count(v1.getVertexIds(Direction.BOTH, AUTHORIZATIONS_A)));
        assertEquals(3, count(v1.getVertexIds(Direction.OUT, AUTHORIZATIONS_A)));
        assertEquals(0, count(v1.getVertexIds(Direction.IN, AUTHORIZATIONS_A)));

        v2 = getGraph().getVertex("v2", AUTHORIZATIONS_A);
        assertEquals(3, count(v2.getVertexIds(Direction.BOTH, AUTHORIZATIONS_A)));
        assertEquals(2, count(v2.getVertexIds(Direction.OUT, AUTHORIZATIONS_A)));
        assertEquals(1, count(v2.getVertexIds(Direction.IN, AUTHORIZATIONS_A)));

        v3 = getGraph().getVertex("v3", AUTHORIZATIONS_A);
        assertEquals(2, count(v3.getVertexIds(Direction.BOTH, AUTHORIZATIONS_A)));
        assertEquals(0, count(v3.getVertexIds(Direction.OUT, AUTHORIZATIONS_A)));
        assertEquals(2, count(v3.getVertexIds(Direction.IN, AUTHORIZATIONS_A)));

        v4 = getGraph().getVertex("v4", AUTHORIZATIONS_A);
        assertEquals(1, count(v4.getVertexIds(Direction.BOTH, AUTHORIZATIONS_A)));
        assertEquals(0, count(v4.getVertexIds(Direction.OUT, AUTHORIZATIONS_A)));
        assertEquals(1, count(v4.getVertexIds(Direction.IN, AUTHORIZATIONS_A)));
    }

    @Test
    public void testBlankVisibilityString() {
        Vertex v = getGraph().addVertex("v1", VISIBILITY_EMPTY, AUTHORIZATIONS_EMPTY, CONCEPT_TYPE_THING);
        assertNotNull(v);
        assertEquals("v1", v.getId());
        getGraph().flush();

        v = getGraph().getVertex("v1", AUTHORIZATIONS_EMPTY);
        assertNotNull(v);
        assertEquals("v1", v.getId());
        assertEquals(VISIBILITY_EMPTY, v.getVisibility());
    }

    @Test
    public void testElementMutationDoesntChangeObjectUntilSave() {
        Vertex v = getGraph().addVertex("v1", VISIBILITY_EMPTY, AUTHORIZATIONS_ALL, CONCEPT_TYPE_THING);
        v.setProperty("prop1", stringValue("value1-1"), VISIBILITY_A, AUTHORIZATIONS_ALL);
        getGraph().flush();

        ElementMutation<Vertex> m = v.prepareMutation()
                .setProperty("prop1", stringValue("value1-2"), VISIBILITY_A)
                .setProperty("prop2", stringValue("value2-2"), VISIBILITY_A);
        assertEquals(1, count(v.getProperties()));
        assertEquals(stringValue("value1-1"), v.getPropertyValue("prop1"));

        v = m.save(AUTHORIZATIONS_A_AND_B);
        assertEquals(2, count(v.getProperties()));
        assertEquals(stringValue("value1-2"), v.getPropertyValue("prop1"));
        assertEquals(stringValue("value2-2"), v.getPropertyValue("prop2"));
    }


    @Test
    public void testEmptyPropertyMutation() {
        Vertex v1 = getGraph().addVertex("v1", VISIBILITY_A, AUTHORIZATIONS_ALL, CONCEPT_TYPE_THING);
        v1.prepareMutation().save(AUTHORIZATIONS_ALL);
    }

    @Test
    public void testThreadedInserts() throws InterruptedException {
        AtomicInteger completedThreads = new AtomicInteger();
        new Thread(() -> {
            getGraph().addVertex("v1", VISIBILITY_EMPTY, AUTHORIZATIONS_EMPTY, CONCEPT_TYPE_THING);
            getGraph().flush();
            completedThreads.incrementAndGet();
        }).start();
        new Thread(() -> {
            getGraph().addVertex("v2", VISIBILITY_EMPTY, AUTHORIZATIONS_EMPTY, CONCEPT_TYPE_THING);
            getGraph().flush();
            completedThreads.incrementAndGet();
        }).start();

        while (completedThreads.get() < 2) {
            Thread.sleep(100);
        }

        assertVertexIdsAnyOrder(
                getGraph().getVertices(AUTHORIZATIONS_EMPTY),
                "v1", "v2"
        );
        assertVertexIdsAnyOrder(
                getGraph().query(AUTHORIZATIONS_EMPTY).vertices(),
                "v1", "v2"
        );
    }

    @Test
    public void testThreadedInsertsNoFlushesInThreads() throws InterruptedException {
        AtomicInteger completedThreads = new AtomicInteger();
        new Thread(() -> {
            getGraph().addVertex("v1", VISIBILITY_EMPTY, AUTHORIZATIONS_EMPTY, CONCEPT_TYPE_THING);
            completedThreads.incrementAndGet();
        }).start();
        new Thread(() -> {
            getGraph().addVertex("v2", VISIBILITY_EMPTY, AUTHORIZATIONS_EMPTY, CONCEPT_TYPE_THING);
            completedThreads.incrementAndGet();
        }).start();

        while (completedThreads.get() < 2) {
            Thread.sleep(100);
        }
        getGraph().flush();

        for (int i = 0; i <= 10; i++) {
            try {
                assertVertexIdsAnyOrder(
                        getGraph().getVertices(AUTHORIZATIONS_EMPTY),
                        "v1", "v2"
                );
                assertVertexIdsAnyOrder(
                        getGraph().query(AUTHORIZATIONS_EMPTY).vertices(),
                        "v1", "v2"
                );
                break;
            } catch (AssertionError ex) {
                if (i == 10) {
                    throw ex;
                }
                // try again
                Thread.sleep(500);
            }
        }
    }


    @Test
    public void testVertexHashCodeAndEquals() {
        Vertex v1 = getGraph().prepareVertex("v1", VISIBILITY_A, CONCEPT_TYPE_THING)
                .setProperty("prop1", stringValue("value1"), VISIBILITY_A)
                .save(AUTHORIZATIONS_A);
        Vertex v2 = getGraph().prepareVertex("v2", VISIBILITY_A, CONCEPT_TYPE_THING)
                .setProperty("prop1", stringValue("value1"), VISIBILITY_A)
                .save(AUTHORIZATIONS_A);
        getGraph().flush();

        Vertex v1Loaded = getGraph().getVertex("v1", AUTHORIZATIONS_A);

        assertEquals(v1Loaded.hashCode(), v1.hashCode());
        assertTrue(v1Loaded.equals(v1));

        assertNotEquals(v1Loaded.hashCode(), v2.hashCode());
        assertFalse(v1Loaded.equals(v2));
    }

    @Test
    public void testEdgeHashCodeAndEquals() {
        Vertex v1 = getGraph().prepareVertex("v1", VISIBILITY_A, CONCEPT_TYPE_THING).save(AUTHORIZATIONS_A);
        Vertex v2 = getGraph().prepareVertex("v2", VISIBILITY_A, CONCEPT_TYPE_THING).save(AUTHORIZATIONS_A);
        Edge e1 = getGraph().prepareEdge("e1", v1, v2, LABEL_LABEL1, VISIBILITY_A)
                .setProperty("prop1", stringValue("value1"), VISIBILITY_A)
                .save(AUTHORIZATIONS_A);
        Edge e2 = getGraph().prepareEdge("e2", v1, v2, LABEL_LABEL1, VISIBILITY_A)
                .setProperty("prop1", stringValue("value1"), VISIBILITY_A)
                .save(AUTHORIZATIONS_A);
        getGraph().flush();

        Edge e1Loaded = getGraph().getEdge("e1", AUTHORIZATIONS_A);

        assertEquals(e1Loaded.hashCode(), e1.hashCode());
        assertTrue(e1Loaded.equals(e1));

        assertNotEquals(e1Loaded.hashCode(), e2.hashCode());
        assertFalse(e1Loaded.equals(e2));
    }

    @Test
    public void testAddVertexWithoutIndexing() {
        assumeTrue("add vertex without indexing not supported", !isDefaultSearchIndex());

        getGraph().prepareVertex("v1", VISIBILITY_A, CONCEPT_TYPE_THING)
                .setProperty("prop1", stringValue("value1"), VISIBILITY_A)
                .setIndexHint(IndexHint.DO_NOT_INDEX)
                .save(AUTHORIZATIONS_A);
        getGraph().flush();

        Iterable<Vertex> vertices = getGraph().query(hasFilter("prop1", EQUAL, stringValue("value1")), AUTHORIZATIONS_A_AND_B)
                .vertices();
        assertVertexIds(vertices);
    }

    @Test
    public void testAlterVertexWithoutIndexing() {
        assumeTrue("alter vertex without indexing not supported", !isDefaultSearchIndex());

        getGraph().prepareVertex("v1", VISIBILITY_A, CONCEPT_TYPE_THING)
                .setIndexHint(IndexHint.DO_NOT_INDEX)
                .save(AUTHORIZATIONS_A);
        getGraph().flush();

        Vertex v1 = getGraph().getVertex("v1", AUTHORIZATIONS_A);
        v1.prepareMutation()
                .setProperty("prop1", stringValue("value1"), VISIBILITY_A)
                .setIndexHint(IndexHint.DO_NOT_INDEX)
                .save(AUTHORIZATIONS_A);
        getGraph().flush();

        Iterable<Vertex> vertices = getGraph().query(hasFilter("prop1", EQUAL, stringValue("value1")), AUTHORIZATIONS_A)
                .vertices();
        assertVertexIds(vertices);
    }

    @Test
    public void testAddEdgeWithoutIndexing() {
        assumeTrue("add edge without indexing not supported", !isDefaultSearchIndex());

        Vertex v1 = getGraph().addVertex("v1", VISIBILITY_A, AUTHORIZATIONS_A, CONCEPT_TYPE_THING);
        Vertex v2 = getGraph().addVertex("v2", VISIBILITY_A, AUTHORIZATIONS_A, CONCEPT_TYPE_THING);
        getGraph().prepareEdge("e1", v1, v2, LABEL_LABEL1, VISIBILITY_A)
                .setProperty("prop1", stringValue("value1"), VISIBILITY_A)
                .setIndexHint(IndexHint.DO_NOT_INDEX)
                .save(AUTHORIZATIONS_A);
        getGraph().flush();

        Iterable<Edge> edges = getGraph().query(hasFilter("prop1", EQUAL, stringValue("value1")), AUTHORIZATIONS_A_AND_B)
                .edges();
        assertEdgeIds(edges);
    }


    @Test
    public void testLargeFieldValuesThatAreMarkedWithExactMatch() {
        getGraph().defineProperty("field1").dataType(TextValue.class).textIndexHint(TextIndexHint.EXACT_MATCH).define();

        StringBuilder largeText = new StringBuilder();
        for (int i = 0; i < 10000; i++) {
            largeText.append("test ");
        }

        getGraph().prepareVertex("v1", VISIBILITY_EMPTY, CONCEPT_TYPE_THING)
                .addPropertyValue("", "field1", stringValue(largeText.toString()), VISIBILITY_EMPTY)
                .save(AUTHORIZATIONS_EMPTY);
        getGraph().flush();
    }

    @Test
    public void testReturningAlterConceptAndEdgeType() {
        Vertex v1 = getGraph().prepareVertex("v1", VISIBILITY_A, CONCEPT_TYPE_THING)
                .setProperty("prop1", intValue(1), VISIBILITY_A)
                .save(AUTHORIZATIONS_A_AND_B);
        Vertex v2 = getGraph().prepareVertex("v2", VISIBILITY_A, CONCEPT_TYPE_THING)
                .setProperty("prop2", intValue(1), VISIBILITY_A)
                .save(AUTHORIZATIONS_A_AND_B);
        Edge e1 = getGraph().prepareEdge(v1, v2, "label1", VISIBILITY_A)
                .setProperty("p1", intValue(1), VISIBILITY_A)
                .save(AUTHORIZATIONS_A_AND_B);
        getGraph().flush();
        assertEquals(CONCEPT_TYPE_THING, v1.getConceptType());

        e1 = e1.prepareMutation()
                .alterEdgeLabel("label2")
                .setProperty("p1", intValue(2), VISIBILITY_A)
                .save(AUTHORIZATIONS_A_AND_B);
        getGraph().flush();
        assertEquals(intValue(2), e1.getProperty("p1").getValue());
        assertEquals("label2", e1.getLabel());

        v1 = v1.prepareMutation()
                .alterConceptType(CONCEPT_TYPE_PERSON)
                .save(AUTHORIZATIONS_A_AND_B);
        getGraph().flush();

        assertEquals(CONCEPT_TYPE_PERSON, v1.getConceptType());

        v1 = getGraph().getVertex("v1", AUTHORIZATIONS_A_AND_B);
        assertEquals(CONCEPT_TYPE_PERSON, v1.getConceptType());
    }

    @Test
    public void testMetricsRepositoryStackTraceTracker() {
        StackTraceTracker flushStackTraceTracker = getGraph().getMetricsRegistry().getStackTraceTracker(getGraph().getClass(), "flush", "stack");
        getGraph().addVertex("vPrimer", VISIBILITY_A, AUTHORIZATIONS_A, CONCEPT_TYPE_THING);
        getGraph().flush();
        flushStackTraceTracker.reset();
        assertStackTraceTrackerCount(flushStackTraceTracker, path -> {
            StackTraceTracker.StackTraceItem item = path.get(path.size() - 1);
            assertEquals("count mismatch: " + item, 0, item.getCount());
        });

        getGraph().addVertex("v1", VISIBILITY_A, AUTHORIZATIONS_A, CONCEPT_TYPE_THING);
        getGraph().flush();

        assertStackTraceTrackerCount(flushStackTraceTracker, path -> {
            StackTraceTracker.StackTraceItem item = path.get(path.size() - 1);
            assertEquals("count mismatch: " + item, 1, item.getCount());
        });

        getGraph().addVertex("v2", VISIBILITY_A, AUTHORIZATIONS_A, CONCEPT_TYPE_THING);
        getGraph().flush();

        assertStackTraceTrackerCount(flushStackTraceTracker, path -> {
            int expectedCount = 2;
            for (StackTraceTracker.StackTraceItem item : path) {
                if (item.toString().contains("testMetricsRepositoryStackTraceTracker")) {
                    expectedCount = 1;
                }
                assertEquals("count mismatch: " + item, expectedCount, item.getCount());
            }
        });
    }

    @Test
    public void testDataTypes() {
        getGraph().prepareVertex("v1", VISIBILITY_A, CONCEPT_TYPE_THING).save(AUTHORIZATIONS_A_AND_B);
        getGraph().flush();

        getGraph().getVertex("v1", AUTHORIZATIONS_A_AND_B)
                .prepareMutation()
                .addPropertyValue("", "int", intValue(5), VISIBILITY_A)
                .addPropertyValue("", "intArray", intArray(new int[]{1, 2, 3}), VISIBILITY_A)
                .addPropertyValue("", "double", doubleValue(5.6d), VISIBILITY_A)
                .addPropertyValue("", "doubleArray", doubleArray(new double[]{5.6d, 234.344d, 123.777d}), VISIBILITY_A)
                .addPropertyValue("", "float", floatValue(6.4f), VISIBILITY_A)
                .addPropertyValue("", "float", floatArray(new float[]{6.4f, 45.66f, 12.66f}), VISIBILITY_A)
                .addPropertyValue("", "string", stringValue("test"), VISIBILITY_A)
                .addPropertyValue("", "stringArray", stringArray("str1", "str2", "str3", "str4"), VISIBILITY_A)
                .addPropertyValue("", "byte", byteValue((byte) 5), VISIBILITY_A)
                .addPropertyValue("", "byteArray", byteArray("hello world".getBytes()), VISIBILITY_A)
                .addPropertyValue("", "long", longValue(5L), VISIBILITY_A)
                .addPropertyValue("", "longArray", longArray(new long[]{5L, 6L, 7L}), VISIBILITY_A)
                .addPropertyValue("", "boolean", BooleanValue.TRUE, VISIBILITY_A)
                .addPropertyValue("", "booleanArray", booleanArray(new boolean[]{true, false, true, true}), VISIBILITY_A)
                .addPropertyValue("", "geopoint", geoPointValue(77, -33), VISIBILITY_A)
                .addPropertyValue("", "short", shortValue((short) 5), VISIBILITY_A)
                .addPropertyValue("", "shortArray", shortArray(new short[]{(short) 1, (short) 2, (short) 3}), VISIBILITY_A)
                .addPropertyValue("", "datetime", DateTimeValue.datetime(ZonedDateTime.now()), VISIBILITY_A)
                .addPropertyValue("", "dateTimeArray", Values.dateTimeArray(new ZonedDateTime[]{ZonedDateTime.now(), ZonedDateTime.now().plusMinutes(1), ZonedDateTime.now().plusMinutes(2)}), VISIBILITY_A)
                .addPropertyValue("", "date", DateValue.date(LocalDate.now()), VISIBILITY_A)
                .addPropertyValue("", "dateArray", Values.dateArray(new LocalDate[]{LocalDate.now(), LocalDate.now().plusDays(1), LocalDate.now().plusDays(2)}), VISIBILITY_A)
                .addPropertyValue("", "time", TimeValue.now(Clock.systemUTC()), VISIBILITY_A)
                .addPropertyValue("", "timeArray", Values.timeArray(new OffsetTime[]{OffsetTime.now(), OffsetTime.now().plusMinutes(1), OffsetTime.now().plusMinutes(2)}), VISIBILITY_A)
                .addPropertyValue("", "localDateTime", LocalDateTimeValue.now(Clock.systemUTC()), VISIBILITY_A)
                .addPropertyValue("", "localDateTimeArray", Values.localDateTimeArray(new LocalDateTime[]{LocalDateTime.now(), LocalDateTime.now().plusMinutes(1), LocalDateTime.now().plusMinutes(2)}), VISIBILITY_A)
                .addPropertyValue("", "localTime", LocalTimeValue.now(Clock.systemUTC()), VISIBILITY_A)
                .addPropertyValue("", "localTimeArray", Values.localTimeArray(new LocalTime[]{LocalTime.now(), LocalTime.now().plusMinutes(1), LocalTime.now().plusMinutes(2)}), VISIBILITY_A)
                .addPropertyValue("", "duration", DurationValue.duration(Duration.ofHours(1)), VISIBILITY_A)
                .addPropertyValue("", "durationArray", Values.durationArray(new DurationValue[]{DurationValue.duration(Duration.ofHours(1)), DurationValue.duration(Duration.ofDays(1)), DurationValue.duration(Period.ofDays(1))}), VISIBILITY_A)

                .save(AUTHORIZATIONS_A_AND_B);

        getGraph().flush();
    }

    @Test
    public void testEdgeIdFetchHints() {
        Vertex v1 = graph.prepareVertex("v1", VISIBILITY_A, CONCEPT_TYPE_THING).save(AUTHORIZATIONS_A);
        Vertex v2 = graph.prepareVertex("v2", VISIBILITY_A, CONCEPT_TYPE_THING).save(AUTHORIZATIONS_A);
        Vertex v3 = graph.prepareVertex("v3", VISIBILITY_A, CONCEPT_TYPE_THING).save(AUTHORIZATIONS_A);
        graph.prepareEdge(v1, v2, LABEL_LABEL1, VISIBILITY_A).save(AUTHORIZATIONS_A);
        graph.prepareEdge(v1, v3, LABEL_LABEL1, VISIBILITY_A).save(AUTHORIZATIONS_A);
        graph.flush();

        FetchHints fetchHintsNoEdgeIds = new FetchHintsBuilder(FetchHints.ALL)
                .setIncludeEdgeIds(false)
                .build();
        Vertex v1NoEdgeIds = graph.getVertex("v1", fetchHintsNoEdgeIds, AUTHORIZATIONS_A);
        Vertex v2NoEdgeIds = graph.getVertex("v2", fetchHintsNoEdgeIds, AUTHORIZATIONS_A);
        assertThrowsException(() -> v1NoEdgeIds.getEdgeIds(Direction.BOTH, AUTHORIZATIONS_A));
        assertThrowsException(() -> v1NoEdgeIds.getEdgeIds(Direction.BOTH, LABEL_LABEL1, AUTHORIZATIONS_A));
        assertThrowsException(() -> v1NoEdgeIds.getEdgeIds(Direction.BOTH, new String[]{LABEL_LABEL1}, AUTHORIZATIONS_A));
        assertThrowsException(() -> v1NoEdgeIds.getEdgeIds(v2NoEdgeIds, Direction.BOTH, AUTHORIZATIONS_A));
        assertThrowsException(() -> v1NoEdgeIds.getEdgeIds(v2NoEdgeIds, Direction.BOTH, LABEL_LABEL1, AUTHORIZATIONS_A));
        assertThrowsException(() -> v1NoEdgeIds.getEdgeIds(v2NoEdgeIds, Direction.BOTH, new String[]{LABEL_LABEL1}, AUTHORIZATIONS_A));
        Assert.assertEquals(2, count(v1NoEdgeIds.getVertexIds(Direction.BOTH, AUTHORIZATIONS_A)));
        for (EdgeInfo edgeInfo : v1NoEdgeIds.getEdgeInfos(Direction.BOTH, AUTHORIZATIONS_A)) {
            edgeInfo.getVertexId();
            edgeInfo.getLabel();
            edgeInfo.getDirection();
            assertThrowsException(edgeInfo::getEdgeId);
        }

        FetchHints fetchHintsNoEdgeVertexIds = new FetchHintsBuilder(FetchHints.ALL)
                .setIncludeEdgeVertexIds(false)
                .build();
        Vertex v1NoEdgeVertexIds = graph.getVertex("v1", fetchHintsNoEdgeVertexIds, AUTHORIZATIONS_A);
        assertThrowsException(() -> v1NoEdgeVertexIds.getVertexIds(Direction.BOTH, AUTHORIZATIONS_A));
        assertThrowsException(() -> v1NoEdgeVertexIds.getVertexIds(Direction.BOTH, LABEL_LABEL1, AUTHORIZATIONS_A));
        assertThrowsException(() -> v1NoEdgeVertexIds.getVertexIds(Direction.BOTH, new String[]{LABEL_LABEL1}, AUTHORIZATIONS_A));
        Assert.assertEquals(2, count(v1NoEdgeVertexIds.getEdgeIds(Direction.BOTH, AUTHORIZATIONS_A)));
        for (EdgeInfo edgeInfo : v1NoEdgeVertexIds.getEdgeInfos(Direction.BOTH, AUTHORIZATIONS_A)) {
            assertThrowsException(edgeInfo::getVertexId);
            edgeInfo.getLabel();
            edgeInfo.getDirection();
            edgeInfo.getEdgeId();
        }

        assertThrowsException(() -> {
            new FetchHintsBuilder(FetchHints.ALL)
                    .setIncludeEdgeVertexIds(false)
                    .setIncludeEdgeIds(false)
                    .build();
        });

        new FetchHintsBuilder(FetchHints.ALL)
                .setIncludeAllEdgeRefs(false)
                .setIncludeOutEdgeRefs(false)
                .setIncludeInEdgeRefs(false)
                .setIncludeEdgeVertexIds(false)
                .setIncludeEdgeIds(false)
                .build();
    }

    private boolean isDefaultSearchIndex() {
        Graph graph = getGraph();
        if (!(graph instanceof GraphWithSearchIndex)) {
            return false;
        }

        GraphWithSearchIndex graphWithSearchIndex = (GraphWithSearchIndex) graph;
        return graphWithSearchIndex.getSearchIndex() instanceof DefaultSearchIndex;
    }
}
