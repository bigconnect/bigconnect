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
import com.mware.ge.mutation.ExistingElementMutation;
import com.mware.ge.query.Compare;
import com.mware.ge.query.GeoCompare;
import com.mware.ge.query.IterableWithTotalHits;
import com.mware.ge.query.QueryResultsIterable;
import com.mware.ge.query.aggregations.TermsResult;
import com.mware.ge.util.IOUtils;
import com.mware.ge.util.IncreasingTime;
import com.mware.ge.util.IterableUtils;
import com.mware.ge.util.LargeStringInputStream;
import com.mware.ge.values.storable.GeoPointValue;
import com.mware.ge.values.storable.StreamingPropertyValue;
import com.mware.ge.values.storable.TextValue;
import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.EnumSet;
import java.util.List;
import java.util.Map;

import static com.mware.core.model.schema.SchemaConstants.CONCEPT_TYPE_THING;
import static com.mware.ge.util.GeAssert.*;
import static com.mware.ge.util.GeAssert.assertResultsCount;
import static com.mware.ge.util.IterableUtils.count;
import static com.mware.ge.util.IterableUtils.toList;
import static com.mware.ge.values.storable.Values.*;
import static com.mware.ge.values.storable.Values.geoCircleValue;

@RunWith(JUnit4.class)
public abstract class GraphSecurityTests implements GraphTestSetup {
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
    public void testFilterEdgeIdsByAuthorization() {
        Vertex v1 = getGraph().addVertex("v1", VISIBILITY_A, AUTHORIZATIONS_A, CONCEPT_TYPE_THING);
        Vertex v2 = getGraph().addVertex("v2", VISIBILITY_A, AUTHORIZATIONS_A, CONCEPT_TYPE_THING);
        Metadata metadataPropB = Metadata.create();
        metadataPropB.add("meta1", stringValue("meta1"), VISIBILITY_A);
        getGraph().prepareEdge("e1", v1, v2, LABEL_LABEL1, VISIBILITY_A)
                .setProperty("propA", stringValue("propA"), VISIBILITY_A)
                .setProperty("propB", stringValue("propB"), VISIBILITY_A_AND_B)
                .setProperty("propBmeta", stringValue("propBmeta"), metadataPropB, VISIBILITY_A)
                .save(AUTHORIZATIONS_ALL);
        getGraph().flush();

        List<String> edgeIds = new ArrayList<>();
        edgeIds.add("e1");
        List<String> foundEdgeIds = toList(getGraph().filterEdgeIdsByAuthorization(edgeIds, VISIBILITY_A_STRING, ElementFilter.ALL, AUTHORIZATIONS_ALL));
        IterableUtils.assertContains("e1", foundEdgeIds);

        foundEdgeIds = toList(getGraph().filterEdgeIdsByAuthorization(edgeIds, VISIBILITY_B_STRING, ElementFilter.ALL, AUTHORIZATIONS_ALL));
        IterableUtils.assertContains("e1", foundEdgeIds);

        foundEdgeIds = toList(getGraph().filterEdgeIdsByAuthorization(edgeIds, VISIBILITY_C_STRING, ElementFilter.ALL, AUTHORIZATIONS_ALL));
        assertEquals(0, foundEdgeIds.size());

        foundEdgeIds = toList(getGraph().filterEdgeIdsByAuthorization(edgeIds, VISIBILITY_A_STRING, EnumSet.of(ElementFilter.ELEMENT), AUTHORIZATIONS_ALL));
        IterableUtils.assertContains("e1", foundEdgeIds);

        foundEdgeIds = toList(getGraph().filterEdgeIdsByAuthorization(edgeIds, VISIBILITY_B_STRING, EnumSet.of(ElementFilter.ELEMENT), AUTHORIZATIONS_ALL));
        assertEquals(0, foundEdgeIds.size());

        foundEdgeIds = toList(getGraph().filterEdgeIdsByAuthorization(edgeIds, VISIBILITY_A_STRING, EnumSet.of(ElementFilter.PROPERTY), AUTHORIZATIONS_ALL));
        IterableUtils.assertContains("e1", foundEdgeIds);

        foundEdgeIds = toList(getGraph().filterEdgeIdsByAuthorization(edgeIds, VISIBILITY_C_STRING, EnumSet.of(ElementFilter.PROPERTY), AUTHORIZATIONS_ALL));
        assertEquals(0, foundEdgeIds.size());

        foundEdgeIds = toList(getGraph().filterEdgeIdsByAuthorization(edgeIds, VISIBILITY_A_STRING, EnumSet.of(ElementFilter.PROPERTY_METADATA), AUTHORIZATIONS_ALL));
        IterableUtils.assertContains("e1", foundEdgeIds);

        foundEdgeIds = toList(getGraph().filterEdgeIdsByAuthorization(edgeIds, VISIBILITY_C_STRING, EnumSet.of(ElementFilter.PROPERTY_METADATA), AUTHORIZATIONS_ALL));
        assertEquals(0, foundEdgeIds.size());
    }

    @Test
    public void testFilterVertexIdsByAuthorization() {
        Metadata metadataPropB = Metadata.create();
        metadataPropB.add("meta1", stringValue("meta1"), VISIBILITY_A);
        getGraph().prepareVertex("v1", VISIBILITY_A, CONCEPT_TYPE_THING)
                .setProperty("propA", stringValue("propA"), VISIBILITY_A)
                .setProperty("propB", stringValue("propB"), VISIBILITY_A_AND_B)
                .setProperty("propBmeta", stringValue("propBmeta"), metadataPropB, VISIBILITY_A)
                .save(AUTHORIZATIONS_ALL);
        getGraph().flush();

        List<String> vertexIds = new ArrayList<>();
        vertexIds.add("v1");
        List<String> foundVertexIds = toList(getGraph().filterVertexIdsByAuthorization(vertexIds, VISIBILITY_A_STRING, ElementFilter.ALL, AUTHORIZATIONS_ALL));
        IterableUtils.assertContains("v1", foundVertexIds);

        foundVertexIds = toList(getGraph().filterVertexIdsByAuthorization(vertexIds, VISIBILITY_B_STRING, ElementFilter.ALL, AUTHORIZATIONS_ALL));
        IterableUtils.assertContains("v1", foundVertexIds);

        foundVertexIds = toList(getGraph().filterVertexIdsByAuthorization(vertexIds, VISIBILITY_C_STRING, ElementFilter.ALL, AUTHORIZATIONS_ALL));
        assertEquals(0, foundVertexIds.size());

        foundVertexIds = toList(getGraph().filterVertexIdsByAuthorization(vertexIds, VISIBILITY_A_STRING, EnumSet.of(ElementFilter.ELEMENT), AUTHORIZATIONS_ALL));
        IterableUtils.assertContains("v1", foundVertexIds);

        foundVertexIds = toList(getGraph().filterVertexIdsByAuthorization(vertexIds, VISIBILITY_B_STRING, EnumSet.of(ElementFilter.ELEMENT), AUTHORIZATIONS_ALL));
        assertEquals(0, foundVertexIds.size());

        foundVertexIds = toList(getGraph().filterVertexIdsByAuthorization(vertexIds, VISIBILITY_A_STRING, EnumSet.of(ElementFilter.PROPERTY), AUTHORIZATIONS_ALL));
        IterableUtils.assertContains("v1", foundVertexIds);

        foundVertexIds = toList(getGraph().filterVertexIdsByAuthorization(vertexIds, VISIBILITY_C_STRING, EnumSet.of(ElementFilter.PROPERTY), AUTHORIZATIONS_ALL));
        assertEquals(0, foundVertexIds.size());

        foundVertexIds = toList(getGraph().filterVertexIdsByAuthorization(vertexIds, VISIBILITY_A_STRING, EnumSet.of(ElementFilter.PROPERTY_METADATA), AUTHORIZATIONS_ALL));
        IterableUtils.assertContains("v1", foundVertexIds);

        foundVertexIds = toList(getGraph().filterVertexIdsByAuthorization(vertexIds, VISIBILITY_C_STRING, EnumSet.of(ElementFilter.PROPERTY_METADATA), AUTHORIZATIONS_ALL));
        assertEquals(0, foundVertexIds.size());
    }

    @Test
    public void testChangeVisibilityOnStreamingProperty() throws IOException {
        String expectedLargeValue = IOUtils.toString(new LargeStringInputStream(LARGE_PROPERTY_VALUE_SIZE));
        StreamingPropertyValue propSmall = StreamingPropertyValue.create(new ByteArrayInputStream("value1".getBytes()), TextValue.class);
        StreamingPropertyValue propLarge = StreamingPropertyValue.create(new ByteArrayInputStream(expectedLargeValue.getBytes()), TextValue.class);
        String largePropertyName = "propLarge/\\*!@#$%^&*()[]{}|";
        getGraph().prepareVertex("v1", VISIBILITY_A, CONCEPT_TYPE_THING)
                .setProperty("propSmall", propSmall, VISIBILITY_A)
                .setProperty(largePropertyName, propLarge, VISIBILITY_A)
                .save(AUTHORIZATIONS_A_AND_B);
        getGraph().flush();
        assertEquals(2, count(getGraph().getVertex("v1", AUTHORIZATIONS_A).getProperties()));

        getGraph().getVertex("v1", FetchHints.ALL, AUTHORIZATIONS_A)
                .prepareMutation()
                .alterPropertyVisibility("propSmall", VISIBILITY_B)
                .save(AUTHORIZATIONS_A_AND_B);
        getGraph().flush();
        assertEquals(1, count(getGraph().getVertex("v1", AUTHORIZATIONS_A).getProperties()));

        getGraph().getVertex("v1", FetchHints.ALL, AUTHORIZATIONS_A)
                .prepareMutation()
                .alterPropertyVisibility(largePropertyName, VISIBILITY_B)
                .save(AUTHORIZATIONS_A_AND_B);
        getGraph().flush();
        assertEquals(0, count(getGraph().getVertex("v1", AUTHORIZATIONS_A).getProperties()));

        assertEquals(2, count(getGraph().getVertex("v1", AUTHORIZATIONS_A_AND_B).getProperties()));
    }

    @Test
    public void testChangeVisibilityVertex() {
        getGraph().prepareVertex("v1", VISIBILITY_A, CONCEPT_TYPE_THING)
                .save(AUTHORIZATIONS_A_AND_B);
        getGraph().flush();

        Vertex v1 = getGraph().getVertex("v1", AUTHORIZATIONS_A);
        v1.prepareMutation()
                .alterElementVisibility(VISIBILITY_B)
                .save(AUTHORIZATIONS_A_AND_B);
        getGraph().flush();

        v1 = getGraph().getVertex("v1", AUTHORIZATIONS_A);
        assertNull(v1);
        v1 = getGraph().getVertex("v1", AUTHORIZATIONS_B);
        assertNotNull(v1);

        // change to same visibility
        v1 = getGraph().getVertex("v1", AUTHORIZATIONS_B);
        v1.prepareMutation()
                .alterElementVisibility(VISIBILITY_B)
                .save(AUTHORIZATIONS_A_AND_B);

        v1 = getGraph().getVertex("v1", AUTHORIZATIONS_A);
        assertNull(v1);
        v1 = getGraph().getVertex("v1", AUTHORIZATIONS_B);
        assertNotNull(v1);
    }

    @Test
    public void testChangeVertexVisibility() {
        getGraph().prepareVertex("v1", VISIBILITY_A, CONCEPT_TYPE_THING)
                .save(AUTHORIZATIONS_A_AND_B);

        getGraph().flush();

        Vertex v1 = getGraph().getVertex("v1", AUTHORIZATIONS_A_AND_B);
        v1.prepareMutation()
                .alterElementVisibility(VISIBILITY_B)
                .save(AUTHORIZATIONS_A_AND_B);

        getGraph().flush();

        v1 = getGraph().getVertex("v1", AUTHORIZATIONS_B);
        assertNotNull(v1);
    }

    @Test
    public void testChangeVertexVisibilityAndAlterPropertyVisibilityAndChangePropertyAtTheSameTime() {
        Metadata metadata = Metadata.create();
        metadata.add("m1", stringValue("m1-value1"), VISIBILITY_EMPTY);
        metadata.add("m2", stringValue("m2-value1"), VISIBILITY_EMPTY);
        getGraph().prepareVertex("v1", VISIBILITY_A, CONCEPT_TYPE_THING)
                .addPropertyValue("k1", "age", intValue(25), metadata, VISIBILITY_A)
                .save(AUTHORIZATIONS_A_AND_B);
        getGraph().createAuthorizations(AUTHORIZATIONS_ALL);
        getGraph().flush();

        assertResultsCount(1, 1, getGraph().query(AUTHORIZATIONS_A).has("age", intValue(25)).vertices());

        Vertex v1 = getGraph().getVertex("v1", FetchHints.ALL, AUTHORIZATIONS_ALL);
        ExistingElementMutation<Vertex> m = v1.prepareMutation();
        m.alterElementVisibility(VISIBILITY_B);
        for (Property property : v1.getProperties()) {
            m.alterPropertyVisibility(property, VISIBILITY_B);
            m.setPropertyMetadata(property, "m1", stringValue("m1-value2"), VISIBILITY_EMPTY);
        }
        m.save(AUTHORIZATIONS_ALL);
        getGraph().flush();

        v1 = getGraph().getVertex("v1", FetchHints.ALL, AUTHORIZATIONS_B);
        assertEquals(VISIBILITY_B, v1.getVisibility());
        List<Property> properties = toList(v1.getProperties());
        assertEquals(1, properties.size());
        assertEquals("age", properties.get(0).getName());
        assertEquals(VISIBILITY_B, properties.get(0).getVisibility());
        assertEquals(2, properties.get(0).getMetadata().entrySet().size());
        assertTrue(properties.get(0).getMetadata().containsKey("m1"));
        assertEquals(stringValue("m1-value2"), properties.get(0).getMetadata().getEntry("m1").getValue());
        assertEquals(VISIBILITY_EMPTY, properties.get(0).getMetadata().getEntry("m1").getVisibility());
        assertTrue(properties.get(0).getMetadata().containsKey("m2"));
        assertEquals(stringValue("m2-value1"), properties.get(0).getMetadata().getEntry("m2").getValue());
        assertEquals(VISIBILITY_EMPTY, properties.get(0).getMetadata().getEntry("m2").getVisibility());

        v1 = getGraph().getVertex("v1", AUTHORIZATIONS_A);
        assertNull("v1 should not be returned for auth a", v1);

        assertResultsCount(0, 0, getGraph().query(AUTHORIZATIONS_A).has("age", intValue(25)).vertices());
        assertResultsCount(1, 1, getGraph().query(AUTHORIZATIONS_B).has("age", intValue(25)).vertices());
    }

    @Test
    public void testChangeVisibilityPropertiesWithPropertyKey() {
        getGraph().prepareVertex("v1", VISIBILITY_EMPTY, CONCEPT_TYPE_THING)
                .addPropertyValue("k1", "prop1", stringValue("value1"), VISIBILITY_A)
                .save(AUTHORIZATIONS_A_AND_B);
        getGraph().flush();

        Vertex v1 = getGraph().getVertex("v1", AUTHORIZATIONS_A_AND_B);
        v1.prepareMutation()
                .alterPropertyVisibility("k1", "prop1", VISIBILITY_B)
                .save(AUTHORIZATIONS_A_AND_B);
        getGraph().flush();

        v1 = getGraph().getVertex("v1", AUTHORIZATIONS_A);
        assertNull(v1.getProperty("prop1"));

        assertEquals(1, count(getGraph().query(AUTHORIZATIONS_B).has("prop1", stringValue("value1")).vertices()));
        assertEquals(0, count(getGraph().query(AUTHORIZATIONS_A).has("prop1", stringValue("value1")).vertices()));

        TermsResult aggregationResult = queryGraphQueryWithTermsAggregationResult("prop1", ElementType.VERTEX, AUTHORIZATIONS_A);
        Map<Object, Long> propertyCountByValue = termsBucketToMap(aggregationResult.getBuckets());
        if (propertyCountByValue != null) {
            assertEquals(null, propertyCountByValue.get("value1"));
        }

        propertyCountByValue = queryGraphQueryWithTermsAggregation("prop1", ElementType.VERTEX, AUTHORIZATIONS_B);
        if (propertyCountByValue != null) {
            assertEquals(1L, (long) propertyCountByValue.get("value1"));
        }

        v1 = getGraph().getVertex("v1", AUTHORIZATIONS_A_AND_B);
        Property v1Prop1 = v1.getProperty("prop1");
        assertNotNull(v1Prop1);
        assertEquals(VISIBILITY_B, v1Prop1.getVisibility());

        getGraph().prepareVertex("v2", VISIBILITY_EMPTY, CONCEPT_TYPE_THING)
                .save(AUTHORIZATIONS_A_AND_B);
        getGraph().flush();

        getGraph().prepareEdge("e1", "v1", "v2", LABEL_LABEL1, VISIBILITY_EMPTY)
                .addPropertyValue("k2", "prop2", stringValue("value2"), VISIBILITY_A)
                .save(AUTHORIZATIONS_A_AND_B);
        getGraph().flush();

        Edge e1 = getGraph().getEdge("e1", AUTHORIZATIONS_A_AND_B);
        e1.prepareMutation()
                .alterPropertyVisibility("k2", "prop2", VISIBILITY_B)
                .save(AUTHORIZATIONS_A_AND_B);
        getGraph().flush();

        e1 = getGraph().getEdge("e1", AUTHORIZATIONS_A);
        assertNull(e1.getProperty("prop2"));

        assertEquals(1, count(getGraph().query(AUTHORIZATIONS_B).has("prop2", stringValue("value2")).edges()));
        assertEquals(0, count(getGraph().query(AUTHORIZATIONS_A).has("prop2", stringValue("value2")).edges()));

        propertyCountByValue = queryGraphQueryWithTermsAggregation("prop2", ElementType.EDGE, AUTHORIZATIONS_A);
        if (propertyCountByValue != null) {
            assertEquals(null, propertyCountByValue.get("value2"));
        }

        propertyCountByValue = queryGraphQueryWithTermsAggregation("prop2", ElementType.EDGE, AUTHORIZATIONS_B);
        if (propertyCountByValue != null) {
            assertEquals(1L, (long) propertyCountByValue.get("value2"));
        }

        e1 = getGraph().getEdge("e1", AUTHORIZATIONS_A_AND_B);
        Property e1prop1 = e1.getProperty("prop2");
        assertNotNull(e1prop1);
        assertEquals(VISIBILITY_B, e1prop1.getVisibility());
    }

    @Test
    public void testChangeVisibilityVertexProperties() {
        Metadata prop1Metadata = Metadata.create();
        prop1Metadata.add("prop1_key1", stringValue("value1"), VISIBILITY_EMPTY);

        Metadata prop2Metadata = Metadata.create();
        prop2Metadata.add("prop2_key1", stringValue("value1"), VISIBILITY_EMPTY);

        getGraph().prepareVertex("v1", VISIBILITY_EMPTY, CONCEPT_TYPE_THING)
                .setProperty("prop1", stringValue("value1"), prop1Metadata, VISIBILITY_A)
                .setProperty("prop2", stringValue("value2"), prop2Metadata, VISIBILITY_EMPTY)
                .save(AUTHORIZATIONS_A_AND_B);
        getGraph().flush();

        Vertex v1 = getGraph().getVertex("v1", FetchHints.ALL, AUTHORIZATIONS_A_AND_B);
        v1.prepareMutation()
                .alterPropertyVisibility("prop1", VISIBILITY_B)
                .save(AUTHORIZATIONS_A_AND_B);
        getGraph().flush();

        v1 = getGraph().getVertex("v1", AUTHORIZATIONS_A);
        assertNull(v1.getProperty("prop1"));
        assertNotNull(v1.getProperty("prop2"));

        toList(getGraph().query(AUTHORIZATIONS_A).has("prop1", stringValue("value1")).vertices());
        toList(getGraph().query(AUTHORIZATIONS_B).has("prop1", stringValue("value1")).vertices());

        assertResultsCount(0, 0, getGraph().query(AUTHORIZATIONS_A).has("prop1", stringValue("value1")).vertices());
        assertResultsCount(1, 1, getGraph().query(AUTHORIZATIONS_B).has("prop1", stringValue("value1")).vertices());

        Map<Object, Long> propertyCountByValue = queryGraphQueryWithTermsAggregation("prop1", ElementType.VERTEX, AUTHORIZATIONS_A);
        if (propertyCountByValue != null) {
            assertEquals(null, propertyCountByValue.get("value1"));
        }

        propertyCountByValue = queryGraphQueryWithTermsAggregation("prop1", ElementType.VERTEX, AUTHORIZATIONS_B);
        if (propertyCountByValue != null) {
            assertEquals(1L, (long) propertyCountByValue.get("value1"));
        }

        v1 = getGraph().getVertex("v1", FetchHints.ALL, AUTHORIZATIONS_A_AND_B);
        Property v1Prop1 = v1.getProperty("prop1");
        assertNotNull(v1Prop1);
        assertEquals(1, toList(v1Prop1.getMetadata().entrySet()).size());
        assertEquals(stringValue("value1"), v1Prop1.getMetadata().getValue("prop1_key1"));
        assertNotNull(v1.getProperty("prop2"));

        // alter and set property in one mutation
        v1 = getGraph().getVertex("v1", FetchHints.ALL, AUTHORIZATIONS_A_AND_B);
        v1.prepareMutation()
                .alterPropertyVisibility("prop1", VISIBILITY_A)
                .setProperty("prop1", stringValue("value1New"), VISIBILITY_A)
                .save(AUTHORIZATIONS_A_AND_B);
        getGraph().flush();

        assertResultsCount(0, 0, getGraph().query(AUTHORIZATIONS_B).has("prop1", stringValue("value1")).vertices());
        assertResultsCount(0, 0, getGraph().query(AUTHORIZATIONS_A).has("prop1", stringValue("value1")).vertices());
        assertResultsCount(0, 0, getGraph().query(AUTHORIZATIONS_B).has("prop1", stringValue("value1New")).vertices());
        assertResultsCount(1, 1, getGraph().query(AUTHORIZATIONS_A).has("prop1", stringValue("value1New")).vertices());

        v1 = getGraph().getVertex("v1", AUTHORIZATIONS_A_AND_B);
        assertNotNull(v1.getProperty("prop1"));
        assertEquals(stringValue("value1New"), v1.getPropertyValue("prop1"));

        // alter visibility to the same visibility
        v1 = getGraph().getVertex("v1", FetchHints.ALL, AUTHORIZATIONS_A_AND_B);
        v1.prepareMutation()
                .alterPropertyVisibility("prop1", VISIBILITY_A)
                .setProperty("prop1", stringValue("value1New2"), VISIBILITY_A)
                .save(AUTHORIZATIONS_A_AND_B);
        getGraph().flush();

        v1 = getGraph().getVertex("v1", AUTHORIZATIONS_A_AND_B);
        assertNotNull(v1.getProperty("prop1"));
        assertEquals(stringValue("value1New2"), v1.getPropertyValue("prop1"));

        assertResultsCount(0, 0, getGraph().query(AUTHORIZATIONS_B).has("prop1", stringValue("value1")).vertices());
        assertResultsCount(0, 0, getGraph().query(AUTHORIZATIONS_A).has("prop1", stringValue("value1")).vertices());
        assertResultsCount(0, 0, getGraph().query(AUTHORIZATIONS_B).has("prop1", stringValue("value1New")).vertices());
        assertResultsCount(0, 0, getGraph().query(AUTHORIZATIONS_A).has("prop1", stringValue("value1New")).vertices());
        assertResultsCount(0, 0, getGraph().query(AUTHORIZATIONS_B).has("prop1", stringValue("value1New2")).vertices());
        assertResultsCount(1, 1, getGraph().query(AUTHORIZATIONS_A).has("prop1", stringValue("value1New2")).vertices());
    }

    @Test
    public void testAlterVisibilityAndSetMetadataInOneMutation() {
        Metadata prop1Metadata = Metadata.create();
        prop1Metadata.add("prop1_key1", stringValue("metadata1"), VISIBILITY_EMPTY);

        getGraph().prepareVertex("v1", VISIBILITY_EMPTY, CONCEPT_TYPE_THING)
                .setProperty("prop1", stringValue("value1"), prop1Metadata, VISIBILITY_A)
                .save(AUTHORIZATIONS_A_AND_B);
        getGraph().flush();

        Vertex v1 = getGraph().getVertex("v1", FetchHints.ALL, AUTHORIZATIONS_A_AND_B);
        v1.prepareMutation()
                .alterPropertyVisibility("prop1", VISIBILITY_B)
                .setPropertyMetadata("prop1", "prop1_key1", stringValue("metadata1New"), VISIBILITY_EMPTY)
                .save(AUTHORIZATIONS_A_AND_B);
        getGraph().flush();

        v1 = getGraph().getVertex("v1", FetchHints.ALL, AUTHORIZATIONS_A_AND_B);
        assertNotNull(v1.getProperty("prop1"));
        assertEquals(VISIBILITY_B, v1.getProperty("prop1").getVisibility());
        assertEquals(stringValue("metadata1New"), v1.getProperty("prop1").getMetadata().getValue("prop1_key1"));

        List<HistoricalPropertyValue> historicalPropertyValues = toList(v1.getHistoricalPropertyValues(AUTHORIZATIONS_A_AND_B));
        assertEquals(3, historicalPropertyValues.size());
        assertEquals(stringValue("metadata1New"), historicalPropertyValues.get(0).getMetadata().getValue("prop1_key1"));
        assertTrue(historicalPropertyValues.get(1).isDeleted());
        assertEquals(stringValue("metadata1"), historicalPropertyValues.get(2).getMetadata().getValue("prop1_key1"));
    }

    @Test
    public void testAlterPropertyVisibilityOverwritingProperty() {
        getGraph().prepareVertex("v1", VISIBILITY_EMPTY, CONCEPT_TYPE_THING)
                .addPropertyValue("", "prop1", stringValue("value1"), VISIBILITY_EMPTY)
                .addPropertyValue("", "prop1", stringValue("value2"), VISIBILITY_A)
                .save(AUTHORIZATIONS_A_AND_B);
        getGraph().flush();

        long beforeAlterTimestamp = IncreasingTime.currentTimeMillis();

        Vertex v1 = getGraph().getVertex("v1", FetchHints.ALL, AUTHORIZATIONS_A);
        v1.prepareMutation()
                .alterPropertyVisibility(v1.getProperty("", "prop1", VISIBILITY_A), VISIBILITY_EMPTY)
                .save(AUTHORIZATIONS_A_AND_B);
        getGraph().flush();

        v1 = getGraph().getVertex("v1", AUTHORIZATIONS_A);
        assertEquals(1, count(v1.getProperties()));
        assertNotNull(v1.getProperty("", "prop1", VISIBILITY_EMPTY));
        assertEquals(stringValue("value2"), v1.getProperty("", "prop1", VISIBILITY_EMPTY).getValue());
        assertNull(v1.getProperty("", "prop1", VISIBILITY_A));

        getGraph().dumpGraph();
        v1 = getGraph().getVertex("v1", getGraph().getDefaultFetchHints(), beforeAlterTimestamp, AUTHORIZATIONS_A);
        assertEquals(2, count(v1.getProperties()));
        assertNotNull(v1.getProperty("", "prop1", VISIBILITY_EMPTY));
        assertEquals(stringValue("value1"), v1.getProperty("", "prop1", VISIBILITY_EMPTY).getValue());
        assertNotNull(v1.getProperty("", "prop1", VISIBILITY_A));
        assertEquals(stringValue("value2"), v1.getProperty("", "prop1", VISIBILITY_A).getValue());
    }

    @Test
    public void testAlterPropertyVisibilityOfGeoLocation() {
        getGraph().defineProperty("prop1").dataType(TextValue.class).textIndexHint(TextIndexHint.ALL).define();
        getGraph().defineProperty("prop2").dataType(GeoPointValue.class).textIndexHint(TextIndexHint.ALL).define();
        getGraph().prepareVertex("v1", VISIBILITY_EMPTY, CONCEPT_TYPE_THING)
                .addPropertyValue("key1", "prop1", stringValue("value1"), VISIBILITY_A)
                .addPropertyValue("key1", "prop2", geoPointValue(38.9186, -77.2297), VISIBILITY_A)
                .save(AUTHORIZATIONS_A_AND_B);
        getGraph().flush();
        Vertex v1 = getGraph().getVertex("v1", FetchHints.ALL, AUTHORIZATIONS_A);
        v1.prepareMutation()
                .alterPropertyVisibility(v1.getProperty("key1", "prop1", VISIBILITY_A), VISIBILITY_EMPTY)
                .alterPropertyVisibility(v1.getProperty("key1", "prop2", VISIBILITY_A), VISIBILITY_EMPTY)
                .save(AUTHORIZATIONS_A_AND_B);
        getGraph().flush();
        QueryResultsIterable<Vertex> vertices = getGraph().query(AUTHORIZATIONS_EMPTY).has("prop1", stringValue("value1")).vertices();
        assertVertexIdsAnyOrder(vertices, "v1");
        assertResultsCount(1, 1, vertices);
        QueryResultsIterable<String> vertexIds = getGraph().query(AUTHORIZATIONS_EMPTY).has("prop2", GeoCompare.WITHIN, geoCircleValue(38.9186, -77.2297, 1)).vertexIds();
        assertIdsAnyOrder(vertexIds, "v1");
        assertResultsCount(1, 1, vertexIds);
        vertices = getGraph().query(AUTHORIZATIONS_EMPTY).has("prop2", GeoCompare.WITHIN, geoCircleValue(38.9186, -77.2297, 1)).vertices();
        assertVertexIdsAnyOrder(vertices, "v1");
        assertResultsCount(1, 1, vertices);
    }

    @Test
    public void testChangeVisibilityEdge() {
        Vertex v1 = getGraph().prepareVertex("v1", VISIBILITY_EMPTY, CONCEPT_TYPE_THING)
                .save(AUTHORIZATIONS_A_AND_B);

        Vertex v2 = getGraph().prepareVertex("v2", VISIBILITY_EMPTY, CONCEPT_TYPE_THING)
                .save(AUTHORIZATIONS_A_AND_B);

        getGraph().prepareEdge("e1", v1, v2, LABEL_LABEL1, VISIBILITY_A)
                .save(AUTHORIZATIONS_A_AND_B);
        getGraph().flush();

        // test that we can see the edge with A and not B
        v1 = getGraph().getVertex("v1", AUTHORIZATIONS_A);
        assertEquals(0, count(v1.getEdges(Direction.BOTH, AUTHORIZATIONS_B)));
        v1 = getGraph().getVertex("v1", AUTHORIZATIONS_A);
        assertEquals(1, count(v1.getEdges(Direction.BOTH, AUTHORIZATIONS_A)));

        // change the edge
        Edge e1 = getGraph().getEdge("e1", AUTHORIZATIONS_A);
        e1.prepareMutation()
                .alterElementVisibility(VISIBILITY_B)
                .save(AUTHORIZATIONS_A_AND_B);
        getGraph().flush();

        // test that we can see the edge with B and not A
        v1 = getGraph().getVertex("v1", AUTHORIZATIONS_B);
        assertEquals(1, count(v1.getEdges(Direction.BOTH, AUTHORIZATIONS_B)));
        v1 = getGraph().getVertex("v1", AUTHORIZATIONS_A);
        assertEquals(0, count(v1.getEdges(Direction.BOTH, AUTHORIZATIONS_A)));

        // change the edge visibility to same
        e1 = getGraph().getEdge("e1", AUTHORIZATIONS_B);
        e1.prepareMutation()
                .alterElementVisibility(VISIBILITY_B)
                .save(AUTHORIZATIONS_A_AND_B);

        // test that we can see the edge with B and not A
        v1 = getGraph().getVertex("v1", AUTHORIZATIONS_B);
        assertEquals(1, count(v1.getEdges(Direction.BOTH, AUTHORIZATIONS_B)));
        v1 = getGraph().getVertex("v1", AUTHORIZATIONS_A);
        assertEquals(0, count(v1.getEdges(Direction.BOTH, AUTHORIZATIONS_A)));
    }

    @Test
    public void testChangeVisibilityOnBadPropertyName() {
        getGraph().prepareVertex("v1", VISIBILITY_A, CONCEPT_TYPE_THING)
                .setProperty("prop1", stringValue("value1"), VISIBILITY_EMPTY)
                .setProperty("prop2", stringValue("value2"), VISIBILITY_B)
                .save(AUTHORIZATIONS_A_AND_B);
        getGraph().flush();

        try {
            getGraph().getVertex("v1", AUTHORIZATIONS_A)
                    .prepareMutation()
                    .alterPropertyVisibility("propBad", VISIBILITY_B)
                    .save(AUTHORIZATIONS_A_AND_B);
            fail("show throw");
        } catch (GeException ex) {
            assertNotNull(ex);
        }
    }

    @Test
    public void testMutationChangePropertyVisibilityFollowedByMetadataUsingPropertyObject() {
        Metadata prop1Metadata = Metadata.create();
        prop1Metadata.add("prop1_key1", stringValue("valueOld"), VISIBILITY_A);

        getGraph().prepareVertex("v1", VISIBILITY_A, CONCEPT_TYPE_THING)
                .setProperty("prop1", stringValue("value1"), prop1Metadata, VISIBILITY_A)
                .save(AUTHORIZATIONS_A_AND_B);
        getGraph().flush();

        Vertex v1 = getGraph().getVertex("v1", FetchHints.ALL, AUTHORIZATIONS_A_AND_B);
        Property p1 = v1.getProperty("prop1", VISIBILITY_A);
        v1.prepareMutation()
                .alterPropertyVisibility(p1, VISIBILITY_B)
                .setPropertyMetadata(p1, "prop1_key1", stringValue("valueNew"), VISIBILITY_B)
                .save(AUTHORIZATIONS_A_AND_B);
        getGraph().flush();

        v1 = getGraph().getVertex("v1", FetchHints.ALL, AUTHORIZATIONS_A_AND_B);
        assertEquals(stringValue("valueNew"), v1.getProperty("prop1", VISIBILITY_B).getMetadata().getEntry("prop1_key1", VISIBILITY_B).getValue());
    }


    @Test
    public void testIsVisibilityValid() {
        assertFalse(getGraph().isVisibilityValid(VISIBILITY_A, AUTHORIZATIONS_C));
        assertTrue(getGraph().isVisibilityValid(VISIBILITY_B, AUTHORIZATIONS_A_AND_B));
        assertTrue(getGraph().isVisibilityValid(VISIBILITY_B, AUTHORIZATIONS_B));
        assertTrue(getGraph().isVisibilityValid(VISIBILITY_EMPTY, AUTHORIZATIONS_A));
    }

    @Test
    public void testModifyVertexWithLowerAuthorizationThenOtherProperties() {
        getGraph().prepareVertex("v1", VISIBILITY_A, CONCEPT_TYPE_THING)
                .setProperty("prop1", stringValue("value1"), VISIBILITY_A)
                .setProperty("prop2", stringValue("value2"), VISIBILITY_B)
                .save(AUTHORIZATIONS_A_AND_B);
        getGraph().flush();

        Vertex v1 = getGraph().getVertex("v1", AUTHORIZATIONS_A);
        v1.setProperty("prop1", stringValue("value1New"), VISIBILITY_A, AUTHORIZATIONS_A_AND_B);
        getGraph().flush();

        Iterable<Vertex> vertices = getGraph().query(AUTHORIZATIONS_A_AND_B)
                .has("prop2", stringValue("value2"))
                .vertices();
        assertVertexIds(vertices, "v1");
    }


    @Test
    @Ignore // elasticsearch takes a very long time to run this
    public void manyVisibilities() {
        int count = 1010; // 1000 is the default max for elasticsearch

        List<String> authsToAdd = new ArrayList<>();
        authsToAdd.add("all");
        for (int i = 0; i < count; i++) {
            String itemVisibilityString = String.format("v%d", i);
            authsToAdd.add(itemVisibilityString);
        }
        System.out.println("Adding auths");
        addAuthorizations(authsToAdd.toArray(new String[authsToAdd.size()]));

        System.out.println("Add vertices");
        for (int i = 0; i < count; i++) {
            String itemVisibilityString = String.format("v%d", i);
            Visibility visibility = new Visibility(String.format("v%d|all", i));
            Authorizations authorizations = new Authorizations("all", itemVisibilityString);
            getGraph().prepareVertex(visibility, CONCEPT_TYPE_THING)
                    .addPropertyValue("key1", "name1", stringValue(itemVisibilityString), visibility)
                    .save(authorizations);
        }
        getGraph().flush();

        System.out.println("Getting vertices");
        Authorizations authorizations = new Authorizations("all");
        ArrayList<Vertex> vertices = Lists.newArrayList(getGraph().getVertices(authorizations));
        assertEquals(count, vertices.size());

        System.out.println("Query vertices");
        vertices = Lists.newArrayList(getGraph().query(authorizations)
                .limit((Integer) null)
                .vertices());
        assertEquals(count, vertices.size());
    }

    @Test
    public void testGraphQueryVertexHasWithSecurity() {
        getGraph().prepareVertex("v1", VISIBILITY_A, CONCEPT_TYPE_THING)
                .setProperty("age", intValue(25), VISIBILITY_A)
                .save(AUTHORIZATIONS_A_AND_B);
        getGraph().prepareVertex("v2", VISIBILITY_A, CONCEPT_TYPE_THING)
                .setProperty("age", intValue(25), VISIBILITY_B)
                .save(AUTHORIZATIONS_A_AND_B);
        getGraph().flush();

        Iterable<Vertex> vertices = getGraph().query(AUTHORIZATIONS_A)
                .has("age", Compare.EQUAL, intValue(25))
                .vertices();
        assertEquals(1, count(vertices));
        if (vertices instanceof IterableWithTotalHits) {
            assertEquals(1, ((IterableWithTotalHits) vertices).getTotalHits());
        }

        vertices = getGraph().query(AUTHORIZATIONS_B)
                .has("age", Compare.EQUAL, intValue(25))
                .vertices();
        assertEquals(0, count(vertices)); // need auth A to see the v2 node itself
        if (vertices instanceof IterableWithTotalHits) {
            assertEquals(0, ((IterableWithTotalHits) vertices).getTotalHits());
        }

        vertices = getGraph().query(AUTHORIZATIONS_A_AND_B)
                .has("age", Compare.EQUAL, intValue(25))
                .vertices();
        assertEquals(2, count(vertices));
        if (vertices instanceof IterableWithTotalHits) {
            assertEquals(2, ((IterableWithTotalHits) vertices).getTotalHits());
        }
    }

    @Test
    public void testGraphQueryVertexHasWithSecurityGranularity() {
        getGraph().prepareVertex("v1", VISIBILITY_A, CONCEPT_TYPE_THING)
                .setProperty("description", stringValue("v1"), VISIBILITY_A)
                .setProperty("age", intValue(25), VISIBILITY_A)
                .save(AUTHORIZATIONS_A_AND_B);
        getGraph().prepareVertex("v2", VISIBILITY_A, CONCEPT_TYPE_THING)
                .setProperty("description", stringValue("v2"), VISIBILITY_A)
                .setProperty("age", intValue(25), VISIBILITY_B)
                .save(AUTHORIZATIONS_A_AND_B);
        getGraph().flush();

        Iterable<Vertex> vertices = getGraph().query(AUTHORIZATIONS_A)
                .vertices();
        boolean hasAgeVisA = false;
        boolean hasAgeVisB = false;
        for (Vertex v : vertices) {
            Property prop = v.getProperty("age");
            if (prop == null) {
                continue;
            }
            if (intValue(25).eq(prop.getValue())) {
                if (prop.getVisibility().equals(VISIBILITY_A)) {
                    hasAgeVisA = true;
                } else if (prop.getVisibility().equals(VISIBILITY_B)) {
                    hasAgeVisB = true;
                }
            }
        }
        assertEquals(2, count(vertices));
        assertTrue("has a", hasAgeVisA);
        assertFalse("has b", hasAgeVisB);

        vertices = getGraph().query(AUTHORIZATIONS_A_AND_B)
                .vertices();
        assertEquals(2, count(vertices));
    }

    @Test
    public void testGraphQueryVertexHasWithSecurityComplexFormula() {
        getGraph().prepareVertex("v1", VISIBILITY_MIXED_CASE_a, CONCEPT_TYPE_THING)
                .setProperty("age", intValue(25), VISIBILITY_MIXED_CASE_a)
                .save(AUTHORIZATIONS_ALL);
        getGraph().prepareVertex("v2", VISIBILITY_A, CONCEPT_TYPE_THING)
                .setProperty("age", intValue(25), VISIBILITY_B)
                .save(AUTHORIZATIONS_ALL);
        getGraph().flush();

        Iterable<Vertex> vertices = getGraph().query(AUTHORIZATIONS_MIXED_CASE_a_AND_B)
                .has("age", Compare.EQUAL, intValue(25))
                .vertices();
        assertEquals(1, count(vertices));
    }


    @Test
    public void testGraphQueryVertexNoVisibility() {
        getGraph().prepareVertex("v1", VISIBILITY_EMPTY, CONCEPT_TYPE_THING)
                .setProperty("text", stringValue("hello"), VISIBILITY_EMPTY)
                .setProperty("age", intValue(25), VISIBILITY_EMPTY)
                .save(AUTHORIZATIONS_A_AND_B);
        getGraph().flush();

        Iterable<Vertex> vertices = getGraph().query("hello", AUTHORIZATIONS_A_AND_B)
                .has("age", Compare.EQUAL, intValue(25))
                .vertices();
        assertEquals(1, count(vertices));

        vertices = getGraph().query("hello", AUTHORIZATIONS_A_AND_B)
                .vertices();
        assertEquals(1, count(vertices));
    }


    @Test
    public void testGraphQueryTextVertexDifferentAuths() {
        getGraph().defineProperty("title").dataType(TextValue.class).textIndexHint(TextIndexHint.ALL).define();
        getGraph().defineProperty("fullText").dataType(TextValue.class).textIndexHint(TextIndexHint.FULL_TEXT).define();

        getGraph().prepareVertex("v1", VISIBILITY_A, CONCEPT_TYPE_THING)
                .setProperty("title", stringValue("hello"), VISIBILITY_EMPTY)
                .save(AUTHORIZATIONS_A_AND_B);
        getGraph().prepareVertex("v2", VISIBILITY_B, CONCEPT_TYPE_THING)
                .setProperty("fullText", StreamingPropertyValue.create("this is text with hello"), VISIBILITY_EMPTY)
                .save(AUTHORIZATIONS_A_AND_B);
        getGraph().flush();

        Iterable<Vertex> vertices = getGraph().query("hello", AUTHORIZATIONS_A).vertices();
        assertResultsCount(1, 1, (QueryResultsIterable) vertices);

        vertices = getGraph().query("hello", AUTHORIZATIONS_A_AND_B).vertices();
        assertResultsCount(2, 2, (QueryResultsIterable) vertices);
    }

    @Test
    public void testGraphQueryVertexDifferentAuths() {
        getGraph().prepareVertex("v1", VISIBILITY_A, CONCEPT_TYPE_THING)
                .setProperty("age", intValue(25), VISIBILITY_EMPTY)
                .save(AUTHORIZATIONS_A_AND_B);
        getGraph().prepareVertex("v2", VISIBILITY_B, CONCEPT_TYPE_THING)
                .setProperty("age", intValue(25), VISIBILITY_EMPTY)
                .save(AUTHORIZATIONS_A_AND_B);
        getGraph().flush();
        Iterable<Vertex> vertices = getGraph().query(AUTHORIZATIONS_A)
                .has("age", Compare.EQUAL, intValue(25))
                .vertices();
        assertResultsCount(1, 1, (QueryResultsIterable) vertices);
        vertices = getGraph().query(AUTHORIZATIONS_A_AND_B)
                .has("age", Compare.EQUAL, intValue(25))
                .vertices();
        assertResultsCount(2, 2, (QueryResultsIterable) vertices);
    }

    @Test
    public void testAlterElementVisibility() {
        getGraph().prepareVertex("v1", VISIBILITY_A, CONCEPT_TYPE_THING)
                .save(AUTHORIZATIONS_A_AND_B);
        getGraph().flush();

        Vertex v1 = graph.getVertex("v1", AUTHORIZATIONS_A_AND_B);
        v1.prepareMutation()
                .alterElementVisibility(VISIBILITY_B)
                .save(AUTHORIZATIONS_A_AND_B);
        getGraph().flush();

        long count = graph.query(new String[] { "v1" }, AUTHORIZATIONS_B)
                .vertexIds()
                .getTotalHits();

        assertEquals(count, 1);
    }
}
