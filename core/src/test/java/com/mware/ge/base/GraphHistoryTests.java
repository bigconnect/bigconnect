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

import com.google.common.collect.ImmutableMap;
import com.mware.ge.*;
import com.mware.ge.event.GraphEvent;
import com.mware.ge.event.GraphEventListener;
import com.mware.ge.mutation.ElementMutation;
import com.mware.ge.values.storable.DateTimeValue;
import com.mware.ge.values.storable.StreamingPropertyValue;
import com.mware.ge.values.storable.TextValue;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static com.mware.core.model.schema.SchemaConstants.CONCEPT_TYPE_THING;
import static com.mware.ge.util.GeAssert.*;
import static com.mware.ge.util.GeAssert.assertEquals;
import static com.mware.ge.util.IterableUtils.count;
import static com.mware.ge.util.IterableUtils.toList;
import static com.mware.ge.values.storable.Values.intValue;
import static com.mware.ge.values.storable.Values.stringValue;

@RunWith(JUnit4.class)
public abstract class GraphHistoryTests implements GraphTestSetup {
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

    // Historical Property Value tests
    @Test
    public void historicalPropertyValueAddProp() {
        getGraph().prepareVertex("v1", VISIBILITY_A, CONCEPT_TYPE_THING)
                .setProperty("prop1_A", stringValue("value1"), VISIBILITY_A)
                .setProperty("prop2_B", stringValue("value2"), VISIBILITY_B)
                .save(AUTHORIZATIONS_A_AND_B);
        getGraph().flush();

        // Add property
        Vertex v1 = getGraph().getVertex("v1", AUTHORIZATIONS_A_AND_B);
        v1.prepareMutation()
                .setProperty("prop3_A", stringValue("value3"), VISIBILITY_A)
                .save(AUTHORIZATIONS_A_AND_B);
        getGraph().flush();

        v1 = getGraph().getVertex("v1", AUTHORIZATIONS_A_AND_B);
        List<HistoricalPropertyValue> values = toList(v1.getHistoricalPropertyValues(AUTHORIZATIONS_A_AND_B));
        Collections.reverse(values);

        assertEquals(3, values.size());
        assertEquals("prop1_A", values.get(0).getPropertyName());
        assertEquals("prop2_B", values.get(1).getPropertyName());
        assertEquals("prop3_A", values.get(2).getPropertyName());
    }

    @Test
    public void historicalPropertyValueDeleteProp() {
        getGraph().prepareVertex("v1", VISIBILITY_A, CONCEPT_TYPE_THING)
                .setProperty("prop1_A", stringValue("value1"), VISIBILITY_A)
                .setProperty("prop2_B", stringValue("value2"), VISIBILITY_B)
                .setProperty("prop3_A", stringValue("value3"), VISIBILITY_A)
                .save(AUTHORIZATIONS_A_AND_B);
        getGraph().flush();

        // remove property
        Vertex v1 = getGraph().getVertex("v1", AUTHORIZATIONS_A_AND_B);
        v1.prepareMutation()
                .softDeleteProperties("prop2_B")
                .save(AUTHORIZATIONS_A_AND_B);
        getGraph().flush();

        v1 = getGraph().getVertex("v1", AUTHORIZATIONS_A_AND_B);
        List<HistoricalPropertyValue> values = toList(v1.getHistoricalPropertyValues(AUTHORIZATIONS_A_AND_B));
        Collections.reverse(values);
        assertEquals(4, values.size());

        boolean isDeletedExpected = false;
        for (int i = 0; i < 4; i++) {
            HistoricalPropertyValue item = values.get(i);
            if (item.getPropertyName().equals("prop1_A")) {
                assertEquals("prop1_A", values.get(i).getPropertyName());
                assertFalse(values.get(i).isDeleted());
            } else if (item.getPropertyName().equals("prop2_B")) {
                assertEquals("prop2_B", values.get(i).getPropertyName());
                assertEquals(isDeletedExpected, values.get(i).isDeleted());
                isDeletedExpected = !isDeletedExpected;
            } else if (item.getPropertyName().equals("prop3_A")) {
                assertEquals("prop3_A", values.get(i).getPropertyName());
                assertFalse(values.get(i).isDeleted());
            } else {
                fail("Invalid " + item);
            }
        }

        Metadata metadata = Metadata.create();
        metadata.add("metadata1", stringValue("metadata1Value"), VISIBILITY_A);
        Vertex v2 = getGraph().prepareVertex("v2", VISIBILITY_A, CONCEPT_TYPE_THING)
                .setProperty("prop1_A", stringValue("value1"), metadata, VISIBILITY_A)
                .save(AUTHORIZATIONS_A_AND_B);
        getGraph().flush();

        metadata.add("metadata2", stringValue("metadata2Value"), VISIBILITY_A);
        v2.prepareMutation()
                .setProperty("prop1_A", stringValue("value2"), metadata, VISIBILITY_B)
                .save(AUTHORIZATIONS_A_AND_B);
        getGraph().flush();

        // remove property
        v2 = getGraph().getVertex("v2", AUTHORIZATIONS_A_AND_B);
        v2.prepareMutation()
                .softDeleteProperties("prop1_A")
                .save(AUTHORIZATIONS_A_AND_B);
        getGraph().flush();

        v2 = getGraph().getVertex("v2", AUTHORIZATIONS_A_AND_B);
        values = toList(v2.getHistoricalPropertyValues(AUTHORIZATIONS_A_AND_B));
        Collections.reverse(values);
        assertEquals(3, values.size());
        List<HistoricalPropertyValue> deletedHpv = values.stream()
                .filter(HistoricalPropertyValue::isDeleted)
                .collect(Collectors.toList());
        assertEquals(1, deletedHpv.size());

        for (int i = 0; i < 3; i++) {
            HistoricalPropertyValue item = values.get(i);
            if (item.getPropertyName().equals("prop1_A")) {
                assertEquals("prop1_A", values.get(i).getPropertyName());
                if (item.isDeleted()) {
                    Metadata hpvMetadata = item.getMetadata();
                    assertEquals(2, hpvMetadata.entrySet().size());
                    assertEquals(VISIBILITY_B_STRING, item.getPropertyVisibility().getVisibilityString());
                }
            } else {
                fail("Invalid " + item);
            }
        }

    }

    @Test
    public void historicalPropertyValueModifyPropValue() {
        getGraph().prepareVertex("v1", VISIBILITY_A, CONCEPT_TYPE_THING)
                .setProperty("prop1_A", stringValue("value1"), VISIBILITY_A)
                .setProperty("prop2_B", stringValue("value2"), VISIBILITY_B)
                .setProperty("prop3_A", stringValue("value3"), VISIBILITY_A)
                .save(AUTHORIZATIONS_A_AND_B);
        getGraph().flush();

        // modify property value
        Vertex v1 = getGraph().getVertex("v1", AUTHORIZATIONS_A_AND_B);
        v1.prepareMutation()
                .setProperty("prop3_A", stringValue("value4"), VISIBILITY_A)
                .save(AUTHORIZATIONS_A_AND_B);
        getGraph().flush();

        // Restore
        v1 = getGraph().getVertex("v1", AUTHORIZATIONS_A_AND_B);
        v1.prepareMutation()
                .setProperty("prop3_A", stringValue("value3"), VISIBILITY_A)
                .save(AUTHORIZATIONS_A_AND_B);
        getGraph().flush();

        v1 = getGraph().getVertex("v1", AUTHORIZATIONS_A_AND_B);
        List<HistoricalPropertyValue> values = toList(v1.getHistoricalPropertyValues(AUTHORIZATIONS_A_AND_B));
        Collections.reverse(values);
        assertEquals(5, values.size());
        assertEquals("prop1_A", values.get(0).getPropertyName());
        assertFalse(values.get(0).isDeleted());
        assertEquals(stringValue("value1"), values.get(0).getValue());
        assertEquals("prop2_B", values.get(1).getPropertyName());
        assertFalse(values.get(1).isDeleted());
        assertEquals(stringValue("value2"), values.get(1).getValue());
        assertEquals("prop3_A", values.get(2).getPropertyName());
        assertFalse(values.get(2).isDeleted());
        assertEquals(stringValue("value3"), values.get(2).getValue());
        assertEquals("prop3_A", values.get(3).getPropertyName());
        assertFalse(values.get(3).isDeleted());
        assertEquals(stringValue("value4"), values.get(3).getValue());
        assertEquals("prop3_A", values.get(4).getPropertyName());
        assertFalse(values.get(4).isDeleted());
        assertEquals(stringValue("value3"), values.get(4).getValue());
    }

    @Test
    public void historicalPropertyValueModifyPropVisibility() {
        getGraph().prepareVertex("v1", VISIBILITY_A, CONCEPT_TYPE_THING)
                .setProperty("prop1_A", stringValue("value1"), VISIBILITY_A)
                .setProperty("prop2_B", stringValue("value2"), VISIBILITY_B)
                .setProperty("prop3_A", stringValue("value3"), VISIBILITY_A)
                .save(AUTHORIZATIONS_A_AND_B);
        getGraph().flush();

        // modify property value
        Vertex v1 = getGraph().getVertex("v1", FetchHints.ALL, AUTHORIZATIONS_A_AND_B);
        v1.prepareMutation()
                .alterPropertyVisibility("prop1_A", VISIBILITY_B)
                .save(AUTHORIZATIONS_A_AND_B);
        getGraph().flush();

        // Restore
        v1 = getGraph().getVertex("v1", FetchHints.ALL, AUTHORIZATIONS_A_AND_B);
        v1.prepareMutation()
                .alterPropertyVisibility("prop1_A", VISIBILITY_A)
                .save(AUTHORIZATIONS_A_AND_B);
        getGraph().flush();

        List<HistoricalPropertyValue> values = toList(v1.getHistoricalPropertyValues(AUTHORIZATIONS_A_AND_B));
        Collections.reverse(values);
        assertEquals(5, values.size());
        assertEquals("prop1_A", values.get(0).getPropertyName());
        assertFalse(values.get(0).isDeleted());
        assertEquals(VISIBILITY_A, values.get(0).getPropertyVisibility());
        assertEquals("prop2_B", values.get(1).getPropertyName());
        assertFalse(values.get(1).isDeleted());
        assertEquals(VISIBILITY_B, values.get(1).getPropertyVisibility());
        assertEquals("prop3_A", values.get(2).getPropertyName());
        assertFalse(values.get(2).isDeleted());
        assertEquals(VISIBILITY_A, values.get(2).getPropertyVisibility());
        assertEquals("prop1_A", values.get(3).getPropertyName());
        assertFalse(values.get(3).isDeleted());
        assertEquals(VISIBILITY_B, values.get(3).getPropertyVisibility());
        assertEquals("prop1_A", values.get(4).getPropertyName());
        assertFalse(values.get(4).isDeleted());
        assertEquals(VISIBILITY_A, values.get(4).getPropertyVisibility());
    }

    @Test
    public void testSaveMultipleTimestampedValuesInSameMutationVertex() {
        String vertexId = "v1";
        String propertyKey = "k1";
        String propertyName = "p1";
        Map<String, Long> values = ImmutableMap.of(
                "value1", createDateTime(2016, 4, 6, 9, 20, 0).asObjectCopy().toInstant().toEpochMilli(),
                "value2", createDateTime(2016, 5, 6, 9, 20, 0).asObjectCopy().toInstant().toEpochMilli(),
                "value3", createDateTime(2016, 6, 6, 9, 20, 0).asObjectCopy().toInstant().toEpochMilli(),
                "value4", createDateTime(2016, 7, 6, 9, 20, 0).asObjectCopy().toInstant().toEpochMilli(),
                "value5", createDateTime(2016, 8, 6, 9, 20, 0).asObjectCopy().toInstant().toEpochMilli()
        );

        ElementMutation<Vertex> vertexMutation = getGraph().prepareVertex(vertexId, VISIBILITY_EMPTY, CONCEPT_TYPE_THING);
        for (Map.Entry<String, Long> entry : values.entrySet()) {
            vertexMutation.addPropertyValue(propertyKey, propertyName, stringValue(entry.getKey()), Metadata.create(), entry.getValue(), VISIBILITY_EMPTY);
        }
        vertexMutation.save(AUTHORIZATIONS_EMPTY);
        getGraph().flush();

        Vertex retrievedVertex = getGraph().getVertex(vertexId, AUTHORIZATIONS_EMPTY);
        Iterable<HistoricalPropertyValue> historicalPropertyValues = retrievedVertex.getHistoricalPropertyValues(propertyKey, propertyName, VISIBILITY_EMPTY, null, null, AUTHORIZATIONS_EMPTY);
        compareHistoricalValues(values, historicalPropertyValues);
    }

    @Test
    public void testSaveMultipleTimestampedValuesInSameMutationEdge() {
        Vertex v1 = getGraph().addVertex("v1", VISIBILITY_EMPTY, AUTHORIZATIONS_EMPTY, CONCEPT_TYPE_THING);
        Vertex v2 = getGraph().addVertex("v2", VISIBILITY_EMPTY, AUTHORIZATIONS_EMPTY, CONCEPT_TYPE_THING);

        String edgeId = "e1";
        String propertyKey = "k1";
        String propertyName = "p1";
        Map<String, Long> values = ImmutableMap.of(
                "value1", createDateTime(2016, 4, 6, 9, 20, 0).asObjectCopy().toInstant().toEpochMilli(),
                "value2", createDateTime(2016, 5, 6, 9, 20, 0).asObjectCopy().toInstant().toEpochMilli(),
                "value3", createDateTime(2016, 6, 6, 9, 20, 0).asObjectCopy().toInstant().toEpochMilli(),
                "value4", createDateTime(2016, 7, 6, 9, 20, 0).asObjectCopy().toInstant().toEpochMilli(),
                "value5", createDateTime(2016, 8, 6, 9, 20, 0).asObjectCopy().toInstant().toEpochMilli()
        );

        ElementMutation<Edge> edgeMutation = getGraph().prepareEdge(edgeId, v1, v2, LABEL_LABEL1, VISIBILITY_EMPTY);
        for (Map.Entry<String, Long> entry : values.entrySet()) {
            edgeMutation.addPropertyValue(propertyKey, propertyName, stringValue(entry.getKey()), Metadata.create(), entry.getValue(), VISIBILITY_EMPTY);
        }
        edgeMutation.save(AUTHORIZATIONS_EMPTY);
        getGraph().flush();

        Edge retrievedEdge = getGraph().getEdge(edgeId, AUTHORIZATIONS_EMPTY);
        Iterable<HistoricalPropertyValue> historicalPropertyValues = retrievedEdge.getHistoricalPropertyValues(propertyKey, propertyName, VISIBILITY_EMPTY, null, null, AUTHORIZATIONS_EMPTY);
        compareHistoricalValues(values, historicalPropertyValues);
    }

    @Test
    public void testTimestampsInExistingElementMutation() {
        Long t1 = createDateTime(2017, 1, 18, 9, 20, 0).asObjectCopy().toInstant().toEpochMilli();
        Long t2 = createDateTime(2017, 1, 19, 9, 20, 0).asObjectCopy().toInstant().toEpochMilli();

        getGraph().prepareVertex("v1", VISIBILITY_EMPTY, CONCEPT_TYPE_THING)
                .addPropertyValue("k1", "prop1", stringValue("test1"), Metadata.create(), t1, VISIBILITY_EMPTY)
                .save(AUTHORIZATIONS_ALL);
        getGraph().flush();

        Vertex v1 = getGraph().getVertex("v1", AUTHORIZATIONS_ALL);
        assertEquals(t1, v1.getProperty("k1", "prop1").getTimestamp());

        getGraph().getVertex("v1", AUTHORIZATIONS_ALL)
                .prepareMutation()
                .addPropertyValue("k1", "prop1", stringValue("test2"), Metadata.create(), t2, VISIBILITY_EMPTY)
                .save(AUTHORIZATIONS_ALL);
        getGraph().flush();

        v1 = getGraph().getVertex("v1", AUTHORIZATIONS_ALL);
        assertEquals(t2, v1.getProperty("k1", "prop1").getTimestamp());

        List<HistoricalPropertyValue> historicalValues = toList(v1.getHistoricalPropertyValues("k1", "prop1", VISIBILITY_EMPTY, AUTHORIZATIONS_ALL));
        assertEquals(2, historicalValues.size());
        assertEquals(t1.longValue(), historicalValues.get(1).getTimestamp());
        assertEquals(t2.longValue(), historicalValues.get(0).getTimestamp());
    }

    @Test
    public void testPropertyHistoricalVersions() {
        DateTimeValue time25 = createDateTime(2015, 4, 6, 16, 15, 0);
        long time25Millis = time25.asObjectCopy().toInstant().toEpochMilli();
        DateTimeValue time30 = createDateTime(2015, 4, 6, 16, 16, 0);
        long time30Millis = time30.asObjectCopy().toInstant().toEpochMilli();

        Metadata metadata = Metadata.create();
        metadata.add("author", stringValue("author1"), VISIBILITY_A);
        getGraph().prepareVertex("v1", VISIBILITY_A, CONCEPT_TYPE_THING)
                .addPropertyValue("", "age", intValue(25), metadata, time25Millis, VISIBILITY_A)
                .save(AUTHORIZATIONS_A);

        metadata = Metadata.create();
        metadata.add("author", stringValue("author2"), VISIBILITY_A);
        getGraph().prepareVertex("v1", VISIBILITY_A, CONCEPT_TYPE_THING)
                .addPropertyValue("", "age", intValue(30), metadata, time30Millis, VISIBILITY_A)
                .save(AUTHORIZATIONS_A);
        getGraph().flush();

        Vertex v1 = getGraph().getVertex("v1", FetchHints.ALL, AUTHORIZATIONS_A);
        List<HistoricalPropertyValue> values = toList(v1.getHistoricalPropertyValues("", "age", VISIBILITY_A, AUTHORIZATIONS_A));
        assertEquals(2, values.size());

        assertEquals(intValue(30), values.get(0).getValue());
        assertEquals(time30Millis, values.get(0).getTimestamp());
        assertEquals(stringValue("author2"), values.get(0).getMetadata().getValue("author", VISIBILITY_A));

        assertEquals(intValue(25), values.get(1).getValue());
        assertEquals(time25Millis, values.get(1).getTimestamp());
        assertEquals(stringValue("author1"), values.get(1).getMetadata().getValue("author", VISIBILITY_A));

        // make sure we get the correct age when we only ask for one value
        assertEquals(intValue(30), v1.getPropertyValue("", "age"));
        assertEquals(stringValue("author2"), v1.getProperty("", "age").getMetadata().getValue("author", VISIBILITY_A));
    }

    @Test
    public void testStreamingPropertyHistoricalVersions() {
        DateTimeValue time25 = createDateTime(2015, 4, 6, 16, 15, 0);
        long time25Millis = time25.asObjectCopy().toInstant().toEpochMilli();

        DateTimeValue time30 = createDateTime(2015, 4, 6, 16, 16, 0);
        long time30Millis = time30.asObjectCopy().toInstant().toEpochMilli();

        Metadata metadata = Metadata.create();
        StreamingPropertyValue value1 = StreamingPropertyValue.create("value1");
        getGraph().prepareVertex("v1", VISIBILITY_A, CONCEPT_TYPE_THING)
                .addPropertyValue("", "text", value1, metadata, time25Millis, VISIBILITY_A)
                .save(AUTHORIZATIONS_A);

        StreamingPropertyValue value2 = StreamingPropertyValue.create("value2");
        getGraph().prepareVertex("v1", VISIBILITY_A, CONCEPT_TYPE_THING)
                .addPropertyValue("", "text", value2, metadata, time30Millis, VISIBILITY_A)
                .save(AUTHORIZATIONS_A);
        getGraph().flush();

        Vertex v1 = getGraph().getVertex("v1", AUTHORIZATIONS_A);
        List<HistoricalPropertyValue> values = toList(v1.getHistoricalPropertyValues("", "text", VISIBILITY_A, AUTHORIZATIONS_A));
        assertEquals(2, values.size());

        assertEquals("value2", ((StreamingPropertyValue) values.get(0).getValue()).readToString());
        assertEquals(time30Millis, values.get(0).getTimestamp());

        assertEquals("value1", ((StreamingPropertyValue) values.get(1).getValue()).readToString());
        assertEquals(time25Millis, values.get(1).getTimestamp());

        // make sure we get the correct age when we only ask for one value
        assertEquals("value2", ((StreamingPropertyValue) v1.getPropertyValue("", "text")).readToString());
    }

    @Test
    public void testGetVertexAtASpecificTimeInHistory() {
        DateTimeValue time25 = createDateTime(2015, 4, 6, 16, 15, 0);
        long time25Millis = time25.asObjectCopy().toInstant().toEpochMilli();

        DateTimeValue time30 = createDateTime(2015, 4, 6, 16, 16, 0);
        long time30Millis = time30.asObjectCopy().toInstant().toEpochMilli();

        Metadata metadata = Metadata.create();
        Vertex v1 = getGraph().prepareVertex("v1", time25Millis, VISIBILITY_A, CONCEPT_TYPE_THING)
                .addPropertyValue("", "age", intValue(25), metadata, time25Millis, VISIBILITY_A)
                .save(AUTHORIZATIONS_A);
        Vertex v2 = getGraph().prepareVertex("v2", time25Millis, VISIBILITY_A, CONCEPT_TYPE_THING)
                .addPropertyValue("", "age", intValue(20), metadata, time25Millis, VISIBILITY_A)
                .save(AUTHORIZATIONS_A);
        getGraph().prepareEdge("e1", v1, v2, LABEL_LABEL1, time30Millis, VISIBILITY_A)
                .save(AUTHORIZATIONS_A);

        getGraph().prepareVertex("v1", time30Millis, VISIBILITY_A, CONCEPT_TYPE_THING)
                .addPropertyValue("", "age", intValue(30), metadata, time30Millis, VISIBILITY_A)
                .save(AUTHORIZATIONS_A);
        getGraph().prepareVertex("v3", time30Millis, VISIBILITY_A, CONCEPT_TYPE_THING)
                .addPropertyValue("", "age", intValue(35), metadata, time30Millis, VISIBILITY_A)
                .save(AUTHORIZATIONS_A);
        getGraph().flush();

        // verify current versions
        assertEquals(intValue(30), getGraph().getVertex("v1", AUTHORIZATIONS_A).getPropertyValue("", "age"));
        assertEquals(intValue(20), getGraph().getVertex("v2", AUTHORIZATIONS_A).getPropertyValue("", "age"));
        assertEquals(intValue(35), getGraph().getVertex("v3", AUTHORIZATIONS_A).getPropertyValue("", "age"));
        assertEquals(1, count(getGraph().getEdges(AUTHORIZATIONS_A)));

        // verify old version
        assertEquals(intValue(25), getGraph().getVertex("v1", getGraph().getDefaultFetchHints(), time25Millis, AUTHORIZATIONS_A).getPropertyValue("", "age"));
        assertNull("v3 should not exist at time25", getGraph().getVertex("v3", getGraph().getDefaultFetchHints(), time25Millis, AUTHORIZATIONS_A));
        assertEquals("e1 should not exist", 0, count(getGraph().getEdges(getGraph().getDefaultFetchHints(), time25Millis, AUTHORIZATIONS_A)));
    }

    @Test
    public void testAllPropertyHistoricalVersions() {
        DateTimeValue time25 = createDateTime(2015, 4, 6, 16, 15, 0);
        long time25Millis = time25.asObjectCopy().toInstant().toEpochMilli();
        DateTimeValue time30 = createDateTime(2015, 4, 6, 16, 16, 0);
        long time30Millis = time30.asObjectCopy().toInstant().toEpochMilli();

        Metadata metadata = Metadata.create();
        metadata.add("author", stringValue("author1"), VISIBILITY_A);
        getGraph().prepareVertex("v1", VISIBILITY_A, CONCEPT_TYPE_THING)
                .addPropertyValue("", "age", intValue(25), metadata, time25Millis, VISIBILITY_A)
                .addPropertyValue("k1", "name", stringValue("k1Time25Value"), metadata, time25Millis, VISIBILITY_A)
                .addPropertyValue("k2", "name", stringValue("k2Time25Value"), metadata, time25Millis, VISIBILITY_A)
                .save(AUTHORIZATIONS_A);

        metadata = Metadata.create();
        metadata.add("author", stringValue("author2"), VISIBILITY_A);
        getGraph().prepareVertex("v1", VISIBILITY_A, CONCEPT_TYPE_THING)
                .addPropertyValue("", "age", intValue(30), metadata, time30Millis, VISIBILITY_A)
                .addPropertyValue("k1", "name", stringValue("k1Time30Value"), metadata, time30Millis, VISIBILITY_A)
                .addPropertyValue("k2", "name", stringValue("k2Time30Value"), metadata, time30Millis, VISIBILITY_A)
                .save(AUTHORIZATIONS_A);
        getGraph().flush();

        Vertex v1 = getGraph().getVertex("v1", AUTHORIZATIONS_A);
        List<HistoricalPropertyValue> values = toList(v1.getHistoricalPropertyValues(AUTHORIZATIONS_A));
        assertEquals(6, values.size());

        for (int i = 0; i < 3; i++) {
            HistoricalPropertyValue item = values.get(i);
            assertEquals(time30Millis, values.get(i).getTimestamp());
            if (item.getPropertyName().equals("age")) {
                assertEquals(intValue(30), item.getValue());
            } else if (item.getPropertyName().equals("name") && item.getPropertyKey().equals("k1")) {
                assertEquals(stringValue("k1Time30Value"), item.getValue());
            } else if (item.getPropertyName().equals("name") && item.getPropertyKey().equals("k2")) {
                assertEquals(stringValue("k2Time30Value"), item.getValue());
            } else {
                fail("Invalid " + item);
            }
        }

        for (int i = 3; i < 6; i++) {
            HistoricalPropertyValue item = values.get(i);
            assertEquals(time25Millis, values.get(i).getTimestamp());
            if (item.getPropertyName().equals("age")) {
                assertEquals(intValue(25), item.getValue());
            } else if (item.getPropertyName().equals("name") && item.getPropertyKey().equals("k1")) {
                assertEquals(stringValue("k1Time25Value"), item.getValue());
            } else if (item.getPropertyName().equals("name") && item.getPropertyKey().equals("k2")) {
                assertEquals(stringValue("k2Time25Value"), item.getValue());
            } else {
                fail("Invalid " + item);
            }
        }
    }

    private void compareHistoricalValues(Map<String, Long> expectedValues, Iterable<HistoricalPropertyValue> historicalPropertyValues) {
        Map<String, Long> expectedValuesCopy = new HashMap<>(expectedValues);
        for (HistoricalPropertyValue historicalPropertyValue : historicalPropertyValues) {
            TextValue value = (TextValue) historicalPropertyValue.getValue();
            if (!expectedValuesCopy.containsKey(value.stringValue())) {
                throw new GeException("Expected historical values to contain: " + value.stringValue());
            }
            long expectedValue = expectedValuesCopy.remove(value.stringValue());
            long ts = historicalPropertyValue.getTimestamp();
            assertEquals(expectedValue, ts);
        }
        if (expectedValuesCopy.size() > 0) {
            StringBuilder result = new StringBuilder();
            for (Map.Entry<String, Long> entry : expectedValuesCopy.entrySet()) {
                result.append(entry.getKey()).append(" = ").append(entry.getValue()).append("\n");
            }
            throw new GeException("Missing historical values:\n" + result.toString());
        }
    }
}
