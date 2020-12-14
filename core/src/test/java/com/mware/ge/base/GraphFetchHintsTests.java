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
import com.mware.ge.util.IterableUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import java.util.List;
import java.util.stream.Collectors;

import static com.mware.core.model.schema.SchemaConstants.CONCEPT_TYPE_THING;
import static com.mware.ge.util.GeAssert.*;
import static com.mware.ge.util.GeAssert.assertTrue;
import static com.mware.ge.util.IterableUtils.toList;
import static com.mware.ge.values.storable.Values.stringValue;

@RunWith(JUnit4.class)
public abstract class GraphFetchHintsTests implements GraphTestSetup {
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
    public void testFetchHints() {
        Vertex v1 = getGraph().addVertex("v1", VISIBILITY_A, AUTHORIZATIONS_A, CONCEPT_TYPE_THING);
        v1.addPropertyValue("k1", "n1", stringValue("value1"), VISIBILITY_A, AUTHORIZATIONS_A);
        Vertex v2 = getGraph().addVertex("v2", VISIBILITY_A, AUTHORIZATIONS_A, CONCEPT_TYPE_THING);
        Edge e1 = getGraph().addEdge("e1", v1, v2, LABEL_LABEL1, VISIBILITY_A, AUTHORIZATIONS_A);
        e1.addPropertyValue("k1", "n1", stringValue("value1"), VISIBILITY_A, AUTHORIZATIONS_A);
        getGraph().addEdge("e2", v2, v1, LABEL_LABEL1, VISIBILITY_A, AUTHORIZATIONS_A);
        getGraph().flush();

        Vertex v1_none = getGraph().getVertex("v1", FetchHints.NONE, AUTHORIZATIONS_A);
        assertNotNull(v1_none);
        assertThrowsException(v1_none::getProperties);
        assertThrowsException(() -> v1_none.getEdges(Direction.IN, AUTHORIZATIONS_A));
        assertThrowsException(() -> v1_none.getEdges(Direction.OUT, AUTHORIZATIONS_A));

        v1 = getGraph().getVertex("v1", getGraph().getDefaultFetchHints(), AUTHORIZATIONS_A);
        assertNotNull(v1);
        assertEquals(1, IterableUtils.count(v1.getProperties()));
        assertEquals(1, IterableUtils.count(v1.getEdges(Direction.IN, AUTHORIZATIONS_A)));
        assertEquals(1, IterableUtils.count(v1.getEdges(Direction.OUT, AUTHORIZATIONS_A)));

        FetchHints propertiesFetchHints = FetchHints.builder()
                .setIncludeAllProperties(true)
                .build();
        Vertex v1_withProperties = getGraph().getVertex("v1", propertiesFetchHints, AUTHORIZATIONS_A);
        assertNotNull(v1_withProperties);
        assertEquals(1, IterableUtils.count(v1_withProperties.getProperties()));
        assertThrowsException(() -> v1_withProperties.getEdges(Direction.IN, AUTHORIZATIONS_A));
        assertThrowsException(() -> v1_withProperties.getEdges(Direction.OUT, AUTHORIZATIONS_A));

        Vertex v1_withEdgeRegs = getGraph().getVertex("v1", FetchHints.EDGE_REFS, AUTHORIZATIONS_A);
        assertNotNull(v1_withEdgeRegs);
        assertThrowsException(v1_withEdgeRegs::getProperties);
        assertEquals(1, IterableUtils.count(v1_withEdgeRegs.getEdges(Direction.IN, AUTHORIZATIONS_A)));
        assertEquals(1, IterableUtils.count(v1_withEdgeRegs.getEdges(Direction.OUT, AUTHORIZATIONS_A)));

        FetchHints inEdgeRefFetchHints = FetchHints.builder()
                .setIncludeInEdgeRefs(true)
                .build();
        Vertex v1_withInEdgeRegs = getGraph().getVertex("v1", inEdgeRefFetchHints, AUTHORIZATIONS_A);
        assertNotNull(v1_withInEdgeRegs);
        assertThrowsException(v1_withInEdgeRegs::getProperties);
        assertEquals(1, IterableUtils.count(v1_withInEdgeRegs.getEdges(Direction.IN, AUTHORIZATIONS_A)));
        assertThrowsException(() -> v1_withInEdgeRegs.getEdges(Direction.OUT, AUTHORIZATIONS_A));

        FetchHints outEdgeRefFetchHints = FetchHints.builder()
                .setIncludeOutEdgeRefs(true)
                .build();
        Vertex v1_withOutEdgeRegs = getGraph().getVertex("v1", outEdgeRefFetchHints, AUTHORIZATIONS_A);
        assertNotNull(v1_withOutEdgeRegs);
        assertThrowsException(v1_withOutEdgeRegs::getProperties);
        assertThrowsException(() -> v1_withOutEdgeRegs.getEdges(Direction.IN, AUTHORIZATIONS_A));
        assertEquals(1, IterableUtils.count(v1_withOutEdgeRegs.getEdges(Direction.OUT, AUTHORIZATIONS_A)));

        Edge e1_none = getGraph().getEdge("e1", FetchHints.NONE, AUTHORIZATIONS_A);
        assertNotNull(e1_none);
        assertThrowsException(e1_none::getProperties);
        assertEquals("v1", e1_none.getVertexId(Direction.OUT));
        assertEquals("v2", e1_none.getVertexId(Direction.IN));

        Edge e1_default = getGraph().getEdge("e1", getGraph().getDefaultFetchHints(), AUTHORIZATIONS_A);
        assertEquals(1, IterableUtils.count(e1_default.getProperties()));
        assertEquals("v1", e1_default.getVertexId(Direction.OUT));
        assertEquals("v2", e1_default.getVertexId(Direction.IN));

        Edge e1_properties = getGraph().getEdge("e1", propertiesFetchHints, AUTHORIZATIONS_A);
        assertEquals(1, IterableUtils.count(e1_properties.getProperties()));
        assertEquals("v1", e1_properties.getVertexId(Direction.OUT));
        assertEquals("v2", e1_properties.getVertexId(Direction.IN));
    }

    @Test
    public void testFetchHintsProperties() {
        Vertex v1 = getGraph().addVertex("v1", VISIBILITY_A, AUTHORIZATIONS_A, CONCEPT_TYPE_THING);
        v1.addPropertyValue("k1", "n1", stringValue("value1"), VISIBILITY_A, AUTHORIZATIONS_A);
        v1.addPropertyValue("k1", "n2", stringValue("value2"), VISIBILITY_A, AUTHORIZATIONS_A);
        v1.addPropertyValue("k1", "n3", stringValue("value3"), VISIBILITY_A, AUTHORIZATIONS_A);
        getGraph().flush();

        FetchHints specificPropertiesFetchHints = FetchHints.builder()
                .setPropertyNamesToInclude("n1", "n3")
                .build();
        Vertex v1WithoutN2 = getGraph().getVertex("v1", specificPropertiesFetchHints, AUTHORIZATIONS_A);
        assertEquals(stringValue("value1"), v1WithoutN2.getPropertyValue("n1"));
        assertThrowsException(() -> v1WithoutN2.getProperty("n1").getMetadata());
        assertThrowsException(() -> v1WithoutN2.getPropertyValue("n2"));
        assertEquals(stringValue("value3"), v1WithoutN2.getPropertyValue("n3"));

        FetchHints noPropertiesFetchHints = FetchHints.NONE;
        Vertex v1WithNotProperties = getGraph().getVertex("v1", noPropertiesFetchHints, AUTHORIZATIONS_A);
        assertThrowsException(v1WithNotProperties::getProperties);
        assertThrowsException(() -> v1WithNotProperties.getProperty("n1"));

        FetchHints allPropertiesFetchHints = FetchHints.builder()
                .setIncludeAllProperties(true)
                .setPropertyNamesToInclude("n1", "n3")
                .build();
        v1 = getGraph().getVertex("v1", allPropertiesFetchHints, AUTHORIZATIONS_A);
        assertEquals(stringValue("value1"), v1.getPropertyValue("n1"));
        assertEquals(stringValue("value2"), v1.getPropertyValue("n2"));
        assertEquals(stringValue("value3"), v1.getPropertyValue("n3"));
    }

    @Test
    public void testFetchHintsPropertyMetadata() {
        Vertex v1 = getGraph().addVertex("v1", VISIBILITY_A, AUTHORIZATIONS_A, CONCEPT_TYPE_THING);
        Metadata metadata = Metadata.create();
        metadata.add("m1", stringValue("m1value"), VISIBILITY_A);
        metadata.add("m2", stringValue("m2value"), VISIBILITY_A);
        metadata.add("m3", stringValue("m3value"), VISIBILITY_A);
        v1.addPropertyValue("k1", "n1", stringValue("value1"), metadata, VISIBILITY_A, AUTHORIZATIONS_A);
        getGraph().flush();

        FetchHints specificPropertiesFetchHints = FetchHints.builder()
                .setMetadataKeysToInclude("m1", "m3")
                .build();
        v1 = getGraph().getVertex("v1", specificPropertiesFetchHints, AUTHORIZATIONS_A);
        Metadata n1WithoutM2 = v1.getProperty("n1").getMetadata();
        assertEquals(stringValue("m1value"), n1WithoutM2.getValue("m1"));
        assertThrowsException(() -> n1WithoutM2.getValue("m2"));
        assertEquals(stringValue("m3value"), n1WithoutM2.getValue("m3"));

        FetchHints noPropertiesFetchHints = FetchHints.builder()
                .setIncludeAllProperties(true)
                .build();
        Vertex v1_noMetadata = getGraph().getVertex("v1", noPropertiesFetchHints, AUTHORIZATIONS_A);
        assertThrowsException(() -> v1_noMetadata.getProperty("n1").getMetadata());

        FetchHints allPropertiesFetchHints = FetchHints.builder()
                .setIncludeAllPropertyMetadata(true)
                .setMetadataKeysToInclude("m1", "m3")
                .build();
        Vertex v1_withMetadata = getGraph().getVertex("v1", allPropertiesFetchHints, AUTHORIZATIONS_A);
        Metadata n1 = v1_withMetadata.getProperty("n1").getMetadata();
        assertEquals(stringValue("m1value"), n1.getValue("m1"));
        assertEquals(stringValue("m2value"), n1.getValue("m2"));
        assertEquals(stringValue("m3value"), n1.getValue("m3"));
    }

    @Test
    public void testFetchHintsEdges() {
        Vertex v1 = getGraph().addVertex("v1", VISIBILITY_A, AUTHORIZATIONS_A, CONCEPT_TYPE_THING);
        Vertex v2 = getGraph().addVertex("v2", VISIBILITY_A, AUTHORIZATIONS_A, CONCEPT_TYPE_THING);
        getGraph().addEdge("e1", v1, v2, LABEL_LABEL1, VISIBILITY_A, AUTHORIZATIONS_A);
        getGraph().addEdge("e2", v1, v2, LABEL_LABEL2, VISIBILITY_A, AUTHORIZATIONS_A);
        getGraph().addEdge("e3", v1, v2, LABEL_LABEL3, VISIBILITY_A, AUTHORIZATIONS_A);
        getGraph().addEdge("e4", v1, v2, LABEL_LABEL3, VISIBILITY_A, AUTHORIZATIONS_A);
        getGraph().flush();

        FetchHints specificEdgeLabelFetchHints = FetchHints.builder()
                .setEdgeLabelsOfEdgeRefsToInclude(LABEL_LABEL1, LABEL_LABEL3)
                .build();
        v1 = getGraph().getVertex("v1", specificEdgeLabelFetchHints, AUTHORIZATIONS_A);
        List<EdgeInfo> edgeInfos = toList(v1.getEdgeInfos(Direction.BOTH, AUTHORIZATIONS_A));
        assertTrue(edgeInfos.stream().anyMatch(e -> e.getEdgeId().equals("e1")));
        assertFalse(edgeInfos.stream().anyMatch(e -> e.getEdgeId().equals("e2")));
        assertTrue(edgeInfos.stream().anyMatch(e -> e.getEdgeId().equals("e3")));
        assertTrue(edgeInfos.stream().anyMatch(e -> e.getEdgeId().equals("e4")));

        FetchHints noEdgeLabelsFetchHints = FetchHints.NONE;
        Vertex v1_noEdges = getGraph().getVertex("v1", noEdgeLabelsFetchHints, AUTHORIZATIONS_A);
        assertThrowsException(() -> v1_noEdges.getEdges(Direction.BOTH, AUTHORIZATIONS_A));
        assertThrowsException(() -> v1_noEdges.getEdgeInfos(Direction.BOTH, AUTHORIZATIONS_A));

        FetchHints edgeLabelsAndCountsFetchHints = FetchHints.builder()
                .setIncludeEdgeLabelsAndCounts(true)
                .build();
        Vertex v1_withEdgeLabelsAndCounts = getGraph().getVertex("v1", edgeLabelsAndCountsFetchHints, AUTHORIZATIONS_A);
        assertThrowsException(() -> v1_withEdgeLabelsAndCounts.getEdgeInfos(Direction.BOTH, AUTHORIZATIONS_A));
        assertEquals(
                LABEL_LABEL1 + "," + LABEL_LABEL2 + "," + LABEL_LABEL3,
                v1_withEdgeLabelsAndCounts.getEdgesSummary(AUTHORIZATIONS_A).getEdgeLabels().stream()
                        .sorted()
                        .collect(Collectors.joining(","))
        );

        FetchHints allEdgeInfoFetchHints = FetchHints.builder()
                .setIncludeAllEdgeRefs(true)
                .setEdgeLabelsOfEdgeRefsToInclude("m1", "m3")
                .build();
        Vertex v1_withEdgeRefs = getGraph().getVertex("v1", allEdgeInfoFetchHints, AUTHORIZATIONS_A);
        edgeInfos = toList(v1_withEdgeRefs.getEdgeInfos(Direction.BOTH, AUTHORIZATIONS_A));
        assertTrue(edgeInfos.stream().anyMatch(e -> e.getEdgeId().equals("e1")));
        assertTrue(edgeInfos.stream().anyMatch(e -> e.getEdgeId().equals("e2")));
        assertTrue(edgeInfos.stream().anyMatch(e -> e.getEdgeId().equals("e3")));
        assertTrue(edgeInfos.stream().anyMatch(e -> e.getEdgeId().equals("e4")));
    }

    @Test
    public void testFetchHintsEdgeLabels() {
        Vertex v1 = getGraph().addVertex("v1", VISIBILITY_A, AUTHORIZATIONS_ALL, CONCEPT_TYPE_THING);
        Vertex v2 = getGraph().addVertex("v2", VISIBILITY_A, AUTHORIZATIONS_ALL, CONCEPT_TYPE_THING);
        Vertex v3 = getGraph().addVertex("v3", VISIBILITY_A, AUTHORIZATIONS_ALL, CONCEPT_TYPE_THING);
        getGraph().flush();

        getGraph().addEdge("e v1->v2", v1, v2, LABEL_LABEL1, VISIBILITY_A, AUTHORIZATIONS_ALL);
        getGraph().addEdge("e v1->v3", v1, v3, LABEL_LABEL2, VISIBILITY_A, AUTHORIZATIONS_ALL);
        getGraph().flush();

        v1 = getGraph().getVertex("v1", FetchHints.EDGE_LABELS, AUTHORIZATIONS_ALL);
        List<String> edgeLabels = toList(v1.getEdgesSummary(AUTHORIZATIONS_ALL).getEdgeLabels());
        assertEquals(2, edgeLabels.size());
        assertTrue(LABEL_LABEL1 + " missing", edgeLabels.contains(LABEL_LABEL1));
        assertTrue(LABEL_LABEL2 + " missing", edgeLabels.contains(LABEL_LABEL2));
    }

    @Test
    public void testFetchHintsEdgesSummary() {
        Vertex v1 = getGraph().addVertex("v1", VISIBILITY_A, AUTHORIZATIONS_ALL, CONCEPT_TYPE_THING);
        Vertex v2 = getGraph().addVertex("v2", VISIBILITY_A, AUTHORIZATIONS_ALL, CONCEPT_TYPE_THING);
        Vertex v3 = getGraph().addVertex("v3", VISIBILITY_A, AUTHORIZATIONS_ALL, CONCEPT_TYPE_THING);
        getGraph().flush();
        getGraph().addEdge("e v1->v2", v1, v2, LABEL_LABEL1, VISIBILITY_A, AUTHORIZATIONS_ALL);
        getGraph().addEdge("e v1->v3", v1, v3, LABEL_LABEL2, VISIBILITY_A, AUTHORIZATIONS_ALL);
        getGraph().flush();
        v1 = getGraph().getVertex("v1", FetchHints.EDGE_LABELS, AUTHORIZATIONS_ALL);
        EdgesSummary summary = v1.getEdgesSummary(AUTHORIZATIONS_ALL);
        assertEquals(2, summary.getEdgeLabels().size());
        assertTrue(LABEL_LABEL1 + " missing", summary.getEdgeLabels().contains(LABEL_LABEL1));
        assertTrue(LABEL_LABEL2 + " missing", summary.getEdgeLabels().contains(LABEL_LABEL2));
        assertEquals(2, summary.getOutEdgeLabels().size());
        assertEquals(0, summary.getInEdgeLabels().size());
        assertEquals(1, (int) summary.getOutEdgeCountsByLabels().get(LABEL_LABEL1));
        assertEquals(1, (int) summary.getOutEdgeCountsByLabels().get(LABEL_LABEL2));
    }

    @Test
    public void testFetchHintsExceptions() {
        Metadata prop1Metadata = Metadata.create();
        prop1Metadata.add("metadata1", stringValue("metadata1Value"), VISIBILITY_A);
        prop1Metadata.add("metadata2", stringValue("metadata2Value"), VISIBILITY_A);

        getGraph().prepareVertex("v1", VISIBILITY_A, CONCEPT_TYPE_THING)
                .setProperty("prop1", stringValue("value1"), prop1Metadata, VISIBILITY_A)
                .save(AUTHORIZATIONS_A_AND_B);
        getGraph().flush();

        FetchHints propertyFetchHints = FetchHints.builder()
                .setPropertyNamesToInclude("prop2")
                .build();
        Vertex v1WithOnlyProp2 = getGraph().getVertex("v1", propertyFetchHints, AUTHORIZATIONS_A);
        assertNotNull(v1WithOnlyProp2.getProperties());
        assertThrowsException(() -> v1WithOnlyProp2.getProperty("prop1"));

        FetchHints propertiesFetchHints = FetchHints.builder()
                .setIncludeAllProperties(true)
                .build();
        Vertex v1WithAllProperties = getGraph().getVertex("v1", propertiesFetchHints, AUTHORIZATIONS_A);
        assertThrowsException(v1WithAllProperties.getProperty("prop1")::getMetadata);

        FetchHints metadataFetchHints = FetchHints.builder()
                .setIncludeAllProperties(true)
                .setMetadataKeysToInclude("metadata1")
                .build();
        Vertex v1WithOnlyMetadata1 = getGraph().getVertex("v1", metadataFetchHints, AUTHORIZATIONS_A);
        Property prop1 = v1WithOnlyMetadata1.getProperty("prop1");
        assertNotNull(prop1.getMetadata());
        assertNotNull(v1WithOnlyMetadata1.getProperty("prop1").getMetadata().getEntry("metadata1"));
        assertThrowsException(() -> v1WithOnlyMetadata1.getProperty("prop1").getMetadata().getEntry("metadata2"));
    }
}
