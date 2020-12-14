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
import com.mware.ge.mutation.ExistingElementMutation;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import java.util.List;

import static com.mware.core.model.schema.SchemaConstants.CONCEPT_TYPE_THING;
import static com.mware.ge.util.GeAssert.*;
import static com.mware.ge.util.GeAssert.addGraphEvent;
import static com.mware.ge.util.IterableUtils.toList;
import static com.mware.ge.values.storable.Values.stringValue;

@RunWith(JUnit4.class)
public abstract class GraphMetadataTests implements GraphTestSetup {
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
    public void testGraphMetadata() {
        List<GraphMetadataEntry> existingMetadata = toList(getGraph().getMetadata());

        getGraph().setMetadata("test1", "value1old");
        getGraph().setMetadata("test1", "value1");
        getGraph().setMetadata("test2", "value2");

        assertEquals("value1", getGraph().getMetadata("test1"));
        assertEquals("value2", getGraph().getMetadata("test2"));
        assertEquals(null, getGraph().getMetadata("missingProp"));

        getGraph().dumpGraph();

        List<GraphMetadataEntry> newMetadata = toList(getGraph().getMetadata());
        assertEquals(existingMetadata.size() + 2, newMetadata.size());
    }

    @Test
    public void testGraphMetadataStore() {
        getGraph().setMetadata("testMeta1", "testMeta1Value");
        getGraph().dumpGraph();
        assertEquals(getGraph().getMetadata("testMeta1"), "testMeta1Value");
        getGraph().removeMetadata("testMeta1");
        assertNull(getGraph().getMetadata("testMeta1"));
    }

    @Test
    public void testMetadata() {
        Vertex v1 = getGraph().prepareVertex("v1", VISIBILITY_A, CONCEPT_TYPE_THING)
                .setProperty("prop1", stringValue("value1"), VISIBILITY_A)
                .setProperty("prop1", stringValue("value1"), VISIBILITY_B)
                .save(AUTHORIZATIONS_A_AND_B);
        getGraph().flush();

        Vertex newV1 = getGraph().getVertex("v1", FetchHints.ALL, AUTHORIZATIONS_A_AND_B);
        ExistingElementMutation<Vertex> m = newV1.prepareMutation();
        m.setPropertyMetadata(v1.getProperty("prop1", VISIBILITY_A), "metadata1", stringValue("metadata-value1aa"), VISIBILITY_A);
        m.setPropertyMetadata(v1.getProperty("prop1", VISIBILITY_A), "metadata1", stringValue("metadata-value1ab"), VISIBILITY_B);
        m.setPropertyMetadata(v1.getProperty("prop1", VISIBILITY_B), "metadata1", stringValue("metadata-value1bb"), VISIBILITY_B);
        m.save(AUTHORIZATIONS_A_AND_B);
        getGraph().flush();

        v1 = getGraph().getVertex("v1", FetchHints.ALL, AUTHORIZATIONS_A_AND_B);

        Property prop1A = v1.getProperty("prop1", VISIBILITY_A);
        assertEquals(2, prop1A.getMetadata().entrySet().size());
        assertEquals(stringValue("metadata-value1aa"), prop1A.getMetadata().getValue("metadata1", VISIBILITY_A));
        assertEquals(stringValue("metadata-value1ab"), prop1A.getMetadata().getValue("metadata1", VISIBILITY_B));

        Property prop1B = v1.getProperty("prop1", VISIBILITY_B);
        assertEquals(1, prop1B.getMetadata().entrySet().size());
        assertEquals(stringValue("metadata-value1bb"), prop1B.getMetadata().getValue("metadata1", VISIBILITY_B));
    }


    @Test
    public void testMetadataMutationsOnVertex() {
        Metadata metadataPropB = Metadata.create();
        metadataPropB.add("meta1", stringValue("meta1"), VISIBILITY_A);
        Vertex vertex = getGraph().prepareVertex("v1", VISIBILITY_A, CONCEPT_TYPE_THING)
                .setProperty("propBmeta", stringValue("propBmeta"), metadataPropB, VISIBILITY_A)
                .save(AUTHORIZATIONS_ALL);
        getGraph().flush();

        ExistingElementMutation<Vertex> m = vertex.prepareMutation();
        m.setPropertyMetadata("propBmeta", "meta1", stringValue("meta2"), VISIBILITY_A);
        vertex = m.save(AUTHORIZATIONS_ALL);

        assertEquals(stringValue("meta2"), vertex.getProperty("propBmeta").getMetadata().getEntry("meta1").getValue());
    }

    @Test
    public void testMetadataMutationsOnEdge() {
        getGraph().addVertex("v1", VISIBILITY_A, AUTHORIZATIONS_A, CONCEPT_TYPE_THING);
        getGraph().addVertex("v2", VISIBILITY_A, AUTHORIZATIONS_A, CONCEPT_TYPE_THING);
        Metadata metadataPropB = Metadata.create();
        metadataPropB.add("meta1", stringValue("meta1"), VISIBILITY_A);
        Edge edge = getGraph().prepareEdge("v1", "v2", LABEL_LABEL1, VISIBILITY_A)
                .setProperty("propBmeta", stringValue("propBmeta"), metadataPropB, VISIBILITY_A)
                .save(AUTHORIZATIONS_ALL);
        getGraph().flush();

        ExistingElementMutation<Edge> m = edge.prepareMutation();
        m.setPropertyMetadata("propBmeta", "meta1", stringValue("meta2"), VISIBILITY_A);
        edge = m.save(AUTHORIZATIONS_ALL);

        assertEquals(stringValue("meta2"), edge.getProperty("propBmeta").getMetadata().getEntry("meta1").getValue());
    }

    @Test
    public void testMetadataUpdate() {
        Metadata metadataPropB = Metadata.create();
        metadataPropB.add("meta1", stringValue("value1"), VISIBILITY_A);
        getGraph().prepareVertex("v1", VISIBILITY_A, CONCEPT_TYPE_THING)
                .setProperty("propBmeta", stringValue("propBmeta"), metadataPropB, VISIBILITY_A)
                .save(AUTHORIZATIONS_ALL);
        getGraph().flush();
        FetchHints fetchHints = new FetchHintsBuilder()
                .setPropertyNamesToInclude("propBmeta")
                .build();
        Vertex vertex = getGraph().getVertex("v1", fetchHints, AUTHORIZATIONS_A);
        ExistingElementMutation<Vertex> m = vertex.prepareMutation();
        m.setPropertyMetadata("propBmeta", "meta1", stringValue("value2"), VISIBILITY_A);
        m.save(AUTHORIZATIONS_ALL);
        getGraph().flush();
        vertex = getGraph().getVertex("v1", AUTHORIZATIONS_A);
        assertEquals(stringValue("value2"), vertex.getProperty("propBmeta").getMetadata().getEntry("meta1").getValue());
        fetchHints = new FetchHintsBuilder()
                .setPropertyNamesToInclude("propBmeta")
                .build();
        vertex = getGraph().getVertex("v1", fetchHints, AUTHORIZATIONS_A);
        m = vertex.prepareMutation();
        Metadata newMetadata = Metadata.create();
        newMetadata.add("meta1", stringValue("value3"), VISIBILITY_A);
        m.setProperty("propBmeta", stringValue("propBmeta"), newMetadata, VISIBILITY_A);
        m.save(AUTHORIZATIONS_ALL);
        getGraph().flush();
        vertex = getGraph().getVertex("v1", AUTHORIZATIONS_A);
        assertEquals(stringValue("value3"), vertex.getProperty("propBmeta").getMetadata().getEntry("meta1").getValue());
    }

    @Test
    public void testChangePropertyMetadata() {
        Metadata prop1Metadata = Metadata.create();
        prop1Metadata.add("prop1_key1", stringValue("valueOld"), VISIBILITY_EMPTY);

        getGraph().prepareVertex("v1", VISIBILITY_A, CONCEPT_TYPE_THING)
                .setProperty("prop1", stringValue("value1"), prop1Metadata, VISIBILITY_EMPTY)
                .setProperty("prop2", stringValue("value2"), null, VISIBILITY_EMPTY)
                .save(AUTHORIZATIONS_A_AND_B);
        getGraph().flush();

        Vertex v1 = getGraph().getVertex("v1", FetchHints.ALL, AUTHORIZATIONS_A);
        v1.prepareMutation()
                .setPropertyMetadata("prop1", "prop1_key1", stringValue("valueNew"), VISIBILITY_EMPTY)
                .save(AUTHORIZATIONS_A_AND_B);
        getGraph().flush();
        assertEquals(stringValue("valueNew"), v1.getProperty("prop1").getMetadata().getEntry("prop1_key1", VISIBILITY_EMPTY).getValue());

        v1 = getGraph().getVertex("v1", FetchHints.ALL, AUTHORIZATIONS_A);
        assertEquals(stringValue("valueNew"), v1.getProperty("prop1").getMetadata().getEntry("prop1_key1", VISIBILITY_EMPTY).getValue());

        v1 = getGraph().getVertex("v1", FetchHints.ALL, AUTHORIZATIONS_A);
        v1.prepareMutation()
                .setPropertyMetadata("prop2", "prop2_key1", stringValue("valueNew"), VISIBILITY_EMPTY)
                .save(AUTHORIZATIONS_A_AND_B);
        getGraph().flush();
        assertEquals(stringValue("valueNew"), v1.getProperty("prop2").getMetadata().getEntry("prop2_key1", VISIBILITY_EMPTY).getValue());

        v1 = getGraph().getVertex("v1", FetchHints.ALL, AUTHORIZATIONS_A);
        assertEquals(stringValue("valueNew"), v1.getProperty("prop2").getMetadata().getEntry("prop2_key1", VISIBILITY_EMPTY).getValue());
    }
}
