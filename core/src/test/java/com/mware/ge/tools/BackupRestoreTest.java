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
package com.mware.ge.tools;

import com.mware.ge.*;
import com.mware.ge.base.GraphTestSetup;
import com.mware.ge.id.LongIdGenerator;
import com.mware.ge.id.UUIDIdGenerator;
import com.mware.ge.inmemory.InMemoryGraph;
import com.mware.ge.values.storable.StreamingPropertyValue;
import com.mware.ge.search.DefaultSearchIndex;
import com.mware.ge.util.IterableUtils;
import com.mware.ge.util.LargeStringInputStream;
import com.mware.ge.values.storable.StringValue;
import com.mware.ge.values.storable.TextValue;
import org.apache.commons.io.IOUtils;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import java.io.*;
import java.util.HashMap;
import java.util.Map;

import static com.mware.core.model.schema.SchemaConstants.CONCEPT_TYPE_THING;
import static com.mware.ge.values.storable.Values.stringValue;
import static org.junit.Assert.*;

@RunWith(JUnit4.class)
public class BackupRestoreTest {
    public final Authorizations AUTHORIZATIONS_A;
    public final Authorizations AUTHORIZATIONS_B;
    public final Authorizations AUTHORIZATIONS_C;
    public final Authorizations AUTHORIZATIONS_A_AND_B;

    public BackupRestoreTest() {
        AUTHORIZATIONS_A = createAuthorizations("a");
        AUTHORIZATIONS_B = createAuthorizations("b");
        AUTHORIZATIONS_C = createAuthorizations("c");
        AUTHORIZATIONS_A_AND_B = createAuthorizations("a", "b");
    }

    private Authorizations createAuthorizations(String... auths) {
        return new Authorizations(auths);
    }

    protected Graph createGraph() {
        Map config = new HashMap();
        config.put("", InMemoryGraph.class.getName());
        config.put(GraphConfiguration.IDGENERATOR_PROP_PREFIX, LongIdGenerator.class.getName());
        config.put(GraphConfiguration.SEARCH_INDEX_PROP_PREFIX, DefaultSearchIndex.class.getName());
        return new GraphFactory().createGraph(config);
    }

    @Test
    public void testSaveAndLoad() throws IOException, ClassNotFoundException {
        Graph graph = createGraph();

        Metadata prop1Metadata = Metadata.create();
        prop1Metadata.add("metadata1", stringValue("metadata1Value"), GraphTestSetup.VISIBILITY_A);

        int largePropertyValueSize = 1000;
        String expectedLargeValue = IOUtils.toString(new LargeStringInputStream(largePropertyValueSize));
        StreamingPropertyValue largeDataValue = StreamingPropertyValue.create(new ByteArrayInputStream(expectedLargeValue.getBytes()), StringValue.class);

        Vertex v1 = graph.prepareVertex("v1", GraphTestSetup.VISIBILITY_A, CONCEPT_TYPE_THING)
                .addPropertyValue("id1a", "prop1", stringValue("value1a"), prop1Metadata, GraphTestSetup.VISIBILITY_A)
                .addPropertyValue("id1b", "prop1", stringValue("value1b"), GraphTestSetup.VISIBILITY_A)
                .addPropertyValue("id2", "prop2", stringValue("value2"), GraphTestSetup.VISIBILITY_B)
                .setProperty("largeData", largeDataValue, GraphTestSetup.VISIBILITY_A)
                .save(AUTHORIZATIONS_A_AND_B);
        Vertex v2 = graph.addVertex("v2", GraphTestSetup.VISIBILITY_A, AUTHORIZATIONS_A, CONCEPT_TYPE_THING);
        Vertex v3 = graph.addVertex("v3", GraphTestSetup.VISIBILITY_B, AUTHORIZATIONS_B, CONCEPT_TYPE_THING);
        graph.addEdge("e1to2", v1, v2, "label1", GraphTestSetup.VISIBILITY_A, AUTHORIZATIONS_A);
        graph.addEdge("e1to3", v1, v3, "label1", GraphTestSetup.VISIBILITY_B, AUTHORIZATIONS_B);

        File tmp = File.createTempFile(getClass().getName(), ".json");
        try (FileOutputStream out = new FileOutputStream(tmp)) {
            System.out.println("saving graph to: " + tmp);
            GraphBackup graphBackup = new GraphBackup();
            graphBackup.save(graph, out, AUTHORIZATIONS_A_AND_B);
        }

        try (FileInputStream in = new FileInputStream(tmp)) {
            Graph loadedGraph = createGraph();
            GraphRestore graphRestore = new GraphRestore();
            graphRestore.restore(loadedGraph, in, AUTHORIZATIONS_A_AND_B, 0);

            Assert.assertEquals(3, IterableUtils.count(loadedGraph.getVertices(AUTHORIZATIONS_A_AND_B)));
            Assert.assertEquals(2, IterableUtils.count(loadedGraph.getVertices(AUTHORIZATIONS_A)));
            Assert.assertEquals(1, IterableUtils.count(loadedGraph.getVertices(AUTHORIZATIONS_B)));
            Assert.assertEquals(2, IterableUtils.count(loadedGraph.getEdges(AUTHORIZATIONS_A_AND_B)));
            Assert.assertEquals(1, IterableUtils.count(loadedGraph.getEdges(AUTHORIZATIONS_A)));
            Assert.assertEquals(1, IterableUtils.count(loadedGraph.getEdges(AUTHORIZATIONS_B)));

            v1 = loadedGraph.getVertex("v1", AUTHORIZATIONS_A_AND_B);
            Assert.assertEquals(2, IterableUtils.count(v1.getEdges(Direction.BOTH, AUTHORIZATIONS_A_AND_B)));
            Iterable<Property> properties = v1.getProperties();
            boolean prop1_id1a_found = false;
            boolean prop1_id1b_found = false;
            for (Property property : properties) {
                if (property.getName().equals("prop1")) {
                    if (property.getKey().equals("id1a")) {
                        prop1_id1a_found = true;
                        assertEquals(stringValue("value1a"), property.getValue());
                    }
                    if (property.getKey().equals("id1b")) {
                        prop1_id1b_found = true;
                        assertEquals(stringValue("value1b"), property.getValue());
                    }
                }
            }
            assertTrue("prop1[id1a] not found", prop1_id1a_found);
            assertTrue("prop1[id1b] not found", prop1_id1b_found);
            assertEquals(stringValue("value2"), v1.getPropertyValue("prop2", 0));
            StreamingPropertyValue spv = (StreamingPropertyValue) v1.getPropertyValue("largeData", 0);
            assertNotNull("largeData property not found", spv);
            assertEquals(StringValue.class, spv.getValueType());
            assertEquals(expectedLargeValue, IOUtils.toString(spv.getInputStream()));

            v2 = loadedGraph.getVertex("v2", AUTHORIZATIONS_A_AND_B);
            Assert.assertEquals(1, IterableUtils.count(v2.getEdges(Direction.BOTH, AUTHORIZATIONS_A_AND_B)));

            v3 = loadedGraph.getVertex("v3", AUTHORIZATIONS_A_AND_B);
            Assert.assertEquals(1, IterableUtils.count(v3.getEdges(Direction.BOTH, AUTHORIZATIONS_A_AND_B)));
        }

        tmp.delete();
    }
}
