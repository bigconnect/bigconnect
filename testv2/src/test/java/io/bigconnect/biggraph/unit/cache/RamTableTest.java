/*
 * Copyright 2017 HugeGraph Authors
 *
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership. The ASF
 * licenses this file to You under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations
 * under the License.
 */

package io.bigconnect.biggraph.unit.cache;

import io.bigconnect.biggraph.BigGraphException;
import io.bigconnect.biggraph.BigGraphFactory;
import io.bigconnect.biggraph.BigGraph;
import io.bigconnect.biggraph.backend.id.IdGenerator;
import io.bigconnect.biggraph.backend.store.ram.RamTable;
import io.bigconnect.biggraph.schema.EdgeLabel;
import io.bigconnect.biggraph.schema.SchemaManager;
import io.bigconnect.biggraph.schema.VertexLabel;
import io.bigconnect.biggraph.structure.BigEdge;
import io.bigconnect.biggraph.structure.BigVertex;
import io.bigconnect.biggraph.testutil.Assert;
import io.bigconnect.biggraph.type.define.Directions;
import io.bigconnect.biggraph.unit.FakeObjects;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.Iterator;

public class RamTableTest {

    // max value is 4 billion
    private static final int VERTEX_SIZE = 10000000;
    private static final int EDGE_SIZE = 20000000;

    private BigGraph graph;

    @Before
    public void setup() {
        this.graph = BigGraphFactory.open(FakeObjects.newConfig());
        SchemaManager schema = this.graph.schema();

        schema.propertyKey("p3").asText().create();

        schema.vertexLabel("vl1").useCustomizeNumberId().create();
        schema.vertexLabel("vl2").useCustomizeNumberId().create();
        schema.vertexLabel("vl3").useCustomizeStringId().create();

        schema.edgeLabel("el1")
              .sourceLabel("vl1")
              .targetLabel("vl1")
              .create();
        schema.edgeLabel("el2")
              .sourceLabel("vl2")
              .targetLabel("vl2")
              .create();
        schema.edgeLabel("el3")
              .sourceLabel("vl3")
              .targetLabel("vl3")
              .properties("p3")
              .multiTimes()
              .sortKeys("p3")
              .create();
    }

    @After
    public void teardown() throws Exception {
        this.graph.close();
    }

    private BigGraph graph() {
        return this.graph;
    }

    @Test
    public void testAddAndQuery() throws Exception {
        BigGraph graph = this.graph();
        int el1 = (int) graph.edgeLabel("el1").id().asLong();
        int el2 = (int) graph.edgeLabel("el2").id().asLong();

        RamTable table = new RamTable(graph, VERTEX_SIZE, EDGE_SIZE);
        long oldSize = table.edgesSize();
        // insert edges
        for (int i = 0; i < VERTEX_SIZE; i++) {
            table.addEdge(true, i, i, Directions.OUT, el1);
            Assert.assertEquals(oldSize + 2 * i + 1, table.edgesSize());

            table.addEdge(false, i, i + 1, Directions.IN, el2);
            Assert.assertEquals(oldSize + 2 * i + 2, table.edgesSize());
        }

        // query by BOTH
        for (int i = 0; i < VERTEX_SIZE; i++) {
            Iterator<BigEdge> edges = table.query(i, Directions.BOTH, 0);

            Assert.assertTrue(edges.hasNext());
            BigEdge edge1 = edges.next();
            Assert.assertEquals(i, edge1.id().ownerVertexId().asLong());
            Assert.assertEquals(i, edge1.id().otherVertexId().asLong());
            Assert.assertEquals(Directions.OUT, edge1.direction());
            Assert.assertEquals("el1", edge1.label());

            Assert.assertTrue(edges.hasNext());
            BigEdge edge2 = edges.next();
            Assert.assertEquals(i, edge2.id().ownerVertexId().asLong());
            Assert.assertEquals(i + 1L, edge2.id().otherVertexId().asLong());
            Assert.assertEquals(Directions.IN, edge2.direction());
            Assert.assertEquals("el2", edge2.label());

            Assert.assertFalse(edges.hasNext());
        }
        // query by OUT
        for (int i = 0; i < VERTEX_SIZE; i++) {
            Iterator<BigEdge> edges = table.query(i, Directions.OUT, el1);

            Assert.assertTrue(edges.hasNext());
            BigEdge edge1 = edges.next();
            Assert.assertEquals(i, edge1.id().ownerVertexId().asLong());
            Assert.assertEquals(i, edge1.id().otherVertexId().asLong());
            Assert.assertEquals(Directions.OUT, edge1.direction());
            Assert.assertEquals("el1", edge1.label());

            Assert.assertFalse(edges.hasNext());
        }
        // query by IN
        for (int i = 0; i < VERTEX_SIZE; i++) {
            Iterator<BigEdge> edges = table.query(i, Directions.IN, el2);

            Assert.assertTrue(edges.hasNext());
            BigEdge edge1 = edges.next();
            Assert.assertEquals(i, edge1.id().ownerVertexId().asLong());
            Assert.assertEquals(i + 1L, edge1.id().otherVertexId().asLong());
            Assert.assertEquals(Directions.IN, edge1.direction());
            Assert.assertEquals("el2", edge1.label());

            Assert.assertFalse(edges.hasNext());
        }

        // query by BOTH & label 1
        for (int i = 0; i < VERTEX_SIZE; i++) {
            Iterator<BigEdge> edges = table.query(i, Directions.BOTH, el1);

            Assert.assertTrue(edges.hasNext());
            BigEdge edge1 = edges.next();
            Assert.assertEquals(i, edge1.id().ownerVertexId().asLong());
            Assert.assertEquals(i, edge1.id().otherVertexId().asLong());
            Assert.assertEquals(Directions.OUT, edge1.direction());
            Assert.assertEquals("el1", edge1.label());

            Assert.assertFalse(edges.hasNext());
        }
        // query by BOTH & label 2
        for (int i = 0; i < VERTEX_SIZE; i++) {
            Iterator<BigEdge> edges = table.query(i, Directions.BOTH, el2);

            Assert.assertTrue(edges.hasNext());
            BigEdge edge1 = edges.next();
            Assert.assertEquals(i, edge1.id().ownerVertexId().asLong());
            Assert.assertEquals(i + 1L, edge1.id().otherVertexId().asLong());
            Assert.assertEquals(Directions.IN, edge1.direction());
            Assert.assertEquals("el2", edge1.label());

            Assert.assertFalse(edges.hasNext());
        }

        // query non-exist vertex
        Iterator<BigEdge> edges = table.query(VERTEX_SIZE, Directions.BOTH, 0);
        Assert.assertFalse(edges.hasNext());
    }

    @Test
    public void testAddAndQueryWithoutAdjEdges() throws Exception {
        BigGraph graph = this.graph();
        int el1 = (int) graph.edgeLabel("el1").id().asLong();
        int el2 = (int) graph.edgeLabel("el2").id().asLong();

        RamTable table = new RamTable(graph, VERTEX_SIZE, EDGE_SIZE);
        long oldSize = table.edgesSize();
        // insert edges
        for (int i = 0; i < VERTEX_SIZE; i++) {
            if (i % 3 != 0) {
                // don't insert edges for 2/3 vertices
                continue;
            }

            table.addEdge(true, i, i, Directions.OUT, el1);
            Assert.assertEquals(oldSize + i + 1, table.edgesSize());

            table.addEdge(false, i, i, Directions.OUT, el2);
            Assert.assertEquals(oldSize + i + 2, table.edgesSize());

            table.addEdge(false, i, i + 1, Directions.IN, el2);
            Assert.assertEquals(oldSize + i + 3, table.edgesSize());
        }

        // query by BOTH
        for (int i = 0; i < VERTEX_SIZE; i++) {
            Iterator<BigEdge> edges = table.query(i, Directions.BOTH, 0);

            if (i % 3 != 0) {
                Assert.assertFalse(edges.hasNext());
                continue;
            }

            Assert.assertTrue(edges.hasNext());
            BigEdge edge1 = edges.next();
            Assert.assertEquals(i, edge1.id().ownerVertexId().asLong());
            Assert.assertEquals(i, edge1.id().otherVertexId().asLong());
            Assert.assertEquals(Directions.OUT, edge1.direction());
            Assert.assertEquals("el1", edge1.label());

            Assert.assertTrue(edges.hasNext());
            BigEdge edge2 = edges.next();
            Assert.assertEquals(i, edge2.id().ownerVertexId().asLong());
            Assert.assertEquals(i, edge2.id().otherVertexId().asLong());
            Assert.assertEquals(Directions.OUT, edge2.direction());
            Assert.assertEquals("el2", edge2.label());

            Assert.assertTrue(edges.hasNext());
            BigEdge edge3 = edges.next();
            Assert.assertEquals(i, edge3.id().ownerVertexId().asLong());
            Assert.assertEquals(i + 1L, edge3.id().otherVertexId().asLong());
            Assert.assertEquals(Directions.IN, edge3.direction());
            Assert.assertEquals("el2", edge3.label());

            Assert.assertFalse(edges.hasNext());
        }
    }

    @Test
    public void testAddInvalidVertexOrEdge() {
        BigGraph graph = this.graph();
        VertexLabel vl3 = graph.vertexLabel("vl3");
        EdgeLabel el3 = graph.edgeLabel("el3");

        VertexLabel vl2 = graph.vertexLabel("vl2");
        EdgeLabel el2 = graph.edgeLabel("el2");

        RamTable table = new RamTable(graph, VERTEX_SIZE, EDGE_SIZE);

        BigVertex ownerVertex = new BigVertex(graph, IdGenerator.of(1), vl3);
        BigEdge edge1 = BigEdge.constructEdge(ownerVertex, true, el3, "marko",
                                               IdGenerator.of(2));
        Assert.assertThrows(BigGraphException.class, () -> {
            table.addEdge(true, edge1);
        }, e -> {
            Assert.assertContains("Only edge label without sortkey is " +
                                  "supported by ramtable, but got 'el3(id=3)'",
                                  e.getMessage());
        });

        BigVertex v1 = new BigVertex(graph, IdGenerator.of("s1"), vl2);
        BigEdge edge2 = BigEdge.constructEdge(v1, true, el2, "marko",
                                                IdGenerator.of("s2"));
        Assert.assertThrows(BigGraphException.class, () -> {
            table.addEdge(true, edge2);
        }, e -> {
            Assert.assertContains("Only number id is supported by ramtable, " +
                                  "but got string id 's1'", e.getMessage());
        });

        BigVertex v2 = new BigVertex(graph, IdGenerator.of(2), vl2);
        BigEdge edge3 = BigEdge.constructEdge(v2, true, el2, "marko",
                                                IdGenerator.of("s2"));
        Assert.assertThrows(BigGraphException.class, () -> {
            table.addEdge(true, edge3);
        }, e -> {
            Assert.assertContains("Only number id is supported by ramtable, " +
                                  "but got string id 's2'", e.getMessage());
        });
    }
}
