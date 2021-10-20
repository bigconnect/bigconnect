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

package io.bigconnect.biggraph.core;

import io.bigconnect.biggraph.BigGraph;
import io.bigconnect.biggraph.BigGraphParams;
import io.bigconnect.biggraph.backend.store.BackendFeatures;
import io.bigconnect.biggraph.schema.SchemaManager;
import io.bigconnect.biggraph.testutil.Whitebox;
import io.bigconnect.biggraph.util.Log;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.junit.After;
import org.junit.Before;
import org.slf4j.Logger;

public class BaseCoreTest {

    protected static final Logger LOG = Log.logger(BaseCoreTest.class);

    protected static final int TX_BATCH = 100;

    public BigGraph graph() {
        return CoreTestSuite.graph();
    }

    @Before
    public void setup() {
        this.clearData();
        this.clearSchema();
    }

    @After
    public void teardown() throws Exception {
        // pass
    }

    protected void clearData() {
        BigGraph graph = graph();

        // Clear uncommitted data(maybe none)
        graph.tx().rollback();

        int count = 0;

        // Clear edge
        do {
            count = 0;
            for (Edge e : graph().traversal().E().limit(TX_BATCH).toList()) {
                count++;
                e.remove();
            }
            graph.tx().commit();
        } while (count == TX_BATCH);

        // Clear vertex
        do {
            count = 0;
            for (Vertex v : graph().traversal().V().limit(TX_BATCH).toList()) {
                count++;
                v.remove();
            }
            graph.tx().commit();
        } while (count == TX_BATCH);
    }

    private void clearSchema() {
        SchemaManager schema = graph().schema();

        schema.getIndexLabels().forEach(elem -> {
            schema.indexLabel(elem.name()).remove();
        });

        schema.getEdgeLabels().forEach(elem -> {
            schema.edgeLabel(elem.name()).remove();
        });

        schema.getVertexLabels().forEach(elem -> {
            schema.vertexLabel(elem.name()).remove();
        });

        schema.getPropertyKeys().forEach(elem -> {
            schema.propertyKey(elem.name()).remove();
        });
    }

    protected BackendFeatures storeFeatures() {
        return graph().backendStoreFeatures();
    }

    protected BigGraphParams params() {
        return Whitebox.getInternalState(graph(), "params");
    }
}
