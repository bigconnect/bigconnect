/*
 * Copyright 2021 BigConnect Authors
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

package io.bigconnect.biggraph.schema;

import io.bigconnect.biggraph.BigGraph;
import io.bigconnect.biggraph.backend.tx.SchemaTransaction;
import io.bigconnect.biggraph.exception.NotFoundException;
import io.bigconnect.biggraph.schema.builder.EdgeLabelBuilder;
import io.bigconnect.biggraph.schema.builder.IndexLabelBuilder;
import io.bigconnect.biggraph.schema.builder.PropertyKeyBuilder;
import io.bigconnect.biggraph.schema.builder.VertexLabelBuilder;
import io.bigconnect.biggraph.type.BigType;
import io.bigconnect.biggraph.util.E;
import org.apache.tinkerpop.gremlin.structure.Graph;

import java.util.List;
import java.util.stream.Collectors;

public class SchemaManager {

    private final SchemaTransaction transaction;
    private BigGraph graph;

    public SchemaManager(SchemaTransaction transaction, BigGraph graph) {
        E.checkNotNull(transaction, "transaction");
        E.checkNotNull(graph, "graph");
        this.transaction = transaction;
        this.graph = graph;
    }

    public BigGraph proxy(BigGraph graph) {
        E.checkNotNull(graph, "graph");
        BigGraph old = this.graph;
        this.graph = graph;
        return old;
    }

    public PropertyKey.Builder propertyKey(String name) {
        return new PropertyKeyBuilder(this.transaction, this.graph, name);
    }

    public VertexLabel.Builder vertexLabel(String name) {
        return new VertexLabelBuilder(this.transaction, this.graph, name);
    }

    public EdgeLabel.Builder edgeLabel(String name) {
        return new EdgeLabelBuilder(this.transaction, this.graph, name);
    }

    public IndexLabel.Builder indexLabel(String name) {
        return new IndexLabelBuilder(this.transaction, this.graph, name);
    }

    public PropertyKey getPropertyKey(String name) {
        E.checkArgumentNotNull(name, "The name parameter can't be null");
        PropertyKey propertyKey = this.transaction.getPropertyKey(name);
        checkExists(BigType.PROPERTY_KEY, propertyKey, name);
        return propertyKey;
    }

    public VertexLabel getVertexLabel(String name) {
        E.checkArgumentNotNull(name, "The name parameter can't be null");
        VertexLabel vertexLabel = this.transaction.getVertexLabel(name);
        checkExists(BigType.VERTEX_LABEL, vertexLabel, name);
        return vertexLabel;
    }

    public EdgeLabel getEdgeLabel(String name) {
        E.checkArgumentNotNull(name, "The name parameter can't be null");
        EdgeLabel edgeLabel = this.transaction.getEdgeLabel(name);
        checkExists(BigType.EDGE_LABEL, edgeLabel, name);
        return edgeLabel;
    }

    public IndexLabel getIndexLabel(String name) {
        E.checkArgumentNotNull(name, "The name parameter can't be null");
        IndexLabel indexLabel = this.transaction.getIndexLabel(name);
        checkExists(BigType.INDEX_LABEL, indexLabel, name);
        return indexLabel;
    }

    public List<PropertyKey> getPropertyKeys() {
        return this.graph.propertyKeys().stream()
                   .filter(pk -> !Graph.Hidden.isHidden(pk.name()))
                   .collect(Collectors.toList());
    }

    public List<VertexLabel> getVertexLabels() {
        return this.graph.vertexLabels().stream()
                   .filter(vl -> !Graph.Hidden.isHidden(vl.name()))
                   .collect(Collectors.toList());
    }

    public List<EdgeLabel> getEdgeLabels() {
        return this.graph.edgeLabels().stream()
                   .filter(el -> !Graph.Hidden.isHidden(el.name()))
                   .collect(Collectors.toList());
    }

    public List<IndexLabel> getIndexLabels() {
        return this.graph.indexLabels().stream()
                   .filter(il -> !Graph.Hidden.isHidden(il.name()))
                   .collect(Collectors.toList());
    }

    public void copyFrom(SchemaManager schema) {
        for (PropertyKey pk : schema.getPropertyKeys()) {
            new PropertyKeyBuilder(this.transaction, this.graph, pk).create();
        }
        for (VertexLabel vl : schema.getVertexLabels()) {
            new VertexLabelBuilder(this.transaction, this.graph, vl).create();
        }
        for (EdgeLabel el : schema.getEdgeLabels()) {
            new EdgeLabelBuilder(this.transaction, this.graph, el).create();
        }
        for (IndexLabel il : schema.getIndexLabels()) {
            new IndexLabelBuilder(this.transaction, this.graph, il).create();
        }
    }

    private static void checkExists(BigType type, Object object, String name) {
        if (object == null) {
            throw new NotFoundException("%s with name '%s' does not exist",
                                        type.readableName(), name);
        }
    }
}
