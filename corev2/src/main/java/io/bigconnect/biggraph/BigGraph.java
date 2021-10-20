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

package io.bigconnect.biggraph;

import io.bigconnect.biggraph.auth.AuthManager;
import io.bigconnect.biggraph.backend.id.Id;
import io.bigconnect.biggraph.backend.query.Query;
import io.bigconnect.biggraph.backend.store.BackendFeatures;
import io.bigconnect.biggraph.backend.store.BackendStoreSystemInfo;
import io.bigconnect.biggraph.backend.store.raft.RaftGroupManager;
import io.bigconnect.biggraph.config.TypedOption;
import io.bigconnect.biggraph.rpc.RpcServiceConfig4Client;
import io.bigconnect.biggraph.rpc.RpcServiceConfig4Server;
import io.bigconnect.biggraph.schema.*;
import io.bigconnect.biggraph.structure.BigFeatures;
import io.bigconnect.biggraph.task.TaskScheduler;
import io.bigconnect.biggraph.traversal.optimize.BigCountStepStrategy;
import io.bigconnect.biggraph.traversal.optimize.BigGraphStepStrategy;
import io.bigconnect.biggraph.traversal.optimize.BigVertexStepStrategy;
import io.bigconnect.biggraph.type.BigType;
import io.bigconnect.biggraph.type.define.GraphMode;
import io.bigconnect.biggraph.type.define.GraphReadMode;
import io.bigconnect.biggraph.type.define.NodeRole;
import org.apache.tinkerpop.gremlin.process.traversal.TraversalStrategies;
import org.apache.tinkerpop.gremlin.structure.*;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;

/**
 * Graph interface for Gremlin operations
 */
public interface BigGraph extends Graph {

    BigGraph biggraph();

    SchemaManager schema();

    Id getNextId(BigType type);

    Id addPropertyKey(PropertyKey key);
    Id removePropertyKey(Id key);
    Id clearPropertyKey(PropertyKey propertyKey);
    Collection<PropertyKey> propertyKeys();
    PropertyKey propertyKey(String key);
    PropertyKey propertyKey(Id key);
    boolean existsPropertyKey(String key);

    void addVertexLabel(VertexLabel vertexLabel);
    Id removeVertexLabel(Id label);
    Collection<VertexLabel> vertexLabels();
    VertexLabel vertexLabel(String label);
    VertexLabel vertexLabel(Id label);
    VertexLabel vertexLabelOrNone(Id id);
    boolean existsVertexLabel(String label);
    boolean existsLinkLabel(Id vertexLabel);

    void addEdgeLabel(EdgeLabel edgeLabel);
    Id removeEdgeLabel(Id label);
    Collection<EdgeLabel> edgeLabels();
    EdgeLabel edgeLabel(String label);
    EdgeLabel edgeLabel(Id label);
    EdgeLabel edgeLabelOrNone(Id label);
    boolean existsEdgeLabel(String label);

    void addIndexLabel(SchemaLabel schemaLabel, IndexLabel indexLabel);
    Id removeIndexLabel(Id label);
    Id rebuildIndex(SchemaElement schema);
    Collection<IndexLabel> indexLabels();
    IndexLabel indexLabel(String label);
    IndexLabel indexLabel(Id id);
    boolean existsIndexLabel(String label);

    @Override
    Vertex addVertex(Object... keyValues);
    void removeVertex(Vertex vertex);
    void removeVertex(String label, Object id);
    <V> void addVertexProperty(VertexProperty<V> property);
    <V> void removeVertexProperty(VertexProperty<V> property);

    Edge addEdge(Edge edge);
    void canAddEdge(Edge edge);
    void removeEdge(Edge edge);
    void removeEdge(String label, Object id);
    <V> void addEdgeProperty(Property<V> property);
    <V> void removeEdgeProperty(Property<V> property);

    Vertex vertex(Object object);
    @Override
    Iterator<Vertex> vertices(Object... objects);
    Iterator<Vertex> vertices(Query query);
    Iterator<Vertex> adjacentVertex(Object id);
    boolean checkAdjacentVertexExist();

    Edge edge(Object object);
    @Override
    Iterator<Edge> edges(Object... objects);
    Iterator<Edge> edges(Query query);
    Iterator<Vertex> adjacentVertices(Iterator<Edge> edges) ;
    Iterator<Edge> adjacentEdges(Id vertexId);

    Number queryNumber(Query query);

    String name();
    String backend();
    String backendVersion();
    BackendStoreSystemInfo backendStoreSystemInfo();
    BackendFeatures backendStoreFeatures();

    GraphMode mode();
    void mode(GraphMode mode);

    GraphReadMode readMode();
    void readMode(GraphReadMode readMode);

    void waitStarted();
    void serverStarted(Id serverId, NodeRole serverRole);
    boolean started();
    boolean closed();

    <T> T metadata(BigType type, String meta, Object... args);

    void initBackend();
    void clearBackend();
    void truncateBackend();

    void createSnapshot();
    void resumeSnapshot();

    @Override
    BigFeatures features();

    AuthManager authManager();
    void switchAuthManager(AuthManager authManager);
    TaskScheduler taskScheduler();
    RaftGroupManager raftGroupManager(String group);

    void proxy(BigGraph graph);

    boolean sameAs(BigGraph graph);

    long now();

    <K, V> V option(TypedOption<K, V> option);

    void registerRpcServices(RpcServiceConfig4Server serverConfig,
                             RpcServiceConfig4Client clientConfig);

    default List<String> mapPkId2Name(Collection<Id> ids) {
        List<String> names = new ArrayList<>(ids.size());
        for (Id id : ids) {
            SchemaElement schema = this.propertyKey(id);
            names.add(schema.name());
        }
        return names;
    }

    default List<String> mapVlId2Name(Collection<Id> ids) {
        List<String> names = new ArrayList<>(ids.size());
        for (Id id : ids) {
            SchemaElement schema = this.vertexLabel(id);
            names.add(schema.name());
        }
        return names;
    }

    default List<String> mapElId2Name(Collection<Id> ids) {
        List<String> names = new ArrayList<>(ids.size());
        for (Id id : ids) {
            SchemaElement schema = this.edgeLabel(id);
            names.add(schema.name());
        }
        return names;
    }

    default List<String> mapIlId2Name(Collection<Id> ids) {
        List<String> names = new ArrayList<>(ids.size());
        for (Id id : ids) {
            SchemaElement schema = this.indexLabel(id);
            names.add(schema.name());
        }
        return names;
    }

    default List<Id> mapPkName2Id(Collection<String> pkeys) {
        List<Id> ids = new ArrayList<>(pkeys.size());
        for (String pkey : pkeys) {
            PropertyKey propertyKey = this.propertyKey(pkey);
            ids.add(propertyKey.id());
        }
        return ids;
    }

    default Id[] mapElName2Id(String[] edgeLabels) {
        Id[] ids = new Id[edgeLabels.length];
        for (int i = 0; i < edgeLabels.length; i++) {
            EdgeLabel edgeLabel = this.edgeLabel(edgeLabels[i]);
            ids[i] = edgeLabel.id();
        }
        return ids;
    }

    default Id[] mapVlName2Id(String[] vertexLabels) {
        Id[] ids = new Id[vertexLabels.length];
        for (int i = 0; i < vertexLabels.length; i++) {
            VertexLabel vertexLabel = this.vertexLabel(vertexLabels[i]);
            ids[i] = vertexLabel.id();
        }
        return ids;
    }

    static void registerTraversalStrategies(Class<?> clazz) {
        TraversalStrategies strategies = null;
        strategies = TraversalStrategies.GlobalCache
                                        .getStrategies(Graph.class)
                                        .clone();
        strategies.addStrategies(BigVertexStepStrategy.instance(),
                                 BigGraphStepStrategy.instance(),
                                 BigCountStepStrategy.instance());
        TraversalStrategies.GlobalCache.registerStrategies(clazz, strategies);
    }
}
