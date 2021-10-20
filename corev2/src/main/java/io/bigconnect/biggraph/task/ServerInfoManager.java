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

package io.bigconnect.biggraph.task;

import io.bigconnect.biggraph.BigGraphException;
import io.bigconnect.biggraph.BigGraph;
import io.bigconnect.biggraph.BigGraphParams;
import io.bigconnect.biggraph.backend.id.Id;
import io.bigconnect.biggraph.backend.page.PageInfo;
import io.bigconnect.biggraph.backend.query.Condition;
import io.bigconnect.biggraph.backend.query.ConditionQuery;
import io.bigconnect.biggraph.backend.query.QueryResults;
import io.bigconnect.biggraph.backend.tx.GraphTransaction;
import io.bigconnect.biggraph.event.EventListener;
import io.bigconnect.biggraph.exception.ConnectionException;
import io.bigconnect.biggraph.iterator.ListIterator;
import io.bigconnect.biggraph.iterator.MapperIterator;
import io.bigconnect.biggraph.schema.PropertyKey;
import io.bigconnect.biggraph.schema.VertexLabel;
import io.bigconnect.biggraph.structure.BigVertex;
import io.bigconnect.biggraph.type.BigType;
import io.bigconnect.biggraph.type.define.BigKeys;
import io.bigconnect.biggraph.type.define.NodeRole;
import io.bigconnect.biggraph.util.DateUtil;
import io.bigconnect.biggraph.util.E;
import io.bigconnect.biggraph.util.Events;
import io.bigconnect.biggraph.util.Log;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.slf4j.Logger;

import java.util.Collection;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;

import static io.bigconnect.biggraph.backend.query.Query.NO_LIMIT;

public class ServerInfoManager {

    private static final Logger LOG = Log.logger(ServerInfoManager.class);

    public static final long MAX_SERVERS = 100000L;
    public static final long PAGE_SIZE = 10L;

    private final BigGraphParams graph;
    private final ExecutorService dbExecutor;
    private final EventListener eventListener;

    private Id selfServerId;
    private NodeRole selfServerRole;

    private volatile boolean onlySingleNode;
    private volatile boolean closed;

    public ServerInfoManager(BigGraphParams graph,
                             ExecutorService dbExecutor) {
        E.checkNotNull(graph, "graph");
        E.checkNotNull(dbExecutor, "db executor");

        this.graph = graph;
        this.dbExecutor = dbExecutor;

        this.eventListener = this.listenChanges();

        this.selfServerId = null;
        this.selfServerRole = NodeRole.MASTER;

        this.onlySingleNode = false;
        this.closed = false;
    }

    private EventListener listenChanges() {
        // Listen store event: "store.inited"
        Set<String> storeEvents = ImmutableSet.of(Events.STORE_INITED);
        EventListener eventListener = event -> {
            // Ensure server info schema create after system info initialized
            if (storeEvents.contains(event.name())) {
                try {
                    this.initSchemaIfNeeded();
                } finally {
                    this.graph.closeTx();
                }
                return true;
            }
            return false;
        };
        this.graph.loadSystemStore().provider().listen(eventListener);
        return eventListener;
    }

    private void unlistenChanges() {
        this.graph.loadSystemStore().provider().unlisten(this.eventListener);
    }

    public boolean close() {
        this.closed = true;
        this.unlistenChanges();
        if (!this.dbExecutor.isShutdown()) {
            this.removeSelfServerInfo();
            this.call(() -> {
                try {
                    this.tx().close();
                } catch (ConnectionException ignored) {
                    // ConnectionException means no connection established
                }
                this.graph.closeTx();
                return null;
            });
        }
        return true;
    }

    public synchronized void initServerInfo(Id server, NodeRole role) {
        E.checkArgument(server != null && role != null,
                        "The server id or role can't be null");
        this.selfServerId = server;
        this.selfServerRole = role;

        BigServerInfo existed = this.serverInfo(server);
        E.checkArgument(existed == null || !existed.alive(),
                        "The server with name '%s' already in cluster",
                        server);
        if (role.master()) {
            String page = this.supportsPaging() ? PageInfo.PAGE_NONE : null;
            do {
                Iterator<BigServerInfo> servers = this.serverInfos(PAGE_SIZE,
                                                                    page);
                while (servers.hasNext()) {
                    existed = servers.next();
                    E.checkArgument(!existed.role().master() ||
                                    !existed.alive(),
                                    "Already existed master '%s' in current " +
                                    "cluster", existed.id());
                }
                if (page != null) {
                    page = PageInfo.pageInfo(servers);
                }
            } while (page != null);
        }

        BigServerInfo serverInfo = new BigServerInfo(server, role);
        serverInfo.maxLoad(this.calcMaxLoad());
        this.save(serverInfo);

        LOG.info("Init server info: {}", serverInfo);
    }

    public Id selfServerId() {
        return this.selfServerId;
    }

    public NodeRole selfServerRole() {
        return this.selfServerRole;
    }

    public boolean master() {
        return this.selfServerRole != null && this.selfServerRole.master();
    }

    public boolean onlySingleNode() {
        // Only has one master node
        return this.onlySingleNode;
    }

    public void heartbeat() {
        BigServerInfo serverInfo = this.selfServerInfo();
        if (serverInfo == null) {
            return;
        }
        serverInfo.updateTime(DateUtil.now());
        this.save(serverInfo);
    }

    public synchronized void decreaseLoad(int load) {
        assert load > 0 : load;
        BigServerInfo serverInfo = this.selfServerInfo();
        serverInfo.increaseLoad(-load);
        this.save(serverInfo);
    }

    public int calcMaxLoad() {
        // TODO: calc max load based on CPU and Memory resources
        return 10000;
    }

    protected boolean graphReady() {
        return !this.closed && this.graph.started() && this.graph.initialized();
    }

    protected synchronized BigServerInfo pickWorkerNode(
                                          Collection<BigServerInfo> servers,
                                          BigTask<?> task) {
        BigServerInfo master = null;
        BigServerInfo serverWithMinLoad = null;
        int minLoad = Integer.MAX_VALUE;
        boolean hasWorkerNode = false;
        long now = DateUtil.now().getTime();

        // Iterate servers to find suitable one
        for (BigServerInfo server : servers) {
            if (!server.alive()) {
                continue;
            }

            if (server.role().master()) {
                master = server;
                continue;
            }

            hasWorkerNode = true;
            if (!server.suitableFor(task, now)) {
                continue;
            }
            if (server.load() < minLoad) {
                minLoad = server.load();
                serverWithMinLoad = server;
            }
        }

        this.onlySingleNode = !hasWorkerNode;

        // Only schedule to master if there is no workers and master is suitable
        if (!hasWorkerNode) {
            if (master != null && master.suitableFor(task, now)) {
                serverWithMinLoad = master;
            }
        }

        return serverWithMinLoad;
    }

    private void initSchemaIfNeeded() {
        BigServerInfo.schema(this.graph).initSchemaIfNeeded();
    }

    private GraphTransaction tx() {
        assert Thread.currentThread().getName().contains("server-info-db-worker");
        return this.graph.systemTransaction();
    }

    private Id save(BigServerInfo server) {
        return this.call(() -> {
            // Construct vertex from server info
            BigServerInfo.Schema schema = BigServerInfo.schema(this.graph);
            if (!schema.existVertexLabel(BigServerInfo.P.SERVER)) {
                throw new BigGraphException("Schema is missing for %s '%s'",
                                        BigServerInfo.P.SERVER, server);
            }
            BigVertex vertex = this.tx().constructVertex(false,
                                                          server.asArray());
            // Add or update server info in backend store
            vertex = this.tx().addVertex(vertex);
            return vertex.id();
        });
    }

    private int save(Collection<BigServerInfo> servers) {
        return this.call(() -> {
            if (servers.isEmpty()) {
                return servers.size();
            }
            BigServerInfo.Schema schema = BigServerInfo.schema(this.graph);
            if (!schema.existVertexLabel(BigServerInfo.P.SERVER)) {
                throw new BigGraphException("Schema is missing for %s",
                                        BigServerInfo.P.SERVER);
            }
            // Save server info in batch
            GraphTransaction tx = this.tx();
            int updated = 0;
            for (BigServerInfo server : servers) {
                if (!server.updated()) {
                    continue;
                }
                BigVertex vertex = tx.constructVertex(false, server.asArray());
                tx.addVertex(vertex);
                updated++;
            }
            // NOTE: actually it is auto-commit, to be improved
            tx.commitOrRollback();

            return updated;
        });
    }

    private <V> V call(Callable<V> callable) {
        assert !Thread.currentThread().getName().startsWith(
               "server-info-db-worker") : "can't call by itself";
        try {
            // Pass context for db thread
            callable = new TaskManager.ContextCallable<>(callable);
            // Ensure all db operations are executed in dbExecutor thread(s)
            return this.dbExecutor.submit(callable).get();
        } catch (Throwable e) {
            throw new BigGraphException("Failed to update/query server info: %s",
                                    e, e.toString());
        }
    }

    private BigServerInfo selfServerInfo() {
        return this.serverInfo(this.selfServerId);
    }

    private BigServerInfo serverInfo(Id server) {
        return this.call(() -> {
            Iterator<Vertex> vertices = this.tx().queryVertices(server);
            Vertex vertex = QueryResults.one(vertices);
            if (vertex == null) {
                return null;
            }
            return BigServerInfo.fromVertex(vertex);
        });
    }

    private BigServerInfo removeSelfServerInfo() {
        if (this.graph.initialized()) {
            return this.removeServerInfo(this.selfServerId);
        }
        return null;
    }

    private BigServerInfo removeServerInfo(Id server) {
        if (server == null) {
            return null;
        }
        LOG.info("Remove server info: {}", server);
        return this.call(() -> {
            Iterator<Vertex> vertices = this.tx().queryVertices(server);
            Vertex vertex = QueryResults.one(vertices);
            if (vertex == null) {
                return null;
            }
            this.tx().removeVertex((BigVertex) vertex);
            return BigServerInfo.fromVertex(vertex);
        });
    }

    protected void updateServerInfos(Collection<BigServerInfo> serverInfos) {
        this.save(serverInfos);
    }

    protected Collection<BigServerInfo> allServerInfos() {
        Iterator<BigServerInfo> infos = this.serverInfos(NO_LIMIT, null);
        try (ListIterator<BigServerInfo> iter = new ListIterator<>(
                                                 MAX_SERVERS, infos)) {
            return iter.list();
        } catch (Exception e) {
            throw new BigGraphException("Failed to close server info iterator", e);
        }
    }

    protected Iterator<BigServerInfo> serverInfos(String page) {
        return this.serverInfos(ImmutableMap.of(), PAGE_SIZE, page);
    }

    protected Iterator<BigServerInfo> serverInfos(long limit, String page) {
        return this.serverInfos(ImmutableMap.of(), limit, page);
    }

    private Iterator<BigServerInfo> serverInfos(Map<String, Object> conditions,
                                                long limit, String page) {
        return this.call(() -> {
            ConditionQuery query = new ConditionQuery(BigType.VERTEX);
            if (page != null) {
                query.page(page);
            }

            BigGraph graph = this.graph.graph();
            VertexLabel vl = graph.vertexLabel(BigServerInfo.P.SERVER);
            query.eq(BigKeys.LABEL, vl.id());
            for (Map.Entry<String, Object> entry : conditions.entrySet()) {
                PropertyKey pk = graph.propertyKey(entry.getKey());
                query.query(Condition.eq(pk.id(), entry.getValue()));
            }
            query.showHidden(true);
            if (limit != NO_LIMIT) {
                query.limit(limit);
            }
            Iterator<Vertex> vertices = this.tx().queryVertices(query);
            Iterator<BigServerInfo> servers =
                    new MapperIterator<>(vertices, BigServerInfo::fromVertex);
            // Convert iterator to list to avoid across thread tx accessed
            return QueryResults.toList(servers);
        });
    }

    private boolean supportsPaging() {
        return this.graph.graph().backendStoreFeatures().supportsQueryByPage();
    }
}
