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

package io.bigconnect.biggraph.backend.cache;

import io.bigconnect.biggraph.BigGraphParams;
import io.bigconnect.biggraph.backend.cache.CachedBackendStore.QueryId;
import io.bigconnect.biggraph.backend.id.Id;
import io.bigconnect.biggraph.backend.query.IdQuery;
import io.bigconnect.biggraph.backend.query.Query;
import io.bigconnect.biggraph.backend.query.QueryResults;
import io.bigconnect.biggraph.backend.store.BackendMutation;
import io.bigconnect.biggraph.backend.store.BackendStore;
import io.bigconnect.biggraph.backend.store.ram.RamTable;
import io.bigconnect.biggraph.backend.tx.GraphTransaction;
import io.bigconnect.biggraph.config.CoreOptions;
import io.bigconnect.biggraph.config.BigConfig;
import io.bigconnect.biggraph.event.EventHub;
import io.bigconnect.biggraph.event.EventListener;
import io.bigconnect.biggraph.exception.NotSupportException;
import io.bigconnect.biggraph.iterator.ExtendableIterator;
import io.bigconnect.biggraph.iterator.ListIterator;
import io.bigconnect.biggraph.perf.PerfUtil.Watched;
import io.bigconnect.biggraph.schema.IndexLabel;
import io.bigconnect.biggraph.structure.BigEdge;
import io.bigconnect.biggraph.structure.BigVertex;
import io.bigconnect.biggraph.type.BigType;
import io.bigconnect.biggraph.util.E;
import io.bigconnect.biggraph.util.Events;
import com.google.common.collect.ImmutableSet;

import java.lang.reflect.Array;
import java.util.*;

public final class CachedGraphTransaction extends GraphTransaction {

    private final static int MAX_CACHE_PROPS_PER_VERTEX = 10000;
    private final static int MAX_CACHE_EDGES_PER_QUERY = 100;
    private final static float DEFAULT_LEVEL_RATIO = 0.001f;
    private final static long AVG_VERTEX_ENTRY_SIZE = 40L;
    private final static long AVG_EDGE_ENTRY_SIZE = 100L;

    private final Cache<Id, Object> verticesCache;
    private final Cache<Id, Object> edgesCache;

    private EventListener storeEventListener;
    private EventListener cacheEventListener;

    public CachedGraphTransaction(BigGraphParams graph, BackendStore store) {
        super(graph, store);

        BigConfig conf = graph.configuration();

        String type = conf.get(CoreOptions.VERTEX_CACHE_TYPE);
        long capacity = conf.get(CoreOptions.VERTEX_CACHE_CAPACITY);
        int expire = conf.get(CoreOptions.VERTEX_CACHE_EXPIRE);
        this.verticesCache = this.cache("vertex", type, capacity,
                                        AVG_VERTEX_ENTRY_SIZE, expire);

        type = conf.get(CoreOptions.EDGE_CACHE_TYPE);
        capacity = conf.get(CoreOptions.EDGE_CACHE_CAPACITY);
        expire = conf.get(CoreOptions.EDGE_CACHE_EXPIRE);
        this.edgesCache = this.cache("edge", type, capacity,
                                     AVG_EDGE_ENTRY_SIZE, expire);

        this.listenChanges();
    }

    @Override
    public void close() {
        try {
            super.close();
        } finally {
            this.unlistenChanges();
        }
    }

    private Cache<Id, Object> cache(String prefix, String type, long capacity,
                                    long entrySize, long expire) {
        String name = prefix + "-" + this.params().name();
        Cache<Id, Object> cache;
        switch (type) {
            case "l1":
                cache = CacheManager.instance().cache(name, capacity);
                break;
            case "l2":
                long heapCapacity = (long) (DEFAULT_LEVEL_RATIO * capacity);
                cache = CacheManager.instance().levelCache(super.graph(),
                                                           name, heapCapacity,
                                                           capacity, entrySize);
                break;
            default:
                throw new NotSupportException("cache type '%s'", type);
        }
        // Convert the unit from seconds to milliseconds
        cache.expire(expire * 1000L);
        // Enable metrics for graph cache by default
        cache.enableMetrics(true);
        return cache;
    }

    private void listenChanges() {
        // Listen store event: "store.init", "store.clear", ...
        Set<String> storeEvents = ImmutableSet.of(Events.STORE_INIT,
                                                  Events.STORE_CLEAR,
                                                  Events.STORE_TRUNCATE);
        this.storeEventListener = event -> {
            if (storeEvents.contains(event.name())) {
                LOG.debug("Graph {} clear graph cache on event '{}'",
                          this.graph(), event.name());
                this.clearCache(null, true);
                return true;
            }
            return false;
        };
        this.store().provider().listen(this.storeEventListener);

        // Listen cache event: "cache"(invalid cache item)
        this.cacheEventListener = event -> {
            LOG.debug("Graph {} received graph cache event: {}",
                      this.graph(), event);
            Object[] args = event.args();
            E.checkArgument(args.length > 0 && args[0] instanceof String,
                            "Expect event action argument");
            if (Cache.ACTION_INVALID.equals(args[0])) {
                event.checkArgs(String.class, BigType.class, Object.class);
                BigType type = (BigType) args[1];
                if (type.isVertex()) {
                    // Invalidate vertex cache
                    Object arg2 = args[2];
                    if (arg2 instanceof Id) {
                        Id id = (Id) arg2;
                        this.verticesCache.invalidate(id);
                    } else if (arg2 != null && arg2.getClass().isArray()) {
                        int size = Array.getLength(arg2);
                        for (int i = 0; i < size; i++) {
                            Object id = Array.get(arg2, i);
                            E.checkArgument(id instanceof Id,
                                            "Expect instance of Id in array, " +
                                            "but got '%s'", id.getClass());
                            this.verticesCache.invalidate((Id) id);
                        }
                    } else {
                        E.checkArgument(false,
                                        "Expect Id or Id[], but got: %s",
                                        arg2);
                    }
                } else if (type.isEdge()) {
                    /*
                     * Invalidate edge cache via clear instead of invalidate
                     * because of the cacheKey is QueryId not EdgeId
                     */
                    // this.edgesCache.invalidate(id);
                    this.edgesCache.clear();
                }
                return true;
            } else if (Cache.ACTION_CLEAR.equals(args[0])) {
                event.checkArgs(String.class, BigType.class);
                BigType type = (BigType) args[1];
                this.clearCache(type, false);
                return true;
            }
            return false;
        };
        EventHub graphEventHub = this.params().graphEventHub();
        if (!graphEventHub.containsListener(Events.CACHE)) {
            graphEventHub.listen(Events.CACHE, this.cacheEventListener);
        }
    }

    private void unlistenChanges() {
        // Unlisten store event
        this.store().provider().unlisten(this.storeEventListener);

        // Unlisten cache event
        EventHub graphEventHub = this.params().graphEventHub();
        graphEventHub.unlisten(Events.CACHE, this.cacheEventListener);
    }

    private void notifyChanges(String action, BigType type, Id[] ids) {
        EventHub graphEventHub = this.params().graphEventHub();
        graphEventHub.notify(Events.CACHE, action, type, ids);
    }

    private void clearCache(BigType type, boolean notify) {
        if (type == null || type == BigType.VERTEX) {
            this.verticesCache.clear();
        }
        if (type == null || type == BigType.EDGE) {
            this.edgesCache.clear();
        }

        if (notify) {
            this.notifyChanges(Cache.ACTION_CLEARED, null, null);
        }
    }

    @Override
    protected final Iterator<BigVertex> queryVerticesFromBackend(Query query) {
        if (!query.ids().isEmpty() && query.conditions().isEmpty()) {
            return this.queryVerticesByIds((IdQuery) query);
        } else {
            return super.queryVerticesFromBackend(query);
        }
    }

    private Iterator<BigVertex> queryVerticesByIds(IdQuery query) {
        IdQuery newQuery = new IdQuery(BigType.VERTEX, query);
        List<BigVertex> vertices = new ArrayList<>();
        for (Id vertexId : query.ids()) {
            BigVertex vertex = (BigVertex) this.verticesCache.get(vertexId);
            if (vertex == null) {
                newQuery.query(vertexId);
            } else if (vertex.expired()) {
                newQuery.query(vertexId);
                this.verticesCache.invalidate(vertexId);
            } else {
                vertices.add(vertex);
            }
        }

        // Join results from cache and backend
        ExtendableIterator<BigVertex> results = new ExtendableIterator<>();
        if (!vertices.isEmpty()) {
            results.extend(vertices.iterator());
        } else {
            // Just use the origin query if find none from the cache
            newQuery = query;
        }

        if (!newQuery.empty()) {
            Iterator<BigVertex> rs = super.queryVerticesFromBackend(newQuery);
            // Generally there are not too much data with id query
            ListIterator<BigVertex> listIterator = QueryResults.toList(rs);
            for (BigVertex vertex : listIterator.list()) {
                if (vertex.sizeOfSubProperties() > MAX_CACHE_PROPS_PER_VERTEX) {
                    // Skip large vertex
                    continue;
                }
                this.verticesCache.update(vertex.id(), vertex);
            }
            results.extend(listIterator);
        }

        return results;
    }

    @Override
    @Watched
    protected final Iterator<BigEdge> queryEdgesFromBackend(Query query) {
        RamTable ramtable = this.params().ramtable();
        if (ramtable != null && ramtable.matched(query)) {
            return ramtable.query(query);
        }

        if (query.empty() || query.paging() || query.bigCapacity()) {
            // Query all edges or query edges in paging, don't cache it
            return super.queryEdgesFromBackend(query);
        }

        Id cacheKey = new QueryId(query);
        Object value = this.edgesCache.get(cacheKey);
        @SuppressWarnings("unchecked")
        Collection<BigEdge> edges = (Collection<BigEdge>) value;
        if (value != null) {
            for (BigEdge edge : edges) {
                if (edge.expired()) {
                    this.edgesCache.invalidate(cacheKey);
                    value = null;
                }
            }
        }

        if (value != null) {
            // Not cached or the cache expired
            return edges.iterator();
        }

        Iterator<BigEdge> rs = super.queryEdgesFromBackend(query);

        /*
         * Iterator can't be cached, caching list instead
         * there may be super node and too many edges in a query,
         * try fetch a few of the head results and determine whether to cache.
         */
        final int tryMax = 1 + MAX_CACHE_EDGES_PER_QUERY;
        assert tryMax > MAX_CACHE_EDGES_PER_QUERY;
        edges = new ArrayList<>(tryMax);
        for (int i = 0; rs.hasNext() && i < tryMax; i++) {
            edges.add(rs.next());
        }

        if (edges.size() == 0) {
            this.edgesCache.update(cacheKey, Collections.emptyList());
        } else if (edges.size() <= MAX_CACHE_EDGES_PER_QUERY) {
            this.edgesCache.update(cacheKey, edges);
        }

        return new ExtendableIterator<>(edges.iterator(), rs);
    }

    @Override
    protected final void commitMutation2Backend(BackendMutation... mutations) {
        // Collect changes before commit
        Collection<BigVertex> updates = this.verticesInTxUpdated();
        Collection<BigVertex> deletions = this.verticesInTxRemoved();
        Id[] vertexIds = new Id[updates.size() + deletions.size()];
        int vertexOffset = 0;

        int edgesInTxSize = this.edgesInTxSize();

        try {
            super.commitMutation2Backend(mutations);
            // Update vertex cache
            for (BigVertex vertex : updates) {
                vertexIds[vertexOffset++] = vertex.id();
                if (vertex.sizeOfSubProperties() > MAX_CACHE_PROPS_PER_VERTEX) {
                    // Skip large vertex
                    this.verticesCache.invalidate(vertex.id());
                    continue;
                }
                this.verticesCache.updateIfPresent(vertex.id(), vertex);
            }
        } finally {
            // Update removed vertex in cache whatever success or fail
            for (BigVertex vertex : deletions) {
                vertexIds[vertexOffset++] = vertex.id();
                this.verticesCache.invalidate(vertex.id());
            }
            if (vertexOffset > 0) {
                this.notifyChanges(Cache.ACTION_INVALIDED,
                                   BigType.VERTEX, vertexIds);
            }

            // Update edge cache if any edges change
            if (edgesInTxSize > 0) {
                // TODO: Use a more precise strategy to update the edge cache
                this.edgesCache.clear();
                this.notifyChanges(Cache.ACTION_CLEARED, BigType.EDGE, null);
            }
        }
    }

    @Override
    public final void removeIndex(IndexLabel indexLabel) {
        try {
            super.removeIndex(indexLabel);
        } finally {
            // Update edge cache if needed (any edge-index is deleted)
            if (indexLabel.baseType() == BigType.EDGE_LABEL) {
                // TODO: Use a more precise strategy to update the edge cache
                this.edgesCache.clear();
                this.notifyChanges(Cache.ACTION_CLEARED, BigType.EDGE, null);
            }
        }
    }
}
