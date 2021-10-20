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

package io.bigconnect.biggraph.backend.store.memory;

import io.bigconnect.biggraph.backend.BackendException;
import io.bigconnect.biggraph.backend.LocalCounter;
import io.bigconnect.biggraph.backend.id.Id;
import io.bigconnect.biggraph.backend.query.Query;
import io.bigconnect.biggraph.backend.serializer.TextBackendEntry;
import io.bigconnect.biggraph.backend.store.*;
import io.bigconnect.biggraph.config.BigConfig;
import io.bigconnect.biggraph.type.BigType;
import io.bigconnect.biggraph.util.Log;
import org.slf4j.Logger;

import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

/**
 * NOTE:
 * InMemoryDBStore support:
 * 1.query by id (include query edges by id)
 * 2.query by condition (include query edges by condition)
 * 3.remove by id
 * 4.range query
 * 5.append/subtract index data(element-id) and vertex-property
 * 6.query edge by edge-label
 * InMemoryDBStore not support currently:
 * 1.remove by id + condition
 * 2.append/subtract edge-property
 */
public abstract class InMemoryDBStore
                extends AbstractBackendStore<BackendSession> {

    private static final Logger LOG = Log.logger(InMemoryDBStore.class);

    private final BackendStoreProvider provider;

    private final String store;
    private final String database;

    private final Map<BigType, InMemoryDBTable> tables;

    public InMemoryDBStore(final BackendStoreProvider provider,
                           final String database, final String store) {
        this.provider = provider;
        this.database = database;
        this.store = store;
        this.tables = new HashMap<>();

        this.registerMetaHandlers();
        LOG.debug("Store loaded: {}", store);
    }

    private void registerMetaHandlers() {
        this.registerMetaHandler("metrics", (session, meta, args) -> {
            InMemoryMetrics metrics = new InMemoryMetrics();
            return metrics.metrics();
        });
    }

    protected void registerTableManager(BigType type, InMemoryDBTable table) {
        this.tables.put(type, table);
    }

    protected Collection<InMemoryDBTable> tables() {
        return this.tables.values();
    }

    @Override
    protected final InMemoryDBTable table(BigType type) {
        assert type != null;
        InMemoryDBTable table = this.tables.get(type);
        if (table == null) {
            throw new BackendException("Unsupported table type: %s", type);
        }
        return table;
    }

    @Override
    protected BackendSession session(BigType type) {
        return null;
    }

    @Override
    public Iterator<BackendEntry> query(Query query) {
        InMemoryDBTable table = this.table(InMemoryDBTable.tableType(query));
        Iterator<BackendEntry> rs = table.query(null, query);
        LOG.debug("[store {}] has result({}) for query: {}",
                  this.store, rs.hasNext(), query);
        return rs;
    }

    @Override
    public Number queryNumber(Query query) {
        InMemoryDBTable table = this.table(InMemoryDBTable.tableType(query));
        Number result = table.queryNumber(null, query);
        LOG.debug("[store {}] get result({}) for number query: {}",
                  this.store, result, query);
        return result;
    }

    @Override
    public void mutate(BackendMutation mutation) {
        for (Iterator<BackendAction> it = mutation.mutation(); it.hasNext();) {
            this.mutate(it.next());
        }
    }

    protected void mutate(BackendAction item) {
        BackendEntry e = item.entry();
        assert e instanceof TextBackendEntry;
        TextBackendEntry entry = (TextBackendEntry) e;
        InMemoryDBTable table = this.table(entry.type());
        switch (item.action()) {
            case INSERT:
                LOG.debug("[store {}] add entry: {}", this.store, entry);
                table.insert(null, entry);
                break;
            case DELETE:
                LOG.debug("[store {}] remove id: {}", this.store, entry.id());
                table.delete(null, entry);
                break;
            case APPEND:
                LOG.debug("[store {}] append entry: {}", this.store, entry);
                table.append(null, entry);
                break;
            case ELIMINATE:
                LOG.debug("[store {}] eliminate entry: {}", this.store, entry);
                table.eliminate(null, entry);
                break;
            default:
                throw new BackendException("Unsupported mutate type: %s",
                                           item.action());
        }
    }

    @Override
    public String store() {
        return this.store;
    }

    @Override
    public String database() {
        return this.database;
    }

    @Override
    public BackendStoreProvider provider() {
        return this.provider;
    }

    @Override
    public void open(BigConfig config) {
        LOG.debug("Store opened: {}", this.store);
    }

    @Override
    public void close() throws BackendException {
        LOG.debug("Store closed: {}", this.store);
    }

    @Override
    public void init() {
        for (InMemoryDBTable table : this.tables()) {
            table.init(null);
        }

        LOG.debug("Store initialized: {}", this.store);
    }

    @Override
    public void clear(boolean clearSpace) {
        for (InMemoryDBTable table : this.tables()) {
            table.clear(null);
        }

        LOG.debug("Store cleared: {}", this.store);
    }

    @Override
    public void truncate() {
        for (InMemoryDBTable table : this.tables()) {
            table.clear(null);
        }

        LOG.debug("Store truncated: {}", this.store);
    }

    @Override
    public void beginTx() {
        // pass
    }

    @Override
    public void commitTx() {
        // pass
    }

    @Override
    public void rollbackTx() {
        throw new UnsupportedOperationException(
                  "Unsupported rollback operation by InMemoryDBStore");
    }

    @Override
    public BackendFeatures features() {
        return FEATURES;
    }

    @Override
    public boolean opened() {
        return true;
    }

    @Override
    public boolean initialized() {
        return true;
    }

    /***************************** Store defines *****************************/

    public static class InMemorySchemaStore extends InMemoryDBStore {

        private final LocalCounter counter = new LocalCounter();

        public InMemorySchemaStore(BackendStoreProvider provider,
                                   String database, String store) {
            super(provider, database, store);

            registerTableManager(BigType.VERTEX_LABEL,
                                 new InMemoryDBTable(BigType.VERTEX_LABEL));
            registerTableManager(BigType.EDGE_LABEL,
                                 new InMemoryDBTable(BigType.EDGE_LABEL));
            registerTableManager(BigType.PROPERTY_KEY,
                                 new InMemoryDBTable(BigType.PROPERTY_KEY));
            registerTableManager(BigType.INDEX_LABEL,
                                 new InMemoryDBTable(BigType.INDEX_LABEL));
            registerTableManager(BigType.SECONDARY_INDEX,
                                 new InMemoryDBTables.SecondaryIndex());
        }

        @Override
        public Id nextId(BigType type) {
            return this.counter.nextId(type);
        }

        @Override
        public void increaseCounter(BigType type, long increment) {
            this.counter.increaseCounter(type, increment);
        }

        @Override
        public long getCounter(BigType type) {
            return this.counter.getCounter(type);
        }

        @Override
        public void clear(boolean clearSpace) {
            this.counter.reset();
            super.clear(clearSpace);
        }

        @Override
        public void truncate() {
            this.counter.reset();
            super.truncate();
        }

        @Override
        public boolean isSchemaStore() {
            return true;
        }
    }

    public static class InMemoryGraphStore extends InMemoryDBStore {

        public InMemoryGraphStore(BackendStoreProvider provider,
                                  String database, String store) {
            super(provider, database, store);

            registerTableManager(BigType.VERTEX,
                                 new InMemoryDBTables.Vertex());
            registerTableManager(BigType.EDGE_OUT,
                                 new InMemoryDBTables.Edge(BigType.EDGE_OUT));
            registerTableManager(BigType.EDGE_IN,
                                 new InMemoryDBTables.Edge(BigType.EDGE_IN));
            registerTableManager(BigType.SECONDARY_INDEX,
                                 new InMemoryDBTables.SecondaryIndex());
            registerTableManager(BigType.RANGE_INT_INDEX,
                                 InMemoryDBTables.RangeIndex.rangeInt());
            registerTableManager(BigType.RANGE_FLOAT_INDEX,
                                 InMemoryDBTables.RangeIndex.rangeFloat());
            registerTableManager(BigType.RANGE_LONG_INDEX,
                                 InMemoryDBTables.RangeIndex.rangeLong());
            registerTableManager(BigType.RANGE_DOUBLE_INDEX,
                                 InMemoryDBTables.RangeIndex.rangeDouble());
            registerTableManager(BigType.SEARCH_INDEX,
                                 new InMemoryDBTables.SearchIndex());
            registerTableManager(BigType.SHARD_INDEX,
                                 new InMemoryDBTables.ShardIndex());
            registerTableManager(BigType.UNIQUE_INDEX,
                                 new InMemoryDBTables.UniqueIndex());
        }

        @Override
        public boolean isSchemaStore() {
            return false;
        }

        @Override
        public Id nextId(BigType type) {
            throw new UnsupportedOperationException(
                      "InMemoryGraphStore.nextId()");
        }

        @Override
        public void increaseCounter(BigType type, long num) {
            throw new UnsupportedOperationException(
                      "InMemoryGraphStore.increaseCounter()");
        }

        @Override
        public long getCounter(BigType type) {
            throw new UnsupportedOperationException(
                      "InMemoryGraphStore.getCounter()");
        }
    }

    /**
     * InMemoryDBStore features
     */
    private static final BackendFeatures FEATURES = new BackendFeatures() {

        @Override
        public boolean supportsPersistence() {
            return false;
        }

        @Override
        public boolean supportsSharedStorage() {
            return false;
        }

        @Override
        public boolean supportsScanToken() {
            return false;
        }

        @Override
        public boolean supportsScanKeyPrefix() {
            return false;
        }

        @Override
        public boolean supportsScanKeyRange() {
            return false;
        }

        @Override
        public boolean supportsQuerySchemaByName() {
            // Traversal all data in memory
            return true;
        }

        @Override
        public boolean supportsQueryByLabel() {
            // Traversal all data in memory
            return true;
        }

        @Override
        public boolean supportsQueryWithRangeCondition() {
            return true;
        }

        @Override
        public boolean supportsQueryWithOrderBy() {
            return false;
        }

        @Override
        public boolean supportsQueryWithContains() {
            // NOTE: hasValue tests will skip
            return false;
        }

        @Override
        public boolean supportsQueryWithContainsKey() {
            // NOTE: hasKey tests will skip
            return false;
        }

        @Override
        public boolean supportsQueryByPage() {
            return false;
        }

        @Override
        public boolean supportsQuerySortByInputIds() {
            return true;
        }

        @Override
        public boolean supportsDeleteEdgeByLabel() {
            return false;
        }

        @Override
        public boolean supportsUpdateVertexProperty() {
            return false;
        }

        @Override
        public boolean supportsMergeVertexProperty() {
            return false;
        }

        @Override
        public boolean supportsUpdateEdgeProperty() {
            return false;
        }

        @Override
        public boolean supportsTransaction() {
            return false;
        }

        @Override
        public boolean supportsNumberType() {
            return false;
        }

        @Override
        public boolean supportsAggregateProperty() {
            return false;
        }

        @Override
        public boolean supportsTtl() {
            return false;
        }

        @Override
        public boolean supportsOlapProperties() {
            return false;
        }
    };
}
