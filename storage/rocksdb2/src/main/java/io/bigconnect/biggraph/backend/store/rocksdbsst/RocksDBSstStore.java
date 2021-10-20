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

package io.bigconnect.biggraph.backend.store.rocksdbsst;

import io.bigconnect.biggraph.backend.id.Id;
import io.bigconnect.biggraph.backend.store.BackendStoreProvider;
import io.bigconnect.biggraph.backend.store.rocksdb.RocksDBSessions;
import io.bigconnect.biggraph.backend.store.rocksdb.RocksDBStore;
import io.bigconnect.biggraph.backend.store.rocksdb.RocksDBTables;
import io.bigconnect.biggraph.config.BigConfig;
import io.bigconnect.biggraph.type.BigType;
import org.rocksdb.RocksDBException;

import java.util.List;

public abstract class RocksDBSstStore extends RocksDBStore {

    public RocksDBSstStore(final BackendStoreProvider provider,
                           final String database, final String store) {
        super(provider, database, store);
    }

    @Override
    protected RocksDBSessions openSessionPool(BigConfig config,
                                              String dataPath, String walPath,
                                              List<String> tableNames)
                                              throws RocksDBException {
        if (tableNames == null) {
            return new RocksDBSstSessions(config, this.database(),
                                          this.store(), dataPath);
        } else {
            return new RocksDBSstSessions(config, this.database(), this.store(),
                                          dataPath, tableNames);
        }
    }

    /***************************** Store defines *****************************/

    public static class RocksDBSstGraphStore extends RocksDBSstStore {

        public RocksDBSstGraphStore(BackendStoreProvider provider,
                                    String database, String store) {
            super(provider, database, store);

            registerTableManager(BigType.VERTEX,
                                 new RocksDBTables.Vertex(database));

            registerTableManager(BigType.EDGE_OUT,
                                 RocksDBTables.Edge.out(database));
            registerTableManager(BigType.EDGE_IN,
                                 RocksDBTables.Edge.in(database));

            registerTableManager(BigType.SECONDARY_INDEX,
                                 new RocksDBTables.SecondaryIndex(database));
            registerTableManager(BigType.VERTEX_LABEL_INDEX,
                                 new RocksDBTables.VertexLabelIndex(database));
            registerTableManager(BigType.EDGE_LABEL_INDEX,
                                 new RocksDBTables.EdgeLabelIndex(database));
            registerTableManager(BigType.RANGE_INT_INDEX,
                                 new RocksDBTables.RangeIntIndex(database));
            registerTableManager(BigType.RANGE_FLOAT_INDEX,
                                 new RocksDBTables.RangeFloatIndex(database));
            registerTableManager(BigType.RANGE_LONG_INDEX,
                                 new RocksDBTables.RangeLongIndex(database));
            registerTableManager(BigType.RANGE_DOUBLE_INDEX,
                                 new RocksDBTables.RangeDoubleIndex(database));
            registerTableManager(BigType.SEARCH_INDEX,
                                 new RocksDBTables.SearchIndex(database));
            registerTableManager(BigType.SHARD_INDEX,
                                 new RocksDBTables.ShardIndex(database));
            registerTableManager(BigType.UNIQUE_INDEX,
                                 new RocksDBTables.UniqueIndex(database));
        }

        @Override
        public boolean isSchemaStore() {
            return false;
        }

        @Override
        public Id nextId(BigType type) {
            throw new UnsupportedOperationException(
                      "RocksDBSstGraphStore.nextId()");
        }

        @Override
        public void increaseCounter(BigType type, long increment) {
            throw new UnsupportedOperationException(
                      "RocksDBSstGraphStore.increaseCounter()");
        }

        @Override
        public long getCounter(BigType type) {
            throw new UnsupportedOperationException(
                      "RocksDBSstGraphStore.getCounter()");
        }
    }
}
