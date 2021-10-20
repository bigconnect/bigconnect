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

import io.bigconnect.biggraph.analyzer.Analyzer;
import io.bigconnect.biggraph.backend.serializer.AbstractSerializer;
import io.bigconnect.biggraph.backend.store.BackendFeatures;
import io.bigconnect.biggraph.backend.store.BackendStore;
import io.bigconnect.biggraph.backend.store.ram.RamTable;
import io.bigconnect.biggraph.backend.tx.GraphTransaction;
import io.bigconnect.biggraph.backend.tx.SchemaTransaction;
import io.bigconnect.biggraph.config.BigConfig;
import io.bigconnect.biggraph.event.EventHub;
import io.bigconnect.biggraph.task.ServerInfoManager;
import io.bigconnect.biggraph.type.define.GraphMode;
import io.bigconnect.biggraph.type.define.GraphReadMode;
import com.google.common.util.concurrent.RateLimiter;

/**
 * Graph inner Params interface
 */
public interface BigGraphParams {

    BigGraph graph();
    String name();
    GraphMode mode();
    GraphReadMode readMode();

    SchemaTransaction schemaTransaction();
    GraphTransaction systemTransaction();
    GraphTransaction graphTransaction();

    GraphTransaction openTransaction();
    void closeTx();

    boolean started();
    boolean closed();
    boolean initialized();
    BackendFeatures backendStoreFeatures();

    BackendStore loadSchemaStore();
    BackendStore loadGraphStore();
    BackendStore loadSystemStore();

    EventHub schemaEventHub();
    EventHub graphEventHub();
    EventHub indexEventHub();

    BigConfig configuration();

    ServerInfoManager serverManager();

    AbstractSerializer serializer();
    Analyzer analyzer();
    RateLimiter writeRateLimiter();
    RateLimiter readRateLimiter();
    RamTable ramtable();
}
