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

package io.bigconnect.biggraph.backend.store;

import io.bigconnect.biggraph.BigGraphException;
import io.bigconnect.biggraph.BigGraph;
import io.bigconnect.biggraph.backend.tx.SchemaTransaction;
import io.bigconnect.biggraph.schema.PropertyKey;
import io.bigconnect.biggraph.schema.SchemaElement;
import io.bigconnect.biggraph.type.BigType;
import io.bigconnect.biggraph.util.E;
import io.bigconnect.biggraph.util.Log;
import org.apache.tinkerpop.gremlin.structure.Graph.Hidden;
import org.slf4j.Logger;

import java.util.Map;

public class BackendStoreSystemInfo {

    private static final Logger LOG = Log.logger(BigGraph.class);

    private static final String PK_BACKEND_INFO = Hidden.hide("backend_info");

    private final SchemaTransaction schemaTx;

    public BackendStoreSystemInfo(SchemaTransaction schemaTx) {
        this.schemaTx = schemaTx;
    }

    public synchronized void init() {
        if (this.exists()) {
            return;
        }
        // Set schema counter to reserve primitive system id
        this.schemaTx.setNextIdLowest(BigType.SYS_SCHEMA,
                                      SchemaElement.MAX_PRIMITIVE_SYS_ID);

        BigGraph graph = this.schemaTx.graph();
        E.checkState(this.info() == null,
                     "Already exists backend info of graph '%s' in backend " +
                     "'%s'", graph.name(), graph.backend());
        // Use property key to store backend version
        String backendVersion = graph.backendVersion();
        PropertyKey backendInfo = graph.schema()
                                       .propertyKey(PK_BACKEND_INFO)
                                       .userdata("version", backendVersion)
                                       .build();
        this.schemaTx.addPropertyKey(backendInfo);
    }

    private Map<String, Object> info() {
        PropertyKey pkey;
        try {
            pkey = this.schemaTx.getPropertyKey(PK_BACKEND_INFO);
        } catch (IllegalStateException e) {
            String message = String.format(
                             "Should not exist schema with same name '%s'",
                             PK_BACKEND_INFO);
            if (message.equals(e.getMessage())) {
                BigGraph graph = this.schemaTx.graph();
                throw new BigGraphException("There exists multiple backend info " +
                                        "of graph '%s' in backend '%s'",
                                        graph.name(), graph.backend());
            }
            throw e;
        }
        return pkey != null ? pkey.userdata() : null;
    }

    public boolean exists() {
        if (!this.schemaTx.storeInitialized()) {
            return false;
        }
        return this.info() != null;
    }

    public boolean checkVersion() {
        Map<String, Object> info = this.info();
        E.checkState(info != null, "The backend version info doesn't exist");
        // Backend has been initialized
        BigGraph graph = this.schemaTx.graph();
        String driverVersion = graph.backendVersion();
        String backendVersion = (String) info.get("version");
        if (!driverVersion.equals(backendVersion)) {
            LOG.error("The backend driver version '{}' is inconsistent with " +
                      "the data version '{}' of backend store for graph '{}'",
                      driverVersion, backendVersion, graph.name());
            return false;
        }
        return true;
    }
}
