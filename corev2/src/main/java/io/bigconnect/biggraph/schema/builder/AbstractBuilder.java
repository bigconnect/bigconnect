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

package io.bigconnect.biggraph.schema.builder;

import io.bigconnect.biggraph.BigGraph;
import io.bigconnect.biggraph.backend.id.Id;
import io.bigconnect.biggraph.backend.id.IdGenerator;
import io.bigconnect.biggraph.backend.tx.SchemaTransaction;
import io.bigconnect.biggraph.exception.ExistedException;
import io.bigconnect.biggraph.schema.*;
import io.bigconnect.biggraph.type.BigType;
import io.bigconnect.biggraph.type.define.GraphMode;
import io.bigconnect.biggraph.type.define.SchemaStatus;
import io.bigconnect.biggraph.util.E;
import io.bigconnect.biggraph.util.LockUtil;

import java.util.Set;
import java.util.function.Function;

public abstract class AbstractBuilder {

    private final SchemaTransaction transaction;
    private final BigGraph graph;

    public AbstractBuilder(SchemaTransaction transaction, BigGraph graph) {
        E.checkNotNull(transaction, "transaction");
        E.checkNotNull(graph, "graph");
        this.transaction = transaction;
        this.graph = graph;
    }

    protected BigGraph graph() {
        return this.graph;
    }

    protected Id validOrGenerateId(BigType type, Id id, String name) {
        return this.transaction.validOrGenerateId(type, id, name);
    }

    protected void checkSchemaName(String name) {
        this.transaction.checkSchemaName(name);
    }

    protected Id rebuildIndex(IndexLabel indexLabel, Set<Id> dependencies) {
        return this.transaction.rebuildIndex(indexLabel, dependencies);
    }

    protected <V> V lockCheckAndCreateSchema(BigType type, String name,
                                             Function<String, V> callback) {
        String graph = this.transaction.graphName();
        LockUtil.Locks locks = new LockUtil.Locks(graph);
        try {
            locks.lockWrites(LockUtil.hugeType2Group(type),
                             IdGenerator.of(name));
            return callback.apply(name);
        } finally {
            locks.unlock();
        }
    }

    protected void updateSchemaStatus(SchemaElement element,
                                      SchemaStatus status) {
        this.transaction.updateSchemaStatus(element, status);
    }

    protected void checkSchemaIdIfRestoringMode(BigType type, Id id) {
        if (this.transaction.graphMode() == GraphMode.RESTORING) {
            E.checkArgument(id != null,
                            "Must provide schema id if in RESTORING mode");
            if (this.transaction.existsSchemaId(type, id)) {
                throw new ExistedException(type.readableName() + " id", id);
            }
        }
    }

    protected PropertyKey propertyKeyOrNull(String name) {
        return this.transaction.getPropertyKey(name);
    }

    protected VertexLabel vertexLabelOrNull(String name) {
        return this.transaction.getVertexLabel(name);
    }

    protected EdgeLabel edgeLabelOrNull(String name) {
        return this.transaction.getEdgeLabel(name);
    }

    protected IndexLabel indexLabelOrNull(String name) {
        return this.transaction.getIndexLabel(name);
    }
}
