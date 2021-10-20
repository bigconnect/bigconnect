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

package io.bigconnect.biggraph.backend.serializer;

import io.bigconnect.biggraph.backend.BackendException;
import io.bigconnect.biggraph.backend.id.Id;
import io.bigconnect.biggraph.backend.query.ConditionQuery;
import io.bigconnect.biggraph.backend.query.IdQuery;
import io.bigconnect.biggraph.backend.query.Query;
import io.bigconnect.biggraph.backend.store.BackendEntry;
import io.bigconnect.biggraph.type.BigType;

public abstract class AbstractSerializer
                implements GraphSerializer, SchemaSerializer {

    protected BackendEntry convertEntry(BackendEntry entry) {
        return entry;
    }

    protected abstract BackendEntry newBackendEntry(BigType type, Id id);

    protected abstract Id writeQueryId(BigType type, Id id);

    protected abstract Query writeQueryEdgeCondition(Query query);

    protected abstract Query writeQueryCondition(Query query);

    @Override
    public Query writeQuery(Query query) {
        BigType type = query.resultType();

        // Serialize edge condition query (TODO: add VEQ(for EOUT/EIN))
        if (type.isEdge() && !query.conditions().isEmpty()) {
            if (!query.ids().isEmpty()) {
                throw new BackendException("Not supported query edge by id " +
                                           "and by condition at the same time");
            }

            Query result = this.writeQueryEdgeCondition(query);
            if (result != null) {
                return result;
            }
        }

        // Serialize id in query
        if (query instanceof IdQuery && !query.ids().isEmpty()) {
            IdQuery result = (IdQuery) query.copy();
            result.resetIds();
            for (Id id : query.ids()) {
                result.query(this.writeQueryId(type, id));
            }
            query = result;
        }

        // Serialize condition(key/value) in query
        if (query instanceof ConditionQuery && !query.conditions().isEmpty()) {
            query = this.writeQueryCondition(query);
        }

        return query;
    }
}
