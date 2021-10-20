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

package io.bigconnect.biggraph.backend.tx;

import io.bigconnect.biggraph.BigGraphParams;
import io.bigconnect.biggraph.backend.query.ConditionQuery;
import io.bigconnect.biggraph.backend.query.IdQuery;
import io.bigconnect.biggraph.backend.query.Query;
import io.bigconnect.biggraph.backend.query.QueryResults;
import io.bigconnect.biggraph.backend.store.BackendEntry;
import io.bigconnect.biggraph.backend.store.BackendStore;
import io.bigconnect.biggraph.perf.PerfUtil.Watched;
import io.bigconnect.biggraph.schema.IndexLabel;
import io.bigconnect.biggraph.schema.SchemaElement;
import io.bigconnect.biggraph.structure.BigIndex;
import io.bigconnect.biggraph.type.BigType;
import io.bigconnect.biggraph.type.define.BigKeys;
import io.bigconnect.biggraph.util.E;
import org.apache.tinkerpop.gremlin.structure.util.CloseableIterator;

import java.util.Iterator;

public class SchemaIndexTransaction extends AbstractTransaction {

    public SchemaIndexTransaction(BigGraphParams graph, BackendStore store) {
        super(graph, store);
    }

    @Watched(prefix = "index")
    public void updateNameIndex(SchemaElement element, boolean removed) {
        if (!this.needIndexForName()) {
            return;
        }

        IndexLabel indexLabel = IndexLabel.label(element.type());
        // Update name index if backend store not supports name-query
        BigIndex index = new BigIndex(this.graph(), indexLabel);
        index.fieldValues(element.name());
        index.elementIds(element.id());

        if (removed) {
            this.doEliminate(this.serializer.writeIndex(index));
        } else {
            this.doAppend(this.serializer.writeIndex(index));
        }
    }

    private boolean needIndexForName() {
        return !this.store().features().supportsQuerySchemaByName();
    }

    @Watched(prefix = "index")
    @Override
    public QueryResults<BackendEntry> query(Query query) {
        if (query instanceof ConditionQuery) {
            ConditionQuery q = (ConditionQuery) query;
            if (q.allSysprop() && q.conditions().size() == 1 &&
                q.containsCondition(BigKeys.NAME)) {
                return this.queryByName(q);
            }
        }
        return super.query(query);
    }

    @Watched(prefix = "index")
    private QueryResults<BackendEntry> queryByName(ConditionQuery query) {
        if (!this.needIndexForName()) {
            return super.query(query);
        }
        IndexLabel il = IndexLabel.label(query.resultType());
        String name = (String) query.condition(BigKeys.NAME);
        E.checkState(name != null, "The name in condition can't be null " +
                     "when querying schema by name");

        ConditionQuery indexQuery;
        indexQuery = new ConditionQuery(BigType.SECONDARY_INDEX, query);
        indexQuery.eq(BigKeys.FIELD_VALUES, name);
        indexQuery.eq(BigKeys.INDEX_LABEL_ID, il.id());

        IdQuery idQuery = new IdQuery(query.resultType(), query);
        Iterator<BackendEntry> entries = super.query(indexQuery).iterator();
        try {
            while (entries.hasNext()) {
                BigIndex index = this.serializer.readIndex(graph(), indexQuery,
                                                            entries.next());
                idQuery.query(index.elementIds());
                Query.checkForceCapacity(idQuery.ids().size());
            }
        } finally {
            CloseableIterator.closeIterator(entries);
        }

        if (idQuery.ids().isEmpty()) {
            return QueryResults.empty();
        }

        assert idQuery.ids().size() == 1 : idQuery.ids();
        if (idQuery.ids().size() > 1) {
            LOG.warn("Multiple ids are found with same name '{}': {}",
                     name, idQuery.ids());
        }
        return super.query(idQuery);
    }
}
