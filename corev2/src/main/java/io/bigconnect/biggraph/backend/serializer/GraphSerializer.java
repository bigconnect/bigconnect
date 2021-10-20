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

import io.bigconnect.biggraph.BigGraph;
import io.bigconnect.biggraph.backend.id.Id;
import io.bigconnect.biggraph.backend.query.ConditionQuery;
import io.bigconnect.biggraph.backend.query.Query;
import io.bigconnect.biggraph.backend.store.BackendEntry;
import io.bigconnect.biggraph.structure.*;
import io.bigconnect.biggraph.type.BigType;

public interface GraphSerializer {

    public BackendEntry writeVertex(BigVertex vertex);
    public BackendEntry writeOlapVertex(BigVertex vertex);
    public BackendEntry writeVertexProperty(BigVertexProperty<?> prop);
    public BigVertex readVertex(BigGraph graph, BackendEntry entry);

    public BackendEntry writeEdge(BigEdge edge);
    public BackendEntry writeEdgeProperty(BigEdgeProperty<?> prop);
    public BigEdge readEdge(BigGraph graph, BackendEntry entry);

    public BackendEntry writeIndex(BigIndex index);
    public BigIndex readIndex(BigGraph graph, ConditionQuery query,
                              BackendEntry entry);

    public BackendEntry writeId(BigType type, Id id);
    public Query writeQuery(Query query);
}
