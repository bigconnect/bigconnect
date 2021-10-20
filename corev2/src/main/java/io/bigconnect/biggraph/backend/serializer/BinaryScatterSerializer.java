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
import io.bigconnect.biggraph.backend.id.IdGenerator;
import io.bigconnect.biggraph.backend.store.BackendEntry;
import io.bigconnect.biggraph.backend.store.BackendEntry.BackendColumn;
import io.bigconnect.biggraph.schema.VertexLabel;
import io.bigconnect.biggraph.structure.BigProperty;
import io.bigconnect.biggraph.structure.BigVertex;
import io.bigconnect.biggraph.structure.BigVertexProperty;
import io.bigconnect.biggraph.type.define.BigKeys;

public class BinaryScatterSerializer extends BinarySerializer {

    public BinaryScatterSerializer() {
        super(true, true);
    }

    @Override
    public BackendEntry writeVertex(BigVertex vertex) {
        BinaryBackendEntry entry = newBackendEntry(vertex);

        if (vertex.removed()) {
            return entry;
        }

        // Write vertex label
        entry.column(this.formatLabel(vertex));

        // Write all properties of a Vertex
        for (BigProperty<?> prop : vertex.getProperties().values()) {
            entry.column(this.formatProperty(prop));
        }

        return entry;
    }

    @Override
    public BigVertex readVertex(BigGraph graph, BackendEntry bytesEntry) {
        if (bytesEntry == null) {
            return null;
        }
        BinaryBackendEntry entry = this.convertEntry(bytesEntry);

        // Parse label
        final byte[] VL = this.formatSyspropName(entry.id(), BigKeys.LABEL);
        BackendColumn vl = entry.column(VL);
        VertexLabel vertexLabel = VertexLabel.NONE;
        if (vl != null) {
            Id labelId = BytesBuffer.wrap(vl.value).readId();
            vertexLabel = graph.vertexLabelOrNone(labelId);
        }

        // Parse id
        Id id = entry.id().origin();
        BigVertex vertex = new BigVertex(graph, id, vertexLabel);

        // Parse all properties and edges of a Vertex
        for (BackendColumn col : entry.columns()) {
            this.parseColumn(col, vertex);
        }

        return vertex;
    }

    @Override
    public BackendEntry writeVertexProperty(BigVertexProperty<?> prop) {
        BinaryBackendEntry entry = newBackendEntry(prop.element());
        entry.column(this.formatProperty(prop));
        entry.subId(IdGenerator.of(prop.key()));
        return entry;
    }
}
