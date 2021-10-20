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

package io.bigconnect.biggraph.job.schema;

import io.bigconnect.biggraph.BigGraphException;
import io.bigconnect.biggraph.BigGraphParams;
import io.bigconnect.biggraph.backend.id.Id;
import io.bigconnect.biggraph.backend.tx.GraphTransaction;
import io.bigconnect.biggraph.backend.tx.SchemaTransaction;
import io.bigconnect.biggraph.schema.EdgeLabel;
import io.bigconnect.biggraph.schema.VertexLabel;
import io.bigconnect.biggraph.type.define.SchemaStatus;
import io.bigconnect.biggraph.util.LockUtil;
import com.google.common.collect.ImmutableSet;

import java.util.List;
import java.util.Set;

public class VertexLabelRemoveCallable extends SchemaCallable {

    @Override
    public String type() {
        return REMOVE_SCHEMA;
    }

    @Override
    public Object execute() {
        removeVertexLabel(this.params(), this.schemaId());
        return null;
    }

    private static void removeVertexLabel(BigGraphParams graph, Id id) {
        GraphTransaction graphTx = graph.graphTransaction();
        SchemaTransaction schemaTx = graph.schemaTransaction();
        VertexLabel vertexLabel = schemaTx.getVertexLabel(id);
        // If the vertex label does not exist, return directly
        if (vertexLabel == null) {
            return;
        }

        List<EdgeLabel> edgeLabels = schemaTx.getEdgeLabels();
        for (EdgeLabel edgeLabel : edgeLabels) {
            if (edgeLabel.linkWithLabel(id)) {
                throw new BigGraphException(
                          "Not allowed to remove vertex label '%s' " +
                          "because the edge label '%s' still link with it",
                          vertexLabel.name(), edgeLabel.name());
            }
        }

        /*
         * Copy index label ids because removeIndexLabel will mutate
         * vertexLabel.indexLabels()
         */
        Set<Id> indexLabelIds = ImmutableSet.copyOf(vertexLabel.indexLabels());
        LockUtil.Locks locks = new LockUtil.Locks(graph.name());
        try {
            locks.lockWrites(LockUtil.VERTEX_LABEL_DELETE, id);
            schemaTx.updateSchemaStatus(vertexLabel, SchemaStatus.DELETING);
            try {
                for (Id ilId : indexLabelIds) {
                    IndexLabelRemoveCallable.removeIndexLabel(graph, ilId);
                }
                // TODO: use event to replace direct call
                // Deleting a vertex will automatically deletes the held edge
                graphTx.removeVertices(vertexLabel);
                removeSchema(schemaTx, vertexLabel);
                /*
                 * Should commit changes to backend store before release
                 * delete lock
                 */
                graph.graph().tx().commit();
            } catch (Throwable e) {
                schemaTx.updateSchemaStatus(vertexLabel, SchemaStatus.UNDELETED);
                throw e;
            }
        } finally {
            locks.unlock();
        }
    }
}
