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

package io.bigconnect.biggraph.traversal.algorithm;

import io.bigconnect.biggraph.BigGraph;
import io.bigconnect.biggraph.backend.id.Id;
import io.bigconnect.biggraph.structure.BigEdge;
import io.bigconnect.biggraph.traversal.algorithm.records.KneighborRecords;
import io.bigconnect.biggraph.traversal.algorithm.records.record.RecordType;
import io.bigconnect.biggraph.traversal.algorithm.steps.EdgeStep;
import io.bigconnect.biggraph.type.define.Directions;
import io.bigconnect.biggraph.util.E;
import org.apache.tinkerpop.gremlin.structure.Edge;

import java.util.Iterator;
import java.util.Set;
import java.util.function.Consumer;

public class KneighborTraverser extends OltpTraverser {

    public KneighborTraverser(BigGraph graph) {
        super(graph);
    }

    public Set<Id> kneighbor(Id sourceV, Directions dir,
                             String label, int depth,
                             long degree, long limit) {
        E.checkNotNull(sourceV, "source vertex id");
        this.checkVertexExist(sourceV, "source vertex");
        E.checkNotNull(dir, "direction");
        checkPositive(depth, "k-neighbor max_depth");
        checkDegree(degree);
        checkLimit(limit);

        Id labelId = this.getEdgeLabelId(label);

        Set<Id> latest = newSet();
        Set<Id> all = newSet();

        latest.add(sourceV);

        while (depth-- > 0) {
            long remaining = limit == NO_LIMIT ? NO_LIMIT : limit - all.size();
            latest = this.adjacentVertices(sourceV, latest, dir, labelId,
                                           all, degree, remaining);
            all.addAll(latest);
            if (reachLimit(limit, all.size())) {
                break;
            }
        }

        return all;
    }

    public KneighborRecords customizedKneighbor(Id source, EdgeStep step,
                                                int maxDepth, long limit) {
        E.checkNotNull(source, "source vertex id");
        this.checkVertexExist(source, "source vertex");
        checkPositive(maxDepth, "k-neighbor max_depth");
        checkLimit(limit);

        boolean concurrent = maxDepth >= this.concurrentDepth();

        KneighborRecords records = new KneighborRecords(RecordType.INT,
                                                        concurrent,
                                                        source, true);

        Consumer<Id> consumer = v -> {
            if (this.reachLimit(limit, records.size())) {
                return;
            }
            Iterator<Edge> edges = edgesOfVertex(v, step);
            while (!this.reachLimit(limit, records.size()) && edges.hasNext()) {
                Id target = ((BigEdge) edges.next()).id().otherVertexId();
                records.addPath(v, target);
            }
        };

        while (maxDepth-- > 0) {
            records.startOneLayer(true);
            traverseIds(records.keys(), consumer, concurrent);
            records.finishOneLayer();
        }
        return records;
    }

    private boolean reachLimit(long limit, int size) {
        return limit != NO_LIMIT && size >= limit;
    }
}
