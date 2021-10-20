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

import io.bigconnect.biggraph.BigGraphException;
import io.bigconnect.biggraph.BigGraph;
import io.bigconnect.biggraph.backend.id.Id;
import io.bigconnect.biggraph.structure.BigEdge;
import io.bigconnect.biggraph.traversal.algorithm.records.KoutRecords;
import io.bigconnect.biggraph.traversal.algorithm.records.record.RecordType;
import io.bigconnect.biggraph.traversal.algorithm.steps.EdgeStep;
import io.bigconnect.biggraph.type.define.Directions;
import io.bigconnect.biggraph.util.E;
import org.apache.tinkerpop.gremlin.structure.Edge;

import java.util.Iterator;
import java.util.Set;
import java.util.function.Consumer;

public class KoutTraverser extends OltpTraverser {

    public KoutTraverser(BigGraph graph) {
        super(graph);
    }

    public Set<Id> kout(Id sourceV, Directions dir, String label,
                        int depth, boolean nearest,
                        long degree, long capacity, long limit) {
        E.checkNotNull(sourceV, "source vertex id");
        this.checkVertexExist(sourceV, "source vertex");
        E.checkNotNull(dir, "direction");
        checkPositive(depth, "k-out max_depth");
        checkDegree(degree);
        checkCapacity(capacity);
        checkLimit(limit);
        if (capacity != NO_LIMIT) {
            // Capacity must > limit because sourceV is counted in capacity
            E.checkArgument(capacity >= limit && limit != NO_LIMIT,
                            "Capacity can't be less than limit, " +
                            "but got capacity '%s' and limit '%s'",
                            capacity, limit);
        }

        Id labelId = this.getEdgeLabelId(label);

        Set<Id> latest = newIdSet();
        latest.add(sourceV);

        Set<Id> all = newIdSet();
        all.add(sourceV);

        long remaining = capacity == NO_LIMIT ?
                         NO_LIMIT : capacity - latest.size();
        while (depth-- > 0) {
            // Just get limit nodes in last layer if limit < remaining capacity
            if (depth == 0 && limit != NO_LIMIT &&
                (limit < remaining || remaining == NO_LIMIT)) {
                remaining = limit;
            }
            if (nearest) {
                latest = this.adjacentVertices(sourceV, latest, dir, labelId,
                                               all, degree, remaining);
                all.addAll(latest);
            } else {
                latest = this.adjacentVertices(sourceV, latest, dir, labelId,
                                               null, degree, remaining);
            }
            if (capacity != NO_LIMIT) {
                // Update 'remaining' value to record remaining capacity
                remaining -= latest.size();

                if (remaining <= 0 && depth > 0) {
                    throw new BigGraphException(
                              "Reach capacity '%s' while remaining depth '%s'",
                              capacity, depth);
                }
            }
        }

        return latest;
    }

    public KoutRecords customizedKout(Id source, EdgeStep step,
                                      int maxDepth, boolean nearest,
                                      long capacity, long limit) {
        E.checkNotNull(source, "source vertex id");
        this.checkVertexExist(source, "source vertex");
        checkPositive(maxDepth, "k-out max_depth");
        checkCapacity(capacity);
        checkLimit(limit);
        long[] depth = new long[1];
        depth[0] = maxDepth;
        boolean concurrent = maxDepth >= this.concurrentDepth();

        KoutRecords records = new KoutRecords(RecordType.INT, concurrent,
                                              source, nearest);

        Consumer<Id> consumer = v -> {
            if (this.reachLimit(limit, depth[0], records.size())) {
                return;
            }
            Iterator<Edge> edges = edgesOfVertex(v, step);
            while (!this.reachLimit(limit, depth[0], records.size()) &&
                   edges.hasNext()) {
                Id target = ((BigEdge) edges.next()).id().otherVertexId();
                records.addPath(v, target);
                this.checkCapacity(capacity, records.accessed(), depth[0]);
            }
        };

        while (depth[0]-- > 0) {
            records.startOneLayer(true);
            traverseIds(records.keys(), consumer, concurrent);
            records.finishOneLayer();
        }
        return records;
    }

    private void checkCapacity(long capacity, long accessed, long depth) {
        if (capacity == NO_LIMIT) {
            return;
        }
        if (accessed >= capacity && depth > 0) {
            throw new BigGraphException(
                      "Reach capacity '%s' while remaining depth '%s'",
                      capacity, depth);
        }
    }

    private boolean reachLimit(long limit, long depth, int size) {
        return limit != NO_LIMIT && depth <= 0 && size >= limit;
    }
}
