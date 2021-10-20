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
import io.bigconnect.biggraph.backend.query.QueryResults;
import io.bigconnect.biggraph.iterator.FilterIterator;
import io.bigconnect.biggraph.iterator.FlatMapperIterator;
import io.bigconnect.biggraph.structure.BigEdge;
import io.bigconnect.biggraph.traversal.algorithm.steps.EdgeStep;
import io.bigconnect.biggraph.util.E;
import org.apache.commons.lang.mutable.MutableLong;
import org.apache.tinkerpop.gremlin.structure.Edge;

import java.util.Iterator;
import java.util.List;
import java.util.Set;

public class CountTraverser extends BigTraverser {

    private boolean containsTraversed = false;
    private long dedupSize = 1000000L;
    private final Set<Id> dedupSet = newIdSet();
    private final MutableLong count = new MutableLong(0L);

    public CountTraverser(BigGraph graph) {
        super(graph);
    }

    public long count(Id source, List<EdgeStep> steps,
                      boolean containsTraversed, long dedupSize) {
        E.checkNotNull(source, "source vertex id");
        this.checkVertexExist(source, "source vertex");
        E.checkArgument(steps != null && !steps.isEmpty(),
                        "The steps can't be empty");
        checkDedupSize(dedupSize);

        this.containsTraversed = containsTraversed;
        this.dedupSize = dedupSize;
        if (this.containsTraversed) {
            this.count.increment();
        }

        int stepNum = steps.size();
        EdgeStep firstStep = steps.get(0);
        if (stepNum == 1) {
            // Just one step, query count and return
            long edgesCount = this.edgesCount(source, firstStep);
            this.count.add(edgesCount);
            return this.count.longValue();
        }

        // Multiple steps, construct first step to iterator
        Iterator<Edge> edges = this.edgesOfVertexWithCount(source, firstStep);
        // Wrap steps to Iterator except last step
        for (int i = 1; i < stepNum - 1; i++) {
            EdgeStep currentStep = steps.get(i);
            edges = new FlatMapperIterator<>(edges, (edge) -> {
                Id target = ((BigEdge) edge).id().otherVertexId();
                return this.edgesOfVertexWithCount(target, currentStep);
            });
        }

        // The last step, just query count
        EdgeStep lastStep = steps.get(stepNum - 1);
        while (edges.hasNext()) {
            Id target = ((BigEdge) edges.next()).id().otherVertexId();
            if (this.dedup(target)) {
                continue;
            }
            // Count last layer vertices(without dedup size)
            long edgesCount = this.edgesCount(target, lastStep);
            this.count.add(edgesCount);
        }

        return this.count.longValue();
    }

    private Iterator<Edge> edgesOfVertexWithCount(Id source, EdgeStep step) {
        if (this.dedup(source)) {
            return QueryResults.emptyIterator();
        }
        Iterator<Edge> flatten = this.edgesOfVertex(source, step);
        return new FilterIterator<>(flatten, e -> {
            if (this.containsTraversed) {
                // Count intermediate vertices
                this.count.increment();
            }
            return true;
        });
    }

    private void checkDedupSize(long dedup) {
        checkNonNegativeOrNoLimit(dedup, "dedup size");
    }

    private boolean dedup(Id vertex) {
        if (!this.needDedup()) {
            return false;
        }

        if (this.dedupSet.contains(vertex)) {
            // Skip vertex already traversed
            return true;
        } else if (!this.reachDedup()) {
            // Record vertex not traversed before if not reach dedup size
            this.dedupSet.add(vertex);
        }
        return false;
    }

    private boolean needDedup() {
        return this.dedupSize != 0L;
    }

    private boolean reachDedup() {
        return this.dedupSize != NO_LIMIT &&
               this.dedupSet.size() >= this.dedupSize;
    }
}
