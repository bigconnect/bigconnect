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
import io.bigconnect.biggraph.type.define.Directions;
import io.bigconnect.biggraph.util.CollectionUtil;
import io.bigconnect.biggraph.util.E;
import org.apache.tinkerpop.gremlin.util.iterator.IteratorUtils;

import java.util.Set;

public class SameNeighborTraverser extends BigTraverser {

    public SameNeighborTraverser(BigGraph graph) {
        super(graph);
    }

    public Set<Id> sameNeighbors(Id vertex, Id other, Directions direction,
                                 String label, long degree, long limit) {
        E.checkNotNull(vertex, "vertex id");
        E.checkNotNull(other, "the other vertex id");
        this.checkVertexExist(vertex, "vertex");
        this.checkVertexExist(other, "other vertex");
        E.checkNotNull(direction, "direction");
        checkDegree(degree);
        checkLimit(limit);

        Id labelId = this.getEdgeLabelId(label);

        Set<Id> sourceNeighbors = IteratorUtils.set(this.adjacentVertices(
                                  vertex, direction, labelId, degree));
        Set<Id> targetNeighbors = IteratorUtils.set(this.adjacentVertices(
                                  other, direction, labelId, degree));
        Set<Id> sameNeighbors = (Set<Id>) CollectionUtil.intersect(
                                sourceNeighbors, targetNeighbors);
        if (limit != NO_LIMIT) {
            int end = Math.min(sameNeighbors.size(), (int) limit);
            sameNeighbors = CollectionUtil.subSet(sameNeighbors, 0, end);
        }
        return sameNeighbors;
    }
}
