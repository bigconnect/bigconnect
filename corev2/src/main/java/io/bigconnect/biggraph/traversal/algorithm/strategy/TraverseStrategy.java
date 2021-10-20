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

package io.bigconnect.biggraph.traversal.algorithm.strategy;

import io.bigconnect.biggraph.BigGraph;
import io.bigconnect.biggraph.backend.id.Id;
import io.bigconnect.biggraph.traversal.algorithm.BigTraverser;
import io.bigconnect.biggraph.traversal.algorithm.steps.EdgeStep;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.BiConsumer;

public interface TraverseStrategy {

    public abstract void traverseOneLayer(
                         Map<Id, List<BigTraverser.Node>> vertices,
                         EdgeStep step, BiConsumer<Id, EdgeStep> consumer);

    public abstract Map<Id, List<BigTraverser.Node>> newMultiValueMap();

    public abstract Set<BigTraverser.Path> newPathSet();

    public abstract void addNode(Map<Id, List<BigTraverser.Node>> vertices,
                                 Id id, BigTraverser.Node node);

    public abstract void addNewVerticesToAll(
                         Map<Id, List<BigTraverser.Node>> newVertices,
                         Map<Id, List<BigTraverser.Node>> targets);

    public static TraverseStrategy create(boolean concurrent, BigGraph graph) {
        return concurrent ? new ConcurrentTraverseStrategy(graph) :
                            new SingleTraverseStrategy(graph);
    }
}
