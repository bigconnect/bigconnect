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

import io.bigconnect.biggraph.backend.id.Id;
import io.bigconnect.biggraph.structure.BigEdge;
import io.bigconnect.biggraph.traversal.algorithm.steps.EdgeStep;
import io.bigconnect.biggraph.traversal.algorithm.strategy.TraverseStrategy;
import org.apache.tinkerpop.gremlin.structure.Edge;

import java.util.*;
import java.util.function.BiConsumer;

import static io.bigconnect.biggraph.traversal.algorithm.BigTraverser.NO_LIMIT;

public abstract class PathTraverser {

    protected final BigTraverser traverser;

    protected int stepCount;
    protected final long capacity;
    protected final long limit;
    protected int totalSteps; // TODO: delete or implement abstract method

    protected Map<Id, List<BigTraverser.Node>> sources;
    protected Map<Id, List<BigTraverser.Node>> sourcesAll;
    protected Map<Id, List<BigTraverser.Node>> targets;
    protected Map<Id, List<BigTraverser.Node>> targetsAll;

    protected Map<Id, List<BigTraverser.Node>> newVertices;

    protected Set<BigTraverser.Path> paths;

    protected TraverseStrategy traverseStrategy;

    public PathTraverser(BigTraverser traverser, TraverseStrategy strategy,
                         Collection<Id> sources, Collection<Id> targets,
                         long capacity, long limit) {
        this.traverser = traverser;
        this.traverseStrategy = strategy;

        this.capacity = capacity;
        this.limit = limit;

        this.stepCount = 0;

        this.sources = this.newMultiValueMap();
        this.sourcesAll = this.newMultiValueMap();
        this.targets = this.newMultiValueMap();
        this.targetsAll = this.newMultiValueMap();

        for (Id id : sources) {
            this.addNode(this.sources, id, new BigTraverser.Node(id));
        }
        for (Id id : targets) {
            this.addNode(this.targets, id, new BigTraverser.Node(id));
        }
        this.sourcesAll.putAll(this.sources);
        this.targetsAll.putAll(this.targets);

        this.paths = this.newPathSet();
    }

    public void forward() {
        EdgeStep currentStep = this.nextStep(true);
        if (currentStep == null) {
            return;
        }

        this.beforeTraverse(true);

        // Traversal vertices of previous level
        this.traverseOneLayer(this.sources, currentStep, this::forward);

        this.afterTraverse(currentStep, true);
    }

    public void backward() {
        EdgeStep currentStep = this.nextStep(false);
        if (currentStep == null) {
            return;
        }

        this.beforeTraverse(false);

        currentStep.swithDirection();
        // Traversal vertices of previous level
        this.traverseOneLayer(this.targets, currentStep, this::backward);
        currentStep.swithDirection();

        this.afterTraverse(currentStep, false);
    }

    public abstract EdgeStep nextStep(boolean forward);

    public void beforeTraverse(boolean forward) {
        this.clearNewVertices();
    }

    public void traverseOneLayer(Map<Id, List<BigTraverser.Node>> vertices,
                                 EdgeStep step,
                                 BiConsumer<Id, EdgeStep> consumer) {
        this.traverseStrategy.traverseOneLayer(vertices, step, consumer);
    }

    public void afterTraverse(EdgeStep step, boolean forward) {
        this.reInitCurrentStepIfNeeded(step, forward);
        this.stepCount++;
    }

    private void forward(Id v, EdgeStep step) {
        this.traverseOne(v, step, true);
    }

    private void backward(Id v, EdgeStep step) {
        this.traverseOne(v, step, false);
    }

    private void traverseOne(Id v, EdgeStep step, boolean forward) {
        if (this.reachLimit()) {
            return;
        }

        Iterator<Edge> edges = this.traverser.edgesOfVertex(v, step);
        while (edges.hasNext()) {
            BigEdge edge = (BigEdge) edges.next();
            Id target = edge.id().otherVertexId();

            this.processOne(v, target, forward);
        }
    }

    private void processOne(Id source, Id target, boolean forward) {
        if (forward) {
            this.processOneForForward(source, target);
        } else {
            this.processOneForBackward(source, target);
        }
    }

    protected abstract void processOneForForward(Id source, Id target);

    protected abstract void processOneForBackward(Id source, Id target);

    protected abstract void reInitCurrentStepIfNeeded(EdgeStep step,
                                                      boolean forward);

    public void clearNewVertices() {
        this.newVertices = this.newMultiValueMap();
    }

    public void addNodeToNewVertices(Id id, BigTraverser.Node node) {
        this.addNode(this.newVertices, id, node);
    }

    public Map<Id, List<BigTraverser.Node>> newMultiValueMap() {
        return this.traverseStrategy.newMultiValueMap();
    }

    public Set<BigTraverser.Path> newPathSet() {
        return this.traverseStrategy.newPathSet();
    }

    public void addNode(Map<Id, List<BigTraverser.Node>> vertices, Id id,
                        BigTraverser.Node node) {
        this.traverseStrategy.addNode(vertices, id, node);
    }

    public void addNewVerticesToAll(Map<Id, List<BigTraverser.Node>> targets) {
        this.traverseStrategy.addNewVerticesToAll(this.newVertices, targets);
    }

    public Set<BigTraverser.Path> paths() {
        return this.paths;
    }

    public int pathCount() {
        return this.paths.size();
    }

    protected boolean finished() {
        return this.stepCount >= this.totalSteps || this.reachLimit();
    }

    protected boolean reachLimit() {
        BigTraverser.checkCapacity(this.capacity, this.accessedNodes(),
                                    "template paths");
        if (this.limit == NO_LIMIT || this.pathCount() < this.limit) {
            return false;
        }
        return true;
    }

    protected int accessedNodes() {
        int size = 0;
        for (List<BigTraverser.Node> value : this.sourcesAll.values()) {
            size += value.size();
        }
        for (List<BigTraverser.Node> value : this.targetsAll.values()) {
            size += value.size();
        }
        return size;
    }
}
