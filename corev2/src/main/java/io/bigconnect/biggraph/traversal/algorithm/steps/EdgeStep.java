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

package io.bigconnect.biggraph.traversal.algorithm.steps;

import io.bigconnect.biggraph.BigGraph;
import io.bigconnect.biggraph.backend.id.Id;
import io.bigconnect.biggraph.schema.EdgeLabel;
import io.bigconnect.biggraph.traversal.algorithm.BigTraverser;
import io.bigconnect.biggraph.traversal.optimize.TraversalUtil;
import io.bigconnect.biggraph.type.define.Directions;
import io.bigconnect.biggraph.util.E;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.apache.tinkerpop.gremlin.structure.Edge;

import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import static io.bigconnect.biggraph.traversal.algorithm.BigTraverser.DEFAULT_MAX_DEGREE;
import static io.bigconnect.biggraph.traversal.algorithm.BigTraverser.NO_LIMIT;

public class EdgeStep {

    protected Directions direction;
    protected final Map<Id, String> labels;
    protected final Map<Id, Object> properties;
    protected final long degree;
    protected final long skipDegree;

    public EdgeStep(BigGraph g, Directions direction) {
        this(g, direction, ImmutableList.of());
    }

    public EdgeStep(BigGraph g, List<String> labels) {
        this(g, Directions.BOTH, labels);
    }

    public EdgeStep(BigGraph g, Map<String, Object> properties) {
        this(g, Directions.BOTH, ImmutableList.of(), properties);
    }

    public EdgeStep(BigGraph g, Directions direction, List<String> labels) {
        this(g, direction, labels, ImmutableMap.of());
    }

    public EdgeStep(BigGraph g, Directions direction, List<String> labels,
                    Map<String, Object> properties) {
        this(g, direction, labels, properties,
             Long.parseLong(DEFAULT_MAX_DEGREE), 0L);
    }

    public EdgeStep(BigGraph g, Directions direction, List<String> labels,
                    Map<String, Object> properties,
                    long degree, long skipDegree) {
        E.checkArgument(degree == NO_LIMIT || degree > 0L,
                        "The max degree must be > 0 or == -1, but got: %s",
                        degree);
        BigTraverser.checkSkipDegree(skipDegree, degree,
                                      BigTraverser.NO_LIMIT);
        this.direction = direction;

        // Parse edge labels
        Map<Id, String> labelIds = new HashMap<>();
        if (labels != null) {
            for (String label : labels) {
                EdgeLabel el = g.edgeLabel(label);
                labelIds.put(el.id(), label);
            }
        }
        this.labels = labelIds;

        // Parse properties
        if (properties == null || properties.isEmpty()) {
            this.properties = null;
        } else {
            this.properties = TraversalUtil.transProperties(g, properties);
        }

        this.degree = degree;
        this.skipDegree = skipDegree;
    }

    public Directions direction() {
        return this.direction;
    }

    public Map<Id, String> labels() {
        return this.labels;
    }

    public Map<Id, Object> properties() {
        return this.properties;
    }

    public long degree() {
        return this.degree;
    }

    public long skipDegree() {
        return this.skipDegree;
    }

    public Id[] edgeLabels() {
        int elsSize = this.labels.size();
        Id[] edgeLabels = this.labels.keySet().toArray(new Id[elsSize]);
        return edgeLabels;
    }

    public void swithDirection() {
        this.direction = this.direction.opposite();
    }

    public long limit() {
        long limit = this.skipDegree > 0L ? this.skipDegree : this.degree;
        return limit;
    }

    @Override
    public String toString() {
        return String.format("EdgeStep{direction=%s,labels=%s,properties=%s}",
                             this.direction, this.labels, this.properties);
    }

    public Iterator<Edge> skipSuperNodeIfNeeded(Iterator<Edge> edges) {
        return BigTraverser.skipSuperNodeIfNeeded(edges, this.degree,
                                                   this.skipDegree);
    }
}
