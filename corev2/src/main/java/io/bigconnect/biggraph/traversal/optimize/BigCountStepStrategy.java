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

package io.bigconnect.biggraph.traversal.optimize;

import io.bigconnect.biggraph.backend.query.Aggregate.AggregateFunc;
import org.apache.tinkerpop.gremlin.process.traversal.Step;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.TraversalStrategy.ProviderOptimizationStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.step.TraversalParent;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.CountGlobalStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.GraphStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.NoOpBarrierStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.sideEffect.AggregateStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.sideEffect.IdentityStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.sideEffect.SideEffectStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.CollectingBarrierStep;
import org.apache.tinkerpop.gremlin.process.traversal.strategy.AbstractTraversalStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.util.TraversalHelper;
import org.apache.tinkerpop.gremlin.structure.Element;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;

public final class BigCountStepStrategy
             extends AbstractTraversalStrategy<ProviderOptimizationStrategy>
             implements ProviderOptimizationStrategy {

    private static final long serialVersionUID = -3910433925919057771L;

    private static final BigCountStepStrategy INSTANCE;

    static {
        INSTANCE = new BigCountStepStrategy();
    }

    private BigCountStepStrategy() {
        // pass
    }

    @Override
    @SuppressWarnings({ "rawtypes", "unchecked" })
    public void apply(Traversal.Admin<?, ?> traversal) {
        TraversalUtil.convAllHasSteps(traversal);

        // Extract CountGlobalStep
        List<CountGlobalStep> steps = TraversalHelper.getStepsOfClass(
                                      CountGlobalStep.class, traversal);
        if (steps.isEmpty()) {
            return;
        }

        // Find HugeGraphStep before count()
        CountGlobalStep<?> originStep = steps.get(0);
        List<Step<?, ?>> originSteps = new ArrayList<>();
        BigGraphStep<?, ? extends Element> graphStep = null;
        Step<?, ?> step = originStep;
        do {
            if (!(step instanceof CountGlobalStep ||
                  step instanceof GraphStep ||
                  step instanceof IdentityStep ||
                  step instanceof NoOpBarrierStep ||
                  step instanceof CollectingBarrierStep) ||
                 (step instanceof TraversalParent &&
                  TraversalHelper.anyStepRecursively(s -> {
                      return s instanceof SideEffectStep ||
                             s instanceof AggregateStep;
                  }, (TraversalParent) step))) {
                return;
            }
            originSteps.add(step);
            if (step instanceof BigGraphStep) {
                graphStep = (BigGraphStep<?, ? extends Element>) step;
                break;
            }
            step = step.getPreviousStep();
        } while (step != null);

        if (graphStep == null) {
            return;
        }

        // Replace with HugeCountStep
        graphStep.queryInfo().aggregate(AggregateFunc.COUNT, null);
        BigCountStep<?> countStep = new BigCountStep<>(traversal, graphStep);
        for (Step<?, ?> origin : originSteps) {
            traversal.removeStep(origin);
        }
        traversal.addStep(0, countStep);
    }

    @Override
    public Set<Class<? extends ProviderOptimizationStrategy>> applyPrior() {
        return Collections.singleton(BigGraphStepStrategy.class);
    }

    @Override
    public Set<Class<? extends ProviderOptimizationStrategy>> applyPost() {
        return Collections.singleton(BigVertexStepStrategy.class);
    }

    public static BigCountStepStrategy instance() {
        return INSTANCE;
    }
}
