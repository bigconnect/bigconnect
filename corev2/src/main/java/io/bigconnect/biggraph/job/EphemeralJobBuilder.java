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

package io.bigconnect.biggraph.job;

import io.bigconnect.biggraph.BigGraph;
import io.bigconnect.biggraph.backend.id.Id;
import io.bigconnect.biggraph.backend.id.IdGenerator;
import io.bigconnect.biggraph.task.BigTask;
import io.bigconnect.biggraph.task.TaskScheduler;
import io.bigconnect.biggraph.util.E;

public class EphemeralJobBuilder<V> {

    private final BigGraph graph;

    private String name;
    private String input;
    private EphemeralJob<V> job;

    // Use negative task id for ephemeral task
    private static int ephemeralTaskId = -1;

    public static <T> EphemeralJobBuilder<T> of(final BigGraph graph) {
        return new EphemeralJobBuilder<>(graph);
    }

    public EphemeralJobBuilder(final BigGraph graph) {
        this.graph = graph;
    }

    public EphemeralJobBuilder<V> name(String name) {
        this.name = name;
        return this;
    }

    public EphemeralJobBuilder<V> input(String input) {
        this.input = input;
        return this;
    }

    public EphemeralJobBuilder<V> job(EphemeralJob<V> job) {
        this.job = job;
        return this;
    }

    public BigTask<V> schedule() {
        E.checkArgumentNotNull(this.name, "Job name can't be null");
        E.checkArgumentNotNull(this.job, "Job can't be null");

        BigTask<V> task = new BigTask<>(this.genTaskId(), null, this.job);
        task.type(this.job.type());
        task.name(this.name);
        if (this.input != null) {
            task.input(this.input);
        }

        TaskScheduler scheduler = this.graph.taskScheduler();
        scheduler.schedule(task);

        return task;
    }

    private Id genTaskId() {
        if (ephemeralTaskId >= 0) {
            ephemeralTaskId = -1;
        }
        return IdGenerator.of(ephemeralTaskId--);
    }
}
