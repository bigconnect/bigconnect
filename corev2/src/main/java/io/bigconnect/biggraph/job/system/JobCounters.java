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

package io.bigconnect.biggraph.job.system;

import io.bigconnect.biggraph.BigGraph;
import io.bigconnect.biggraph.config.CoreOptions;
import io.bigconnect.biggraph.structure.BigElement;
import io.bigconnect.biggraph.structure.BigIndex;

import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

public class JobCounters {

    private ConcurrentHashMap<String, JobCounter> jobCounters =
                                                  new ConcurrentHashMap<>();

    public JobCounter jobCounter(BigGraph g) {
        int batch = g.option(CoreOptions.TASK_TTL_DELETE_BATCH);
        String graph = g.name();
        if (!this.jobCounters.containsKey(graph)) {
            this.jobCounters.putIfAbsent(graph, new JobCounter(batch));
        }
        return this.jobCounters.get(graph);
    }

    public static class JobCounter {

        private AtomicInteger jobs;
        private Set<BigElement> elements;
        private Set<BigIndex> indexes;
        private int batchSize;

        public JobCounter(int batchSize) {
            this.jobs = new AtomicInteger(0);
            this.elements = ConcurrentHashMap.newKeySet();
            this.indexes = ConcurrentHashMap.newKeySet();
            this.batchSize = batchSize;
        }

        public int jobs() {
            return this.jobs.get();
        }

        public void decrement() {
            this.jobs.decrementAndGet();
        }

        public void increment() {
            this.jobs.incrementAndGet();
        }

        public Set<BigElement> elements() {
            return this.elements;
        }

        public Set<BigIndex> indexes() {
            return this.indexes;
        }

        public void clear(Object object) {
            if (object instanceof BigElement) {
                this.elements = ConcurrentHashMap.newKeySet();
            } else {
                assert object instanceof BigIndex;
                this.indexes = ConcurrentHashMap.newKeySet();
            }
        }

        public boolean addAndTriggerDelete(Object object) {
            return object instanceof BigElement ?
                   addElementAndTriggerDelete((BigElement) object) :
                   addIndexAndTriggerDelete((BigIndex) object);
        }

        /**
         * Try to add element in collection waiting to be deleted
         * @param element
         * @return true if should create a new delete job, false otherwise
         */
        public boolean addElementAndTriggerDelete(BigElement element) {
            if (this.elements.size() >= this.batchSize) {
                return true;
            }
            this.elements.add(element);
            return this.elements.size() >= this.batchSize;
        }

        /**
         * Try to add edge in collection waiting to be deleted
         * @param index
         * @return true if should create a new delete job, false otherwise
         */
        public boolean addIndexAndTriggerDelete(BigIndex index) {
            if (this.indexes.size() >= this.batchSize) {
                return true;
            }
            this.indexes.add(index);
            return this.indexes.size() >= this.batchSize;
        }
    }
}
