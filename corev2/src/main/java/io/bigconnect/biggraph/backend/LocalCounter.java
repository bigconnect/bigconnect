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

package io.bigconnect.biggraph.backend;

import io.bigconnect.biggraph.backend.id.Id;
import io.bigconnect.biggraph.backend.id.IdGenerator;
import io.bigconnect.biggraph.type.BigType;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

public class LocalCounter {

    private final Map<BigType, AtomicLong> counters;

    public LocalCounter() {
        this.counters = new ConcurrentHashMap<>();
    }

    public synchronized Id nextId(BigType type) {
        AtomicLong counter = this.counters.get(type);
        if (counter == null) {
            counter = new AtomicLong(0);
            AtomicLong previous = this.counters.putIfAbsent(type, counter);
            if (previous != null) {
                counter = previous;
            }
        }
        return IdGenerator.of(counter.incrementAndGet());
    }

    public long getCounter(BigType type) {
        AtomicLong counter = this.counters.get(type);
        if (counter == null) {
            counter = new AtomicLong(0);
            AtomicLong previous = this.counters.putIfAbsent(type, counter);
            if (previous != null) {
                counter = previous;
            }
        }
        return counter.longValue();
    }

    public synchronized void increaseCounter(BigType type, long increment) {
        AtomicLong counter = this.counters.get(type);
        if (counter == null) {
            counter = new AtomicLong(0);
            AtomicLong previous = this.counters.putIfAbsent(type, counter);
            if (previous != null) {
                counter = previous;
            }
        }
        long oldValue = counter.longValue();
        AtomicLong value = new AtomicLong(oldValue + increment);
        this.counters.put(type, value);
    }

    public void reset() {
        this.counters.clear();
    }
}
