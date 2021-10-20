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

package io.bigconnect.biggraph.plugin;

import io.bigconnect.biggraph.analyzer.AnalyzerFactory;
import io.bigconnect.biggraph.backend.serializer.SerializerFactory;
import io.bigconnect.biggraph.backend.store.BackendProviderFactory;
import io.bigconnect.biggraph.config.OptionSpace;

public interface BigGraphPlugin {

    String name();

    void register();

    String supportsMinVersion();

    String supportsMaxVersion();

    static void registerOptions(String name, String classPath) {
        OptionSpace.register(name, classPath);
    }

    static void registerBackend(String name, String classPath) {
        BackendProviderFactory.register(name, classPath);
    }

    static void registerSerializer(String name, String classPath) {
        SerializerFactory.register(name, classPath);
    }

    static void registerAnalyzer(String name, String classPath) {
        AnalyzerFactory.register(name, classPath);
    }
}
