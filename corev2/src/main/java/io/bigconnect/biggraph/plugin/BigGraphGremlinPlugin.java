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

import io.bigconnect.biggraph.BigGraphException;
import io.bigconnect.biggraph.BigGraphFactory;
import io.bigconnect.biggraph.util.ReflectionUtil;
import com.google.common.reflect.ClassPath;
import org.apache.tinkerpop.gremlin.jsr223.AbstractGremlinPlugin;
import org.apache.tinkerpop.gremlin.jsr223.DefaultImportCustomizer;
import org.apache.tinkerpop.gremlin.jsr223.ImportCustomizer;

import java.io.IOException;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

public class BigGraphGremlinPlugin extends AbstractGremlinPlugin {

    private static final String PACKAGE = "io.bigconnect.biggraph.type.define";
    private static final String NAME = "BigGraph";

    private static final BigGraphGremlinPlugin instance;
    private static final ImportCustomizer imports;

    static {
        instance = new BigGraphGremlinPlugin();

        Iterator<ClassPath.ClassInfo> classInfos;
        try {
            classInfos = ReflectionUtil.classes(PACKAGE);
        } catch (IOException e) {
            throw new BigGraphException("Failed to scan classes under package %s",
                                    e, PACKAGE);
        }

        @SuppressWarnings("rawtypes")
        Set<Class> classes = new HashSet<>();
        classInfos.forEachRemaining(classInfo -> classes.add(classInfo.load()));
        // Add entrance class: graph = HugeFactory.open("biggraph.properties")
        classes.add(BigGraphFactory.class);

        imports = DefaultImportCustomizer.build()
                                         .addClassImports(classes)
                                         .create();
    }

    public BigGraphGremlinPlugin() {
        super(NAME, imports);
    }

    public static BigGraphGremlinPlugin instance() {
        return instance;
    }
}
