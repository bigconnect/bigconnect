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

package io.bigconnect.biggraph.backend.cache;

import io.bigconnect.biggraph.backend.id.Id;
import io.bigconnect.biggraph.type.BigType;

public interface CacheNotifier extends AutoCloseable {

    public void invalid(BigType type, Id id);

    public void invalid2(BigType type, Object[] ids);

    public void clear(BigType type);

    public void reload();

    public interface GraphCacheNotifier extends CacheNotifier {}

    public interface SchemaCacheNotifier extends CacheNotifier {}
}
