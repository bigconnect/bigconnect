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

package io.bigconnect.biggraph.schema;

import io.bigconnect.biggraph.BigGraph;
import io.bigconnect.biggraph.backend.id.Id;
import io.bigconnect.biggraph.schema.builder.SchemaBuilder;
import io.bigconnect.biggraph.type.BigType;
import io.bigconnect.biggraph.type.define.IdStrategy;
import com.google.common.base.Objects;

import java.util.*;

public class VertexLabel extends SchemaLabel {

    public static final VertexLabel NONE = new VertexLabel(null, NONE_ID, UNDEF);

    // OLAP_VL means all of vertex labels
    public static final VertexLabel OLAP_VL = new VertexLabel(null, OLAP_ID,
                                                              SchemaElement.OLAP);

    private IdStrategy idStrategy;
    private List<Id> primaryKeys;

    public VertexLabel(final BigGraph graph, Id id, String name) {
        super(graph, id, name);
        this.idStrategy = IdStrategy.DEFAULT;
        this.primaryKeys = new ArrayList<>();
    }

    @Override
    public BigType type() {
        return BigType.VERTEX_LABEL;
    }

    public IdStrategy idStrategy() {
        return this.idStrategy;
    }

    public void idStrategy(IdStrategy idStrategy) {
        this.idStrategy = idStrategy;
    }

    public List<Id> primaryKeys() {
        return Collections.unmodifiableList(this.primaryKeys);
    }

    public void primaryKey(Id id) {
        this.primaryKeys.add(id);
    }

    public void primaryKeys(Id... ids) {
        this.primaryKeys.addAll(Arrays.asList(ids));
    }

    public boolean existsLinkLabel() {
        return this.graph().existsLinkLabel(this.id());
    }

    public boolean hasSameContent(VertexLabel other) {
        return super.hasSameContent(other) &&
               this.idStrategy == other.idStrategy &&
               Objects.equal(this.graph.mapPkId2Name(this.primaryKeys),
                             other.graph.mapPkId2Name(other.primaryKeys));
    }

    public static VertexLabel undefined(BigGraph graph) {
        return new VertexLabel(graph, NONE_ID, UNDEF);
    }

    public static VertexLabel undefined(BigGraph graph, Id id) {
        return new VertexLabel(graph, id, UNDEF);
    }

    public interface Builder extends SchemaBuilder<VertexLabel> {

        Id rebuildIndex();

        Builder idStrategy(IdStrategy idStrategy);

        Builder useAutomaticId();

        Builder usePrimaryKeyId();

        Builder useCustomizeStringId();

        Builder useCustomizeNumberId();

        Builder useCustomizeUuidId();

        Builder properties(String... properties);

        Builder primaryKeys(String... keys);

        Builder nullableKeys(String... keys);

        Builder ttl(long ttl);

        Builder ttlStartTime(String ttlStartTime);

        Builder enableLabelIndex(boolean enable);

        Builder userdata(String key, Object value);

        Builder userdata(Map<String, Object> userdata);
    }
}
