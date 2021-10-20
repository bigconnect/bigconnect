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

package io.bigconnect.biggraph.auth;

import io.bigconnect.biggraph.BigGraphParams;
import io.bigconnect.biggraph.auth.SchemaDefine.Relationship;
import io.bigconnect.biggraph.backend.id.Id;
import io.bigconnect.biggraph.schema.EdgeLabel;
import io.bigconnect.biggraph.type.define.DataType;
import io.bigconnect.biggraph.util.E;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Graph.Hidden;
import org.apache.tinkerpop.gremlin.structure.T;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class BigAccess extends Relationship {

    private static final long serialVersionUID = -7644007602408729385L;

    private final Id group;
    private final Id target;
    private BigPermission permission;
    private String description;

    public BigAccess(Id group, Id target) {
        this(group, target, null);
    }

    public BigAccess(Id group, Id target, BigPermission permission) {
        this.group = group;
        this.target = target;
        this.permission = permission;
        this.description = null;
    }

    @Override
    public ResourceType type() {
        return ResourceType.GRANT;
    }

    @Override
    public String label() {
        return P.ACCESS;
    }

    @Override
    public String sourceLabel() {
        return P.GROUP;
    }

    @Override
    public String targetLabel() {
        return P.TARGET;
    }

    @Override
    public Id source() {
        return this.group;
    }

    @Override
    public Id target() {
        return this.target;
    }

    public BigPermission permission() {
        return this.permission;
    }

    public void permission(BigPermission permission) {
        this.permission = permission;
    }

    public String description() {
        return this.description;
    }

    public void description(String description) {
        this.description = description;
    }

    @Override
    public String toString() {
        return String.format("HugeAccess(%s->%s)%s",
                             this.group, this.target, this.asMap());
    }

    @Override
    protected boolean property(String key, Object value) {
        if (super.property(key, value)) {
            return true;
        }
        switch (key) {
            case P.PERMISSION:
                this.permission = BigPermission.fromCode((Byte) value);
                break;
            case P.DESCRIPTION:
                this.description = (String) value;
                break;
            default:
                throw new AssertionError("Unsupported key: " + key);
        }
        return true;
    }

    @Override
    protected Object[] asArray() {
        E.checkState(this.permission != null,
                     "Access permission can't be null");

        List<Object> list = new ArrayList<>(12);

        list.add(T.label);
        list.add(P.ACCESS);

        list.add(P.PERMISSION);
        list.add(this.permission.code());

        if (this.description != null) {
            list.add(P.DESCRIPTION);
            list.add(this.description);
        }

        return super.asArray(list);
    }

    @Override
    public Map<String, Object> asMap() {
        E.checkState(this.permission != null,
                     "Access permission can't be null");

        Map<String, Object> map = new HashMap<>();

        map.put(Hidden.unHide(P.GROUP), this.group);
        map.put(Hidden.unHide(P.TARGET), this.target);

        map.put(Hidden.unHide(P.PERMISSION), this.permission);

        if (this.description != null) {
            map.put(Hidden.unHide(P.DESCRIPTION), this.description);
        }

        return super.asMap(map);
    }

    public static BigAccess fromEdge(Edge edge) {
        BigAccess access = new BigAccess((Id) edge.outVertex().id(),
                                           (Id) edge.inVertex().id());
        return fromEdge(edge, access);
    }

    public static Schema schema(BigGraphParams graph) {
        return new Schema(graph);
    }

    public static final class P {

        public static final String ACCESS = Hidden.hide("access");

        public static final String LABEL = T.label.getAccessor();

        public static final String GROUP = BigGroup.P.GROUP;
        public static final String TARGET = BigTarget.P.TARGET;

        public static final String PERMISSION = "~access_permission";
        public static final String DESCRIPTION = "~access_description";

        public static String unhide(String key) {
            final String prefix = Hidden.hide("access_");
            if (key.startsWith(prefix)) {
                return key.substring(prefix.length());
            }
            return key;
        }
    }

    public static final class Schema extends SchemaDefine {

        public Schema(BigGraphParams graph) {
            super(graph, P.ACCESS);
        }

        @Override
        public void initSchemaIfNeeded() {
            if (this.existEdgeLabel(this.label)) {
                return;
            }

            String[] properties = this.initProperties();

            // Create edge label
            EdgeLabel label = this.schema().edgeLabel(this.label)
                                  .sourceLabel(P.GROUP)
                                  .targetLabel(P.TARGET)
                                  .properties(properties)
                                  .nullableKeys(P.DESCRIPTION)
                                  .sortKeys(P.PERMISSION)
                                  .enableLabelIndex(true)
                                  .build();
            this.graph.schemaTransaction().addEdgeLabel(label);
        }

        private String[] initProperties() {
            List<String> props = new ArrayList<>();

            props.add(createPropertyKey(P.PERMISSION, DataType.BYTE));
            props.add(createPropertyKey(P.DESCRIPTION));

            return super.initProperties(props);
        }
    }
}
