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
import io.bigconnect.biggraph.auth.SchemaDefine.Entity;
import io.bigconnect.biggraph.backend.id.Id;
import io.bigconnect.biggraph.backend.id.IdGenerator;
import io.bigconnect.biggraph.schema.VertexLabel;
import io.bigconnect.biggraph.type.define.Cardinality;
import io.bigconnect.biggraph.type.define.DataType;
import io.bigconnect.biggraph.util.E;
import org.apache.commons.lang.StringUtils;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;

import java.util.*;

public class BigProject extends Entity {

    private String name;
    private Id adminGroupId;
    private Id opGroupId;
    private Set<String> graphs;
    private Id targetId;
    private String description;

    public BigProject(Id id) {
        this(id, null, null, null, null, null, null);
    }

    public BigProject(String name) {
        this(name, null);
    }

    public BigProject(String name, String description) {
        this(null, name, description, null, null, null, null);
    }

    public BigProject(Id id, String name, String description, Id adminGroupId,
                      Id opGroupId, Set<String> graphs, Id targetId) {
        this.name = name;
        this.description = description;
        this.adminGroupId = adminGroupId;
        this.opGroupId = opGroupId;
        this.graphs = graphs;
        this.id = id;
        this.targetId = targetId;
    }

    @Override
    public ResourceType type() {
        return ResourceType.PROJECT;
    }

    @Override
    public String label() {
        return P.PROJECT;
    }

    public Id adminGroupId() {
        return this.adminGroupId;
    }

    public void adminGroupId(Id id) {
        this.adminGroupId = id;
    }

    public Id opGroupId() {
        return this.opGroupId;
    }

    public void opGroupId(Id id) {
        this.opGroupId = id;
    }

    public Set<String> graphs() {
        return this.graphs == null ? Collections.emptySet() :
               Collections.unmodifiableSet(this.graphs);
    }

    public void graphs(Set<String> graphs) {
        this.graphs = graphs;
    }

    public Id targetId() {
        return this.targetId;
    }

    public void targetId(Id targetId) {
        this.targetId = targetId;
    }

    public String description() {
        return this.description;
    }

    public void description(String desc) {
        this.description = desc;
    }

    @Override
    public Map<String, Object> asMap() {
        E.checkState(!StringUtils.isEmpty(this.name),
                     "The name of project can't be null");
        E.checkState(this.adminGroupId != null,
                     "The admin group id of project '%s' can't be null",
                     this.name);
        E.checkState(this.opGroupId != null,
                     "The op group id of project '%s' can't be null",
                     this.name);

        Map<String, Object> map = new HashMap<>();

        map.put(Graph.Hidden.unHide(P.NAME), this.name);
        map.put(Graph.Hidden.unHide(P.ADMIN_GROUP),
                this.adminGroupId.toString());
        map.put(Graph.Hidden.unHide(P.OP_GROUP),
                this.opGroupId.toString());
        if (this.graphs != null && !this.graphs.isEmpty()) {
            map.put(Graph.Hidden.unHide(P.GRAPHS), this.graphs);
        }
        if (!StringUtils.isEmpty(this.description)) {
            map.put(Graph.Hidden.unHide(P.DESCRIPTIONS),
                    this.description);
        }
        if (this.targetId != null) {
            map.put(Graph.Hidden.unHide(P.TARGET),
                    this.targetId.toString());
        }

        return super.asMap(map);
    }

    @Override
    protected Object[] asArray() {
        E.checkState(!StringUtils.isEmpty(this.name),
                     "The name of project can't be null");
        E.checkState(this.adminGroupId != null,
                     "The admin group id of project '%s' can't be null",
                     this.name);
        E.checkState(this.opGroupId != null,
                     "The op group id of project '%s' can't be null",
                     this.name);

        List<Object> list = new ArrayList<>(16);

        list.add(T.label);
        list.add(P.PROJECT);

        list.add(P.NAME);
        list.add(this.name);

        if (!StringUtils.isEmpty(this.description)) {
            list.add(P.DESCRIPTIONS);
            list.add(this.description);
        }

        if (this.graphs != null && !this.graphs.isEmpty()) {
            list.add(P.GRAPHS);
            list.add(this.graphs);
        }

        list.add(P.ADMIN_GROUP);
        list.add(this.adminGroupId.toString());

        list.add(P.OP_GROUP);
        list.add(this.opGroupId.toString());

        if (this.targetId != null) {
            list.add(P.TARGET);
            list.add(this.targetId.toString());
        }

        return super.asArray(list);
    }

    @SuppressWarnings("unchecked")
    @Override
    protected boolean property(String key, Object value) {
        if (super.property(key, value)) {
            return true;
        }
        switch (key) {
            case P.NAME:
                this.name = (String) value;
                break;
            case P.GRAPHS:
                this.graphs = (Set<String>) value;
                break;
            case P.DESCRIPTIONS:
                this.description = (String) value;
                break;
            case P.ADMIN_GROUP:
                this.adminGroupId = IdGenerator.of(value);
                break;
            case P.OP_GROUP:
                this.opGroupId = IdGenerator.of(value);
                break;
            case P.TARGET:
                this.targetId = IdGenerator.of(value);
                break;
            default:
                throw new AssertionError("Unsupported key: " + key);
        }
        return true;
    }

    public static BigProject fromVertex(Vertex vertex) {
        BigProject target = new BigProject((Id) vertex.id());
        return fromVertex(vertex, target);
    }

    @Override
    public String name() {
        return this.name;
    }

    public static Schema schema(BigGraphParams graph) {
        return new Schema(graph);
    }

    public static final class P {

        public static final String PROJECT = Graph.Hidden.hide("project");
        public static final String LABEL = T.label.getAccessor();
        public static final String ADMIN_GROUP = "~project_admin_group";
        public static final String OP_GROUP = "~project_op_group";
        public static final String GRAPHS = "~project_graphs";
        public static final String NAME = "~project_name";
        public static final String DESCRIPTIONS = "~project_description";
        public static final String TARGET = "~project_target";

        public static String unhide(String key) {
            final String prefix = Graph.Hidden.hide("project_");
            if (key.startsWith(prefix)) {
                return key.substring(prefix.length());
            }
            return key;
        }
    }

    public static final class Schema extends SchemaDefine {

        public Schema(BigGraphParams graph) {
            super(graph, P.PROJECT);
        }

        @Override
        public void initSchemaIfNeeded() {
            if (this.existEdgeLabel(this.label)) {
                return;
            }

            String[] properties = this.initProperties();

            VertexLabel label = this.schema().vertexLabel(this.label)
                                    .enableLabelIndex(true)
                                    .usePrimaryKeyId()
                                    .primaryKeys(P.NAME)
                                    .nullableKeys(P.DESCRIPTIONS,
                                                  P.GRAPHS,
                                                  P.TARGET)
                                    .properties(properties)
                                    .build();
            this.graph.schemaTransaction().addVertexLabel(label);
        }

        private String[] initProperties() {
            List<String> props = new ArrayList<>();

            props.add(createPropertyKey(P.ADMIN_GROUP,
                                        DataType.TEXT));
            props.add(createPropertyKey(P.OP_GROUP,
                                        DataType.TEXT));
            props.add(createPropertyKey(P.GRAPHS, DataType.TEXT,
                                        Cardinality.SET));
            props.add(createPropertyKey(P.NAME, DataType.TEXT));
            props.add(createPropertyKey(P.DESCRIPTIONS,
                                        DataType.TEXT));
            props.add(createPropertyKey(P.TARGET,
                                        DataType.TEXT));

            return super.initProperties(props);
        }
    }
}
