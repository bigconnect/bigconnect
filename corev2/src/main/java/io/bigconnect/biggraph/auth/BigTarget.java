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

import io.bigconnect.biggraph.BigGraphException;
import io.bigconnect.biggraph.BigGraphParams;
import io.bigconnect.biggraph.auth.SchemaDefine.Entity;
import io.bigconnect.biggraph.backend.id.Id;
import io.bigconnect.biggraph.schema.VertexLabel;
import io.bigconnect.biggraph.util.E;
import io.bigconnect.biggraph.util.JsonUtil;
import jersey.repackaged.com.google.common.collect.ImmutableList;
import org.apache.tinkerpop.gremlin.structure.Graph.Hidden;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class BigTarget extends Entity {

    private static final long serialVersionUID = -3361487778656878418L;

    private String name;
    private String graph;
    private String url;
    private List<BigResource> resources;

    private static final List<BigResource> EMPTY = ImmutableList.of();

    public BigTarget(Id id) {
        this(id, null, null, null, EMPTY);
    }

    public BigTarget(String name, String url) {
        this(null, name, name, url, EMPTY);
    }

    public BigTarget(String name, String graph, String url) {
        this(null, name, graph, url, EMPTY);
    }

    public BigTarget(String name, String graph, String url,
                     List<BigResource> resources) {
        this(null, name, graph, url, resources);
    }

    private BigTarget(Id id, String name, String graph, String url,
                      List<BigResource> resources) {
        this.id = id;
        this.name = name;
        this.graph = graph;
        this.url = url;
        this.resources = resources;
    }

    @Override
    public ResourceType type() {
        return ResourceType.TARGET;
    }

    @Override
    public String label() {
        return P.TARGET;
    }

    @Override
    public String name() {
        return this.name;
    }

    public String graph() {
        return this.graph;
    }

    public String url() {
        return this.url;
    }

    public void url(String url) {
        this.url = url;
    }

    public List<BigResource> resources() {
        return this.resources;
    }

    public void resources(String resources) {
        try {
            this.resources = BigResource.parseResources(resources);
        } catch (Exception e) {
            throw new BigGraphException("Invalid format of resources: %s",
                                    e, resources);
        }
    }

    public void resources(List<BigResource> resources) {
        E.checkNotNull(resources, "resources");
        this.resources = resources;
    }

    @Override
    public String toString() {
        return String.format("HugeTarget(%s)%s", this.id, this.asMap());
    }

    @Override
    protected boolean property(String key, Object value) {
        if (super.property(key, value)) {
            return true;
        }
        switch (key) {
            case P.NAME:
                this.name = (String) value;
                break;
            case P.GRAPH:
                this.graph = (String) value;
                break;
            case P.URL:
                this.url = (String) value;
                break;
            case P.RESS:
                this.resources = BigResource.parseResources((String) value);
                break;
            default:
                throw new AssertionError("Unsupported key: " + key);
        }
        return true;
    }

    @Override
    protected Object[] asArray() {
        E.checkState(this.name != null, "Target name can't be null");
        E.checkState(this.url != null, "Target url can't be null");

        List<Object> list = new ArrayList<>(16);

        list.add(T.label);
        list.add(P.TARGET);

        list.add(P.NAME);
        list.add(this.name);

        list.add(P.GRAPH);
        list.add(this.graph);

        list.add(P.URL);
        list.add(this.url);

        if (this.resources != null && this.resources != EMPTY) {
            list.add(P.RESS);
            list.add(JsonUtil.toJson(this.resources));
        }

        return super.asArray(list);
    }

    @Override
    public Map<String, Object> asMap() {
        E.checkState(this.name != null, "Target name can't be null");
        E.checkState(this.url != null, "Target url can't be null");

        Map<String, Object> map = new HashMap<>();

        map.put(Hidden.unHide(P.NAME), this.name);
        map.put(Hidden.unHide(P.GRAPH), this.graph);
        map.put(Hidden.unHide(P.URL), this.url);

        if (this.resources != null && this.resources != EMPTY) {
            map.put(Hidden.unHide(P.RESS), this.resources);
        }

        return super.asMap(map);
    }

    public static BigTarget fromVertex(Vertex vertex) {
        BigTarget target = new BigTarget((Id) vertex.id());
        return fromVertex(vertex, target);
    }

    public static Schema schema(BigGraphParams graph) {
        return new Schema(graph);
    }

    public static final class P {

        public static final String TARGET = Hidden.hide("target");

        public static final String ID = T.id.getAccessor();
        public static final String LABEL = T.label.getAccessor();

        public static final String NAME = "~target_name";
        public static final String GRAPH = "~target_graph";
        public static final String URL = "~target_url";
        public static final String RESS = "~target_resources";

        public static String unhide(String key) {
            final String prefix = Hidden.hide("target_");
            if (key.startsWith(prefix)) {
                return key.substring(prefix.length());
            }
            return key;
        }
    }

    public static final class Schema extends SchemaDefine {

        public Schema(BigGraphParams graph) {
            super(graph, P.TARGET);
        }

        @Override
        public void initSchemaIfNeeded() {
            if (this.existVertexLabel(this.label)) {
                return;
            }

            String[] properties = this.initProperties();

            // Create vertex label
            VertexLabel label = this.schema().vertexLabel(this.label)
                                    .properties(properties)
                                    .usePrimaryKeyId()
                                    .primaryKeys(P.NAME)
                                    .nullableKeys(P.RESS)
                                    .enableLabelIndex(true)
                                    .build();
            this.graph.schemaTransaction().addVertexLabel(label);
        }

        private String[] initProperties() {
            List<String> props = new ArrayList<>();

            props.add(createPropertyKey(P.NAME));
            props.add(createPropertyKey(P.GRAPH));
            props.add(createPropertyKey(P.URL));
            props.add(createPropertyKey(P.RESS));

            return super.initProperties(props);
        }
    }
}
