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

package io.bigconnect.biggraph.task;

import io.bigconnect.biggraph.BigGraph;
import io.bigconnect.biggraph.BigGraphParams;
import io.bigconnect.biggraph.backend.id.Id;
import io.bigconnect.biggraph.backend.id.IdGenerator;
import io.bigconnect.biggraph.schema.IndexLabel;
import io.bigconnect.biggraph.schema.PropertyKey;
import io.bigconnect.biggraph.schema.SchemaManager;
import io.bigconnect.biggraph.schema.VertexLabel;
import io.bigconnect.biggraph.type.BigType;
import io.bigconnect.biggraph.type.define.Cardinality;
import io.bigconnect.biggraph.type.define.DataType;
import io.bigconnect.biggraph.type.define.NodeRole;
import io.bigconnect.biggraph.type.define.SerialEnum;
import io.bigconnect.biggraph.util.DateUtil;
import io.bigconnect.biggraph.util.E;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.VertexProperty;

import java.util.*;

public class BigServerInfo {

    // Unit millisecond
    private static final long EXPIRED_INTERVAL =
                              TaskManager.SCHEDULE_PERIOD * 1000L * 3;

    private Id id;
    private NodeRole role;
    private int maxLoad;
    private int load;
    private Date updateTime;

    private transient boolean updated = false;

    public BigServerInfo(String name, NodeRole role) {
        this(IdGenerator.of(name), role);
    }

    public BigServerInfo(Id id) {
        this.id = id;
        this.role = NodeRole.WORKER;
        this.maxLoad = 0;
        this.load = 0;
        this.updateTime = DateUtil.now();
    }

    public BigServerInfo(Id id, NodeRole role) {
        this.id = id;
        this.load = 0;
        this.role = role;
        this.updateTime = DateUtil.now();
    }

    public Id id() {
        return this.id;
    }

    public String name() {
        return this.id.asString();
    }

    public NodeRole role() {
        return this.role;
    }

    public void role(NodeRole role) {
        this.role = role;
    }

    public int maxLoad() {
        return this.maxLoad;
    }

    public void maxLoad(int maxLoad) {
        this.maxLoad = maxLoad;
    }

    public int load() {
        return this.load;
    }

    public void load(int load) {
        this.load = load;
    }

    public void increaseLoad(int delta) {
        this.load += delta;
        this.updated = true;
    }

    public Date updateTime() {
        return this.updateTime;
    }

    public void updateTime(Date updateTime) {
        this.updateTime = updateTime;
    }

    public boolean alive() {
        long now = DateUtil.now().getTime();
        return this.updateTime != null &&
               this.updateTime.getTime() + EXPIRED_INTERVAL > now;
    }

    public boolean updated() {
        return this.updated;
    }

    @Override
    public String toString() {
        return String.format("HugeServerInfo(%s)%s", this.id, this.asMap());
    }

    protected boolean property(String key, Object value) {
        switch (key) {
            case P.ROLE:
                this.role = SerialEnum.fromCode(NodeRole.class, (byte) value);
                break;
            case P.MAX_LOAD:
                this.maxLoad = (int) value;
                break;
            case P.LOAD:
                this.load = (int) value;
                break;
            case P.UPDATE_TIME:
                this.updateTime = (Date) value;
                break;
            default:
                throw new AssertionError("Unsupported key: " + key);
        }
        return true;
    }

    protected Object[] asArray() {
        E.checkState(this.id != null, "Server id can't be null");

        List<Object> list = new ArrayList<>(12);

        list.add(T.label);
        list.add(P.SERVER);

        list.add(T.id);
        list.add(this.id);

        list.add(P.ROLE);
        list.add(this.role.code());

        list.add(P.MAX_LOAD);
        list.add(this.maxLoad);

        list.add(P.LOAD);
        list.add(this.load);

        list.add(P.UPDATE_TIME);
        list.add(this.updateTime);

        return list.toArray();
    }

    public Map<String, Object> asMap() {
        E.checkState(this.id != null, "Server id can't be null");

        Map<String, Object> map = new HashMap<>();

        map.put(Graph.Hidden.unHide(P.ID), this.id);
        map.put(Graph.Hidden.unHide(P.LABEL), P.SERVER);
        map.put(Graph.Hidden.unHide(P.ROLE), this.role);
        map.put(Graph.Hidden.unHide(P.MAX_LOAD), this.maxLoad);
        map.put(Graph.Hidden.unHide(P.LOAD), this.load);
        map.put(Graph.Hidden.unHide(P.UPDATE_TIME), this.updateTime);

        return map;
    }

    public static BigServerInfo fromVertex(Vertex vertex) {
        BigServerInfo serverInfo = new BigServerInfo((Id) vertex.id());
        for (Iterator<VertexProperty<Object>> iter = vertex.properties();
             iter.hasNext();) {
            VertexProperty<Object> prop = iter.next();
            serverInfo.property(prop.key(), prop.value());
        }
        return serverInfo;
    }

    public <V> boolean suitableFor(BigTask<V> task, long now) {
        if (task.computer() != this.role.computer()) {
            return false;
        }
        if (this.updateTime.getTime() + EXPIRED_INTERVAL < now ||
            this.load() + task.load() > this.maxLoad) {
            return false;
        }
        return true;
    }

    public static Schema schema(BigGraphParams graph) {
        return new Schema(graph);
    }

    public static final class P {

        public static final String SERVER = Graph.Hidden.hide("server");

        public static final String ID = T.id.getAccessor();
        public static final String LABEL = T.label.getAccessor();

        public static final String NAME = "~server_name";
        public static final String ROLE = "~server_role";
        public static final String LOAD = "~server_load";
        public static final String MAX_LOAD = "~server_max_load";
        public static final String UPDATE_TIME = "~server_update_time";

        public static String unhide(String key) {
            final String prefix = Graph.Hidden.hide("server_");
            if (key.startsWith(prefix)) {
                return key.substring(prefix.length());
            }
            return key;
        }
    }

    public static final class Schema {

        public static final String SERVER = P.SERVER;

        protected final BigGraphParams graph;

        public Schema(BigGraphParams graph) {
            this.graph = graph;
        }

        public void initSchemaIfNeeded() {
            if (this.existVertexLabel(SERVER)) {
                return;
            }

            BigGraph graph = this.graph.graph();
            String[] properties = this.initProperties();

            // Create vertex label '~server'
            VertexLabel label = graph.schema().vertexLabel(SERVER)
                                     .properties(properties)
                                     .useCustomizeStringId()
                                     .nullableKeys(P.ROLE, P.MAX_LOAD,
                                                   P.LOAD, P.UPDATE_TIME)
                                     .enableLabelIndex(true)
                                     .build();
            this.graph.schemaTransaction().addVertexLabel(label);
        }

        private String[] initProperties() {
            List<String> props = new ArrayList<>();

            props.add(createPropertyKey(P.ROLE, DataType.BYTE));
            props.add(createPropertyKey(P.MAX_LOAD, DataType.INT));
            props.add(createPropertyKey(P.LOAD, DataType.INT));
            props.add(createPropertyKey(P.UPDATE_TIME, DataType.DATE));

            return props.toArray(new String[0]);
        }

        public boolean existVertexLabel(String label) {
            return this.graph.schemaTransaction()
                       .getVertexLabel(label) != null;
        }

        @SuppressWarnings("unused")
        private String createPropertyKey(String name) {
            return this.createPropertyKey(name, DataType.TEXT);
        }

        private String createPropertyKey(String name, DataType dataType) {
            return this.createPropertyKey(name, dataType, Cardinality.SINGLE);
        }

        private String createPropertyKey(String name, DataType dataType,
                                         Cardinality cardinality) {
            SchemaManager schema = this.graph.graph().schema();
            PropertyKey propertyKey = schema.propertyKey(name)
                                            .dataType(dataType)
                                            .cardinality(cardinality)
                                            .build();
            this.graph.schemaTransaction().addPropertyKey(propertyKey);
            return name;
        }

        @SuppressWarnings("unused")
        private IndexLabel createIndexLabel(VertexLabel label, String field) {
            SchemaManager schema = this.graph.graph().schema();
            String name = Graph.Hidden.hide("server-index-by-" + field);
            IndexLabel indexLabel = schema.indexLabel(name)
                                          .on(BigType.VERTEX_LABEL, SERVER)
                                          .by(field)
                                          .build();
            this.graph.schemaTransaction().addIndexLabel(label, indexLabel);
            return indexLabel;
        }

        @SuppressWarnings("unused")
        private IndexLabel indexLabel(String field) {
            String name = Graph.Hidden.hide("server-index-by-" + field);
            return this.graph.graph().indexLabel(name);
        }
    }
}
