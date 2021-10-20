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

package io.bigconnect.biggraph.io;

import io.bigconnect.biggraph.BigGraph;
import io.bigconnect.biggraph.schema.EdgeLabel;
import io.bigconnect.biggraph.schema.IndexLabel;
import io.bigconnect.biggraph.schema.PropertyKey;
import io.bigconnect.biggraph.schema.VertexLabel;
import io.bigconnect.biggraph.type.BigType;
import io.bigconnect.biggraph.type.define.BigKeys;

import java.util.LinkedHashMap;
import java.util.Map;

public class GraphSONSchemaSerializer {

    public Map<BigKeys, Object> writeVertexLabel(VertexLabel vertexLabel) {
        BigGraph graph = vertexLabel.graph();
        assert graph != null;

        Map<BigKeys, Object> map = new LinkedHashMap<>();
        map.put(BigKeys.ID, vertexLabel.id().asLong());
        map.put(BigKeys.NAME, vertexLabel.name());
        map.put(BigKeys.ID_STRATEGY, vertexLabel.idStrategy());
        map.put(BigKeys.PRIMARY_KEYS,
                graph.mapPkId2Name(vertexLabel.primaryKeys()));
        map.put(BigKeys.NULLABLE_KEYS,
                graph.mapPkId2Name(vertexLabel.nullableKeys()));
        map.put(BigKeys.INDEX_LABELS,
                graph.mapIlId2Name(vertexLabel.indexLabels()));
        map.put(BigKeys.PROPERTIES,
                graph.mapPkId2Name(vertexLabel.properties()));
        map.put(BigKeys.STATUS, vertexLabel.status());
        map.put(BigKeys.TTL, vertexLabel.ttl());
        String ttlStartTimeName = vertexLabel.ttlStartTimeName();
        if (ttlStartTimeName != null) {
            map.put(BigKeys.TTL_START_TIME, ttlStartTimeName);
        }
        map.put(BigKeys.ENABLE_LABEL_INDEX, vertexLabel.enableLabelIndex());
        map.put(BigKeys.USER_DATA, vertexLabel.userdata());
        return map;
    }

    public Map<BigKeys, Object> writeEdgeLabel(EdgeLabel edgeLabel) {
        BigGraph graph = edgeLabel.graph();
        assert graph != null;

        Map<BigKeys, Object> map = new LinkedHashMap<>();
        map.put(BigKeys.ID, edgeLabel.id().asLong());
        map.put(BigKeys.NAME, edgeLabel.name());
        map.put(BigKeys.SOURCE_LABEL, edgeLabel.sourceLabelName());
        map.put(BigKeys.TARGET_LABEL, edgeLabel.targetLabelName());
        map.put(BigKeys.FREQUENCY, edgeLabel.frequency());
        map.put(BigKeys.SORT_KEYS,
                graph.mapPkId2Name(edgeLabel.sortKeys()));
        map.put(BigKeys.NULLABLE_KEYS,
                graph.mapPkId2Name(edgeLabel.nullableKeys()));
        map.put(BigKeys.INDEX_LABELS,
                graph.mapIlId2Name(edgeLabel.indexLabels()));
        map.put(BigKeys.PROPERTIES,
                graph.mapPkId2Name(edgeLabel.properties()));
        map.put(BigKeys.STATUS, edgeLabel.status());
        map.put(BigKeys.TTL, edgeLabel.ttl());
        String ttlStartTimeName = edgeLabel.ttlStartTimeName();
        if (ttlStartTimeName != null) {
            map.put(BigKeys.TTL_START_TIME, ttlStartTimeName);
        }
        map.put(BigKeys.ENABLE_LABEL_INDEX, edgeLabel.enableLabelIndex());
        map.put(BigKeys.USER_DATA, edgeLabel.userdata());
        return map;
    }

    public Map<BigKeys, Object> writePropertyKey(PropertyKey propertyKey) {
        BigGraph graph = propertyKey.graph();
        assert graph != null;

        Map<BigKeys, Object> map = new LinkedHashMap<>();
        map.put(BigKeys.ID, propertyKey.id().asLong());
        map.put(BigKeys.NAME, propertyKey.name());
        map.put(BigKeys.DATA_TYPE, propertyKey.dataType());
        map.put(BigKeys.CARDINALITY, propertyKey.cardinality());
        map.put(BigKeys.AGGREGATE_TYPE, propertyKey.aggregateType());
        map.put(BigKeys.WRITE_TYPE, propertyKey.writeType());
        map.put(BigKeys.PROPERTIES,
                graph.mapPkId2Name(propertyKey.properties()));
        map.put(BigKeys.STATUS, propertyKey.status());
        map.put(BigKeys.USER_DATA, propertyKey.userdata());
        return map;
    }

    public Map<BigKeys, Object> writeIndexLabel(IndexLabel indexLabel) {
        BigGraph graph = indexLabel.graph();
        assert graph != null;

        Map<BigKeys, Object> map = new LinkedHashMap<>();
        map.put(BigKeys.ID, indexLabel.id().asLong());
        map.put(BigKeys.NAME, indexLabel.name());
        map.put(BigKeys.BASE_TYPE, indexLabel.baseType());
        if (indexLabel.baseType() == BigType.VERTEX_LABEL) {
            map.put(BigKeys.BASE_VALUE,
                    graph.vertexLabel(indexLabel.baseValue()).name());
        } else {
            assert indexLabel.baseType() == BigType.EDGE_LABEL;
            map.put(BigKeys.BASE_VALUE,
                    graph.edgeLabel(indexLabel.baseValue()).name());
        }
        map.put(BigKeys.INDEX_TYPE, indexLabel.indexType());
        map.put(BigKeys.FIELDS, graph.mapPkId2Name(indexLabel.indexFields()));
        map.put(BigKeys.STATUS, indexLabel.status());
        map.put(BigKeys.USER_DATA, indexLabel.userdata());
        return map;
    }
}
