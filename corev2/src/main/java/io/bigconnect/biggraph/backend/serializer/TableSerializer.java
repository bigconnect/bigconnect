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

package io.bigconnect.biggraph.backend.serializer;

import io.bigconnect.biggraph.BigGraph;
import io.bigconnect.biggraph.backend.BackendException;
import io.bigconnect.biggraph.backend.id.EdgeId;
import io.bigconnect.biggraph.backend.id.Id;
import io.bigconnect.biggraph.backend.id.IdGenerator;
import io.bigconnect.biggraph.backend.id.IdUtil;
import io.bigconnect.biggraph.backend.query.Condition;
import io.bigconnect.biggraph.backend.query.ConditionQuery;
import io.bigconnect.biggraph.backend.query.Query;
import io.bigconnect.biggraph.backend.store.BackendEntry;
import io.bigconnect.biggraph.schema.*;
import io.bigconnect.biggraph.structure.*;
import io.bigconnect.biggraph.type.BigType;
import io.bigconnect.biggraph.type.define.*;
import io.bigconnect.biggraph.util.E;
import io.bigconnect.biggraph.util.JsonUtil;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Set;

public abstract class TableSerializer extends AbstractSerializer {

    @Override
    public TableBackendEntry newBackendEntry(BigType type, Id id) {
        return new TableBackendEntry(type, id);
    }

    protected TableBackendEntry newBackendEntry(BigElement e) {
        return newBackendEntry(e.type(), e.id());
    }

    protected TableBackendEntry newBackendEntry(SchemaElement e) {
        return newBackendEntry(e.type(), e.id());
    }

    protected TableBackendEntry newBackendEntry(BigIndex index) {
        return newBackendEntry(index.type(), index.id());
    }

    protected abstract TableBackendEntry newBackendEntry(TableBackendEntry.Row row);

    @Override
    protected abstract TableBackendEntry convertEntry(BackendEntry backendEntry);

    protected void formatProperty(BigProperty<?> property,
                                  TableBackendEntry.Row row) {
        long pkid = property.propertyKey().id().asLong();
        row.column(BigKeys.PROPERTIES, pkid, this.writeProperty(property));
    }

    protected void parseProperty(Id key, Object colValue, BigElement owner) {
        // Get PropertyKey by PropertyKey id
        PropertyKey pkey = owner.graph().propertyKey(key);

        // Parse value
        Object value = this.readProperty(pkey, colValue);

        // Set properties of vertex/edge
        if (pkey.cardinality() == Cardinality.SINGLE) {
            owner.addProperty(pkey, value);
        } else {
            if (!(value instanceof Collection)) {
                throw new BackendException(
                          "Invalid value of non-single property: %s", value);
            }
            owner.addProperty(pkey, value);
        }
    }

    protected Object writeProperty(BigProperty<?> property) {
        return this.writeProperty(property.propertyKey(), property.value());
    }

    protected Object writeProperty(PropertyKey propertyKey, Object value) {
        return JsonUtil.toJson(value);
    }

    @SuppressWarnings("unchecked")
    protected <T> T readProperty(PropertyKey pkey, Object value) {
        Class<T> clazz = (Class<T>) pkey.implementClazz();
        T result = JsonUtil.fromJson(value.toString(), clazz);
        if (pkey.cardinality() != Cardinality.SINGLE) {
            Collection<?> values = (Collection<?>) result;
            List<Object> newValues = new ArrayList<>(values.size());
            for (Object v : values) {
                newValues.add(JsonUtil.castNumber(v, pkey.dataType().clazz()));
            }
            result = (T) newValues;
        }
        return result;
    }

    protected TableBackendEntry.Row formatEdge(BigEdge edge) {
        EdgeId id = edge.idWithDirection();
        TableBackendEntry.Row row = new TableBackendEntry.Row(edge.type(), id);
        if (edge.hasTtl()) {
            row.ttl(edge.ttl());
            row.column(BigKeys.EXPIRED_TIME, edge.expiredTime());
        }
        // Id: ownerVertex + direction + edge-label + sortValues + otherVertex
        row.column(BigKeys.OWNER_VERTEX, this.writeId(id.ownerVertexId()));
        row.column(BigKeys.DIRECTION, id.directionCode());
        row.column(BigKeys.LABEL, id.edgeLabelId().asLong());
        row.column(BigKeys.SORT_VALUES, id.sortValues());
        row.column(BigKeys.OTHER_VERTEX, this.writeId(id.otherVertexId()));

        this.formatProperties(edge, row);
        return row;
    }

    /**
     * Parse an edge from a entry row
     * @param row edge entry
     * @param vertex null or the source vertex
     * @param graph the HugeGraph context object
     * @return the source vertex
     */
    protected BigEdge parseEdge(TableBackendEntry.Row row,
                                BigVertex vertex, BigGraph graph) {
        Object ownerVertexId = row.column(BigKeys.OWNER_VERTEX);
        Number dir = row.column(BigKeys.DIRECTION);
        boolean direction = EdgeId.isOutDirectionFromCode(dir.byteValue());
        Number label = row.column(BigKeys.LABEL);
        String sortValues = row.column(BigKeys.SORT_VALUES);
        Object otherVertexId = row.column(BigKeys.OTHER_VERTEX);
        Number expiredTime = row.column(BigKeys.EXPIRED_TIME);

        if (vertex == null) {
            Id ownerId = this.readId(ownerVertexId);
            vertex = new BigVertex(graph, ownerId, VertexLabel.NONE);
        }

        EdgeLabel edgeLabel = graph.edgeLabelOrNone(this.toId(label));
        Id otherId = this.readId(otherVertexId);

        // Construct edge
        BigEdge edge = BigEdge.constructEdge(vertex, direction, edgeLabel,
                                               sortValues, otherId);

        // Parse edge properties
        this.parseProperties(edge, row);

        // The expired time is null when the edge is non-ttl
        long expired = edge.hasTtl() ? expiredTime.longValue() : 0L;
        edge.expiredTime(expired);

        return edge;
    }

    @Override
    public BackendEntry writeVertex(BigVertex vertex) {
        if (vertex.olap()) {
            return this.writeOlapVertex(vertex);
        }
        TableBackendEntry entry = newBackendEntry(vertex);
        if (vertex.hasTtl()) {
            entry.ttl(vertex.ttl());
            entry.column(BigKeys.EXPIRED_TIME, vertex.expiredTime());
        }
        entry.column(BigKeys.ID, this.writeId(vertex.id()));
        entry.column(BigKeys.LABEL, vertex.schemaLabel().id().asLong());
        // Add all properties of a Vertex
        this.formatProperties(vertex, entry.row());
        return entry;
    }

    @Override
    public BackendEntry writeVertexProperty(BigVertexProperty<?> prop) {
        BigVertex vertex = prop.element();
        TableBackendEntry entry = newBackendEntry(vertex);
        if (vertex.hasTtl()) {
            entry.ttl(vertex.ttl());
            entry.column(BigKeys.EXPIRED_TIME, vertex.expiredTime());
        }
        entry.subId(IdGenerator.of(prop.key()));
        entry.column(BigKeys.ID, this.writeId(vertex.id()));
        entry.column(BigKeys.LABEL, vertex.schemaLabel().id().asLong());

        this.formatProperty(prop, entry.row());
        return entry;
    }

    @Override
    public BigVertex readVertex(BigGraph graph, BackendEntry backendEntry) {
        E.checkNotNull(graph, "serializer graph");
        if (backendEntry == null) {
            return null;
        }

        TableBackendEntry entry = this.convertEntry(backendEntry);
        assert entry.type().isVertex();

        Id id = this.readId(entry.column(BigKeys.ID));
        Number label = entry.column(BigKeys.LABEL);
        Number expiredTime = entry.column(BigKeys.EXPIRED_TIME);

        VertexLabel vertexLabel = VertexLabel.NONE;
        if (label != null) {
            vertexLabel = graph.vertexLabelOrNone(this.toId(label));
        }
        BigVertex vertex = new BigVertex(graph, id, vertexLabel);

        // Parse all properties of a Vertex
        this.parseProperties(vertex, entry.row());
        // Parse all edges of a Vertex
        for (TableBackendEntry.Row edge : entry.subRows()) {
            this.parseEdge(edge, vertex, graph);
        }
        // The expired time is null when this is fake vertex of edge or non-ttl
        if (expiredTime != null) {
            vertex.expiredTime(expiredTime.longValue());
        }
        return vertex;
    }

    @Override
    public BackendEntry writeEdge(BigEdge edge) {
        return newBackendEntry(this.formatEdge(edge));
    }

    @Override
    public BackendEntry writeEdgeProperty(BigEdgeProperty<?> prop) {
        BigEdge edge = prop.element();
        EdgeId id = edge.idWithDirection();
        TableBackendEntry.Row row = new TableBackendEntry.Row(edge.type(), id);
        if (edge.hasTtl()) {
            row.ttl(edge.ttl());
            row.column(BigKeys.EXPIRED_TIME, edge.expiredTime());
        }
        // Id: ownerVertex + direction + edge-label + sortValues + otherVertex
        row.column(BigKeys.OWNER_VERTEX, this.writeId(id.ownerVertexId()));
        row.column(BigKeys.DIRECTION, id.directionCode());
        row.column(BigKeys.LABEL, id.edgeLabelId().asLong());
        row.column(BigKeys.SORT_VALUES, id.sortValues());
        row.column(BigKeys.OTHER_VERTEX, this.writeId(id.otherVertexId()));

        // Format edge property
        this.formatProperty(prop, row);

        TableBackendEntry entry = newBackendEntry(row);
        entry.subId(IdGenerator.of(prop.key()));
        return entry;
    }

    @Override
    public BigEdge readEdge(BigGraph graph, BackendEntry backendEntry) {
        E.checkNotNull(graph, "serializer graph");
        if (backendEntry == null) {
            return null;
        }

        TableBackendEntry entry = this.convertEntry(backendEntry);
        return this.parseEdge(entry.row(), null, graph);
    }

    @Override
    public BackendEntry writeIndex(BigIndex index) {
        TableBackendEntry entry = newBackendEntry(index);
        /*
         * When field-values is null and elementIds size is 0, it is
         * meaningful for deletion of index data in secondary/range index.
         */
        if (index.fieldValues() == null && index.elementIds().size() == 0) {
            entry.column(BigKeys.INDEX_LABEL_ID, index.indexLabel().longId());
        } else {
            entry.column(BigKeys.FIELD_VALUES, index.fieldValues());
            entry.column(BigKeys.INDEX_LABEL_ID, index.indexLabel().longId());
            entry.column(BigKeys.ELEMENT_IDS, this.writeId(index.elementId()));
            entry.subId(index.elementId());
            if (index.hasTtl()) {
                entry.ttl(index.ttl());
                entry.column(BigKeys.EXPIRED_TIME, index.expiredTime());
            }
        }
        return entry;
    }

    @Override
    public BigIndex readIndex(BigGraph graph, ConditionQuery query,
                              BackendEntry backendEntry) {
        E.checkNotNull(graph, "serializer graph");
        if (backendEntry == null) {
            return null;
        }

        TableBackendEntry entry = this.convertEntry(backendEntry);

        Object indexValues = entry.column(BigKeys.FIELD_VALUES);
        Number indexLabelId = entry.column(BigKeys.INDEX_LABEL_ID);
        Set<Object> elemIds = this.parseIndexElemIds(entry);
        Number expiredTime = entry.column(BigKeys.EXPIRED_TIME);

        IndexLabel indexLabel = graph.indexLabel(this.toId(indexLabelId));
        BigIndex index = new BigIndex(graph, indexLabel);
        index.fieldValues(indexValues);
        long expired = index.hasTtl() ? expiredTime.longValue() : 0L;
        for (Object elemId : elemIds) {
            index.elementIds(this.readId(elemId), expired);
        }
        return index;
    }

    @Override
    public BackendEntry writeId(BigType type, Id id) {
        return newBackendEntry(type, id);
    }

    @Override
    protected Id writeQueryId(BigType type, Id id) {
        if (type.isEdge()) {
            if (!(id instanceof EdgeId)) {
                id = EdgeId.parse(id.asString());
            }
        } else if (type.isGraph()) {
            id = IdGenerator.of(this.writeId(id));
        }
        return id;
    }

    @Override
    protected Query writeQueryEdgeCondition(Query query) {
        ConditionQuery result = (ConditionQuery) query;
        for (Condition.Relation r : result.relations()) {
            Object value = r.value();
            if (value instanceof Id) {
                if (r.key() == BigKeys.OWNER_VERTEX ||
                    r.key() == BigKeys.OTHER_VERTEX) {
                    // Serialize vertex id
                    r.serialValue(this.writeId((Id) value));
                } else {
                    // Serialize label id
                    r.serialValue(((Id) value).asObject());
                }
            } else if (value instanceof Directions) {
                r.serialValue(((Directions) value).type().code());
            }
        }
        return null;
    }

    @Override
    protected Query writeQueryCondition(Query query) {
        ConditionQuery result = (ConditionQuery) query;
        // No user-prop when serialize
        assert result.allSysprop();
        for (Condition.Relation r : result.relations()) {
            if (!(r.value().equals(r.serialValue()))) {
                continue;
            }
            if (r.relation() == Condition.RelationType.IN) {
                List<?> values = (List<?>) r.value();
                List<Object> serializedValues = new ArrayList<>(values.size());
                for (Object v : values) {
                    serializedValues.add(this.serializeValue(v));
                }
                r.serialValue(serializedValues);
            } else {
                r.serialValue(this.serializeValue(r.value()));
            }

            if (query.resultType().isGraph() &&
                r.relation() == Condition.RelationType.CONTAINS_VALUE) {
                r.serialValue(this.writeProperty(null, r.serialValue()));
            }
        }

        return query;
    }

    @Override
    public BackendEntry writeVertexLabel(VertexLabel vertexLabel) {
        TableBackendEntry entry = newBackendEntry(vertexLabel);
        entry.column(BigKeys.ID, vertexLabel.id().asLong());
        entry.column(BigKeys.NAME, vertexLabel.name());
        entry.column(BigKeys.ID_STRATEGY, vertexLabel.idStrategy().code());
        entry.column(BigKeys.PROPERTIES,
                     this.toLongSet(vertexLabel.properties()));
        entry.column(BigKeys.PRIMARY_KEYS,
                     this.toLongList(vertexLabel.primaryKeys()));
        entry.column(BigKeys.NULLABLE_KEYS,
                     this.toLongSet(vertexLabel.nullableKeys()));
        entry.column(BigKeys.INDEX_LABELS,
                     this.toLongSet(vertexLabel.indexLabels()));
        this.writeEnableLabelIndex(vertexLabel, entry);
        this.writeUserdata(vertexLabel, entry);
        entry.column(BigKeys.STATUS, vertexLabel.status().code());
        entry.column(BigKeys.TTL, vertexLabel.ttl());
        entry.column(BigKeys.TTL_START_TIME,
                     vertexLabel.ttlStartTime().asLong());
        return entry;
    }

    @Override
    public BackendEntry writeEdgeLabel(EdgeLabel edgeLabel) {
        TableBackendEntry entry = newBackendEntry(edgeLabel);
        entry.column(BigKeys.ID, edgeLabel.id().asLong());
        entry.column(BigKeys.NAME, edgeLabel.name());
        entry.column(BigKeys.FREQUENCY, edgeLabel.frequency().code());
        entry.column(BigKeys.SOURCE_LABEL, edgeLabel.sourceLabel().asLong());
        entry.column(BigKeys.TARGET_LABEL, edgeLabel.targetLabel().asLong());
        entry.column(BigKeys.PROPERTIES,
                     this.toLongSet(edgeLabel.properties()));
        entry.column(BigKeys.SORT_KEYS,
                     this.toLongList(edgeLabel.sortKeys()));
        entry.column(BigKeys.NULLABLE_KEYS,
                     this.toLongSet(edgeLabel.nullableKeys()));
        entry.column(BigKeys.INDEX_LABELS,
                     this.toLongSet(edgeLabel.indexLabels()));
        this.writeEnableLabelIndex(edgeLabel, entry);
        this.writeUserdata(edgeLabel, entry);
        entry.column(BigKeys.STATUS, edgeLabel.status().code());
        entry.column(BigKeys.TTL, edgeLabel.ttl());
        entry.column(BigKeys.TTL_START_TIME,
                     edgeLabel.ttlStartTime().asLong());
        return entry;
    }

    @Override
    public BackendEntry writePropertyKey(PropertyKey propertyKey) {
        TableBackendEntry entry = newBackendEntry(propertyKey);
        entry.column(BigKeys.ID, propertyKey.id().asLong());
        entry.column(BigKeys.NAME, propertyKey.name());
        entry.column(BigKeys.DATA_TYPE, propertyKey.dataType().code());
        entry.column(BigKeys.CARDINALITY, propertyKey.cardinality().code());
        entry.column(BigKeys.AGGREGATE_TYPE,
                     propertyKey.aggregateType().code());
        entry.column(BigKeys.WRITE_TYPE,
                     propertyKey.writeType().code());
        entry.column(BigKeys.PROPERTIES,
                     this.toLongSet(propertyKey.properties()));
        this.writeUserdata(propertyKey, entry);
        entry.column(BigKeys.STATUS, propertyKey.status().code());
        return entry;
    }

    @Override
    public VertexLabel readVertexLabel(BigGraph graph,
                                       BackendEntry backendEntry) {
        if (backendEntry == null) {
            return null;
        }

        TableBackendEntry entry = this.convertEntry(backendEntry);

        Number id = schemaColumn(entry, BigKeys.ID);
        String name = schemaColumn(entry, BigKeys.NAME);
        IdStrategy idStrategy = schemaEnum(entry, BigKeys.ID_STRATEGY,
                                           IdStrategy.class);
        Object properties = schemaColumn(entry, BigKeys.PROPERTIES);
        Object primaryKeys = schemaColumn(entry, BigKeys.PRIMARY_KEYS);
        Object nullableKeys = schemaColumn(entry, BigKeys.NULLABLE_KEYS);
        Object indexLabels = schemaColumn(entry, BigKeys.INDEX_LABELS);
        SchemaStatus status = schemaEnum(entry, BigKeys.STATUS,
                                         SchemaStatus.class);
        Number ttl = schemaColumn(entry, BigKeys.TTL);
        Number ttlStartTime = schemaColumn(entry, BigKeys.TTL_START_TIME);

        VertexLabel vertexLabel = new VertexLabel(graph, this.toId(id), name);
        vertexLabel.idStrategy(idStrategy);
        vertexLabel.properties(this.toIdArray(properties));
        vertexLabel.primaryKeys(this.toIdArray(primaryKeys));
        vertexLabel.nullableKeys(this.toIdArray(nullableKeys));
        vertexLabel.indexLabels(this.toIdArray(indexLabels));
        vertexLabel.status(status);
        vertexLabel.ttl(ttl.longValue());
        vertexLabel.ttlStartTime(this.toId(ttlStartTime));
        this.readEnableLabelIndex(vertexLabel, entry);
        this.readUserdata(vertexLabel, entry);
        return vertexLabel;
    }

    @Override
    public EdgeLabel readEdgeLabel(BigGraph graph, BackendEntry backendEntry) {
        if (backendEntry == null) {
            return null;
        }

        TableBackendEntry entry = this.convertEntry(backendEntry);

        Number id = schemaColumn(entry, BigKeys.ID);
        String name = schemaColumn(entry, BigKeys.NAME);
        Frequency frequency = schemaEnum(entry, BigKeys.FREQUENCY,
                                         Frequency.class);
        Number sourceLabel = schemaColumn(entry, BigKeys.SOURCE_LABEL);
        Number targetLabel = schemaColumn(entry, BigKeys.TARGET_LABEL);
        Object sortKeys = schemaColumn(entry, BigKeys.SORT_KEYS);
        Object nullableKeys = schemaColumn(entry, BigKeys.NULLABLE_KEYS);
        Object properties = schemaColumn(entry, BigKeys.PROPERTIES);
        Object indexLabels = schemaColumn(entry, BigKeys.INDEX_LABELS);
        SchemaStatus status = schemaEnum(entry, BigKeys.STATUS,
                                         SchemaStatus.class);
        Number ttl = schemaColumn(entry, BigKeys.TTL);
        Number ttlStartTime = schemaColumn(entry, BigKeys.TTL_START_TIME);

        EdgeLabel edgeLabel = new EdgeLabel(graph, this.toId(id), name);
        edgeLabel.frequency(frequency);
        edgeLabel.sourceLabel(this.toId(sourceLabel));
        edgeLabel.targetLabel(this.toId(targetLabel));
        edgeLabel.properties(this.toIdArray(properties));
        edgeLabel.sortKeys(this.toIdArray(sortKeys));
        edgeLabel.nullableKeys(this.toIdArray(nullableKeys));
        edgeLabel.indexLabels(this.toIdArray(indexLabels));
        edgeLabel.status(status);
        edgeLabel.ttl(ttl.longValue());
        edgeLabel.ttlStartTime(this.toId(ttlStartTime));
        this.readEnableLabelIndex(edgeLabel, entry);
        this.readUserdata(edgeLabel, entry);
        return edgeLabel;
    }

    @Override
    public PropertyKey readPropertyKey(BigGraph graph,
                                       BackendEntry backendEntry) {
        if (backendEntry == null) {
            return null;
        }

        TableBackendEntry entry = this.convertEntry(backendEntry);

        Number id = schemaColumn(entry, BigKeys.ID);
        String name = schemaColumn(entry, BigKeys.NAME);
        DataType dataType = schemaEnum(entry, BigKeys.DATA_TYPE,
                                       DataType.class);
        Cardinality cardinality = schemaEnum(entry, BigKeys.CARDINALITY,
                                             Cardinality.class);
        AggregateType aggregateType = schemaEnum(entry, BigKeys.AGGREGATE_TYPE,
                                                 AggregateType.class);
        WriteType writeType = schemaEnumOrDefault(
                              entry, BigKeys.WRITE_TYPE,
                              WriteType.class, WriteType.OLTP);
        Object properties = schemaColumn(entry, BigKeys.PROPERTIES);
        SchemaStatus status = schemaEnum(entry, BigKeys.STATUS,
                                         SchemaStatus.class);

        PropertyKey propertyKey = new PropertyKey(graph, this.toId(id), name);
        propertyKey.dataType(dataType);
        propertyKey.cardinality(cardinality);
        propertyKey.aggregateType(aggregateType);
        propertyKey.writeType(writeType);
        propertyKey.properties(this.toIdArray(properties));
        propertyKey.status(status);
        this.readUserdata(propertyKey, entry);
        return propertyKey;
    }

    @Override
    public BackendEntry writeIndexLabel(IndexLabel indexLabel) {
        TableBackendEntry entry = newBackendEntry(indexLabel);
        entry.column(BigKeys.ID, indexLabel.id().asLong());
        entry.column(BigKeys.NAME, indexLabel.name());
        entry.column(BigKeys.BASE_TYPE, indexLabel.baseType().code());
        entry.column(BigKeys.BASE_VALUE, indexLabel.baseValue().asLong());
        entry.column(BigKeys.INDEX_TYPE, indexLabel.indexType().code());
        entry.column(BigKeys.FIELDS,
                     this.toLongList(indexLabel.indexFields()));
        this.writeUserdata(indexLabel, entry);
        entry.column(BigKeys.STATUS, indexLabel.status().code());
        return entry;
    }

    @Override
    public IndexLabel readIndexLabel(BigGraph graph,
                                     BackendEntry backendEntry) {
        if (backendEntry == null) {
            return null;
        }

        TableBackendEntry entry = this.convertEntry(backendEntry);

        Number id = schemaColumn(entry, BigKeys.ID);
        String name = schemaColumn(entry, BigKeys.NAME);
        BigType baseType = schemaEnum(entry, BigKeys.BASE_TYPE,
                                       BigType.class);
        Number baseValueId = schemaColumn(entry, BigKeys.BASE_VALUE);
        IndexType indexType = schemaEnum(entry, BigKeys.INDEX_TYPE,
                                         IndexType.class);
        Object indexFields = schemaColumn(entry, BigKeys.FIELDS);
        SchemaStatus status = schemaEnum(entry, BigKeys.STATUS,
                                         SchemaStatus.class);

        IndexLabel indexLabel = new IndexLabel(graph, this.toId(id), name);
        indexLabel.baseType(baseType);
        indexLabel.baseValue(this.toId(baseValueId));
        indexLabel.indexType(indexType);
        indexLabel.indexFields(this.toIdArray(indexFields));
        indexLabel.status(status);
        this.readUserdata(indexLabel, entry);
        return indexLabel;
    }

    protected abstract Id toId(Number number);

    protected abstract Id[] toIdArray(Object object);

    protected abstract Object toLongSet(Collection<Id> ids);

    protected abstract Object toLongList(Collection<Id> ids);

    protected abstract Set<Object> parseIndexElemIds(TableBackendEntry entry);

    protected abstract void formatProperties(BigElement element,
                                             TableBackendEntry.Row row);

    protected abstract void parseProperties(BigElement element,
                                            TableBackendEntry.Row row);

    protected Object writeId(Id id) {
        return IdUtil.writeStoredString(id);
    }

    protected Id readId(Object id) {
        return IdUtil.readStoredString(id.toString());
    }

    protected Object serializeValue(Object value) {
        if (value instanceof Id) {
            value = ((Id) value).asObject();
        }
        return value;
    }

    protected void writeEnableLabelIndex(SchemaLabel schema,
                                         TableBackendEntry entry) {
        entry.column(BigKeys.ENABLE_LABEL_INDEX, schema.enableLabelIndex());
    }

    protected void readEnableLabelIndex(SchemaLabel schema,
                                        TableBackendEntry entry) {
        Boolean enableLabelIndex = schemaColumn(entry,
                                                BigKeys.ENABLE_LABEL_INDEX);
        schema.enableLabelIndex(enableLabelIndex);
    }

    protected abstract void writeUserdata(SchemaElement schema,
                                          TableBackendEntry entry);

    protected abstract void readUserdata(SchemaElement schema,
                                         TableBackendEntry entry);

    private static <T> T schemaColumn(TableBackendEntry entry, BigKeys key) {
        assert entry.type().isSchema();

        T value = entry.column(key);
        E.checkState(value != null,
                     "Not found key '%s' from entry %s", key, entry);
        return value;
    }

    private static <T extends SerialEnum> T schemaEnum(TableBackendEntry entry,
                                                       BigKeys key,
                                                       Class<T> clazz) {
        Number value = schemaColumn(entry, key);
        return SerialEnum.fromCode(clazz, value.byteValue());
    }

    private static <T extends SerialEnum> T schemaEnumOrDefault(
            TableBackendEntry entry,
            BigKeys key, Class<T> clazz,
            T defaultValue) {
        assert entry.type().isSchema();

        Number value = entry.column(key);
        if (value == null) {
            return defaultValue;
        }
        return SerialEnum.fromCode(clazz, value.byteValue());
    }
}
