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
import io.bigconnect.biggraph.backend.id.IdGenerator;
import io.bigconnect.biggraph.schema.builder.SchemaBuilder;
import io.bigconnect.biggraph.type.BigType;
import io.bigconnect.biggraph.type.define.IndexType;
import io.bigconnect.biggraph.util.E;
import com.google.common.base.Objects;

import java.util.*;

public class IndexLabel extends SchemaElement {

    private BigType baseType;
    private Id baseValue;
    private IndexType indexType;
    private List<Id> indexFields;

    public IndexLabel(final BigGraph graph, Id id, String name) {
        super(graph, id, name);
        this.baseType = BigType.SYS_SCHEMA;
        this.baseValue = NONE_ID;
        this.indexType = IndexType.SECONDARY;
        this.indexFields = new ArrayList<>();
    }

    protected IndexLabel(long id, String name) {
        this(null, IdGenerator.of(id), name);
    }

    @Override
    public BigType type() {
        return BigType.INDEX_LABEL;
    }

    public BigType baseType() {
        return this.baseType;
    }

    public void baseType(BigType baseType) {
        this.baseType = baseType;
    }

    public Id baseValue() {
        return this.baseValue;
    }

    public void baseValue(Id id) {
        this.baseValue = id;
    }

    public IndexType indexType() {
        return this.indexType;
    }

    public void indexType(IndexType indexType) {
        this.indexType = indexType;
    }

    public BigType queryType() {
        switch (this.baseType) {
            case VERTEX_LABEL:
                return BigType.VERTEX;
            case EDGE_LABEL:
                return BigType.EDGE;
            case SYS_SCHEMA:
                return BigType.SYS_SCHEMA;
            default:
                throw new AssertionError(String.format(
                          "Query type of index label is either '%s' or '%s', " +
                          "but '%s' is used",
                          BigType.VERTEX_LABEL, BigType.EDGE_LABEL,
                          this.baseType));
        }
    }

    public List<Id> indexFields() {
        return Collections.unmodifiableList(this.indexFields);
    }

    public void indexFields(Id... ids) {
        this.indexFields.addAll(Arrays.asList(ids));
    }

    public void indexField(Id id) {
        this.indexFields.add(id);
    }

    public Id indexField() {
        E.checkState(this.indexFields.size() == 1,
                     "There should be only one field in %s index label, " +
                     "but got: %s", this.indexType.string(), this.indexFields);
        return this.indexFields.get(0);
    }

    public SchemaLabel baseElement() {
        return getElement(this.graph, this.baseType, this.baseValue);
    }

    public boolean hasSameContent(IndexLabel other) {
        return super.hasSameContent(other) &&
               this.indexType == other.indexType &&
               this.baseType == other.baseType &&
               Objects.equal(this.graph.mapPkId2Name(this.indexFields),
                             other.graph.mapPkId2Name(other.indexFields));
    }

    public boolean olap() {
        return OLAP_ID.equals(this.baseValue);
    }

    public Object validValue(Object value) {
        if (!(value instanceof Number)) {
            return value;
        }

        Number number = (Number) value;
        switch (this.indexType()) {
            case RANGE_INT:
                return number.intValue();
            case RANGE_LONG:
                return number.longValue();
            case RANGE_FLOAT:
                return number.floatValue();
            case RANGE_DOUBLE:
                return number.doubleValue();
            default:
                return value;
        }
    }

    // ABS of System index id must be below SchemaElement.MAX_PRIMITIVE_SYS_ID
    private static final int VL_IL_ID = -1;
    private static final int EL_IL_ID = -2;
    private static final int PKN_IL_ID = -3;
    private static final int VLN_IL_ID = -4;
    private static final int ELN_IL_ID = -5;
    private static final int ILN_IL_ID = -6;

    // Label index
    private static final IndexLabel VL_IL = new IndexLabel(VL_IL_ID, "~vli");
    private static final IndexLabel EL_IL = new IndexLabel(EL_IL_ID, "~eli");

    // Schema name index
    private static final IndexLabel PKN_IL = new IndexLabel(PKN_IL_ID, "~pkni");
    private static final IndexLabel VLN_IL = new IndexLabel(VLN_IL_ID, "~vlni");
    private static final IndexLabel ELN_IL = new IndexLabel(ELN_IL_ID, "~elni");
    private static final IndexLabel ILN_IL = new IndexLabel(ILN_IL_ID, "~ilni");

    public static IndexLabel label(BigType type) {
        switch (type) {
            case VERTEX:
                return VL_IL;
            case EDGE:
            case EDGE_OUT:
            case EDGE_IN:
                return EL_IL;
            case PROPERTY_KEY:
                return PKN_IL;
            case VERTEX_LABEL:
                return VLN_IL;
            case EDGE_LABEL:
                return ELN_IL;
            case INDEX_LABEL:
                return ILN_IL;
            default:
                throw new AssertionError(String.format(
                          "No primitive index label for '%s'", type));
        }
    }

    public static IndexLabel label(BigGraph graph, Id id) {
        // Primitive IndexLabel first
        if (id.asLong() < 0 && id.asLong() > -NEXT_PRIMITIVE_SYS_ID) {
            switch ((int) id.asLong()) {
                case VL_IL_ID:
                    return VL_IL;
                case EL_IL_ID:
                    return EL_IL;
                case PKN_IL_ID:
                    return PKN_IL;
                case VLN_IL_ID:
                    return VLN_IL;
                case ELN_IL_ID:
                    return ELN_IL;
                case ILN_IL_ID:
                    return ILN_IL;
                default:
                    throw new AssertionError(String.format(
                              "No primitive index label for '%s'", id));
            }
        }
        return graph.indexLabel(id);
    }

    public static SchemaLabel getElement(BigGraph graph,
                                         BigType baseType, Object baseValue) {
        E.checkNotNull(baseType, "base type", "index label");
        E.checkNotNull(baseValue, "base value", "index label");
        E.checkArgument(baseValue instanceof String || baseValue instanceof Id,
                        "The base value must be instance of String or Id, " +
                        "but got %s(%s)", baseValue,
                        baseValue.getClass().getSimpleName());

        SchemaLabel label;
        switch (baseType) {
            case VERTEX_LABEL:
                if (baseValue instanceof String) {
                    label = graph.vertexLabel((String) baseValue);
                } else {
                    assert baseValue instanceof Id;
                    label = graph.vertexLabel((Id) baseValue);
                }
                break;
            case EDGE_LABEL:
                if (baseValue instanceof String) {
                    label = graph.edgeLabel((String) baseValue);
                } else {
                    assert baseValue instanceof Id;
                    label = graph.edgeLabel((Id) baseValue);
                }
                break;
            default:
                throw new AssertionError(String.format(
                          "Unsupported base type '%s' of index label",
                          baseType));
        }

        E.checkArgumentNotNull(label, "Can't find the %s with name '%s'",
                               baseType.readableName(), baseValue);
        return label;
    }

    public interface Builder extends SchemaBuilder<IndexLabel> {

        TaskWithSchema createWithTask();

        Id rebuild();

        Builder onV(String baseValue);

        Builder onE(String baseValue);

        Builder by(String... fields);

        Builder secondary();

        Builder range();

        Builder search();

        Builder shard();

        Builder unique();

        Builder on(BigType baseType, String baseValue);

        Builder indexType(IndexType indexType);

        Builder userdata(String key, Object value);

        Builder userdata(Map<String, Object> userdata);

        Builder rebuild(boolean rebuild);
    }

}
