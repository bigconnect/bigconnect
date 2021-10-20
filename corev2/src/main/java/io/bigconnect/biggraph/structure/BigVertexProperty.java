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

package io.bigconnect.biggraph.structure;

import io.bigconnect.biggraph.exception.NotSupportException;
import io.bigconnect.biggraph.schema.PropertyKey;
import io.bigconnect.biggraph.schema.VertexLabel;
import io.bigconnect.biggraph.type.BigType;
import io.bigconnect.biggraph.util.E;
import org.apache.tinkerpop.gremlin.structure.Element;
import org.apache.tinkerpop.gremlin.structure.Property;
import org.apache.tinkerpop.gremlin.structure.VertexProperty;
import org.apache.tinkerpop.gremlin.structure.util.ElementHelper;

import java.util.Iterator;

public class BigVertexProperty<V> extends BigProperty<V>
                                   implements VertexProperty<V> {

    public BigVertexProperty(BigElement owner, PropertyKey key, V value) {
        super(owner, key, value);
    }

    @Override
    public BigType type() {
        return this.pkey.aggregateType().isNone() ?
               BigType.PROPERTY : BigType.AGGR_PROPERTY_V;
    }

    @Override
    public <U> Property<U> property(String key, U value) {
        throw new NotSupportException("nested property");
    }

    @Override
    public BigVertex element() {
        assert this.owner instanceof BigVertex;
        return (BigVertex) this.owner;
    }

    @Override
    public void remove() {
        assert this.owner instanceof BigVertex;
        VertexLabel vertexLabel = ((BigVertex) this.owner).schemaLabel();
        E.checkArgument(vertexLabel.nullableKeys().contains(
                        this.propertyKey().id()),
                        "Can't remove non-null vertex property '%s'", this);
        this.owner.graph().removeVertexProperty(this);
    }

    @Override
    public <U> Iterator<Property<U>> properties(String... propertyKeys) {
        throw new NotSupportException("nested property");
    }

    @Override
    public boolean equals(Object obj) {
        if (!(obj instanceof VertexProperty)) {
            return false;
        }
        @SuppressWarnings("unchecked")
        VertexProperty<V> other = (VertexProperty<V>) obj;
        return this.id().equals(other.id());
    }

    @Override
    public int hashCode() {
        return ElementHelper.hashCode((Element) this);
    }
}
