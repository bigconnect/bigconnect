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

import io.bigconnect.biggraph.backend.id.SplicingIdGenerator;
import io.bigconnect.biggraph.schema.PropertyKey;
import io.bigconnect.biggraph.type.BigType;
import io.bigconnect.biggraph.util.E;
import org.apache.tinkerpop.gremlin.structure.Property;
import org.apache.tinkerpop.gremlin.structure.util.ElementHelper;
import org.apache.tinkerpop.gremlin.structure.util.StringFactory;

import java.util.NoSuchElementException;

public abstract class BigProperty<V> implements Property<V>, GraphType {

    protected final BigElement owner;
    protected final PropertyKey pkey;
    protected final V value;

    public BigProperty(BigElement owner, PropertyKey pkey, V value) {
        E.checkArgument(owner != null, "Property owner can't be null");
        E.checkArgument(pkey != null, "Property key can't be null");
        E.checkArgument(value != null, "Property value can't be null");

        this.owner = owner;
        this.pkey = pkey;
        this.value = pkey.validValueOrThrow(value);
    }

    public PropertyKey propertyKey() {
        return this.pkey;
    }

    public Object id() {
        return SplicingIdGenerator.concat(this.owner.id().asString(),
                                          this.key());
    }

    @Override
    public BigType type() {
        return BigType.PROPERTY;
    }

    @Override
    public String name() {
        return this.pkey.name();
    }

    @Override
    public String key() {
        return this.pkey.name();
    }

    @Override
    public V value() throws NoSuchElementException {
        return this.value;
    }

    public Object serialValue(boolean encodeNumber) {
        return this.pkey.serialValue(this.value, encodeNumber);
    }

    @Override
    public boolean isPresent() {
        return null != this.value;
    }

    public boolean isAggregateType() {
        return !this.pkey.aggregateType().isNone();
    }

    @Override
    public BigElement element() {
        return this.owner;
    }

    @Override
    public boolean equals(Object obj) {
        if (!(obj instanceof BigProperty)) {
            return false;
        }

        BigProperty<?> other = (BigProperty<?>) obj;
        return this.owner.equals(other.owner) && this.pkey.equals(other.pkey);
    }

    @Override
    public int hashCode() {
        return ElementHelper.hashCode(this);
    }

    @Override
    public String toString() {
        return StringFactory.propertyString(this);
    }
}
