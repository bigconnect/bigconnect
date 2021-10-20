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

import io.bigconnect.biggraph.schema.EdgeLabel;
import io.bigconnect.biggraph.schema.PropertyKey;
import io.bigconnect.biggraph.type.BigType;
import io.bigconnect.biggraph.util.E;
import org.apache.tinkerpop.gremlin.structure.Property;
import org.apache.tinkerpop.gremlin.structure.util.ElementHelper;

public class BigEdgeProperty<V> extends BigProperty<V> {

    public BigEdgeProperty(BigElement owner, PropertyKey key, V value) {
        super(owner, key, value);
    }

    @Override
    public BigType type() {
        return this.pkey.aggregateType().isNone() ?
               BigType.PROPERTY : BigType.AGGR_PROPERTY_E;
    }

    @Override
    public BigEdge element() {
        assert this.owner instanceof BigEdge;
        return (BigEdge) this.owner;
    }

    @Override
    public void remove() {
        assert this.owner instanceof BigEdge;
        EdgeLabel edgeLabel = ((BigEdge) this.owner).schemaLabel();
        E.checkArgument(edgeLabel.nullableKeys().contains(
                        this.propertyKey().id()),
                        "Can't remove non-null edge property '%s'", this);
        this.owner.graph().removeEdgeProperty(this);
    }

    @Override
    public boolean equals(Object obj) {
        if (!(obj instanceof Property)) {
            return false;
        }
        return ElementHelper.areEqual(this, obj);
    }

    public BigEdgeProperty<V> switchEdgeOwner() {
        assert this.owner instanceof BigEdge;
        return new BigEdgeProperty<V>(((BigEdge) this.owner).switchOwner(),
                                       this.pkey, this.value);
    }
}
