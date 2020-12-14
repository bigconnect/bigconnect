/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package com.mware.bolt.v1.messaging.decoder;

import com.mware.ge.Authorizations;
import com.mware.ge.cypher.util.BaseToObjectValueWriter;
import com.mware.ge.values.AnyValue;
import com.mware.ge.values.AnyValueWriter;
import com.mware.ge.values.virtual.NodeValue;
import com.mware.ge.values.virtual.RelationshipValue;

import java.time.LocalDate;

/**
 * {@link AnyValueWriter Writer} that allows to convert {@link AnyValue} to any primitive Java type. It explicitly
 * prohibits conversion of nodes, relationships, spatial and temporal types. They are not expected in auth token map.
 */
public class PrimitiveOnlyValueWriter extends BaseToObjectValueWriter<RuntimeException> {
    public Object valueAsObject(AnyValue value) {
        value.writeTo(this, null);
        return value();
    }

    @Override
    protected NodeValue newNodeProxyById(String id, Authorizations authorizations) {
        throw new UnsupportedOperationException("INIT message metadata should not contain nodes");
    }

    @Override
    protected RelationshipValue newRelationshipProxyById(String id, Authorizations authorizations) {
        throw new UnsupportedOperationException("INIT message metadata should not contain relationships");
    }

    @Override
    public void writeByteArray(byte[] value) {
        throw new UnsupportedOperationException("INIT message metadata should not contain byte arrays");
    }

    @Override
    public void writeDate(LocalDate localDate) {
        throw new UnsupportedOperationException("INIT message metadata should not contain dates");
    }
}
