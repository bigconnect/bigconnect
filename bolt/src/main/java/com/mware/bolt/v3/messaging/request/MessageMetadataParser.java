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
package com.mware.bolt.v3.messaging.request;

import com.mware.bolt.messaging.BoltIOException;
import com.mware.bolt.runtime.Status;
import com.mware.ge.Authorizations;
import com.mware.ge.cypher.util.BaseToObjectValueWriter;
import com.mware.ge.values.AnyValue;
import com.mware.ge.values.storable.LongValue;
import com.mware.ge.values.storable.Values;
import com.mware.ge.values.virtual.MapValue;
import com.mware.ge.values.virtual.NodeValue;
import com.mware.ge.values.virtual.RelationshipValue;

import java.time.Duration;
import java.util.HashMap;
import java.util.Map;

/**
 * The parsing methods in this class returns null if the specified key is not found in the input message metadata map.
 */
final class MessageMetadataParser {
    private static final String TX_TIMEOUT_KEY = "tx_timeout";
    private static final String TX_META_DATA_KEY = "tx_metadata";

    private MessageMetadataParser() {
    }

    static Duration parseTransactionTimeout(MapValue meta) throws BoltIOException {
        AnyValue anyValue = meta.get(TX_TIMEOUT_KEY);
        if (anyValue == Values.NO_VALUE) {
            return null;
        } else if (anyValue instanceof LongValue) {
            return Duration.ofMillis(((LongValue) anyValue).longValue());
        } else {
            throw new BoltIOException(Status.Request.Invalid, "Expecting transaction timeout value to be a Long value, but got: " + anyValue);
        }
    }

    static Map<String, Object> parseTransactionMetadata(MapValue meta) throws BoltIOException {
        AnyValue anyValue = meta.get(TX_META_DATA_KEY);
        if (anyValue == Values.NO_VALUE) {
            return null;
        } else if (anyValue instanceof MapValue) {
            MapValue mapValue = (MapValue) anyValue;
            TransactionMetadataWriter writer = new TransactionMetadataWriter();
            Map<String, Object> txMeta = new HashMap<>(mapValue.size());
            mapValue.foreach((key, value) -> txMeta.put(key, writer.valueAsObject(value)));
            return txMeta;
        } else {
            throw new BoltIOException(Status.Request.Invalid, "Expecting transaction metadata value to be a Map value, but got: " + anyValue);
        }
    }

    private static class TransactionMetadataWriter extends BaseToObjectValueWriter<RuntimeException> {
        @Override
        protected NodeValue newNodeProxyById(String id, Authorizations authorizations) {
            throw new UnsupportedOperationException("Transaction metadata should not contain nodes");
        }

        @Override
        protected RelationshipValue newRelationshipProxyById(String id, Authorizations authorizations) {
            throw new UnsupportedOperationException("Transaction metadata should not contain relationships");
        }



        Object valueAsObject(AnyValue value) {
            value.writeTo(this, null);
            return value();
        }
    }
}
