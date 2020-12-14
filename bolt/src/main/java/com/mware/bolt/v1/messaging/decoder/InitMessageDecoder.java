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

import com.mware.bolt.messaging.BigConnectPack;
import com.mware.bolt.messaging.RequestMessage;
import com.mware.bolt.messaging.RequestMessageDecoder;
import com.mware.bolt.runtime.BoltResponseHandler;
import com.mware.bolt.v1.messaging.request.InitMessage;
import com.mware.ge.values.virtual.MapValue;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class InitMessageDecoder implements RequestMessageDecoder {
    private final BoltResponseHandler responseHandler;

    public InitMessageDecoder(BoltResponseHandler responseHandler) {
        this.responseHandler = responseHandler;
    }

    @Override
    public int signature() {
        return InitMessage.SIGNATURE;
    }

    @Override
    public BoltResponseHandler responseHandler() {
        return responseHandler;
    }

    @Override
    public RequestMessage decode(BigConnectPack.Unpacker unpacker) throws IOException {
        String userAgent = unpacker.unpackString();
        Map<String,Object> authToken = readMetaDataMap( unpacker );
        return new InitMessage(userAgent, authToken);
    }

    public static Map<String, Object> readMetaDataMap(BigConnectPack.Unpacker unpacker) throws IOException {
        MapValue metaDataMapValue = unpacker.unpackMap();
        PrimitiveOnlyValueWriter writer = new PrimitiveOnlyValueWriter();
        Map<String, Object> metaDataMap = new HashMap<>(metaDataMapValue.size());
        metaDataMapValue.foreach((key, value) ->
        {
            Object convertedValue = writer.valueAsObject(value);
            metaDataMap.put(key, convertedValue);
        });
        return metaDataMap;
    }
}
