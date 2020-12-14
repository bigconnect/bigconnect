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
package com.mware.bolt.v3.messaging.decoder;

import com.mware.bolt.messaging.BigConnectPack;
import com.mware.bolt.messaging.RequestMessage;
import com.mware.bolt.messaging.RequestMessageDecoder;
import com.mware.bolt.runtime.BoltResponseHandler;
import com.mware.bolt.v3.messaging.request.RunMessage;
import com.mware.ge.values.virtual.MapValue;

import java.io.IOException;

public class RunMessageDecoder implements RequestMessageDecoder {
    private final BoltResponseHandler responseHandler;

    public RunMessageDecoder(BoltResponseHandler responseHandler) {
        this.responseHandler = responseHandler;
    }

    @Override
    public int signature() {
        return RunMessage.SIGNATURE;
    }

    @Override
    public BoltResponseHandler responseHandler() {
        return responseHandler;
    }

    @Override
    public RequestMessage decode(BigConnectPack.Unpacker unpacker) throws IOException {
        String statement = unpacker.unpackString();
        MapValue params = unpacker.unpackMap();
        MapValue meta = unpacker.unpackMap();
        return new RunMessage( statement, params, meta );
    }
}

