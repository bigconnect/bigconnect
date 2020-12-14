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
import com.mware.bolt.runtime.BoltConnection;
import com.mware.bolt.runtime.BoltResponseHandler;
import com.mware.bolt.v3.messaging.request.GoodbyeMessage;

import java.io.IOException;

public class GoodbyeMessageDecoder implements RequestMessageDecoder {
    private final BoltConnection connection;
    private final BoltResponseHandler responseHandler;

    public GoodbyeMessageDecoder(BoltConnection connection, BoltResponseHandler responseHandler) {
        this.connection = connection;
        this.responseHandler = responseHandler;
    }

    @Override
    public int signature() {
        return GoodbyeMessage.SIGNATURE;
    }

    @Override
    public BoltResponseHandler responseHandler() {
        return responseHandler;
    }

    @Override
    public RequestMessage decode(BigConnectPack.Unpacker unpacker) throws IOException {
        connection.stop();
        return GoodbyeMessage.GOODBYE_MESSAGE;
    }
}

