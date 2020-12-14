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
package com.mware.bolt.v1.messaging;

import com.mware.bolt.messaging.BoltRequestMessageReader;
import com.mware.bolt.messaging.BoltResponseMessageWriter;
import com.mware.bolt.messaging.RequestMessageDecoder;
import com.mware.bolt.runtime.BoltConnection;
import com.mware.bolt.runtime.BoltResponseHandler;
import com.mware.bolt.v1.messaging.decoder.*;

import java.util.Arrays;
import java.util.List;

public class BoltRequestMessageReaderV1 extends BoltRequestMessageReader {
    public BoltRequestMessageReaderV1(BoltConnection connection, BoltResponseMessageWriter responseMessageWriter) {
        super(connection, newSimpleResponseHandler(connection, responseMessageWriter),
                buildDecoders(connection, responseMessageWriter));
    }

    private static List<RequestMessageDecoder> buildDecoders(BoltConnection connection, BoltResponseMessageWriter responseMessageWriter) {
        BoltResponseHandler initHandler = newSimpleResponseHandler(connection, responseMessageWriter);
        BoltResponseHandler runHandler = newSimpleResponseHandler(connection, responseMessageWriter);
        BoltResponseHandler resultHandler = new ResultHandler(responseMessageWriter, connection);
        BoltResponseHandler defaultHandler = newSimpleResponseHandler(connection, responseMessageWriter);

        return Arrays.asList(
                new InitMessageDecoder(initHandler),
                new AckFailureMessageDecoder(defaultHandler),
                new ResetMessageDecoder(connection, defaultHandler),
                new RunMessageDecoder(runHandler),
                new DiscardAllMessageDecoder(resultHandler),
                new PullAllMessageDecoder(resultHandler)
        );
    }

    private static BoltResponseHandler newSimpleResponseHandler(BoltConnection connection,
                                                                BoltResponseMessageWriter responseMessageWriter) {
        return new MessageProcessingHandler(responseMessageWriter, connection);
    }
}
