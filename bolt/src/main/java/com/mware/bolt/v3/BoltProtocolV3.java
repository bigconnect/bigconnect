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
package com.mware.bolt.v3;

import com.mware.bolt.BoltChannel;
import com.mware.bolt.messaging.BigConnectPack;
import com.mware.bolt.messaging.BoltRequestMessageReader;
import com.mware.bolt.runtime.BoltConnection;
import com.mware.bolt.runtime.BoltConnectionFactory;
import com.mware.bolt.runtime.BoltStateMachineFactory;
import com.mware.bolt.v1.BoltProtocolV1;
import com.mware.bolt.v1.messaging.BoltResponseMessageWriterV1;
import com.mware.bolt.v2.messaging.BigConnectPackV2;
import com.mware.bolt.v3.messaging.BoltRequestMessageReaderV3;

/**
 * Bolt protocol V3. It hosts all the components that are specific to BoltV3
 */
public class BoltProtocolV3 extends BoltProtocolV1 {
    public static final long VERSION = 3;

    public BoltProtocolV3(BoltChannel channel, BoltConnectionFactory connectionFactory, BoltStateMachineFactory stateMachineFactory) {
        super(channel, connectionFactory, stateMachineFactory);
    }

    @Override
    protected BigConnectPack createPack() {
        return new BigConnectPackV2();
    }

    @Override
    public long version() {
        return VERSION;
    }

    @Override
    protected BoltRequestMessageReader createMessageReader(BoltChannel channel, BigConnectPack bigConnectPack, BoltConnection connection) {
        BoltResponseMessageWriterV1 responseWriter = new BoltResponseMessageWriterV1(bigConnectPack, connection.output());
        return new BoltRequestMessageReaderV3(connection, responseWriter);
    }
}
