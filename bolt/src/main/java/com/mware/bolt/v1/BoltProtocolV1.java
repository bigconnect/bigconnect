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
package com.mware.bolt.v1;

import com.mware.bolt.BoltChannel;
import com.mware.bolt.messaging.BigConnectPack;
import com.mware.bolt.messaging.BoltRequestMessageReader;
import com.mware.bolt.runtime.BoltConnection;
import com.mware.bolt.runtime.BoltConnectionFactory;
import com.mware.bolt.runtime.BoltStateMachine;
import com.mware.bolt.runtime.BoltStateMachineFactory;
import com.mware.bolt.transport.BoltProtocol;
import com.mware.bolt.transport.pipeline.ChunkDecoder;
import com.mware.bolt.transport.pipeline.HouseKeeper;
import com.mware.bolt.transport.pipeline.MessageAccumulator;
import com.mware.bolt.transport.pipeline.MessageDecoder;
import com.mware.bolt.v1.messaging.BigConnectPackV1;
import com.mware.bolt.v1.messaging.BoltRequestMessageReaderV1;
import com.mware.bolt.v1.messaging.BoltResponseMessageWriterV1;
import io.netty.channel.ChannelPipeline;

/**
 * Bolt protocol V1. It hosts all the components that are specific to BoltV1
 */
public class BoltProtocolV1 implements BoltProtocol {
    public static final long VERSION = 1;

    private final BigConnectPack bigConnectPack;
    private final BoltConnection connection;
    private final BoltRequestMessageReader messageReader;

    private final BoltChannel channel;

    public BoltProtocolV1(BoltChannel channel, BoltConnectionFactory connectionFactory, BoltStateMachineFactory stateMachineFactory) {
        this.channel = channel;

        BoltStateMachine stateMachine = stateMachineFactory.newStateMachine(version(), channel);
        this.connection = connectionFactory.newConnection(channel, stateMachine);

        this.bigConnectPack = createPack();
        this.messageReader = createMessageReader(channel, bigConnectPack, connection);
    }

    /**
     * Install chunker, packstream, message reader, message handler, message encoder for protocol v1
     */
    @Override
    public void install() {
        ChannelPipeline pipeline = channel.rawChannel().pipeline();

        pipeline.addLast(new ChunkDecoder());
        pipeline.addLast(new MessageAccumulator());
        pipeline.addLast(new MessageDecoder(bigConnectPack, messageReader));
        pipeline.addLast(new HouseKeeper(connection));
    }

    protected BigConnectPack createPack() {
        return new BigConnectPackV1();
    }

    @Override
    public long version() {
        return VERSION;
    }

    protected BoltRequestMessageReader createMessageReader(BoltChannel channel, BigConnectPack bigConnectPack, BoltConnection connection) {
        BoltResponseMessageWriterV1 responseWriter = new BoltResponseMessageWriterV1(bigConnectPack, connection.output());
        return new BoltRequestMessageReaderV1(connection, responseWriter);
    }
}
