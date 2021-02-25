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
package com.mware.bolt.transport;

import com.mware.bolt.BoltChannel;
import com.mware.bolt.BoltConnector;
import com.mware.bolt.util.ListenSocketAddress;
import com.mware.ge.cypher.connection.NetworkConnectionTracker;
import io.netty.buffer.PooledByteBufAllocator;
import io.netty.channel.Channel;
import io.netty.channel.ChannelInitializer;
import io.netty.handler.ssl.SslContext;

public class SocketTransport implements NettyServer.ProtocolInitializer {
    private final ListenSocketAddress address;
    private final SslContext sslCtx;
    private boolean encryptionRequired;
    private final TransportThrottleGroup throttleGroup;
    private final BoltProtocolFactory boltProtocolFactory;
    private final NetworkConnectionTracker connectionTracker;

    public SocketTransport(
            ListenSocketAddress address,
            SslContext sslCtx,
            boolean encryptionRequired,
            TransportThrottleGroup throttleGroup,
            BoltProtocolFactory boltProtocolFactory,
            NetworkConnectionTracker connectionTracker
    ) {
        this.address = address;
        this.sslCtx = sslCtx;
        this.encryptionRequired = encryptionRequired;
        this.throttleGroup = throttleGroup;
        this.boltProtocolFactory = boltProtocolFactory;
        this.connectionTracker = connectionTracker;
    }

    @Override
    public ChannelInitializer<Channel> channelInitializer() {
        return new ChannelInitializer<Channel>() {
            @Override
            protected void initChannel(Channel ch) throws Exception {
                ch.config().setAllocator(PooledByteBufAllocator.DEFAULT);
                BoltChannel boltChannel = newBoltChannel(ch);
                connectionTracker.add( boltChannel );
                // install throttles
                throttleGroup.install(ch);

                // add a close listener that will uninstall throttles
                ch.closeFuture().addListener(future -> { throttleGroup.uninstall(ch); connectionTracker.remove( boltChannel ); });

                TransportSelectionHandler transportSelectionHandler = new TransportSelectionHandler(boltChannel, sslCtx,
                        encryptionRequired, false, boltProtocolFactory);

                ch.pipeline().addLast(transportSelectionHandler);
            }
        };
    }

    @Override
    public ListenSocketAddress address() {
        return address;
    }

    private BoltChannel newBoltChannel(Channel ch) {
        return new BoltChannel(connectionTracker.newConnectionId(BoltConnector.ID), BoltConnector.ID, ch);
    }
}
