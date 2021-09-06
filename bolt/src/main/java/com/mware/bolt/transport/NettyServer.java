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

import com.mware.bolt.transport.configuration.EpollConfigurationProvider;
import com.mware.bolt.transport.configuration.NioConfigurationProvider;
import com.mware.bolt.transport.configuration.ServerConfigurationProvider;
import com.mware.bolt.util.ListenSocketAddress;
import com.mware.core.util.BcLogger;
import com.mware.core.util.BcLoggerFactory;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.buffer.PooledByteBufAllocator;
import io.netty.channel.*;
import io.netty.channel.epoll.Epoll;

import java.net.InetSocketAddress;
import java.util.concurrent.ThreadFactory;

public class NettyServer {
    private static final BcLogger LOGGER = BcLoggerFactory.getLogger(NettyServer.class);
    private static final boolean USE_EPOLL = true;
    private static final int NUM_SELECTOR_THREADS = Runtime.getRuntime().availableProcessors() * 2;

    private final ThreadFactory tf;
    private final ProtocolInitializer protocolInitializer;
    private EventLoopGroup bossGroup;
    private EventLoopGroup selectorGroup;

    public NettyServer(ThreadFactory tf, ProtocolInitializer protocolInitializer) {
        this.tf = tf;
        this.protocolInitializer = protocolInitializer;
    }

    public void start() throws Throwable {
        boolean useEpoll = USE_EPOLL && Epoll.isAvailable();
        ServerConfigurationProvider configurationProvider = useEpoll ? EpollConfigurationProvider.INSTANCE :
                NioConfigurationProvider.INSTANCE;
        bossGroup = configurationProvider.createEventLoopGroup(1, tf);

        // These threads handle live channels. Each thread has a set of channels it is responsible for, and it will
        // continuously run a #select() loop to react to new events on these channels.
        selectorGroup = configurationProvider.createEventLoopGroup(NUM_SELECTOR_THREADS, tf);
        LOGGER.debug("NUM_SELECTOR_THREADS = %s", NUM_SELECTOR_THREADS);

        try {
            ServerBootstrap serverBootstrap = createServerBootstrap(configurationProvider, protocolInitializer);
            ChannelFuture channelFuture = serverBootstrap.bind(protocolInitializer.address().socketAddress()).sync();
            InetSocketAddress localAddress = (InetSocketAddress) channelFuture.channel().localAddress();
            String host = protocolInitializer.address().getHostname();
            int port = localAddress.getPort();
            if (host.contains(":")) {
                // IPv6
                LOGGER.info("Bolt enabled on [%s]:%s.", host, port);
            } else {
                // IPv4
                LOGGER.info("Bolt enabled on %s:%s.", host, port);
            }
        } catch (Throwable e) {
            // We catch throwable here because netty uses clever tricks to have method signatures that look like they do not
            // throw checked exceptions, but they actually do. The compiler won't let us catch them explicitly because in theory
            // they shouldn't be possible, so we have to catch Throwable and do our own checks to grab them
            throw new PortBindException(protocolInitializer.address(), e);
        }
    }

    public void stop() {
        bossGroup.shutdownGracefully();
        selectorGroup.shutdownGracefully();
    }

    private ServerBootstrap createServerBootstrap( ServerConfigurationProvider configurationProvider, ProtocolInitializer protocolInitializer )
    {
        return new ServerBootstrap()
                .group( bossGroup, selectorGroup )
                .channel( configurationProvider.getChannelClass() )
                .option( ChannelOption.ALLOCATOR, PooledByteBufAllocator.DEFAULT )
                .option( ChannelOption.SO_REUSEADDR, true )
                .childOption( ChannelOption.SO_KEEPALIVE, true )
                .childHandler( protocolInitializer.channelInitializer() );
    }

    /**
     * Describes how to initialize new channels for a protocol, and which address the protocol should be bolted into.
     */
    public interface ProtocolInitializer
    {
        ChannelInitializer<Channel> channelInitializer();
        ListenSocketAddress address();
    }
}
