/*
 * Copyright (c) 2013-2020 "BigConnect,"
 * MWARE SOLUTIONS SRL
 *
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
package com.mware.bigconnect.driver.internal.async.connection;

import io.netty.bootstrap.Bootstrap;
import io.netty.channel.*;
import com.mware.bigconnect.driver.AuthToken;
import com.mware.bigconnect.driver.AuthTokens;
import com.mware.bigconnect.driver.Logging;
import com.mware.bigconnect.driver.Value;
import com.mware.bigconnect.driver.exceptions.ClientException;
import com.mware.bigconnect.driver.internal.BoltServerAddress;
import com.mware.bigconnect.driver.internal.ConnectionSettings;
import com.mware.bigconnect.driver.internal.async.inbound.ConnectTimeoutHandler;
import com.mware.bigconnect.driver.internal.security.InternalAuthToken;
import com.mware.bigconnect.driver.internal.security.SecurityPlan;
import com.mware.bigconnect.driver.internal.util.Clock;

import java.util.Map;

import static java.util.Objects.requireNonNull;

public class ChannelConnectorImpl implements ChannelConnector
{
    private final String userAgent;
    private final Map<String,Value> authToken;
    private final SecurityPlan securityPlan;
    private final ChannelPipelineBuilder pipelineBuilder;
    private final int connectTimeoutMillis;
    private final Logging logging;
    private final Clock clock;

    public ChannelConnectorImpl( ConnectionSettings connectionSettings, SecurityPlan securityPlan, Logging logging,
            Clock clock )
    {
        this( connectionSettings, securityPlan, new ChannelPipelineBuilderImpl(), logging, clock );
    }

    public ChannelConnectorImpl( ConnectionSettings connectionSettings, SecurityPlan securityPlan,
            ChannelPipelineBuilder pipelineBuilder, Logging logging, Clock clock )
    {
        this.userAgent = connectionSettings.userAgent();
        this.authToken = tokenAsMap( connectionSettings.authToken() );
        this.connectTimeoutMillis = connectionSettings.connectTimeoutMillis();
        this.securityPlan = requireNonNull( securityPlan );
        this.pipelineBuilder = pipelineBuilder;
        this.logging = requireNonNull( logging );
        this.clock = requireNonNull( clock );
    }

    @Override
    public ChannelFuture connect( BoltServerAddress address, Bootstrap bootstrap )
    {
        bootstrap.option( ChannelOption.CONNECT_TIMEOUT_MILLIS, connectTimeoutMillis );
        bootstrap.handler( new NettyChannelInitializer( address, securityPlan, connectTimeoutMillis, clock, logging ) );

        ChannelFuture channelConnected = bootstrap.connect( address.toSocketAddress() );

        Channel channel = channelConnected.channel();
        ChannelPromise handshakeCompleted = channel.newPromise();
        ChannelPromise connectionInitialized = channel.newPromise();

        installChannelConnectedListeners( address, channelConnected, handshakeCompleted );
        installHandshakeCompletedListeners( handshakeCompleted, connectionInitialized );

        return connectionInitialized;
    }

    private void installChannelConnectedListeners( BoltServerAddress address, ChannelFuture channelConnected,
            ChannelPromise handshakeCompleted )
    {
        ChannelPipeline pipeline = channelConnected.channel().pipeline();

        // add timeout handler to the pipeline when channel is connected. it's needed to limit amount of time code
        // spends in TLS and Bolt handshakes. prevents infinite waiting when database does not respond
        channelConnected.addListener( future ->
                pipeline.addFirst( new ConnectTimeoutHandler( connectTimeoutMillis ) ) );

        // add listener that sends Bolt handshake bytes when channel is connected
        channelConnected.addListener(
                new ChannelConnectedListener( address, pipelineBuilder, handshakeCompleted, logging ) );
    }

    private void installHandshakeCompletedListeners( ChannelPromise handshakeCompleted,
            ChannelPromise connectionInitialized )
    {
        ChannelPipeline pipeline = handshakeCompleted.channel().pipeline();

        // remove timeout handler from the pipeline once TLS and Bolt handshakes are completed. regular protocol
        // messages will flow next and we do not want to have read timeout for them
        handshakeCompleted.addListener( future -> pipeline.remove( ConnectTimeoutHandler.class ) );

        // add listener that sends an INIT message. connection is now fully established. channel pipeline if fully
        // set to send/receive messages for a selected protocol version
        handshakeCompleted.addListener( new HandshakeCompletedListener( userAgent, authToken, connectionInitialized ) );
    }

    private static Map<String,Value> tokenAsMap(AuthToken token )
    {
        if ( token instanceof InternalAuthToken )
        {
            return ((InternalAuthToken) token).toMap();
        }
        else
        {
            throw new ClientException(
                    "Unknown authentication token, `" + token + "`. Please use one of the supported " +
                    "tokens from `" + AuthTokens.class.getSimpleName() + "`." );
        }
    }
}
