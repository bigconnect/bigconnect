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

import io.netty.channel.Channel;
import io.netty.channel.ChannelInitializer;
import io.netty.handler.ssl.SslHandler;
import com.mware.bigconnect.driver.Logging;
import com.mware.bigconnect.driver.internal.BoltServerAddress;
import com.mware.bigconnect.driver.internal.async.inbound.InboundMessageDispatcher;
import com.mware.bigconnect.driver.internal.security.SecurityPlan;
import com.mware.bigconnect.driver.internal.util.Clock;

import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLEngine;
import javax.net.ssl.SSLParameters;

import static com.mware.bigconnect.driver.internal.async.connection.ChannelAttributes.*;

public class NettyChannelInitializer extends ChannelInitializer<Channel>
{
    private final BoltServerAddress address;
    private final SecurityPlan securityPlan;
    private final int connectTimeoutMillis;
    private final Clock clock;
    private final Logging logging;

    public NettyChannelInitializer( BoltServerAddress address, SecurityPlan securityPlan, int connectTimeoutMillis,
            Clock clock, Logging logging )
    {
        this.address = address;
        this.securityPlan = securityPlan;
        this.connectTimeoutMillis = connectTimeoutMillis;
        this.clock = clock;
        this.logging = logging;
    }

    @Override
    protected void initChannel( Channel channel )
    {
        if ( securityPlan.requiresEncryption() )
        {
            SslHandler sslHandler = createSslHandler();
            channel.pipeline().addFirst( sslHandler );
        }

        updateChannelAttributes( channel );
    }

    private SslHandler createSslHandler()
    {
        SSLEngine sslEngine = createSslEngine();
        SslHandler sslHandler = new SslHandler( sslEngine );
        sslHandler.setHandshakeTimeoutMillis( connectTimeoutMillis );
        return sslHandler;
    }

    private SSLEngine createSslEngine()
    {
        SSLContext sslContext = securityPlan.sslContext();
        SSLEngine sslEngine = sslContext.createSSLEngine( address.host(), address.port() );
        sslEngine.setUseClientMode( true );
        if ( securityPlan.requiresHostnameVerification() )
        {
            SSLParameters sslParameters = sslEngine.getSSLParameters();
            sslParameters.setEndpointIdentificationAlgorithm( "HTTPS" );
            sslEngine.setSSLParameters( sslParameters );
        }
        return sslEngine;
    }

    private void updateChannelAttributes( Channel channel )
    {
        setServerAddress( channel, address );
        setCreationTimestamp( channel, clock.millis() );
        setMessageDispatcher( channel, new InboundMessageDispatcher( channel, logging ) );
    }
}
