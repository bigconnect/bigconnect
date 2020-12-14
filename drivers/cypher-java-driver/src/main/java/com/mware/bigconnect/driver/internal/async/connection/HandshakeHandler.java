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

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelPromise;
import io.netty.handler.codec.DecoderException;
import io.netty.handler.codec.ReplayingDecoder;
import com.mware.bigconnect.driver.Logger;
import com.mware.bigconnect.driver.Logging;
import com.mware.bigconnect.driver.exceptions.ClientException;
import com.mware.bigconnect.driver.exceptions.ServiceUnavailableException;
import com.mware.bigconnect.driver.internal.logging.ChannelActivityLogger;
import com.mware.bigconnect.driver.internal.messaging.BoltProtocol;
import com.mware.bigconnect.driver.internal.messaging.MessageFormat;
import com.mware.bigconnect.driver.internal.util.ErrorUtil;

import javax.net.ssl.SSLHandshakeException;
import java.util.List;

import static com.mware.bigconnect.driver.internal.async.connection.BoltProtocolUtil.HTTP;
import static com.mware.bigconnect.driver.internal.async.connection.BoltProtocolUtil.NO_PROTOCOL_VERSION;

public class HandshakeHandler extends ReplayingDecoder<Void>
{
    private final ChannelPipelineBuilder pipelineBuilder;
    private final ChannelPromise handshakeCompletedPromise;
    private final Logging logging;

    private boolean failed;
    private Logger log;

    public HandshakeHandler( ChannelPipelineBuilder pipelineBuilder, ChannelPromise handshakeCompletedPromise,
            Logging logging )
    {
        this.pipelineBuilder = pipelineBuilder;
        this.handshakeCompletedPromise = handshakeCompletedPromise;
        this.logging = logging;
    }

    @Override
    public void handlerAdded( ChannelHandlerContext ctx )
    {
        log = new ChannelActivityLogger( ctx.channel(), logging, getClass() );
    }

    @Override
    protected void handlerRemoved0( ChannelHandlerContext ctx )
    {
        failed = false;
        log = null;
    }

    @Override
    public void channelInactive( ChannelHandlerContext ctx )
    {
        log.debug( "Channel is inactive" );

        if ( !failed )
        {
            // channel became inactive while doing bolt handshake, not because of some previous error
            ServiceUnavailableException error = ErrorUtil.newConnectionTerminatedError();
            fail( ctx, error );
        }
    }

    @Override
    public void exceptionCaught( ChannelHandlerContext ctx, Throwable error )
    {
        if ( failed )
        {
            log.warn( "Another fatal error occurred in the pipeline", error );
        }
        else
        {
            failed = true;
            Throwable cause = transformError( error );
            fail( ctx, cause );
        }
    }

    @Override
    protected void decode( ChannelHandlerContext ctx, ByteBuf in, List<Object> out )
    {
        int serverSuggestedVersion = in.readInt();
        log.debug( "S: [Bolt Handshake] %d", serverSuggestedVersion );

        // this is a one-time handler, remove it when protocol version has been read
        ctx.pipeline().remove( this );

        BoltProtocol protocol = protocolForVersion( serverSuggestedVersion );
        if ( protocol != null )
        {
            protocolSelected( serverSuggestedVersion, protocol.createMessageFormat(), ctx );
        }
        else
        {
            handleUnknownSuggestedProtocolVersion( serverSuggestedVersion, ctx );
        }
    }

    private BoltProtocol protocolForVersion( int version )
    {
        try
        {
            return BoltProtocol.forVersion( version );
        }
        catch ( ClientException e )
        {
            return null;
        }
    }

    private void protocolSelected( int version, MessageFormat messageFormat, ChannelHandlerContext ctx )
    {
        ChannelAttributes.setProtocolVersion( ctx.channel(), version );
        pipelineBuilder.build( messageFormat, ctx.pipeline(), logging );
        handshakeCompletedPromise.setSuccess();
    }

    private void handleUnknownSuggestedProtocolVersion( int version, ChannelHandlerContext ctx )
    {
        switch ( version )
        {
        case NO_PROTOCOL_VERSION:
            fail( ctx, protocolNoSupportedByServerError() );
            break;
        case HTTP:
            fail( ctx, httpEndpointError() );
            break;
        default:
            fail( ctx, protocolNoSupportedByDriverError( version ) );
            break;
        }
    }

    private void fail( ChannelHandlerContext ctx, Throwable error )
    {
        ctx.close().addListener( future -> handshakeCompletedPromise.tryFailure( error ) );
    }

    private static Throwable protocolNoSupportedByServerError()
    {
        return new ClientException( "The server does not support any of the protocol versions supported by " +
                                    "this driver. Ensure that you are using driver and server versions that " +
                                    "are compatible with one another." );
    }

    private static Throwable httpEndpointError()
    {
        return new ClientException(
                "Server responded HTTP. Make sure you are not trying to connect to the http endpoint " +
                "(HTTP defaults to port 7474 whereas BOLT defaults to port 7687)" );
    }

    private static Throwable protocolNoSupportedByDriverError(int suggestedProtocolVersion )
    {
        return new ClientException(
                "Protocol error, server suggested unexpected protocol version: " + suggestedProtocolVersion );
    }

    private static Throwable transformError(Throwable error )
    {
        if ( error instanceof DecoderException && error.getCause() != null )
        {
            // unwrap the DecoderException if it has a cause
            error = error.getCause();
        }

        if ( error instanceof ServiceUnavailableException )
        {
            return error;
        }
        else if ( error instanceof SSLHandshakeException)
        {
            return new SecurityException( "Failed to establish secured connection with the server", error );
        }
        else
        {
            return new ServiceUnavailableException( "Failed to establish connection with the server", error );
        }
    }
}
