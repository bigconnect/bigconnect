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
package com.mware.bigconnect.driver.internal.async.inbound;

import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.handler.codec.CodecException;
import com.mware.bigconnect.driver.Logger;
import com.mware.bigconnect.driver.Logging;
import com.mware.bigconnect.driver.exceptions.ServiceUnavailableException;
import com.mware.bigconnect.driver.internal.logging.ChannelActivityLogger;
import com.mware.bigconnect.driver.internal.util.ErrorUtil;

import java.io.IOException;

import static java.util.Objects.requireNonNull;
import static com.mware.bigconnect.driver.internal.async.connection.ChannelAttributes.messageDispatcher;
import static com.mware.bigconnect.driver.internal.async.connection.ChannelAttributes.terminationReason;

public class ChannelErrorHandler extends ChannelInboundHandlerAdapter
{
    private final Logging logging;

    private InboundMessageDispatcher messageDispatcher;
    private Logger log;
    private boolean failed;

    public ChannelErrorHandler( Logging logging )
    {
        this.logging = logging;
    }

    @Override
    public void handlerAdded( ChannelHandlerContext ctx )
    {
        messageDispatcher = requireNonNull( messageDispatcher( ctx.channel() ) );
        log = new ChannelActivityLogger( ctx.channel(), logging, getClass() );
    }

    @Override
    public void handlerRemoved( ChannelHandlerContext ctx )
    {
        messageDispatcher = null;
        log = null;
        failed = false;
    }

    @Override
    public void channelInactive( ChannelHandlerContext ctx )
    {
        log.debug( "Channel is inactive" );

        if ( !failed )
        {
            // channel became inactive not because of a fatal exception that came from exceptionCaught
            // it is most likely inactive because actual network connection broke or was explicitly closed by the driver

            String terminationReason = terminationReason( ctx.channel() );
            ServiceUnavailableException error = ErrorUtil.newConnectionTerminatedError( terminationReason );
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
            log.warn( "Fatal error occurred in the pipeline", error );
            fail( ctx, error );
        }
    }

    private void fail( ChannelHandlerContext ctx, Throwable error )
    {
        Throwable cause = transformError( error );
        messageDispatcher.handleChannelError( cause );
        log.debug( "Closing channel because of a failure '%s'", error );
        ctx.close();
    }

    private static Throwable transformError(Throwable error )
    {
        if ( error instanceof CodecException && error.getCause() != null )
        {
            // unwrap the CodecException if it has a cause
            error = error.getCause();
        }

        if ( error instanceof IOException)
        {
            return new ServiceUnavailableException( "Connection to the database failed", error );
        }
        else
        {
            return error;
        }
    }
}
