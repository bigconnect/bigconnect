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

import io.netty.channel.*;
import com.mware.bigconnect.driver.Logger;
import com.mware.bigconnect.driver.Logging;
import com.mware.bigconnect.driver.exceptions.ServiceUnavailableException;
import com.mware.bigconnect.driver.internal.BoltServerAddress;
import com.mware.bigconnect.driver.internal.logging.ChannelActivityLogger;

import static com.mware.bigconnect.driver.internal.async.connection.BoltProtocolUtil.handshakeBuf;
import static com.mware.bigconnect.driver.internal.async.connection.BoltProtocolUtil.handshakeString;
import static java.lang.String.format;

public class ChannelConnectedListener implements ChannelFutureListener
{
    private final BoltServerAddress address;
    private final ChannelPipelineBuilder pipelineBuilder;
    private final ChannelPromise handshakeCompletedPromise;
    private final Logging logging;

    public ChannelConnectedListener( BoltServerAddress address, ChannelPipelineBuilder pipelineBuilder,
            ChannelPromise handshakeCompletedPromise, Logging logging )
    {
        this.address = address;
        this.pipelineBuilder = pipelineBuilder;
        this.handshakeCompletedPromise = handshakeCompletedPromise;
        this.logging = logging;
    }

    @Override
    public void operationComplete( ChannelFuture future )
    {
        Channel channel = future.channel();
        Logger log = new ChannelActivityLogger( channel, logging, getClass() );

        if ( future.isSuccess() )
        {
            log.trace( "Channel %s connected, initiating bolt handshake", channel );

            ChannelPipeline pipeline = channel.pipeline();
            pipeline.addLast( new HandshakeHandler( pipelineBuilder, handshakeCompletedPromise, logging ) );
            log.debug( "C: [Bolt Handshake] %s", handshakeString() );
            channel.writeAndFlush( handshakeBuf(), channel.voidPromise() );
        }
        else
        {
            handshakeCompletedPromise.setFailure( databaseUnavailableError( address, future.cause() ) );
        }
    }

    private static Throwable databaseUnavailableError(BoltServerAddress address, Throwable cause )
    {
        return new ServiceUnavailableException( format(
                "Unable to connect to %s, ensure the database is running and that there " +
                "is a working network connection to it.", address ), cause );
    }
}
