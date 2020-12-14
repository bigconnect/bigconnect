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
package com.mware.bigconnect.driver.internal.async;

import io.netty.channel.Channel;
import io.netty.channel.pool.ChannelPool;
import com.mware.bigconnect.driver.internal.BoltServerAddress;
import com.mware.bigconnect.driver.internal.async.connection.ChannelAttributes;
import com.mware.bigconnect.driver.internal.async.inbound.InboundMessageDispatcher;
import com.mware.bigconnect.driver.internal.handlers.ChannelReleasingResetResponseHandler;
import com.mware.bigconnect.driver.internal.handlers.ResetResponseHandler;
import com.mware.bigconnect.driver.internal.messaging.BoltProtocol;
import com.mware.bigconnect.driver.internal.messaging.Message;
import com.mware.bigconnect.driver.internal.messaging.request.ResetMessage;
import com.mware.bigconnect.driver.internal.metrics.ListenerEvent;
import com.mware.bigconnect.driver.internal.metrics.MetricsListener;
import com.mware.bigconnect.driver.internal.spi.Connection;
import com.mware.bigconnect.driver.internal.spi.ResponseHandler;
import com.mware.bigconnect.driver.internal.util.Clock;
import com.mware.bigconnect.driver.internal.util.ServerVersion;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.atomic.AtomicReference;

import static java.util.Collections.emptyMap;
import static com.mware.bigconnect.driver.internal.async.connection.ChannelAttributes.poolId;
import static com.mware.bigconnect.driver.internal.async.connection.ChannelAttributes.setTerminationReason;

/**
 * This connection represents a simple network connection to a remote server.
 * It wraps a channel obtained from a connection pool.
 * The life cycle of this connection start from the moment the channel is borrowed out of the pool
 * and end at the time the connection is released back to the pool.
 */
public class NetworkConnection implements Connection
{
    private final Channel channel;
    private final InboundMessageDispatcher messageDispatcher;
    private final BoltServerAddress serverAddress;
    private final ServerVersion serverVersion;
    private final BoltProtocol protocol;
    private final ChannelPool channelPool;
    private final CompletableFuture<Void> releaseFuture;
    private final Clock clock;

    private final AtomicReference<Status> status = new AtomicReference<>( Status.OPEN );
    private final MetricsListener metricsListener;
    private final ListenerEvent inUseEvent;

    public NetworkConnection( Channel channel, ChannelPool channelPool, Clock clock, MetricsListener metricsListener )
    {
        this.channel = channel;
        this.messageDispatcher = ChannelAttributes.messageDispatcher( channel );
        this.serverAddress = ChannelAttributes.serverAddress( channel );
        this.serverVersion = ChannelAttributes.serverVersion( channel );
        this.protocol = BoltProtocol.forChannel( channel );
        this.channelPool = channelPool;
        this.releaseFuture = new CompletableFuture<>();
        this.clock = clock;
        this.metricsListener = metricsListener;
        this.inUseEvent = metricsListener.createListenerEvent();
        metricsListener.afterConnectionCreated( poolId( this.channel ), this.inUseEvent );
    }

    @Override
    public boolean isOpen()
    {
        return status.get() == Status.OPEN;
    }

    @Override
    public void enableAutoRead()
    {
        if ( isOpen() )
        {
            setAutoRead( true );
        }
    }

    @Override
    public void disableAutoRead()
    {
        if ( isOpen() )
        {
            setAutoRead( false );
        }
    }

    @Override
    public void flush()
    {
        if ( verifyOpen( null, null ) )
        {
            flushInEventLoop();
        }
    }

    @Override
    public void write( Message message, ResponseHandler handler )
    {
        if ( verifyOpen( handler, null ) )
        {
            writeMessageInEventLoop( message, handler, false );
        }
    }

    @Override
    public void write( Message message1, ResponseHandler handler1, Message message2, ResponseHandler handler2 )
    {
        if ( verifyOpen( handler1, handler2 ) )
        {
            writeMessagesInEventLoop( message1, handler1, message2, handler2, false );
        }
    }

    @Override
    public void writeAndFlush( Message message, ResponseHandler handler )
    {
        if ( verifyOpen( handler, null ) )
        {
            writeMessageInEventLoop( message, handler, true );
        }
    }

    @Override
    public void writeAndFlush( Message message1, ResponseHandler handler1, Message message2, ResponseHandler handler2 )
    {
        if ( verifyOpen( handler1, handler2 ) )
        {
            writeMessagesInEventLoop( message1, handler1, message2, handler2, true );
        }
    }

    @Override
    public CompletionStage<Void> reset()
    {
        CompletableFuture<Void> result = new CompletableFuture<>();
        ResetResponseHandler handler = new ResetResponseHandler( messageDispatcher, result );
        writeResetMessageIfNeeded( handler, true );
        return result;
    }

    @Override
    public CompletionStage<Void> release()
    {
        if ( status.compareAndSet( Status.OPEN, Status.RELEASED ) )
        {
            ChannelReleasingResetResponseHandler handler = new ChannelReleasingResetResponseHandler( channel,
                    channelPool, messageDispatcher, clock, releaseFuture );

            writeResetMessageIfNeeded( handler, false );
            metricsListener.afterConnectionReleased( poolId( this.channel ), this.inUseEvent );
        }
        return releaseFuture;
    }

    @Override
    public void terminateAndRelease( String reason )
    {
        if ( status.compareAndSet( Status.OPEN, Status.TERMINATED ) )
        {
            setTerminationReason( channel, reason );
            channel.close();
            channelPool.release( channel );
            releaseFuture.complete( null );
            metricsListener.afterConnectionReleased( poolId( this.channel ), this.inUseEvent );
        }
    }

    @Override
    public BoltServerAddress serverAddress()
    {
        return serverAddress;
    }

    @Override
    public ServerVersion serverVersion()
    {
        return serverVersion;
    }

    @Override
    public BoltProtocol protocol()
    {
        return protocol;
    }

    private void writeResetMessageIfNeeded( ResponseHandler resetHandler, boolean isSessionReset )
    {
        channel.eventLoop().execute( () ->
        {
            if ( isSessionReset && !isOpen() )
            {
                resetHandler.onSuccess( emptyMap() );
            }
            else
            {
                // auto-read could've been disabled, re-enable it to automatically receive response for RESET
                setAutoRead( true );

                messageDispatcher.enqueue( resetHandler );
                channel.writeAndFlush( ResetMessage.RESET, channel.voidPromise() );
            }
        } );
    }

    private void flushInEventLoop()
    {
        channel.eventLoop().execute( channel::flush );
    }

    private void writeMessageInEventLoop( Message message, ResponseHandler handler, boolean flush )
    {
        channel.eventLoop().execute( () ->
        {
            messageDispatcher.enqueue( handler );

            if ( flush )
            {
                channel.writeAndFlush( message, channel.voidPromise() );
            }
            else
            {
                channel.write( message, channel.voidPromise() );
            }
        } );
    }

    private void writeMessagesInEventLoop( Message message1, ResponseHandler handler1, Message message2, ResponseHandler handler2, boolean flush )
    {
        channel.eventLoop().execute( () ->
        {
            messageDispatcher.enqueue( handler1 );
            messageDispatcher.enqueue( handler2 );

            channel.write( message1, channel.voidPromise() );

            if ( flush )
            {
                channel.writeAndFlush( message2, channel.voidPromise() );
            }
            else
            {
                channel.write( message2, channel.voidPromise() );
            }
        } );
    }

    private void setAutoRead( boolean value )
    {
        channel.config().setAutoRead( value );
    }

    private boolean verifyOpen( ResponseHandler handler1, ResponseHandler handler2 )
    {
        Status connectionStatus = this.status.get();
        switch ( connectionStatus )
        {
        case OPEN:
            return true;
        case RELEASED:
            Exception error = new IllegalStateException( "Connection has been released to the pool and can't be used" );
            if ( handler1 != null )
            {
                handler1.onFailure( error );
            }
            if ( handler2 != null )
            {
                handler2.onFailure( error );
            }
            return false;
        case TERMINATED:
            Exception terminatedError = new IllegalStateException( "Connection has been terminated and can't be used" );
            if ( handler1 != null )
            {
                handler1.onFailure( terminatedError );
            }
            if ( handler2 != null )
            {
                handler2.onFailure( terminatedError );
            }
            return false;
        default:
            throw new IllegalStateException( "Unknown status: " + connectionStatus );
        }
    }

    private enum Status
    {
        OPEN,
        RELEASED,
        TERMINATED
    }
}
