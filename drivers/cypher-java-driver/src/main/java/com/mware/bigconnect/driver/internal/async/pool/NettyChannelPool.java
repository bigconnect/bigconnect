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
package com.mware.bigconnect.driver.internal.async.pool;

import io.netty.bootstrap.Bootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.pool.ChannelHealthChecker;
import io.netty.channel.pool.FixedChannelPool;
import com.mware.bigconnect.driver.internal.BoltServerAddress;
import com.mware.bigconnect.driver.internal.async.connection.ChannelConnector;
import com.mware.bigconnect.driver.internal.metrics.ListenerEvent;

import java.util.concurrent.atomic.AtomicBoolean;

import static java.util.Objects.requireNonNull;
import static com.mware.bigconnect.driver.internal.async.connection.ChannelAttributes.setPoolId;

public class NettyChannelPool extends FixedChannelPool implements ExtendedChannelPool
{
    /**
     * Unlimited amount of parties are allowed to request channels from the pool.
     */
    private static final int MAX_PENDING_ACQUIRES = Integer.MAX_VALUE;
    /**
     * Do not check channels when they are returned to the pool.
     */
    private static final boolean RELEASE_HEALTH_CHECK = false;

    private final BoltServerAddress address;
    private final ChannelConnector connector;
    private final NettyChannelTracker handler;
    private final AtomicBoolean closed = new AtomicBoolean( false );
    private final String id;

    public NettyChannelPool( BoltServerAddress address, ChannelConnector connector, Bootstrap bootstrap, NettyChannelTracker handler,
            ChannelHealthChecker healthCheck, long acquireTimeoutMillis, int maxConnections )
    {
        super( bootstrap, handler, healthCheck, AcquireTimeoutAction.FAIL, acquireTimeoutMillis, maxConnections,
                MAX_PENDING_ACQUIRES, RELEASE_HEALTH_CHECK );

        this.address = requireNonNull( address );
        this.connector = requireNonNull( connector );
        this.handler = requireNonNull( handler );
        this.id = poolId( address );
    }

    @Override
    protected ChannelFuture connectChannel( Bootstrap bootstrap )
    {
        ListenerEvent creatingEvent = handler.channelCreating( this.id );
        ChannelFuture channelFuture = connector.connect( address, bootstrap );
        channelFuture.addListener( future ->
        {
            if ( future.isSuccess() )
            {
                // notify pool handler about a successful connection
                Channel channel = channelFuture.channel();
                setPoolId( channel, this.id );
                handler.channelCreated( channel, creatingEvent );
            }
            else
            {
                handler.channelFailedToCreate( this.id );
            }
        } );
        return channelFuture;
    }

    @Override
    public void close()
    {
        if ( closed.compareAndSet( false, true ) )
        {
            super.close();
        }
    }

    public boolean isClosed()
    {
        return closed.get();
    }

    public String id()
    {
        return this.id;
    }

    private String poolId(BoltServerAddress serverAddress )
    {
        return String.format( "%s:%d-%d", serverAddress.host(), serverAddress.port(), this.hashCode() );
    }
}
