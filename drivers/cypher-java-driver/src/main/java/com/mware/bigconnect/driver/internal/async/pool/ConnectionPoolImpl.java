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
import io.netty.channel.EventLoopGroup;
import io.netty.channel.pool.ChannelPool;
import io.netty.util.concurrent.Future;
import com.mware.bigconnect.driver.Logger;
import com.mware.bigconnect.driver.Logging;
import com.mware.bigconnect.driver.exceptions.ClientException;
import com.mware.bigconnect.driver.exceptions.ServiceUnavailableException;
import com.mware.bigconnect.driver.internal.BoltServerAddress;
import com.mware.bigconnect.driver.internal.async.connection.ChannelConnector;
import com.mware.bigconnect.driver.internal.metrics.ListenerEvent;
import com.mware.bigconnect.driver.internal.metrics.MetricsListener;
import com.mware.bigconnect.driver.internal.spi.Connection;
import com.mware.bigconnect.driver.internal.spi.ConnectionPool;
import com.mware.bigconnect.driver.internal.util.Clock;
import com.mware.bigconnect.driver.internal.util.Futures;

import java.util.Map;
import java.util.Set;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;

import static java.lang.String.format;

public class ConnectionPoolImpl implements ConnectionPool
{
    private final ChannelConnector connector;
    private final Bootstrap bootstrap;
    private final NettyChannelTracker nettyChannelTracker;
    private final NettyChannelHealthChecker channelHealthChecker;
    private final PoolSettings settings;
    private final Logger log;
    private final MetricsListener metricsListener;
    private final boolean ownsEventLoopGroup;

    private final ConcurrentMap<BoltServerAddress,ExtendedChannelPool> pools = new ConcurrentHashMap<>();
    private final AtomicBoolean closed = new AtomicBoolean();
    private final ConnectionFactory connectionFactory;

    public ConnectionPoolImpl( ChannelConnector connector, Bootstrap bootstrap, PoolSettings settings, MetricsListener metricsListener, Logging logging,
            Clock clock, boolean ownsEventLoopGroup )
    {
        this( connector, bootstrap, new NettyChannelTracker( metricsListener, bootstrap.config().group().next(), logging ), settings, metricsListener, logging,
                clock, ownsEventLoopGroup, new NetworkConnectionFactory( clock, metricsListener ) );
    }

    public ConnectionPoolImpl( ChannelConnector connector, Bootstrap bootstrap, NettyChannelTracker nettyChannelTracker, PoolSettings settings,
            MetricsListener metricsListener, Logging logging, Clock clock, boolean ownsEventLoopGroup, ConnectionFactory connectionFactory )
    {
        this.connector = connector;
        this.bootstrap = bootstrap;
        this.nettyChannelTracker = nettyChannelTracker;
        this.channelHealthChecker = new NettyChannelHealthChecker( settings, clock, logging );
        this.settings = settings;
        this.metricsListener = metricsListener;
        this.log = logging.getLog( ConnectionPool.class.getSimpleName() );
        this.ownsEventLoopGroup = ownsEventLoopGroup;
        this.connectionFactory = connectionFactory;
    }

    @Override
    public CompletionStage<Connection> acquire(BoltServerAddress address )
    {
        log.trace( "Acquiring a connection from pool towards %s", address );

        assertNotClosed();
        ExtendedChannelPool pool = getOrCreatePool( address );

        ListenerEvent acquireEvent = metricsListener.createListenerEvent();
        metricsListener.beforeAcquiringOrCreating( pool.id(), acquireEvent );
        Future<Channel> connectionFuture = pool.acquire();

        return Futures.asCompletionStage( connectionFuture ).handle( ( channel, error ) ->
        {
            try
            {
                processAcquisitionError( pool, address, error );
                assertNotClosed( address, channel, pool );
                Connection connection = connectionFactory.createConnection( channel, pool );

                metricsListener.afterAcquiredOrCreated( pool.id(), acquireEvent );
                return connection;
            }
            finally
            {
                metricsListener.afterAcquiringOrCreating( pool.id() );
            }
        } );
    }

    @Override
    public void retainAll( Set<BoltServerAddress> addressesToRetain )
    {
        for ( BoltServerAddress address : pools.keySet() )
        {
            if ( !addressesToRetain.contains( address ) )
            {
                int activeChannels = nettyChannelTracker.inUseChannelCount( address );
                if ( activeChannels == 0 )
                {
                    // address is not present in updated routing table and has no active connections
                    // it's now safe to terminate corresponding connection pool and forget about it
                    ExtendedChannelPool pool = pools.remove( address );
                    if ( pool != null )
                    {
                        log.info( "Closing connection pool towards %s, it has no active connections " +
                                  "and is not in the routing table", address );
                        closePool( pool );
                    }
                }
            }
        }
    }

    @Override
    public int inUseConnections( BoltServerAddress address )
    {
        return nettyChannelTracker.inUseChannelCount( address );
    }

    @Override
    public int idleConnections( BoltServerAddress address )
    {
        return nettyChannelTracker.idleChannelCount( address );
    }

    @Override
    public CompletionStage<Void> close()
    {
        if ( closed.compareAndSet( false, true ) )
        {
            try
            {
                nettyChannelTracker.prepareToCloseChannels();
                for ( Map.Entry<BoltServerAddress,ExtendedChannelPool> entry : pools.entrySet() )
                {
                    BoltServerAddress address = entry.getKey();
                    ExtendedChannelPool pool = entry.getValue();
                    log.info( "Closing connection pool towards %s", address );
                    closePool( pool );
                }

                pools.clear();
            }
            finally
            {

                if (ownsEventLoopGroup) {
                    // This is an attempt to speed up the shut down procedure of the driver
                    // Feel free return this back to shutdownGracefully() method with default values
                    // if this proves troublesome!!!
                    eventLoopGroup().shutdownGracefully(200, 15_000, TimeUnit.MILLISECONDS);
                }
            }
        }
        if (!ownsEventLoopGroup)
        {
            return Futures.completedWithNull();
        }

        return Futures.asCompletionStage( eventLoopGroup().terminationFuture() )
                .thenApply( ignore -> null );
    }

    @Override
    public boolean isOpen( BoltServerAddress address )
    {
        return pools.containsKey( address );
    }

    private ExtendedChannelPool getOrCreatePool( BoltServerAddress address )
    {
        return pools.computeIfAbsent( address, this::newPool );
    }

    private void closePool( ExtendedChannelPool pool )
    {
        pool.close();
        // after the connection pool is removed/close, I can remove its metrics.
        metricsListener.removePoolMetrics( pool.id() );
    }

    ExtendedChannelPool newPool( BoltServerAddress address )
    {
        NettyChannelPool pool =
                new NettyChannelPool( address, connector, bootstrap, nettyChannelTracker, channelHealthChecker, settings.connectionAcquisitionTimeout(),
                        settings.maxConnectionPoolSize() );
        // before the connection pool is added I can add the metrics for the pool.
        metricsListener.putPoolMetrics( pool.id(), address, this );
        return pool;
    }

    private EventLoopGroup eventLoopGroup()
    {
        return bootstrap.config().group();
    }

    private void processAcquisitionError( ExtendedChannelPool pool, BoltServerAddress serverAddress, Throwable error )
    {
        Throwable cause = Futures.completionExceptionCause( error );
        if ( cause != null )
        {
            if ( cause instanceof TimeoutException)
            {
                // NettyChannelPool returns future failed with TimeoutException if acquire operation takes more than
                // configured time, translate this exception to a prettier one and re-throw
                metricsListener.afterTimedOutToAcquireOrCreate( pool.id() );
                throw new ClientException(
                        "Unable to acquire connection from the pool within configured maximum time of " +
                        settings.connectionAcquisitionTimeout() + "ms" );
            }
            else if ( pool.isClosed() )
            {
                // There is a race condition where a thread tries to acquire a connection while the pool is closed by another concurrent thread.
                // Treat as failed to obtain connection for a direct driver. For a routing driver, this error should be retried.
                throw new ServiceUnavailableException( format( "Connection pool for server %s is closed while acquiring a connection.", serverAddress ),
                        cause );
            }
            else
            {
                // some unknown error happened during connection acquisition, propagate it
                throw new CompletionException( cause );
            }
        }
    }

    private void assertNotClosed()
    {
        if ( closed.get() )
        {
            throw new IllegalStateException( "Pool closed" );
        }
    }

    private void assertNotClosed( BoltServerAddress address, Channel channel, ChannelPool pool )
    {
        if ( closed.get() )
        {
            pool.release( channel );
            pool.close();
            pools.remove( address );
            assertNotClosed();
        }
    }

    @Override
    public String toString()
    {
        return "ConnectionPoolImpl{" + "pools=" + pools + '}';
    }

    // for testing only
    ExtendedChannelPool getPool( BoltServerAddress address )
    {
        return pools.get( address );
    }
}
