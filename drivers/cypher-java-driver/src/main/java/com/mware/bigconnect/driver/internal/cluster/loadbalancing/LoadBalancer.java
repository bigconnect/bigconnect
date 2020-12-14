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
package com.mware.bigconnect.driver.internal.cluster.loadbalancing;

import io.netty.util.concurrent.EventExecutorGroup;
import com.mware.bigconnect.driver.AccessMode;
import com.mware.bigconnect.driver.Logger;
import com.mware.bigconnect.driver.Logging;
import com.mware.bigconnect.driver.exceptions.ServiceUnavailableException;
import com.mware.bigconnect.driver.exceptions.SessionExpiredException;
import com.mware.bigconnect.driver.internal.BoltServerAddress;
import com.mware.bigconnect.driver.internal.async.ConnectionContext;
import com.mware.bigconnect.driver.internal.async.connection.RoutingConnection;
import com.mware.bigconnect.driver.internal.cluster.*;
import com.mware.bigconnect.driver.internal.spi.Connection;
import com.mware.bigconnect.driver.internal.spi.ConnectionPool;
import com.mware.bigconnect.driver.internal.spi.ConnectionProvider;
import com.mware.bigconnect.driver.internal.util.Clock;
import com.mware.bigconnect.driver.internal.util.Futures;
import com.mware.bigconnect.driver.net.ServerAddressResolver;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

import static com.mware.bigconnect.driver.internal.async.ImmutableConnectionContext.simple;
import static java.lang.String.format;

public class LoadBalancer implements ConnectionProvider
{
    private static final String LOAD_BALANCER_LOG_NAME = "LoadBalancer";
    private final ConnectionPool connectionPool;
    private final RoutingTableRegistry routingTables;
    private final LoadBalancingStrategy loadBalancingStrategy;
    private final EventExecutorGroup eventExecutorGroup;
    private final Logger log;

    public LoadBalancer( BoltServerAddress initialRouter, RoutingSettings settings, ConnectionPool connectionPool,
            EventExecutorGroup eventExecutorGroup, Clock clock, Logging logging,
            LoadBalancingStrategy loadBalancingStrategy, ServerAddressResolver resolver )
    {
        this( connectionPool, createRoutingTables( connectionPool, eventExecutorGroup, initialRouter, resolver, settings, clock, logging ),
                loadBalancerLogger( logging ), loadBalancingStrategy, eventExecutorGroup );
    }

    LoadBalancer( ConnectionPool connectionPool, RoutingTableRegistry routingTables, Logger log, LoadBalancingStrategy loadBalancingStrategy,
            EventExecutorGroup eventExecutorGroup )
    {
        this.connectionPool = connectionPool;
        this.routingTables = routingTables;
        this.loadBalancingStrategy = loadBalancingStrategy;
        this.eventExecutorGroup = eventExecutorGroup;
        this.log = log;
    }

    @Override
    public CompletionStage<Connection> acquireConnection(ConnectionContext context )
    {
        return routingTables.refreshRoutingTable( context )
                .thenCompose( handler -> acquire( context.mode(), handler.routingTable() )
                        .thenApply( connection -> new RoutingConnection( connection, context.databaseName(), context.mode(), handler ) ) );
    }

    @Override
    public CompletionStage<Void> verifyConnectivity()
    {
        return routingTables.refreshRoutingTable( simple() ).handle( ( ignored, error ) -> {
            if ( error != null )
            {
                Throwable cause = Futures.completionExceptionCause( error );
                if ( cause instanceof ServiceUnavailableException )
                {
                    throw Futures.asCompletionException( new ServiceUnavailableException(
                            "Unable to connect to database, ensure the database is running and that there is a working network connection to it.", cause ) );
                }
                throw Futures.asCompletionException( cause );
            }
            return null;
        } );
    }

    @Override
    public CompletionStage<Void> close()
    {
        return connectionPool.close();
    }

    private CompletionStage<Connection> acquire(AccessMode mode, RoutingTable routingTable )
    {
        AddressSet addresses = addressSet( mode, routingTable );
        CompletableFuture<Connection> result = new CompletableFuture<>();
        acquire( mode, routingTable, addresses, result );
        return result;
    }

    private void acquire( AccessMode mode, RoutingTable routingTable, AddressSet addresses, CompletableFuture<Connection> result )
    {
        BoltServerAddress address = selectAddress( mode, addresses );

        if ( address == null )
        {
            result.completeExceptionally( new SessionExpiredException(
                    "Failed to obtain connection towards " + mode + " server. " +
                    "Known routing table is: " + routingTable ) );
            return;
        }

        connectionPool.acquire( address ).whenComplete( ( connection, completionError ) ->
        {
            Throwable error = Futures.completionExceptionCause( completionError );
            if ( error != null )
            {
                if ( error instanceof ServiceUnavailableException )
                {
                    SessionExpiredException errorToLog = new SessionExpiredException( format( "Server at %s is no longer available", address ), error );
                    log.warn( "Failed to obtain a connection towards address " + address, errorToLog );
                    routingTable.forget( address );
                    eventExecutorGroup.next().execute( () -> acquire( mode, routingTable, addresses, result ) );
                }
                else
                {
                    result.completeExceptionally( error );
                }
            }
            else
            {
                result.complete( connection );
            }
        } );
    }

    private static AddressSet addressSet( AccessMode mode, RoutingTable routingTable )
    {
        switch ( mode )
        {
        case READ:
            return routingTable.readers();
        case WRITE:
            return routingTable.writers();
        default:
            throw unknownMode( mode );
        }
    }

    private BoltServerAddress selectAddress( AccessMode mode, AddressSet servers )
    {
        BoltServerAddress[] addresses = servers.toArray();

        switch ( mode )
        {
        case READ:
            return loadBalancingStrategy.selectReader( addresses );
        case WRITE:
            return loadBalancingStrategy.selectWriter( addresses );
        default:
            throw unknownMode( mode );
        }
    }

    private static RoutingTableRegistry createRoutingTables( ConnectionPool connectionPool, EventExecutorGroup eventExecutorGroup, BoltServerAddress initialRouter,
            ServerAddressResolver resolver, RoutingSettings settings, Clock clock, Logging logging )
    {
        Logger log = loadBalancerLogger( logging );
        Rediscovery rediscovery = createRediscovery( eventExecutorGroup, initialRouter, resolver, settings, clock, log );
        return new RoutingTableRegistryImpl( connectionPool, rediscovery, clock, log, settings.routingTablePurgeDelayMs() );
    }

    private static Rediscovery createRediscovery( EventExecutorGroup eventExecutorGroup, BoltServerAddress initialRouter, ServerAddressResolver resolver,
            RoutingSettings settings, Clock clock, Logger log )
    {
        ClusterCompositionProvider clusterCompositionProvider = new RoutingProcedureClusterCompositionProvider( clock, settings.routingContext() );
        return new RediscoveryImpl( initialRouter, settings, clusterCompositionProvider, eventExecutorGroup, resolver, log );
    }

    private static Logger loadBalancerLogger( Logging logging )
    {
        return logging.getLog( LOAD_BALANCER_LOG_NAME );
    }

    private static RuntimeException unknownMode(AccessMode mode )
    {
        return new IllegalArgumentException( "Mode '" + mode + "' is not supported" );
    }
}
