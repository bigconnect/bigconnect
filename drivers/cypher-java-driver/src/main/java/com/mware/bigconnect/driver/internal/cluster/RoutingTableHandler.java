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
package com.mware.bigconnect.driver.internal.cluster;

import com.mware.bigconnect.driver.Logger;
import com.mware.bigconnect.driver.internal.BoltServerAddress;
import com.mware.bigconnect.driver.internal.RoutingErrorHandler;
import com.mware.bigconnect.driver.internal.async.ConnectionContext;
import com.mware.bigconnect.driver.internal.spi.ConnectionPool;
import com.mware.bigconnect.driver.internal.util.Futures;

import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

import static java.util.concurrent.CompletableFuture.completedFuture;

public class RoutingTableHandler implements RoutingErrorHandler
{
    private final RoutingTable routingTable;
    private final String databaseName;
    private final RoutingTableRegistry routingTableRegistry;
    private volatile CompletableFuture<RoutingTable> refreshRoutingTableFuture;
    private final ConnectionPool connectionPool;
    private final Rediscovery rediscovery;
    private final Logger log;
    private final long routingTablePurgeDelayMs;

    public RoutingTableHandler( RoutingTable routingTable, Rediscovery rediscovery, ConnectionPool connectionPool, RoutingTableRegistry routingTableRegistry,
            Logger log, long routingTablePurgeDelayMs )
    {
        this.routingTable = routingTable;
        this.databaseName = routingTable.database();
        this.rediscovery = rediscovery;
        this.connectionPool = connectionPool;
        this.routingTableRegistry = routingTableRegistry;
        this.log = log;
        this.routingTablePurgeDelayMs = routingTablePurgeDelayMs;
    }

    @Override
    public void onConnectionFailure( BoltServerAddress address )
    {
        // remove server from the routing table, to prevent concurrent threads from making connections to this address
        routingTable.forget( address );
    }

    @Override
    public void onWriteFailure( BoltServerAddress address )
    {
        routingTable.forgetWriter( address );
    }

    synchronized CompletionStage<RoutingTable> refreshRoutingTable(ConnectionContext context )
    {
        if ( refreshRoutingTableFuture != null )
        {
            // refresh is already happening concurrently, just use it's result
            return refreshRoutingTableFuture;
        }
        else if ( routingTable.isStaleFor( context.mode() ) )
        {
            // existing routing table is not fresh and should be updated
            log.info( "Routing table for database '%s' is stale. %s", databaseName, routingTable );

            CompletableFuture<RoutingTable> resultFuture = new CompletableFuture<>();
            refreshRoutingTableFuture = resultFuture;

            rediscovery.lookupClusterComposition( routingTable, connectionPool, context.rediscoveryBookmark() )
                    .whenComplete( ( composition, completionError ) ->
                    {
                        Throwable error = Futures.completionExceptionCause( completionError );
                        if ( error != null )
                        {
                            clusterCompositionLookupFailed( error );
                        }
                        else
                        {
                            freshClusterCompositionFetched( composition );
                        }
                    } );

            return resultFuture;
        }
        else
        {
            // existing routing table is fresh, use it
            return completedFuture( routingTable );
        }
    }

    private synchronized void freshClusterCompositionFetched( ClusterComposition composition )
    {
        try
        {
            routingTable.update( composition );
            routingTableRegistry.purgeAged();
            connectionPool.retainAll( routingTableRegistry.allServers() );

            log.info( "Updated routing table for database '%s'. %s", databaseName, routingTable );

            CompletableFuture<RoutingTable> routingTableFuture = refreshRoutingTableFuture;
            refreshRoutingTableFuture = null;
            routingTableFuture.complete( routingTable );
        }
        catch ( Throwable error )
        {
            clusterCompositionLookupFailed( error );
        }
    }

    private synchronized void clusterCompositionLookupFailed( Throwable error )
    {
        log.error( String.format( "Failed to update routing table for database '%s'. Current routing table: %s.", databaseName, routingTable ), error );
        routingTableRegistry.remove( databaseName );
        CompletableFuture<RoutingTable> routingTableFuture = refreshRoutingTableFuture;
        refreshRoutingTableFuture = null;
        routingTableFuture.completeExceptionally( error );
    }

    // This method cannot be synchronized as it will be visited by all routing table handler's threads concurrently
    public Set<BoltServerAddress> servers()
    {
        return routingTable.servers();
    }

    // This method cannot be synchronized as it will be visited by all routing table handler's threads concurrently
    public boolean isRoutingTableAged()
    {
        return refreshRoutingTableFuture == null && routingTable.hasBeenStaleFor( routingTablePurgeDelayMs );
    }

    // for testing only
    public RoutingTable routingTable()
    {
        return routingTable;
    }
}
