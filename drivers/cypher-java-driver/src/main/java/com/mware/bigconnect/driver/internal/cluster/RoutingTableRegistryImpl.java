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
import com.mware.bigconnect.driver.internal.async.ConnectionContext;
import com.mware.bigconnect.driver.internal.spi.ConnectionPool;
import com.mware.bigconnect.driver.internal.util.Clock;

import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

public class RoutingTableRegistryImpl implements RoutingTableRegistry
{
    private final ConcurrentMap<String,RoutingTableHandler> routingTableHandlers;
    private final RoutingTableHandlerFactory factory;
    private final Logger logger;

    public RoutingTableRegistryImpl( ConnectionPool connectionPool, Rediscovery rediscovery, Clock clock, Logger logger, long routingTablePurgeDelayMs )
    {
        this( new ConcurrentHashMap<>(), new RoutingTableHandlerFactory( connectionPool, rediscovery, clock, logger, routingTablePurgeDelayMs ), logger );
    }

    RoutingTableRegistryImpl(ConcurrentMap<String,RoutingTableHandler> routingTableHandlers, RoutingTableHandlerFactory factory, Logger logger )
    {
        this.factory = factory;
        this.routingTableHandlers = routingTableHandlers;
        this.logger = logger;
    }

    @Override
    public CompletionStage<RoutingTableHandler> refreshRoutingTable(ConnectionContext context )
    {
        RoutingTableHandler handler = getOrCreate( context.databaseName() );
        return handler.refreshRoutingTable( context ).thenApply( ignored -> handler );
    }

    @Override
    public Set<BoltServerAddress> allServers()
    {
        // obviously we just had a snapshot of all servers in all routing tables
        // after we read it, the set could already be changed.
        Set<BoltServerAddress> servers = new HashSet<>();
        for ( RoutingTableHandler tableHandler : routingTableHandlers.values() )
        {
            servers.addAll( tableHandler.servers() );
        }
        return servers;
    }

    @Override
    public void remove( String databaseName )
    {
        routingTableHandlers.remove( databaseName );
        logger.debug( "Routing table handler for database '%s' is removed.", databaseName );
    }

    @Override
    public void purgeAged()
    {
        routingTableHandlers.forEach( ( databaseName, handler ) -> {
            if ( handler.isRoutingTableAged() )
            {
                logger.info( "Routing table handler for database '%s' is removed because it has not been used for a long time. Routing table: %s",
                        databaseName, handler.routingTable() );
                routingTableHandlers.remove( databaseName );
            }
        } );
    }

    // For tests
    public boolean contains( String databaseName )
    {
        return routingTableHandlers.containsKey( databaseName );
    }

    private RoutingTableHandler getOrCreate( String databaseName )
    {
        return routingTableHandlers.computeIfAbsent( databaseName, name -> {
            RoutingTableHandler handler = factory.newInstance( name, this );
            logger.debug( "Routing table handler for database '%s' is added.", databaseName );
            return handler;
        } );
    }

    static class RoutingTableHandlerFactory
    {
        private final ConnectionPool connectionPool;
        private final Rediscovery rediscovery;
        private final Logger log;
        private final Clock clock;
        private final long routingTablePurgeDelayMs;

        RoutingTableHandlerFactory( ConnectionPool connectionPool, Rediscovery rediscovery, Clock clock, Logger log, long routingTablePurgeDelayMs )
        {
            this.connectionPool = connectionPool;
            this.rediscovery = rediscovery;
            this.clock = clock;
            this.log = log;
            this.routingTablePurgeDelayMs = routingTablePurgeDelayMs;
        }

        RoutingTableHandler newInstance(String databaseName, RoutingTableRegistry allTables )
        {
            ClusterRoutingTable routingTable = new ClusterRoutingTable( databaseName, clock );
            return new RoutingTableHandler( routingTable, rediscovery, connectionPool, allTables, log, routingTablePurgeDelayMs );
        }
    }
}
