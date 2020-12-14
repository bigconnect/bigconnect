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

import com.mware.bigconnect.driver.internal.BoltServerAddress;
import com.mware.bigconnect.driver.internal.async.ConnectionContext;

import java.util.Set;
import java.util.concurrent.CompletionStage;

/**
 * A generic interface to access all routing tables as a whole.
 * It also provides methods to obtain a routing table or manage a routing table for a specified database.
 */
public interface RoutingTableRegistry
{
    /**
     * Fresh the routing table for the database with given access mode.
     * For server version lower than 4.0, the database name will be ignored while refreshing routing table.
     * @return The future of a new routing table handler.
     */
    CompletionStage<RoutingTableHandler> refreshRoutingTable(ConnectionContext context);

    /**
     * @return all servers in the registry
     */
    Set<BoltServerAddress> allServers();

    /**
     * Removes a routing table of the given database from registry.
     */
    void remove(String databaseName);

    /**
     * Removes all routing tables that has been not used for a long time.
     */
    void purgeAged();
}
