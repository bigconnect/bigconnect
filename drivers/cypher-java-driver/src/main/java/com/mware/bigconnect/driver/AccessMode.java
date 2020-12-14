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
package com.mware.bigconnect.driver;

/**
 * Used by Routing Driver to decide if a transaction should be routed to a write server or a read server in a cluster.
 * When running a transaction, a write transaction requires a server that supports writes.
 * A read transaction, on the other hand, requires a server that supports read operations.
 * This classification is key for routing driver to route transactions to a cluster correctly.
 *
 * While any {@link AccessMode} will be ignored while running transactions via a driver towards a single server.
 * As the single server serves both read and write operations at the same time.
 */
public enum AccessMode
{
    /**
     * Use this for transactions that requires a read server in a cluster
     */
    READ,
    /**
     * Use this for transactions that requires a write server in a cluster
     */
    WRITE
}
