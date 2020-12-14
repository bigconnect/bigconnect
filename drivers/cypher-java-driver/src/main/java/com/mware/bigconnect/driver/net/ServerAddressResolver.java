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
package com.mware.bigconnect.driver.net;

import java.util.Set;

/**
 * A resolver function used by the routing driver to resolve the initial address used to create the driver.
 */
@FunctionalInterface
public interface ServerAddressResolver
{
    /**
     * Resolve the given address to a set of other addresses.
     * Exceptions thrown by this method will be logged and driver will continue using the original address.
     *
     * @param address the address to resolve.
     * @return new set of addresses.
     */
    Set<ServerAddress> resolve(ServerAddress address);
}
