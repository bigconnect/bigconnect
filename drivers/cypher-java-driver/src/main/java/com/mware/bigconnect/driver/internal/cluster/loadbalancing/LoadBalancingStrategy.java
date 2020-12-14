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

import com.mware.bigconnect.driver.internal.BoltServerAddress;

/**
 * A facility to select most appropriate reader or writer among the given addresses for request processing.
 */
public interface LoadBalancingStrategy
{
    /**
     * Select most appropriate read address from the given array of addresses.
     *
     * @param knownReaders array of all known readers.
     * @return most appropriate reader or {@code null} if it can't be selected.
     */
    BoltServerAddress selectReader(BoltServerAddress[] knownReaders);

    /**
     * Select most appropriate write address from the given array of addresses.
     *
     * @param knownWriters array of all known writers.
     * @return most appropriate writer or {@code null} if it can't be selected.
     */
    BoltServerAddress selectWriter(BoltServerAddress[] knownWriters);
}
