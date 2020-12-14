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

import com.mware.bigconnect.driver.Logger;
import com.mware.bigconnect.driver.Logging;
import com.mware.bigconnect.driver.internal.BoltServerAddress;

/**
 * Load balancing strategy that selects addresses in round-robin fashion. It maintains separate indices for readers and
 * writers.
 */
public class RoundRobinLoadBalancingStrategy implements LoadBalancingStrategy
{
    private static final String LOGGER_NAME = RoundRobinLoadBalancingStrategy.class.getSimpleName();

    private final RoundRobinArrayIndex readersIndex = new RoundRobinArrayIndex();
    private final RoundRobinArrayIndex writersIndex = new RoundRobinArrayIndex();

    private final Logger log;

    public RoundRobinLoadBalancingStrategy( Logging logging )
    {
        this.log = logging.getLog( LOGGER_NAME );
    }

    @Override
    public BoltServerAddress selectReader( BoltServerAddress[] knownReaders )
    {
        return select( knownReaders, readersIndex, "reader" );
    }

    @Override
    public BoltServerAddress selectWriter( BoltServerAddress[] knownWriters )
    {
        return select( knownWriters, writersIndex, "writer" );
    }

    private BoltServerAddress select( BoltServerAddress[] addresses, RoundRobinArrayIndex roundRobinIndex,
            String addressType )
    {
        int length = addresses.length;
        if ( length == 0 )
        {
            log.trace( "Unable to select %s, no known addresses given", addressType );
            return null;
        }

        int index = roundRobinIndex.next( length );
        BoltServerAddress address = addresses[index];
        log.trace( "Selected %s with address: '%s'", addressType, address );
        return address;
    }
}
