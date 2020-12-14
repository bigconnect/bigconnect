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
import com.mware.bigconnect.driver.internal.spi.ConnectionPool;

/**
 * Load balancing strategy that finds server with least amount of active (checked out of the pool) connections from
 * given readers or writers. It finds a start index for iteration in a round-robin fashion. This is done to prevent
 * choosing same first address over and over when all addresses have same amount of active connections.
 */
public class LeastConnectedLoadBalancingStrategy implements LoadBalancingStrategy
{
    private static final String LOGGER_NAME = LeastConnectedLoadBalancingStrategy.class.getSimpleName();

    private final RoundRobinArrayIndex readersIndex = new RoundRobinArrayIndex();
    private final RoundRobinArrayIndex writersIndex = new RoundRobinArrayIndex();

    private final ConnectionPool connectionPool;
    private final Logger log;

    public LeastConnectedLoadBalancingStrategy( ConnectionPool connectionPool, Logging logging )
    {
        this.connectionPool = connectionPool;
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

    private BoltServerAddress select( BoltServerAddress[] addresses, RoundRobinArrayIndex addressesIndex,
            String addressType )
    {
        int size = addresses.length;
        if ( size == 0 )
        {
            log.trace( "Unable to select %s, no known addresses given", addressType );
            return null;
        }

        // choose start index for iteration in round-robin fashion
        int startIndex = addressesIndex.next( size );
        int index = startIndex;

        BoltServerAddress leastConnectedAddress = null;
        int leastActiveConnections = Integer.MAX_VALUE;

        // iterate over the array to find least connected address
        do
        {
            BoltServerAddress address = addresses[index];
            int activeConnections = connectionPool.inUseConnections( address );

            if ( activeConnections < leastActiveConnections )
            {
                leastConnectedAddress = address;
                leastActiveConnections = activeConnections;
            }

            // loop over to the start of the array when end is reached
            if ( index == size - 1 )
            {
                index = 0;
            }
            else
            {
                index++;
            }
        }
        while ( index != startIndex );

        log.trace( "Selected %s with address: '%s' and active connections: %s",
                addressType, leastConnectedAddress, leastActiveConnections );

        return leastConnectedAddress;
    }
}
