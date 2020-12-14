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

import java.util.concurrent.atomic.AtomicInteger;

public class RoundRobinArrayIndex
{
    private final AtomicInteger offset;

    RoundRobinArrayIndex()
    {
        this( 0 );
    }

    // only for testing
    RoundRobinArrayIndex( int initialOffset )
    {
        this.offset = new AtomicInteger( initialOffset );
    }

    public int next( int arrayLength )
    {
        if ( arrayLength == 0 )
        {
            return -1;
        }

        int nextOffset;
        while ( (nextOffset = offset.getAndIncrement()) < 0 )
        {
            // overflow, try resetting back to zero
            offset.compareAndSet( nextOffset + 1, 0 );
        }
        return nextOffset % arrayLength;
    }
}
