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

import java.util.Arrays;
import java.util.Set;

public class AddressSet
{
    private static final BoltServerAddress[] NONE = {};

    private volatile BoltServerAddress[] addresses = NONE;

    public BoltServerAddress[] toArray()
    {
        return addresses;
    }

    public int size()
    {
        return addresses.length;
    }

    public synchronized void update( Set<BoltServerAddress> addresses )
    {
        this.addresses = addresses.toArray( NONE );
    }

    public synchronized void remove( BoltServerAddress address )
    {
        BoltServerAddress[] addresses = this.addresses;
        if ( addresses != null )
        {
            for ( int i = 0; i < addresses.length; i++ )
            {
                if ( addresses[i].equals( address ) )
                {
                    if ( addresses.length == 1 )
                    {
                        this.addresses = NONE;
                        return;
                    }
                    BoltServerAddress[] copy = new BoltServerAddress[addresses.length - 1];
                    System.arraycopy( addresses, 0, copy, 0, i );
                    System.arraycopy( addresses, i + 1, copy, i, addresses.length - i - 1 );
                    this.addresses = copy;
                    return;
                }
            }
        }
    }

    @Override
    public String toString()
    {
        return "AddressSet=" + Arrays.toString( addresses );
    }
}
