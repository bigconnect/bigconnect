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

import com.mware.bigconnect.driver.AccessMode;
import com.mware.bigconnect.driver.internal.BoltServerAddress;
import com.mware.bigconnect.driver.internal.util.Clock;

import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.Set;

import static java.lang.String.format;
import static java.util.Arrays.asList;

public class ClusterRoutingTable implements RoutingTable
{
    private static final int MIN_ROUTERS = 1;

    private final Clock clock;
    private volatile long expirationTimestamp;
    private final AddressSet readers;
    private final AddressSet writers;
    private final AddressSet routers;

    private final String databaseName; // specifies the database this routing table is acquired for
    private boolean preferInitialRouter;

    public ClusterRoutingTable(String ofDatabase, Clock clock, BoltServerAddress... routingAddresses )
    {
        this( ofDatabase, clock );
        routers.update( new LinkedHashSet<>( asList( routingAddresses ) ) );
    }

    private ClusterRoutingTable(String ofDatabase, Clock clock )
    {
        this.databaseName = ofDatabase;
        this.clock = clock;
        this.expirationTimestamp = clock.millis() - 1;
        this.preferInitialRouter = true;

        this.readers = new AddressSet();
        this.writers = new AddressSet();
        this.routers = new AddressSet();
    }

    @Override
    public boolean isStaleFor( AccessMode mode )
    {
        return expirationTimestamp < clock.millis() ||
               routers.size() < MIN_ROUTERS ||
               mode == AccessMode.READ && readers.size() == 0 ||
               mode == AccessMode.WRITE && writers.size() == 0;
    }

    @Override
    public boolean hasBeenStaleFor( long extraTime )
    {
        long totalTime = expirationTimestamp + extraTime;
        if ( totalTime < 0 )
        {
            totalTime = Long.MAX_VALUE;
        }
        return  totalTime < clock.millis();
    }

    @Override
    public synchronized void update( ClusterComposition cluster )
    {
        expirationTimestamp = cluster.expirationTimestamp();
        readers.update( cluster.readers() );
        writers.update( cluster.writers() );
        routers.update( cluster.routers() );
        preferInitialRouter = !cluster.hasWriters();
    }

    @Override
    public synchronized void forget( BoltServerAddress address )
    {
        routers.remove( address );
        readers.remove( address );
        writers.remove( address );
    }

    @Override
    public AddressSet readers()
    {
        return readers;
    }

    @Override
    public AddressSet writers()
    {
        return writers;
    }

    @Override
    public AddressSet routers()
    {
        return routers;
    }

    @Override
    public Set<BoltServerAddress> servers()
    {
        Set<BoltServerAddress> servers = new HashSet<>();
        Collections.addAll( servers, readers.toArray() );
        Collections.addAll( servers, writers.toArray() );
        Collections.addAll( servers, routers.toArray() );
        return servers;
    }

    public String database()
    {
        return databaseName;
    }

    @Override
    public void forgetWriter( BoltServerAddress toRemove )
    {
        writers.remove( toRemove );
    }

    @Override
    public boolean preferInitialRouter()
    {
        return preferInitialRouter;
    }

    @Override
    public synchronized String toString()
    {
        return format( "Ttl %s, currentTime %s, routers %s, writers %s, readers %s, database '%s'", expirationTimestamp, clock.millis(), routers, writers, readers, databaseName );
    }
}
