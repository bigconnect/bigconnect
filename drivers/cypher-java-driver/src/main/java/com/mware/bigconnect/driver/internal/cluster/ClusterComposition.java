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

import com.mware.bigconnect.driver.Record;
import com.mware.bigconnect.driver.Value;
import com.mware.bigconnect.driver.internal.BoltServerAddress;

import java.util.LinkedHashSet;
import java.util.Objects;
import java.util.Set;
import java.util.function.Function;

public final class ClusterComposition
{
    private static final long MAX_TTL = Long.MAX_VALUE / 1000L;
    private static final Function<Value,BoltServerAddress> OF_BoltServerAddress = value -> new BoltServerAddress( value.asString() );

    private final Set<BoltServerAddress> readers;
    private final Set<BoltServerAddress> writers;
    private final Set<BoltServerAddress> routers;
    private final long expirationTimestamp;

    private ClusterComposition( long expirationTimestamp )
    {
        this.readers = new LinkedHashSet<>();
        this.writers = new LinkedHashSet<>();
        this.routers = new LinkedHashSet<>();
        this.expirationTimestamp = expirationTimestamp;
    }

    /** For testing */
    public ClusterComposition(
            long expirationTimestamp,
            Set<BoltServerAddress> readers,
            Set<BoltServerAddress> writers,
            Set<BoltServerAddress> routers )
    {
        this( expirationTimestamp );
        this.readers.addAll( readers );
        this.writers.addAll( writers );
        this.routers.addAll( routers );
    }

    public boolean hasWriters()
    {
        return !writers.isEmpty();
    }

    public boolean hasRoutersAndReaders()
    {
        return !routers.isEmpty() && !readers.isEmpty();
    }

    public Set<BoltServerAddress> readers()
    {
        return new LinkedHashSet<>( readers );
    }

    public Set<BoltServerAddress> writers()
    {
        return new LinkedHashSet<>( writers );
    }

    public Set<BoltServerAddress> routers()
    {
        return new LinkedHashSet<>( routers );
    }

    public long expirationTimestamp() {
        return this.expirationTimestamp;
    }

    @Override
    public boolean equals( Object o )
    {
        if ( this == o )
        {
            return true;
        }
        if ( o == null || getClass() != o.getClass() )
        {
            return false;
        }
        ClusterComposition that = (ClusterComposition) o;
        return expirationTimestamp == that.expirationTimestamp &&
               Objects.equals( readers, that.readers ) &&
               Objects.equals( writers, that.writers ) &&
               Objects.equals( routers, that.routers );
    }

    @Override
    public int hashCode()
    {
        return Objects.hash( readers, writers, routers, expirationTimestamp );
    }

    @Override
    public String toString()
    {
        return "ClusterComposition{" +
               "readers=" + readers +
               ", writers=" + writers +
               ", routers=" + routers +
               ", expirationTimestamp=" + expirationTimestamp +
               '}';
    }

    public static ClusterComposition parse( Record record, long now )
    {
        if ( record == null )
        {
            return null;
        }

        final ClusterComposition result = new ClusterComposition( expirationTimestamp( now, record ) );
        record.get( "servers" ).asList( new Function<Value, Void>()
        {
            @Override
            public Void apply(Value value )
            {
                result.servers( value.get( "role" ).asString() )
                        .addAll( value.get( "addresses" ).asList( OF_BoltServerAddress ) );
                return null;
            }
        } );
        return result;
    }

    private static long expirationTimestamp( long now, Record record )
    {
        long ttl = record.get( "ttl" ).asLong();
        long expirationTimestamp = now + ttl * 1000;
        if ( ttl < 0 || ttl >= MAX_TTL || expirationTimestamp < 0 )
        {
            expirationTimestamp = Long.MAX_VALUE;
        }
        return expirationTimestamp;
    }

    private Set<BoltServerAddress> servers(String role )
    {
        switch ( role )
        {
        case "READ":
            return readers;
        case "WRITE":
            return writers;
        case "ROUTE":
            return routers;
        default:
            throw new IllegalArgumentException( "invalid server role: " + role );
        }
    }
}
