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
package com.mware.bigconnect.driver.internal;

import com.mware.bigconnect.driver.util.Pair;

import java.util.Objects;

public class InternalPair<K, V> implements Pair<K, V>
{
    private final K key;
    private final V value;

    protected InternalPair( K key, V value )
    {
        Objects.requireNonNull( key );
        Objects.requireNonNull( value );
        this.key = key;
        this.value = value;
    }

    public K key()
    {
        return key;
    }

    public V value()
    {
        return value;
    }

    public static <K, V> Pair<K, V> of( K key, V value )
    {
        return new InternalPair<>( key, value );
    }

    @Override
    public String toString()
    {
        return String.format( "%s: %s", Objects.toString( key ), Objects.toString( value ) );
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

        InternalPair<?, ?> that = (InternalPair<?, ?>) o;

        return key.equals( that.key ) && value.equals( that.value );
    }

    @Override
    public int hashCode()
    {
        int result = key.hashCode();
        result = 31 * result + value.hashCode();
        return result;
    }
}
