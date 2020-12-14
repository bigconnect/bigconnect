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
package com.mware.bigconnect.driver.internal.value;

import com.mware.bigconnect.driver.Value;
import com.mware.bigconnect.driver.Values;
import com.mware.bigconnect.driver.internal.types.InternalTypeSystem;
import com.mware.bigconnect.driver.internal.util.Extract;
import com.mware.bigconnect.driver.types.Type;

import java.util.Map;
import java.util.function.Function;

import static com.mware.bigconnect.driver.Values.ofObject;
import static com.mware.bigconnect.driver.Values.ofValue;
import static com.mware.bigconnect.driver.internal.util.Format.formatPairs;

public class MapValue extends ValueAdapter
{
    private final Map<String, Value> val;

    public MapValue( Map<String, Value> val )
    {
        if ( val == null )
        {
            throw new IllegalArgumentException( "Cannot construct MapValue from null" );
        }
        this.val = val;
    }

    @Override
    public boolean isEmpty()
    {
        return val.isEmpty();
    }

    @Override
    public Map<String, Object> asObject()
    {
        return asMap( ofObject() );
    }

    @Override
    public Map<String, Object> asMap()
    {
        return Extract.map( val, ofObject() );
    }

    @Override
    public <T> Map<String, T> asMap(Function<Value, T> mapFunction )
    {
        return Extract.map( val, mapFunction );
    }

    @Override
    public int size()
    {
        return val.size();
    }

    @Override
    public boolean containsKey( String key )
    {
        return val.containsKey( key );
    }

    @Override
    public Iterable<String> keys()
    {
        return val.keySet();
    }

    @Override
    public Iterable<Value> values()
    {
        return val.values();
    }

    @Override
    public <T> Iterable<T> values(Function<Value, T> mapFunction )
    {
        return Extract.map( val, mapFunction ).values();
    }

    @Override
    public Value get( String key )
    {
        Value value = val.get( key );
        return value == null ? Values.NULL: value;
    }

    @Override
    public String toString()
    {
        return formatPairs( asMap( ofValue() ) );
    }

    @Override
    public Type type()
    {
        return InternalTypeSystem.TYPE_SYSTEM.MAP();
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

        MapValue values = (MapValue) o;
        return val.equals( values.val );
    }

    @Override
    public int hashCode()
    {
        return val.hashCode();
    }
}
