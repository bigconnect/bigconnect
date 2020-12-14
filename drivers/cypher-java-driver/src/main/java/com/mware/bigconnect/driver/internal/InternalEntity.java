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

import com.mware.bigconnect.driver.Value;
import com.mware.bigconnect.driver.Values;
import com.mware.bigconnect.driver.internal.util.Extract;
import com.mware.bigconnect.driver.internal.util.Iterables;
import com.mware.bigconnect.driver.internal.value.MapValue;
import com.mware.bigconnect.driver.types.Entity;

import java.util.Map;
import java.util.function.Function;

import static com.mware.bigconnect.driver.Values.ofObject;

public abstract class InternalEntity implements Entity, AsValue
{
    private final String id;
    private final Map<String,Value> properties;

    public InternalEntity( String id, Map<String, Value> properties )
    {
        this.id = id;
        this.properties = properties;
    }

    @Override
    public String id()
    {
        return id;
    }

    @Override
    public int size()
    {
        return properties.size();
    }

    @Override
    public Map<String, Object> asMap()
    {
        return asMap( ofObject() );
    }

    @Override
    public <T> Map<String,T> asMap(Function<Value,T> mapFunction )
    {
        return Extract.map( properties, mapFunction );
    }

    @Override
    public Value asValue()
    {
        return new MapValue( properties );
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

        InternalEntity that = (InternalEntity) o;

        return id == that.id;

    }

    @Override
    public int hashCode()
    {
        return id.hashCode();
    }

    @Override
    public String toString()
    {
        return "Entity{" +
               "id=" + id +
               ", properties=" + properties +
               '}';
    }

    @Override
    public boolean containsKey( String key )
    {
        return properties.containsKey( key );
    }

    @Override
    public Iterable<String> keys()
    {
        return properties.keySet();
    }

    @Override
    public Value get( String key )
    {
        Value value = properties.get( key );
        return value == null ? Values.NULL : value;
    }

    @Override
    public Iterable<Value> values()
    {
        return properties.values();
    }

    @Override
    public <T> Iterable<T> values(Function<Value,T> mapFunction )
    {
        return Iterables.map( properties.values(), mapFunction );
    }
}
