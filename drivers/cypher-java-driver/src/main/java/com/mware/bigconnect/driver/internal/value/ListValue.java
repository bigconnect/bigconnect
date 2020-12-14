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

import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.function.Function;

import static com.mware.bigconnect.driver.Values.ofObject;

public class ListValue extends ValueAdapter
{
    private final Value[] values;

    public ListValue( Value... values )
    {
        if ( values == null )
        {
            throw new IllegalArgumentException( "Cannot construct ListValue from null" );
        }
        this.values = values;
    }

    @Override
    public boolean isEmpty()
    {
        return values.length == 0;
    }

    @Override
    public List<Object> asObject()
    {
        return asList( ofObject() );
    }

    @Override
    public List<Object> asList()
    {
        return Extract.list( values, ofObject() );
    }

    @Override
    public <T> List<T> asList(Function<Value,T> mapFunction )
    {
        return Extract.list( values, mapFunction );
    }

    @Override
    public int size()
    {
        return values.length;
    }

    @Override
    public Value get( int index )
    {
        return index >= 0 && index < values.length ? values[index] : Values.NULL;
    }

    @Override
    public <T> Iterable<T> values(final Function<Value,T> mapFunction )
    {
        return new Iterable<T>()
        {
            @Override
            public Iterator<T> iterator()
            {
                return new Iterator<T>()
                {
                    private int cursor = 0;

                    @Override
                    public boolean hasNext()
                    {
                        return cursor < values.length;
                    }

                    @Override
                    public T next()
                    {
                        return mapFunction.apply( values[cursor++] );
                    }

                    @Override
                    public void remove()
                    {
                    }
                };
            }
        };
    }

    @Override
    public Type type()
    {
        return InternalTypeSystem.TYPE_SYSTEM.LIST();
    }

    @Override
    public String toString()
    {
        return Arrays.toString( values );
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

        ListValue otherValues = (ListValue) o;
        return Arrays.equals( values, otherValues.values );
    }

    @Override
    public int hashCode()
    {
        return Arrays.hashCode( values );
    }
}
