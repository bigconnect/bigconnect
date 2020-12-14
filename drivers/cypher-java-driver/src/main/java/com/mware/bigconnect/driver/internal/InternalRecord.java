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

import com.mware.bigconnect.driver.Record;
import com.mware.bigconnect.driver.Value;
import com.mware.bigconnect.driver.Values;
import com.mware.bigconnect.driver.internal.types.InternalMapAccessorWithDefaultValue;
import com.mware.bigconnect.driver.internal.util.Extract;
import com.mware.bigconnect.driver.util.Pair;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.function.Function;

import static com.mware.bigconnect.driver.Values.ofObject;
import static com.mware.bigconnect.driver.Values.ofValue;
import static com.mware.bigconnect.driver.internal.util.Format.formatPairs;
import static java.lang.String.format;

public class InternalRecord extends InternalMapAccessorWithDefaultValue implements Record
{
    private final List<String> keys;
    private final Value[] values;
    private int hashCode = 0;

    public InternalRecord(List<String> keys, Value[] values )
    {
        this.keys = keys;
        this.values = values;
    }

    @Override
    public List<String> keys()
    {
        return keys;
    }

    @Override
    public List<Value> values()
    {
        return Arrays.asList( values );
    }

    @Override
    public List<Pair<String, Value>> fields()
    {
        return Extract.fields( this, ofValue() );
    }

    @Override
    public int index( String key )
    {
        int result = keys.indexOf( key );
        if ( result == -1 )
        {
            throw new NoSuchElementException( "Unknown key: " + key );
        }
        else
        {
            return result;
        }
    }

    @Override
    public boolean containsKey( String key )
    {
        return keys.contains( key );
    }

    @Override
    public Value get( String key )
    {
        int fieldIndex = keys.indexOf( key );

        if ( fieldIndex == -1 )
        {
            return Values.NULL;
        }
        else
        {
            return values[fieldIndex];
        }
    }

    @Override
    public Value get( int index )
    {
        return index >= 0 && index < values.length ? values[index] : Values.NULL;
    }

    @Override
    public int size()
    {
        return values.length;
    }

    @Override
    public Map<String, Object> asMap()
    {
        return Extract.map( this, ofObject() );
    }

    @Override
    public <T> Map<String,T> asMap(Function<Value,T> mapper )
    {
        return Extract.map( this, mapper );
    }

    @Override
    public String toString()
    {
        return format( "Record<%s>", formatPairs( asMap( ofValue() ) ) );
    }

    @Override
    public boolean equals( Object other )
    {
        if ( this == other )
        {
            return true;
        }
        else if ( other instanceof Record )
        {
            Record otherRecord = (Record) other;
            int size = size();
            if ( ! ( size == otherRecord.size() ) )
            {
                return false;
            }
            if ( ! keys.equals( otherRecord.keys() ) )
            {
                return false;
            }
            for ( int i = 0; i < size; i++ )
            {
                Value value = get( i );
                Value otherValue = otherRecord.get( i );
                if ( ! value.equals( otherValue ) )
                {
                    return false;
                }
            }
            return true;
        }
        else
        {
            return false;
        }
    }

    @Override
    public int hashCode()
    {
        if ( hashCode == 0 )
        {
            hashCode = 31 * keys.hashCode() + Arrays.hashCode( values );
        }
        return hashCode;
    }
}
