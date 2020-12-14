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
package com.mware.bigconnect.driver.internal.util;

import com.mware.bigconnect.driver.Record;
import com.mware.bigconnect.driver.Value;
import com.mware.bigconnect.driver.exceptions.ClientException;
import com.mware.bigconnect.driver.internal.InternalPair;
import com.mware.bigconnect.driver.internal.value.NodeValue;
import com.mware.bigconnect.driver.internal.value.PathValue;
import com.mware.bigconnect.driver.internal.value.RelationshipValue;
import com.mware.bigconnect.driver.types.MapAccessor;
import com.mware.bigconnect.driver.types.Node;
import com.mware.bigconnect.driver.types.Path;
import com.mware.bigconnect.driver.types.Relationship;
import com.mware.bigconnect.driver.util.Pair;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

import static java.util.Collections.*;
import static com.mware.bigconnect.driver.Values.value;
import static com.mware.bigconnect.driver.internal.util.Iterables.newHashMapWithSize;

/**
 * Utility class for extracting data.
 */
public final class Extract
{
    private Extract()
    {
        throw new UnsupportedOperationException();
    }

    public static List<Value> list(Value[] values )
    {
        switch ( values.length )
        {
            case 0:
                return emptyList();
            case 1:
                return singletonList( values[0] );
            default:
                return unmodifiableList( Arrays.asList( values ) );
        }
    }

    public static <T> List<T> list(Value[] data, Function<Value, T> mapFunction )
    {
        int size = data.length;
        switch ( size )
        {
            case 0:
                return emptyList();
            case 1:
                return singletonList( mapFunction.apply( data[0] ) );
            default:
                List<T> result = new ArrayList<>( size );
                for ( Value value : data )
                {
                    result.add( mapFunction.apply( value ) );
                }
                return unmodifiableList( result );
        }
    }

    public static <T> Map<String, T> map(Map<String, Value> data, Function<Value, T> mapFunction )
    {
        if ( data.isEmpty() ) {
            return emptyMap();
        } else {
            int size = data.size();
            if ( size == 1 ) {
                Map.Entry<String, Value> head = data.entrySet().iterator().next();
                return singletonMap( head.getKey(), mapFunction.apply( head.getValue() ) );
            } else {
                Map<String,T> map = Iterables.newLinkedHashMapWithSize( size );
                for ( Map.Entry<String, Value> entry : data.entrySet() )
                {
                    map.put( entry.getKey(), mapFunction.apply( entry.getValue() ) );
                }
                return unmodifiableMap( map );
            }
        }
    }

    public static <T> Map<String, T> map(Record record, Function<Value, T> mapFunction )
    {
        int size = record.size();
        switch ( size )
        {
            case 0:
                return emptyMap();

            case 1:
                return singletonMap( record.keys().get( 0 ), mapFunction.apply( record.get( 0 ) ) );

            default:
                Map<String,T> map = Iterables.newLinkedHashMapWithSize( size );
                List<String> keys = record.keys();
                for ( int i = 0; i < size; i++ )
                {
                    map.put( keys.get( i ), mapFunction.apply( record.get( i ) ) );
                }
                return unmodifiableMap( map );
        }
    }

    public static <V> Iterable<Pair<String, V>> properties(final MapAccessor map, final Function<Value, V> mapFunction )
    {
        int size = map.size();
        switch ( size )
        {
            case 0:
                return emptyList();

            case 1:
            {
                String key = map.keys().iterator().next();
                Value value = map.get( key );
                return singletonList( InternalPair.of( key, mapFunction.apply( value ) ) );
            }

            default:
            {
                List<Pair<String, V>> list = new ArrayList<>( size );
                for ( String key : map.keys() )
                {
                    Value value = map.get( key );
                    list.add( InternalPair.of( key, mapFunction.apply( value ) ) );
                }
                return unmodifiableList( list );
            }
        }
    }

    public static <V> List<Pair<String, V>> fields(final Record map, final Function<Value, V> mapFunction )
    {
        int size = map.keys().size();
        switch ( size )
        {
            case 0:
                return emptyList();

            case 1:
            {
                String key = map.keys().iterator().next();
                Value value = map.get( key );
                return singletonList( InternalPair.of( key, mapFunction.apply( value ) ) );
            }

            default:
            {
                List<Pair<String, V>> list = new ArrayList<>( size );
                List<String> keys = map.keys();
                for ( int i = 0; i < size; i++ )
                {
                    String key = keys.get( i );
                    Value value = map.get( i );
                    list.add( InternalPair.of( key, mapFunction.apply( value ) ) );
                }
                return unmodifiableList( list );
            }
        }
    }

    public static Map<String,Value> mapOfValues(Map<String, Object> map )
    {
        if ( map == null || map.isEmpty() )
        {
            return emptyMap();
        }

        Map<String,Value> result = newHashMapWithSize( map.size() );
        for ( Map.Entry<String, Object> entry : map.entrySet() )
        {
            Object value = entry.getValue();
            assertParameter( value );
            result.put( entry.getKey(), value( value ) );
        }
        return result;
    }

    public static void assertParameter( Object value )
    {
        if ( value instanceof Node || value instanceof NodeValue )
        {
            throw new ClientException( "Nodes can't be used as parameters." );
        }
        if ( value instanceof Relationship || value instanceof RelationshipValue )
        {
            throw new ClientException( "Relationships can't be used as parameters." );
        }
        if ( value instanceof Path || value instanceof PathValue )
        {
            throw new ClientException( "Paths can't be used as parameters." );
        }
    }
}
