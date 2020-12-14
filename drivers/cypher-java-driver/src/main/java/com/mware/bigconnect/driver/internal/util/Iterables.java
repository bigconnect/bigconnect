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

import java.util.*;
import java.util.function.Function;

public class Iterables
{
    @SuppressWarnings( "rawtypes" )
    private static final Queue EMPTY_QUEUE = new EmptyQueue();
    private static final float DEFAULT_HASH_MAP_LOAD_FACTOR = 0.75F;

    public static int count( Iterable<?> it )
    {
        if ( it instanceof Collection) { return ((Collection) it).size(); }
        int size = 0;
        for ( Object o : it )
        {
            size++;
        }
        return size;
    }

    public static <T> List<T> asList(Iterable<T> it )
    {
        if ( it instanceof List) { return (List<T>) it; }
        List<T> list = new ArrayList<>();
        for ( T t : it )
        {
            list.add( t );
        }
        return list;
    }

    public static <T> T single( Iterable<T> it )
    {
        Iterator<T> iterator = it.iterator();
        if ( !iterator.hasNext() )
        {
            throw new IllegalArgumentException( "Given iterable is empty" );
        }
        T result = iterator.next();
        if ( iterator.hasNext() )
        {
            throw new IllegalArgumentException( "Given iterable contains more than one element: " + it );
        }
        return result;
    }

    public static Map<String, String> map(String... alternatingKeyValue )
    {
        Map<String, String> out = newHashMapWithSize( alternatingKeyValue.length / 2 );
        for ( int i = 0; i < alternatingKeyValue.length; i+=2 )
        {
            out.put( alternatingKeyValue[i], alternatingKeyValue[i+1] );
        }
        return out;
    }

    public static <A,B> Iterable<B> map(final Iterable<A> it, final Function<A,B> f)
    {
        return new Iterable<B>()
        {
            @Override
            public Iterator<B> iterator()
            {
                final Iterator<A> aIterator = it.iterator();
                return new Iterator<B>()
                {
                    @Override
                    public boolean hasNext()
                    {
                        return aIterator.hasNext();
                    }

                    @Override
                    public B next()
                    {
                        return f.apply( aIterator.next() );
                    }

                    @Override
                    public void remove()
                    {
                        aIterator.remove();
                    }
                };
            }
        };
    }

    @SuppressWarnings( "unchecked" )
    public static <T> Queue<T> emptyQueue()
    {
        return (Queue<T>) EMPTY_QUEUE;
    }

    public static <K, V> HashMap<K,V> newHashMapWithSize(int expectedSize )
    {
        return new HashMap<>( hashMapCapacity( expectedSize ) );
    }

    public static <K, V> LinkedHashMap<K,V> newLinkedHashMapWithSize(int expectedSize )
    {
        return new LinkedHashMap<>( hashMapCapacity( expectedSize ) );
    }

    private static int hashMapCapacity( int expectedSize )
    {
        if ( expectedSize < 3 )
        {
            if ( expectedSize < 0 )
            {
                throw new IllegalArgumentException( "Illegal map size: " + expectedSize );
            }
            return expectedSize + 1;
        }
        return (int) ((float) expectedSize / DEFAULT_HASH_MAP_LOAD_FACTOR + 1.0F);
    }

    private static class EmptyQueue<T> extends AbstractQueue<T>
    {
        @Override
        public Iterator<T> iterator()
        {
            return Collections.emptyIterator();
        }

        @Override
        public int size()
        {
            return 0;
        }

        @Override
        public boolean offer( T t )
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public T poll()
        {
            return null;
        }

        @Override
        public T peek()
        {
            return null;
        }
    }
}
