/*
 * Copyright (c) 2013-2020 "BigConnect,"
 * MWARE SOLUTIONS SRL
 *
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j.
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
package com.mware.ge.cypher.util;

import com.mware.ge.Authorizations;
import com.mware.ge.type.GeoPoint;
import com.mware.ge.cypher.Path;
import com.mware.ge.collection.ReverseArrayIterator;
import com.mware.ge.values.AnyValueWriter;
import com.mware.ge.values.VirtualValue;
import com.mware.ge.values.storable.DurationValue;
import com.mware.ge.values.storable.TextArray;
import com.mware.ge.values.storable.TextValue;
import com.mware.ge.values.virtual.MapValue;
import com.mware.ge.values.virtual.NodeValue;
import com.mware.ge.values.virtual.RelationshipValue;

import java.lang.reflect.Array;
import java.time.*;
import java.util.*;

import static com.mware.ge.collection.Iterators.iteratorsEqual;

/**
 * Base class for converting AnyValue to normal java objects.
 * <p>
 * This base class takes care of converting all "normal" java types such as
 * number types, booleans, strings, arrays and lists. It leaves to the extending
 * class to handle neo4j specific types such as nodes, edges and points.
 *
 * @param <E> the exception thrown on error.
 */
public abstract class BaseToObjectValueWriter<E extends Exception> implements AnyValueWriter<E>
{
    private final Deque<Writer> stack = new ArrayDeque<>();

    public BaseToObjectValueWriter()
    {
        stack.push( new ObjectWriter() );
    }

    protected abstract NodeValue newNodeProxyById(String id, Authorizations authorizations );

    protected abstract RelationshipValue newRelationshipProxyById(String id, Authorizations authorizations );

    public Object value()
    {
        assert stack.size() == 1;
        return stack.getLast().value();
    }

    private void writeValue( Object value )
    {
        assert !stack.isEmpty();
        Writer head = stack.peek();
        head.write( value );
    }

    @Override
    public void writeNodeReference( String nodeId ) throws RuntimeException
    {
        throw new UnsupportedOperationException( "Cannot write a raw node reference" );
    }

    @Override
    public void writeNode(String nodeId, TextArray ignore, MapValue properties, Authorizations authorizations ) throws RuntimeException
    {
        if ( nodeId != null )
        {
            writeValue( newNodeProxyById( nodeId, authorizations ) );
        }
    }

    @Override
    public void writeVirtualNodeHack( Object node )
    {
        writeValue( node );
    }

    @Override
    public void writeRelationshipReference( String relId ) throws RuntimeException
    {
        throw new UnsupportedOperationException( "Cannot write a raw edge reference" );
    }

    @Override
    public void writeRelationship(String relId, String startNodeId, String endNodeId, TextValue type, MapValue properties, Authorizations authorizations )
            throws RuntimeException
    {
        if ( relId != null )
        {
            writeValue( newRelationshipProxyById( relId, authorizations ) );
        }
    }

    @Override
    public void writeVirtualRelationshipHack( Object relationship )
    {
        writeValue( relationship );
    }

    @Override
    public void beginMap( int size ) throws RuntimeException
    {
        stack.push( new MapWriter( size ) );
    }

    @Override
    public void endMap() throws RuntimeException
    {
        assert !stack.isEmpty();
        writeValue( stack.pop().value() );
    }

    @Override
    public void beginList( int size ) throws RuntimeException
    {
        stack.push( new ListWriter( size ) );
    }

    @Override
    public void endList() throws RuntimeException
    {
        assert !stack.isEmpty();
        writeValue( stack.pop().value() );
    }

    @Override
    public void writePath(NodeValue[] nodes, RelationshipValue[] relationships, Authorizations authorizations ) throws RuntimeException
    {
        assert nodes != null;
        assert nodes.length > 0;
        assert relationships != null;
        assert nodes.length == relationships.length + 1;

        NodeValue[] nodeProxies = new NodeValue[nodes.length];
        for ( int i = 0; i < nodes.length; i++ )
        {
            nodeProxies[i] = newNodeProxyById( nodes[i].id(), authorizations );
        }
        RelationshipValue[] relationship = new RelationshipValue[relationships.length];
        for ( int i = 0; i < relationships.length; i++ )
        {
            relationship[i] = newRelationshipProxyById( relationships[i].id(), authorizations );
        }
        writeValue( new Path()
        {
            @Override
            public NodeValue startNode()
            {
                return nodeProxies[0];
            }

            @Override
            public NodeValue endNode()
            {
                return nodeProxies[nodeProxies.length - 1];
            }

            @Override
            public RelationshipValue lastRelationship()
            {
                return relationship[relationship.length - 1];
            }

            @Override
            public Iterable<RelationshipValue> relationships()
            {
                return Arrays.asList( relationship );
            }

            @Override
            public Iterable<NodeValue> nodes()
            {
                return Arrays.asList( nodeProxies );
            }

            @Override
            public int length()
            {
                return relationship.length;
            }

            @Override
            public int hashCode()
            {
                if ( relationship.length == 0 )
                {
                    return startNode().hashCode();
                }
                else
                {
                    return Arrays.hashCode( relationship );
                }
            }

            @Override
            public boolean equals( Object obj )
            {
                if ( this == obj )
                {
                    return true;
                }
                else if ( obj instanceof Path)
                {
                    Path other = (Path) obj;
                    return startNode().equals( other.startNode() ) &&
                           iteratorsEqual( this.relationships().iterator(), other.relationships().iterator() );

                }
                else
                {
                    return false;
                }
            }

            @Override
            public Iterator<VirtualValue> iterator()
            {
                return new Iterator<VirtualValue>()
                {
                    Iterator<? extends VirtualValue> current = nodes().iterator();
                    Iterator<? extends VirtualValue> next = relationships().iterator();

                    @Override
                    public boolean hasNext()
                    {
                        return current.hasNext();
                    }

                    @Override
                    public VirtualValue next()
                    {
                        try
                        {
                            return current.next();
                        }
                        finally
                        {
                            Iterator<? extends VirtualValue> temp = current;
                            current = next;
                            next = temp;
                        }
                    }

                    @Override
                    public void remove()
                    {
                        next.remove();
                    }
                };
            }
        } );
    }

    @Override
    public void writeGeoPoint(GeoPoint geoPoint) throws E {
        writeValue( geoPoint );
    }

    @Override
    public void writeNull() throws RuntimeException
    {
        writeValue( null );
    }

    @Override
    public void writeBoolean( boolean value ) throws RuntimeException
    {
        writeValue( value );
    }

    @Override
    public void writeInteger( byte value ) throws RuntimeException
    {
        writeValue( value );
    }

    @Override
    public void writeInteger( short value ) throws RuntimeException
    {
        writeValue( value );
    }

    @Override
    public void writeInteger( int value ) throws RuntimeException
    {
        writeValue( value );
    }

    @Override
    public void writeInteger( long value ) throws RuntimeException
    {
        writeValue( value );
    }

    @Override
    public void writeFloatingPoint( float value ) throws RuntimeException
    {
        writeValue( value );
    }

    @Override
    public void writeFloatingPoint( double value ) throws RuntimeException
    {
        writeValue( value );
    }

    @Override
    public void writeString( String value ) throws RuntimeException
    {
        writeValue( value );
    }

    @Override
    public void writeString( char value ) throws RuntimeException
    {
        writeValue( value );
    }

    @Override
    public void beginArray( int size, ArrayType arrayType ) throws RuntimeException
    {
        stack.push( new ArrayWriter( size, arrayType ) );
    }

    @Override
    public void endArray() throws RuntimeException
    {
        assert !stack.isEmpty();
        writeValue( stack.pop().value() );
    }

    @Override
    public void writeByteArray( byte[] value ) throws RuntimeException
    {
        writeValue( value );
    }

    @Override
    public void writeDuration( long months, long days, long seconds, int nanos )
    {
        writeValue( DurationValue.duration( months, days, seconds, nanos ) );
    }

    @Override
    public void writeDate( LocalDate localDate ) throws RuntimeException
    {
        writeValue( localDate );
    }

    @Override
    public void writeLocalTime( LocalTime localTime ) throws RuntimeException
    {
        writeValue( localTime );
    }

    @Override
    public void writeTime( OffsetTime offsetTime ) throws RuntimeException
    {
        writeValue( offsetTime );
    }

    @Override
    public void writeLocalDateTime( LocalDateTime localDateTime ) throws RuntimeException
    {
        writeValue( localDateTime );
    }

    @Override
    public void writeDateTime( ZonedDateTime zonedDateTime ) throws RuntimeException
    {
        writeValue( zonedDateTime );
    }

    private interface Writer
    {
        void write(Object value);

        Object value();
    }

    private static class ObjectWriter implements Writer
    {
        private Object value;

        @Override
        public void write( Object value )
        {
            this.value = value;
        }

        @Override
        public Object value()
        {
            return value;
        }
    }

    private static class MapWriter implements Writer
    {
        private String key;
        private boolean isKey = true;
        private final HashMap<String,Object> map;

        MapWriter( int size )
        {
            this.map = new HashMap<>( size );
        }

        @Override
        public void write( Object value )
        {
            if ( isKey )
            {
                key = (String) value;
                isKey = false;
            }
            else
            {
                map.put( key, value );
                isKey = true;
            }
        }

        @Override
        public Object value()
        {
            return map;
        }
    }

    private static class ArrayWriter implements Writer
    {
        protected final Object array;
        private int index;

        ArrayWriter( int size, ArrayType arrayType )
        {
            switch ( arrayType )
            {
            case SHORT:
                this.array = Array.newInstance( short.class, size );
                break;
            case INT:
                this.array = Array.newInstance( int.class, size );
                break;
            case BYTE:
                this.array = Array.newInstance( byte.class, size );
                break;
            case LONG:
                this.array = Array.newInstance( long.class, size );
                break;
            case FLOAT:
                this.array = Array.newInstance( float.class, size );
                break;
            case DOUBLE:
                this.array = Array.newInstance( double.class, size );
                break;
            case BOOLEAN:
                this.array = Array.newInstance( boolean.class, size );
                break;
            case STRING:
                this.array = Array.newInstance( String.class, size );
                break;
            case CHAR:
                this.array = Array.newInstance( char.class, size );
                break;
            default:
                this.array = new Object[size];
            }
        }

        @Override
        public void write( Object value )
        {
            Array.set( array, index++, value );
        }

        @Override
        public Object value()
        {
            return array;
        }
    }

    private static class ListWriter implements Writer
    {
        private final List<Object> list;

        ListWriter( int size )
        {
            this.list = new ArrayList<>( size );
        }

        @Override
        public void write( Object value )
        {
            list.add( value );
        }

        @Override
        public Object value()
        {
            return list;
        }
    }
}
