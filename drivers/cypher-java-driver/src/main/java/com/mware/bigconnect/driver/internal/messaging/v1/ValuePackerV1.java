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
package com.mware.bigconnect.driver.internal.messaging.v1;

import com.mware.bigconnect.driver.Value;
import com.mware.bigconnect.driver.internal.messaging.ValuePacker;
import com.mware.bigconnect.driver.internal.packstream.PackOutput;
import com.mware.bigconnect.driver.internal.packstream.PackStream;
import com.mware.bigconnect.driver.internal.value.InternalValue;

import java.io.IOException;
import java.util.Map;

public class ValuePackerV1 implements ValuePacker
{
    protected final PackStream.Packer packer;

    public ValuePackerV1( PackOutput output )
    {
        this.packer = new PackStream.Packer( output );
    }

    @Override
    public final void packStructHeader( int size, byte signature ) throws IOException
    {
        packer.packStructHeader( size, signature );
    }

    @Override
    public final void pack( String string ) throws IOException
    {
        packer.pack( string );
    }

    @Override
    public final void pack( Value value ) throws IOException
    {
        if ( value instanceof InternalValue )
        {
            packInternalValue( ((InternalValue) value) );
        }
        else
        {
            throw new IllegalArgumentException( "Unable to pack: " + value );
        }
    }

    @Override
    public final void pack( Map<String,Value> map ) throws IOException
    {
        if ( map == null || map.size() == 0 )
        {
            packer.packMapHeader( 0 );
            return;
        }
        packer.packMapHeader( map.size() );
        for ( Map.Entry<String,Value> entry : map.entrySet() )
        {
            packer.pack( entry.getKey() );
            pack( entry.getValue() );
        }
    }

    protected void packInternalValue( InternalValue value ) throws IOException
    {
        switch ( value.typeConstructor() )
        {
        case NULL:
            packer.packNull();
            break;

        case BYTES:
            packer.pack( value.asByteArray() );
            break;

        case STRING:
            packer.pack( value.asString() );
            break;

        case BOOLEAN:
            packer.pack( value.asBoolean() );
            break;

        case INTEGER:
            packer.pack( value.asLong() );
            break;

        case FLOAT:
            packer.pack( value.asDouble() );
            break;

        case MAP:
            packer.packMapHeader( value.size() );
            for ( String s : value.keys() )
            {
                packer.pack( s );
                pack( value.get( s ) );
            }
            break;

        case LIST:
            packer.packListHeader( value.size() );
            for ( Value item : value.values() )
            {
                pack( item );
            }
            break;

        default:
            throw new IOException( "Unknown type: " + value.type().name() );
        }
    }
}
