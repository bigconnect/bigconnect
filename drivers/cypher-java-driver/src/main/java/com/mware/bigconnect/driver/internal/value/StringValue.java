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

import com.mware.bigconnect.driver.internal.types.InternalTypeSystem;
import com.mware.bigconnect.driver.types.Type;

import java.util.Objects;

public class StringValue extends ValueAdapter
{
    private final String val;

    public StringValue( String val )
    {
        if ( val == null )
        {
            throw new IllegalArgumentException( "Cannot construct StringValue from null" );
        }
        this.val = val;
    }

    @Override
    public boolean isEmpty()
    {
        return val.isEmpty();
    }

    @Override
    public int size()
    {
        return val.length();
    }

    @Override
    public String asObject()
    {
        return asString();
    }

    @Override
    public String asString()
    {
        return val;
    }

    @Override
    public String toString()
    {
        return String.format( "\"%s\"", val.replace( "\"", "\\\"" ) );
    }

    @Override
    public Type type()
    {
        return InternalTypeSystem.TYPE_SYSTEM.STRING();
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
        StringValue that = (StringValue) o;
        return Objects.equals( val, that.val );
    }

    @Override
    public int hashCode()
    {
        return val.hashCode();
    }
}
