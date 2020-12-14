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

import java.util.Arrays;

public class BytesValue extends ValueAdapter
{
    private final byte[] val;

    public BytesValue( byte[] val )
    {
        if ( val == null )
        {
            throw new IllegalArgumentException( "Cannot construct BytesValue from null" );
        }
        this.val = val;
    }

    @Override
    public boolean isEmpty()
    {
        return val.length == 0;
    }

    @Override
    public int size()
    {
        return val.length;
    }

    @Override
    public byte[] asObject()
    {
        return val;
    }

    @Override
    public byte[] asByteArray()
    {
        return val;
    }

    @Override
    public Type type()
    {
        return InternalTypeSystem.TYPE_SYSTEM.BYTES();
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

        BytesValue values = (BytesValue) o;
        return Arrays.equals(val, values.val);
    }

    @Override
    public int hashCode()
    {
        return Arrays.hashCode(val);
    }

    @Override
    public String toString()
    {
        StringBuilder s = new StringBuilder("#");
        for (byte b : val)
        {
            if (b < 0x10)
            {
                s.append('0');
            }
            s.append(Integer.toHexString(b));
        }
        return s.toString();
    }
}
