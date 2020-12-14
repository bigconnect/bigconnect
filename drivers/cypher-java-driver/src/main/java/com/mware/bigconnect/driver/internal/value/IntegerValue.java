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

import com.mware.bigconnect.driver.exceptions.value.LossyCoercion;
import com.mware.bigconnect.driver.internal.types.InternalTypeSystem;
import com.mware.bigconnect.driver.types.Type;

public class IntegerValue extends NumberValueAdapter<Long>
{
    private final long val;

    public IntegerValue( long val )
    {
        this.val = val;
    }

    @Override
    public Type type()
    {
        return InternalTypeSystem.TYPE_SYSTEM.INTEGER();
    }

    @Override
    public Long asNumber()
    {
        return val;
    }

    @Override
    public long asLong()
    {
        return val;
    }

    @Override
    public int asInt()
    {
        if (val > Integer.MAX_VALUE || val < Integer.MIN_VALUE)
        {
            throw new LossyCoercion( type().name(), "Java int" );
        }
        return (int) val;
    }

    @Override
    public double asDouble()
    {
        double doubleVal = (double) val;
        if ( (long) doubleVal != val)
        {
            throw new LossyCoercion( type().name(), "Java double" );
        }

        return (double) val;
    }

    @Override
    public float asFloat()
    {
        return (float) val;
    }

    @Override
    public String toString()
    {
        return Long.toString( val );
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

        IntegerValue values = (IntegerValue) o;
        return val == values.val;
    }

    @Override
    public int hashCode()
    {
        return (int) (val ^ (val >>> 32));
    }
}
