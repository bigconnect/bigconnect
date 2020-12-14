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

public class FloatValue extends NumberValueAdapter<Double>
{
    private final double val;

    public FloatValue( double val )
    {
        this.val = val;
    }

    @Override
    public Type type()
    {
        return InternalTypeSystem.TYPE_SYSTEM.FLOAT();
    }

    @Override
    public Double asNumber()
    {
        return val;
    }

    @Override
    public long asLong()
    {
        long longVal = (long) val;
        if ((double) longVal != val)
        {
            throw new LossyCoercion( type().name(), "Java long" );
        }

        return longVal;
    }

    @Override
    public int asInt()
    {
        int intVal = (int) val;
        if ((double) intVal != val)
        {
            throw new LossyCoercion( type().name(), "Java int" );
        }

        return intVal;
    }

    @Override
    public double asDouble()
    {
        return val;
    }

    @Override
    public float asFloat()
    {
        float floatVal = (float) val;
        if ((double) floatVal != val)
        {
            throw new LossyCoercion( type().name(), "Java float" );
        }

        return floatVal;
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

        FloatValue values = (FloatValue) o;
        return Double.compare( values.val, val ) == 0;
    }

    @Override
    public int hashCode()
    {
        long temp = Double.doubleToLongBits( val );
        return (int) (temp ^ (temp >>> 32));
    }

    @Override
    public String toString()
    {
        return Double.toString( val );
    }
}
