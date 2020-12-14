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

public abstract class BooleanValue extends ValueAdapter
{
    private BooleanValue()
    {
        //do nothing
    }

    public static BooleanValue TRUE = new TrueValue();
    public static BooleanValue FALSE = new FalseValue();

    public static BooleanValue fromBoolean( boolean value )
    {
        return value ? TRUE : FALSE;
    }

    @Override
    public abstract Boolean asObject();

    @Override
    public Type type()
    {
        return InternalTypeSystem.TYPE_SYSTEM.BOOLEAN();
    }

    @Override
    public int hashCode()
    {
        Boolean value = asBoolean() ? Boolean.TRUE : Boolean.FALSE;
        return value.hashCode();
    }

    private static class TrueValue extends BooleanValue {

        @Override
        public Boolean asObject()
        {
            return Boolean.TRUE;
        }

        @Override
        public boolean asBoolean()
        {
            return true;
        }

        @Override
        public boolean isTrue()
        {
            return true;
        }

        @Override
        public boolean isFalse()
        {
            return false;
        }

        @SuppressWarnings("EqualsWhichDoesntCheckParameterClass")
        @Override
        public boolean equals( Object obj )
        {
            return obj == TRUE;
        }

        @Override
        public String toString()
        {
            return "TRUE";
        }
    }

    private static class FalseValue extends BooleanValue
    {
        @Override
        public Boolean asObject()
        {
            return Boolean.FALSE;
        }

        @Override
        public boolean asBoolean()
        {
            return false;
        }

        @Override
        public boolean isTrue()
        {
            return false;
        }

        @Override
        public boolean isFalse()
        {
            return true;
        }

        @SuppressWarnings("EqualsWhichDoesntCheckParameterClass")
        @Override
        public boolean equals( Object obj )
        {
            return obj == FALSE;
        }

        @Override
        public String toString()
        {
            return "FALSE";
        }
    }
}
