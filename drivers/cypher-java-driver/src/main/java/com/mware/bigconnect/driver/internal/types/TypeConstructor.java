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
package com.mware.bigconnect.driver.internal.types;

import com.mware.bigconnect.driver.Value;
import com.mware.bigconnect.driver.internal.value.InternalValue;

public enum TypeConstructor
{
    ANY
            {
                @Override
                public boolean covers( Value value )
                {
                    return !value.isNull();
                }
            },
    BOOLEAN,
    BYTES,
    STRING,
    NUMBER
            {
                @Override
                public boolean covers( Value value )
                {
                    TypeConstructor valueType = typeConstructorOf( value );
                    return valueType == this || valueType == INTEGER || valueType == FLOAT;
                }
            },
    INTEGER,
    FLOAT,
    LIST,
    MAP
            {
                @Override
                public boolean covers( Value value )
                {
                    TypeConstructor valueType = typeConstructorOf( value );
                    return valueType == MAP || valueType == NODE || valueType == RELATIONSHIP;
                }
            },
    NODE,
    RELATIONSHIP,
    PATH,
    POINT,
    DATE,
    TIME,
    LOCAL_TIME,
    LOCAL_DATE_TIME,
    DATE_TIME,
    DURATION,
    NULL;

    private static TypeConstructor typeConstructorOf( Value value )
    {
        return ((InternalValue) value).typeConstructor();
    }

    public boolean covers( Value value )
    {
        return this == typeConstructorOf( value );
    }
}
