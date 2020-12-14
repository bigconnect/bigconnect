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
import com.mware.bigconnect.driver.types.Type;

import static com.mware.bigconnect.driver.internal.types.TypeConstructor.LIST;

public class TypeRepresentation implements Type
{
    private final TypeConstructor tyCon;

    public TypeRepresentation( TypeConstructor tyCon )
    {
        this.tyCon = tyCon;
    }

    @Override
    public boolean isTypeOf( Value value )
    {
        return tyCon.covers( value );
    }

    @Override
    public String name()
    {
        if ( tyCon == LIST )
        {
            return "LIST OF ANY?";
        }

        return tyCon.toString();
    }

    public TypeConstructor constructor()
    {
        return tyCon;
    }

    @Override
    public boolean equals( Object o )
    {
        if ( this == o )
        { return true; }
        if ( o == null || getClass() != o.getClass() )
        { return false; }

        TypeRepresentation that = (TypeRepresentation) o;

        return tyCon == that.tyCon;
    }

    @Override
    public int hashCode()
    {
        return tyCon.hashCode();
    }
}
