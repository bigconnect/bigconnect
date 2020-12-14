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
package com.mware.ge.cypher;

public interface RelationshipType
{
    /**
     * Returns the name of the relationship type. The name uniquely identifies a
     * relationship type, i.e. two different RelationshipType instances with
     * different object identifiers (and possibly even different classes) are
     * semantically equivalent if they have {@link String#equals(Object) equal}
     * names.
     *
     * @return the name of the relationship type
     */
    String name();

    /**
     * Instantiates a new {@linkplain RelationshipType} with the given name.
     *
     * @param name the name of the dynamic relationship type
     * @return a {@link RelationshipType} with the given name
     * @throws IllegalArgumentException if name is {@code null}
     */
    static RelationshipType withName(String name)
    {
        if ( name == null )
        {
            throw new IllegalArgumentException( "A relationship type cannot have a null name" );
        }
        return new RelationshipType()
        {
            @Override
            public String name()
            {
                return name;
            }

            @Override
            public String toString()
            {
                return name;
            }

            @Override
            public boolean equals( Object that )
            {
                if ( this == that )
                {
                    return true;
                }
                if ( that == null || that.getClass() != getClass() )
                {
                    return false;
                }
                return name.equals( ((RelationshipType) that).name() );
            }

            @Override
            public int hashCode()
            {
                return name.hashCode();
            }
        };
    }
}
