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
package com.mware.bigconnect.driver.types;

/**
 * The <strong>Relationship</strong> interface describes the characteristics of a relationship from a BigConnect graph.
 * @since 1.0
 */
public interface Relationship extends Entity
{
    /**
     * Id of the node where this relationship starts.
     * @return the node id
     */
    String startNodeId();

    /**
     * Id of the node where this relationship ends.
     * @return the node id
     */
    String endNodeId();

    /**
     * Return the <em>type</em> of this relationship.
     *
     * @return the type name
     */
    String type();

    /**
     * Test if this relationship has the given type
     *
     * @param relationshipType the give relationship type
     * @return {@code true} if this relationship has the given relationship type otherwise {@code false}
     */
    boolean hasType(String relationshipType);
}
