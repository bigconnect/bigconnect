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
 * The <strong>Node</strong> interface describes the characteristics of a node from a BigConnect graph.
 * @since 1.0
 */
public interface Node extends Entity
{
    /**
     * Return all labels.
     *
     * @return a label Collection
     */
    Iterable<String> labels();

    /**
     * Test if this node has a given label
     *
     * @param label the label
     * @return {@code true} if this node has the label otherwise {@code false}
     */
    boolean hasLabel(String label);
}
