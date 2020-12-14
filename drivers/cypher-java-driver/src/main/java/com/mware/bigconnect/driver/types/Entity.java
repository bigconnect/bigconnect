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

import com.mware.bigconnect.driver.util.Immutable;

/**
 * A uniquely identifiable property container that can form part of a BigConnect graph.
 * @since 1.0
 */
@Immutable
public interface Entity extends MapAccessor
{
    /**
     * A unique id for this Entity. Ids are guaranteed to remain stable for the duration of the session they
     * were found in, but may be re-used for other entities after that. As such, if you want a public identity to use
     * for your entities, attaching an explicit 'id' property or similar persistent and unique identifier is a better
     * choice.
     *
     * @return the id of this entity
     */
    String id();
}
