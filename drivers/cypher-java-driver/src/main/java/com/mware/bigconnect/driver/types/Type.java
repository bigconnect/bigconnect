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

import com.mware.bigconnect.driver.Value;
import com.mware.bigconnect.driver.util.Experimental;
import com.mware.bigconnect.driver.util.Immutable;

/**
 * The type of a {@link Value} as defined by the Cypher language
 * @since 1.0
 */
@Immutable
@Experimental
public interface Type
{
    /**
     * @return the name of the Cypher type (as defined by Cypher)
     */
    String name();

    /**
     * Test if the given value has this type
     *
     * @param value the value
     * @return {@code true} if the value is a value of this type otherwise {@code false}
     */
    boolean isTypeOf(Value value);
}
