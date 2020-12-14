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
package com.mware.bigconnect.driver.summary;

import com.mware.bigconnect.driver.util.Immutable;

/**
 * An input position refers to a specific character in a statement.
 * @since 1.0
 */
@Immutable
public interface InputPosition
{
    /**
     * The character offset referred to by this position; offset numbers start at 0.
     *
     * @return the offset of this position.
     */
    int offset();

    /**
     * The line number referred to by the position; line numbers start at 1.
     *
     * @return the line number of this position.
     */
    int line();

    /**
     * The column number referred to by the position; column numbers start at 1.
     *
     * @return the column number of this position.
     */
    int column();
}
