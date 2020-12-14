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

import com.mware.bigconnect.driver.Values;
import com.mware.bigconnect.driver.util.Immutable;

/**
 * Represents a single point in a particular coordinate reference system.
 * <p>
 * Value that represents a point can be created using {@link Values#point(int, double, double)}
 * or {@link Values#point(int, double, double, double)} method.
 */
@Immutable
public interface Point
{
    /**
     * Retrieve identifier of the coordinate reference system for this point.
     *
     * @return coordinate reference system identifier.
     */
    int srid();

    /**
     * Retrieve {@code x} coordinate of this point.
     *
     * @return the {@code x} coordinate value.
     */
    double x();

    /**
     * Retrieve {@code y} coordinate of this point.
     *
     * @return the {@code y} coordinate value.
     */
    double y();

    /**
     * Retrieve {@code z} coordinate of this point.
     *
     * @return the {@code z} coordinate value or {@link Double#NaN} if not applicable.
     */
    double z();
}
