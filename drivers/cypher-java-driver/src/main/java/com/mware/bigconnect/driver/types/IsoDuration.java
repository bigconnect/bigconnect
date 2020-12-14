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

import java.time.temporal.TemporalAmount;

/**
 * Represents temporal amount containing months, days, seconds and nanoseconds of the second. A duration can be negative.
 * <p>
 * Value that represents a duration can be created using {@link Values#isoDuration(long, long, long, int)} method.
 */
@Immutable
public interface IsoDuration extends TemporalAmount
{
    /**
     * Retrieve amount of months in this duration.
     *
     * @return number of months.
     */
    long months();

    /**
     * Retrieve amount of days in this duration.
     *
     * @return number of days.
     */
    long days();

    /**
     * Retrieve amount of seconds in this duration.
     *
     * @return number of seconds.
     */
    long seconds();

    /**
     * Retrieve amount of nanoseconds of the second in this duration.
     *
     * @return number of nanoseconds.
     */
    int nanoseconds();
}
