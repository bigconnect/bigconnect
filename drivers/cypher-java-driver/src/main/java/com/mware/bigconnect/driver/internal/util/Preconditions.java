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
package com.mware.bigconnect.driver.internal.util;

public final class Preconditions
{
    private Preconditions()
    {
    }

    /**
     * Assert that given expression is true.
     *
     * @param expression the value to check.
     * @param message the message.
     * @throws IllegalArgumentException if given value is {@code false}.
     */
    public static void checkArgument( boolean expression, String message )
    {
        if ( !expression )
        {
            throw new IllegalArgumentException( message );
        }
    }

    /**
     * Assert that given argument is of expected type.
     *
     * @param argument the object to check.
     * @param expectedClass the expected type.
     * @throws IllegalArgumentException if argument is not of expected type.
     */
    public static void checkArgument(Object argument, Class<?> expectedClass )
    {
        if ( !expectedClass.isInstance( argument ) )
        {
            throw new IllegalArgumentException( "Argument expected to be of type: " + expectedClass.getName() + " but was: " + argument );
        }
    }
}
