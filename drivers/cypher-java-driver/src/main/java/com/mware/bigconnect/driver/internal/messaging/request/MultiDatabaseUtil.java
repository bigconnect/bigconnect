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
package com.mware.bigconnect.driver.internal.messaging.request;

import com.mware.bigconnect.driver.exceptions.ClientException;

import java.util.Objects;

public final class MultiDatabaseUtil
{
    public static final String ABSENT_DB_NAME = ""; // TODO _default
    public static final String SYSTEM_DB_NAME = "system"; // TODO _dbms

    public static void assertEmptyDatabaseName(String databaseName, int boltVersion )
    {
        if ( !Objects.equals( ABSENT_DB_NAME, databaseName ) )
        {
            throw new ClientException( String.format( "Database name parameter for selecting database is not supported in Bolt Protocol Version %s. " +
                    "Database name: `%s`", boltVersion, databaseName ) );
        }
    }
}
