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
package com.mware.bigconnect.driver.exceptions;

/**
 * A <em>SessionExpiredException</em> indicates that the session can no longer satisfy the criteria under which it
 * was acquired, e.g. a server no longer accepts write requests. A new session needs to be acquired from the driver
 * and all actions taken on the expired session must be replayed.
 * @since 1.1
 */
public class SessionExpiredException extends BigConnectException
{
    public SessionExpiredException( String message)
    {
        super( message );
    }

    public SessionExpiredException(String message, Throwable throwable )
    {
        super( message, throwable );
    }
}
