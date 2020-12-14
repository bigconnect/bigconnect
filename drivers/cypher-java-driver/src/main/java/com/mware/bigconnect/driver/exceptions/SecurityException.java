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
 * Failed to communicate with the server due to security errors.
 * When this type of error happens, the security cause of the error should be fixed to ensure the safety of your data.
 * Restart of server/driver/cluster might be required to recover from this error.
 * @since 1.1
 */
public class SecurityException extends ClientException
{
    public SecurityException(String code, String message )
    {
        super( code, message );
    }

    public SecurityException(String message, Throwable t )
    {
        super( message, t );
    }
}
