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
package com.mware.bigconnect.driver.internal.logging;

import com.mware.bigconnect.driver.Logger;
import com.mware.bigconnect.driver.Logging;
import org.slf4j.LoggerFactory;

/**
 * Internal implementation of the SLF4J logging.
 * <b>This class should not be used directly.</b> Please use {@link Logging#slf4j()} factory method instead.
 *
 * @see Logging#slf4j()
 */
public class Slf4jLogging implements Logging
{
    @Override
    public Logger getLog( String name )
    {
        return new Slf4jLogger( LoggerFactory.getLogger( name ) );
    }

    public static RuntimeException checkAvailability()
    {
        try
        {
            Class.forName( "org.slf4j.LoggerFactory" );
            return null;
        }
        catch ( Throwable error )
        {
            return new IllegalStateException(
                    "SLF4J logging is not available. Please add dependencies on slf4j-api and SLF4J binding (Logback, Log4j, etc.)",
                    error );
        }
    }
}
