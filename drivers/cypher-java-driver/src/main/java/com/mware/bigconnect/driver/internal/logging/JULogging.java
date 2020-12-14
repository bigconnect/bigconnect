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

import java.util.logging.Level;

/**
 * Internal implementation of the JUL.
 * <b>This class should not be used directly.</b> Please use {@link Logging#javaUtilLogging(Level)} factory method instead.
 *
 * @see Logging#javaUtilLogging(Level)
 */
public class JULogging implements Logging
{
    private final Level loggingLevel;

    public JULogging( Level loggingLevel )
    {
        this.loggingLevel = loggingLevel;
    }

    @Override
    public Logger getLog( String name )
    {
        return new JULogger( name, loggingLevel );
    }
}
