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

import java.util.logging.Level;

public class JULogger implements Logger
{
    private final java.util.logging.Logger delegate;
    private final boolean debugEnabled;
    private final boolean traceEnabled;

    public JULogger(String name, Level loggingLevel )
    {
        delegate = java.util.logging.Logger.getLogger( name );
        delegate.setLevel( loggingLevel );
        debugEnabled = delegate.isLoggable( Level.FINE );
        traceEnabled = delegate.isLoggable( Level.FINEST );
    }

    @Override
    public void error(String message, Throwable cause )
    {
        delegate.log( Level.SEVERE, message, cause );
    }

    @Override
    public void info(String format, Object... params )
    {
        delegate.log( Level.INFO, String.format( format, params ) );
    }

    @Override
    public void warn(String format, Object... params )
    {
        delegate.log( Level.WARNING, String.format( format, params ) );
    }

    @Override
    public void warn(String message, Throwable cause )
    {
        delegate.log( Level.WARNING, message, cause );
    }

    @Override
    public void debug(String format, Object... params )
    {
        if( debugEnabled )
        {
            delegate.log( Level.FINE, String.format( format, params ) );
        }
    }

    @Override
    public void trace(String format, Object... params )
    {
        if( traceEnabled )
        {
            delegate.log( Level.FINEST, String.format( format, params ) );
        }
    }

    @Override
    public boolean isTraceEnabled()
    {
        return traceEnabled;
    }

    @Override
    public boolean isDebugEnabled()
    {
        return debugEnabled;
    }
}
