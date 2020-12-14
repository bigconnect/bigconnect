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

import io.netty.util.internal.logging.AbstractInternalLogger;
import com.mware.bigconnect.driver.Logger;

import java.util.regex.Pattern;

import static java.lang.String.format;

public class NettyLogger extends AbstractInternalLogger
{
    private Logger log;
    private static final Pattern PLACE_HOLDER_PATTERN = Pattern.compile("\\{\\}");

    public NettyLogger(String name, Logger log )
    {
        super( name );
        this.log = log;
    }

    @Override
    public boolean isTraceEnabled()
    {
        return log.isTraceEnabled();
    }

    @Override
    public void trace( String msg )
    {
        log.trace( msg );
    }

    @Override
    public void trace(String format, Object arg )
    {
        log.trace( toDriverLoggerFormat( format ), arg );
    }

    @Override
    public void trace(String format, Object argA, Object argB )
    {
        log.trace( toDriverLoggerFormat( format ), argA, argB );
    }

    @Override
    public void trace(String format, Object... arguments )
    {
        log.trace( toDriverLoggerFormat( format ), arguments );
    }

    @Override
    public void trace(String msg, Throwable t )
    {
        log.trace( "%s%n%s", msg, t );
    }

    @Override
    public boolean isDebugEnabled()
    {
        return log.isDebugEnabled();
    }

    @Override
    public void debug( String msg )
    {
        log.debug( msg );
    }

    @Override
    public void debug(String format, Object arg )
    {
        log.debug( toDriverLoggerFormat( format ), arg );
    }

    @Override
    public void debug(String format, Object argA, Object argB )
    {
        log.debug( toDriverLoggerFormat( format ), argA, argB );
    }

    @Override
    public void debug(String format, Object... arguments )
    {
        log.debug( toDriverLoggerFormat( format ), arguments );
    }

    @Override
    public void debug(String msg, Throwable t )
    {
        log.debug( "%s%n%s", msg, t );
    }

    @Override
    public boolean isInfoEnabled()
    {
        return true;
    }

    @Override
    public void info( String msg )
    {
        log.info( msg );
    }

    @Override
    public void info(String format, Object arg )
    {
        log.info( toDriverLoggerFormat( format ), arg );
    }

    @Override
    public void info(String format, Object argA, Object argB )
    {
        log.info( toDriverLoggerFormat( format ), argA, argB );
    }

    @Override
    public void info(String format, Object... arguments )
    {
        log.info( toDriverLoggerFormat( format ), arguments );
    }

    @Override
    public void info(String msg, Throwable t )
    {
        log.info( "%s%n%s", msg, t );
    }

    @Override
    public boolean isWarnEnabled()
    {
        return true;
    }

    @Override
    public void warn( String msg )
    {
        log.warn( msg );
    }

    @Override
    public void warn(String format, Object arg )
    {
        log.warn( toDriverLoggerFormat( format ), arg );
    }

    @Override
    public void warn(String format, Object... arguments )
    {
        log.warn( toDriverLoggerFormat( format ), arguments );
    }

    @Override
    public void warn(String format, Object argA, Object argB )
    {
        log.warn( toDriverLoggerFormat( format ), argA, argB );
    }

    @Override
    public void warn(String msg, Throwable t )
    {
        log.warn( "%s%n%s", msg, t );
    }

    @Override
    public boolean isErrorEnabled()
    {
        return true;
    }

    @Override
    public void error( String msg )
    {
        log.error( msg, null );
    }

    @Override
    public void error(String format, Object arg )
    {
        error( format, new Object[]{arg} );
    }

    @Override
    public void error(String format, Object argA, Object argB )
    {
        error( format, new Object[]{argA, argB} );
    }

    @Override
    public void error(String format, Object... arguments )
    {
        format = toDriverLoggerFormat( format );
        if ( arguments.length == 0 )
        {
            log.error( format, null );
            return;
        }

        Object arg = arguments[arguments.length - 1];
        if ( arg instanceof Throwable)
        {
            // still give all arguments to string format,
            // for the worst case, the redundant parameter will be ignored.
            log.error( format( format, arguments ), (Throwable) arg );
        }
    }

    @Override
    public void error(String msg, Throwable t )
    {
        log.error( msg, t );
    }

    private String toDriverLoggerFormat(String format )
    {
        return PLACE_HOLDER_PATTERN.matcher( format ).replaceAll( "%s" );
    }
}
