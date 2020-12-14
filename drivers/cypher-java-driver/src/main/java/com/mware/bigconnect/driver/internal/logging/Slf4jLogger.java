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

import java.util.Objects;

class Slf4jLogger implements Logger
{
    private final org.slf4j.Logger delegate;

    Slf4jLogger( org.slf4j.Logger delegate )
    {
        this.delegate = Objects.requireNonNull( delegate );
    }

    @Override
    public void error(String message, Throwable cause )
    {
        if ( delegate.isErrorEnabled() )
        {
            delegate.error( message, cause );
        }
    }

    @Override
    public void info(String message, Object... params )
    {
        if ( delegate.isInfoEnabled() )
        {
            delegate.info( formatMessage( message, params ) );
        }
    }

    @Override
    public void warn(String message, Object... params )
    {
        if ( delegate.isWarnEnabled() )
        {
            delegate.warn( formatMessage( message, params ) );
        }
    }

    @Override
    public void warn(String message, Throwable cause )
    {
        if ( delegate.isWarnEnabled() )
        {
            delegate.warn( message, cause );
        }
    }

    @Override
    public void debug(String message, Object... params )
    {
        if ( isDebugEnabled() )
        {
            delegate.debug( formatMessage( message, params ) );
        }
    }

    @Override
    public void trace(String message, Object... params )
    {
        if ( isTraceEnabled() )
        {
            delegate.trace( formatMessage( message, params ) );
        }
    }

    @Override
    public boolean isTraceEnabled()
    {
        return delegate.isTraceEnabled();
    }

    @Override
    public boolean isDebugEnabled()
    {
        return delegate.isDebugEnabled();
    }

    /**
     * Creates a fully formatted message. Such formatting is needed because driver uses {@link String#format(String, Object...)} parameters in message
     * templates, i.e. '%s' or '%d' while SLF4J uses '{}'. Thus this logger passes fully formatted messages to SLF4J.
     *
     * @param messageTemplate the message template.
     * @param params the parameters.
     * @return fully formatted message string.
     */
    private static String formatMessage(String messageTemplate, Object... params )
    {
        return String.format( messageTemplate, params );
    }
}
