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

import static java.util.Objects.requireNonNull;

public abstract class ReformattedLogger implements Logger
{
    private final Logger delegate;

    protected ReformattedLogger(Logger delegate)
    {
        this.delegate = requireNonNull( delegate );
    }

    @Override
    public void error(String message, Throwable cause )
    {
        delegate.error( reformat( message ), cause );
    }

    @Override
    public void info(String message, Object... params )
    {
        delegate.info( reformat( message ), params );
    }

    @Override
    public void warn(String message, Object... params )
    {
        delegate.warn( reformat( message ), params );
    }

    @Override
    public void warn(String message, Throwable cause )
    {
        delegate.warn( reformat( message ), cause );
    }

    @Override
    public void debug(String message, Object... params )
    {
        if ( isDebugEnabled() )
        {
            delegate.debug( reformat( message ), params );
        }
    }

    @Override
    public void trace(String message, Object... params )
    {
        if ( isTraceEnabled() )
        {
            delegate.trace( reformat( message ), params );
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

    protected abstract String reformat(String message );
}
