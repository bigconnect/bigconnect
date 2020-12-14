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
package com.mware.bigconnect.driver.internal.async;

import com.mware.bigconnect.driver.AccessMode;
import com.mware.bigconnect.driver.Logging;
import com.mware.bigconnect.driver.internal.BookmarkHolder;
import com.mware.bigconnect.driver.internal.retry.RetryLogic;
import com.mware.bigconnect.driver.internal.spi.ConnectionProvider;
import com.mware.bigconnect.driver.internal.util.Futures;

import static java.lang.System.lineSeparator;

public class LeakLoggingNetworkSession extends NetworkSession
{
    private final String stackTrace;

    public LeakLoggingNetworkSession(ConnectionProvider connectionProvider, RetryLogic retryLogic, String databaseName, AccessMode mode,
                                     BookmarkHolder bookmarkHolder, Logging logging )
    {
        super( connectionProvider, retryLogic, databaseName, mode, bookmarkHolder, logging );
        this.stackTrace = captureStackTrace();
    }

    @Override
    protected void finalize() throws Throwable
    {
        logLeakIfNeeded();
        super.finalize();
    }

    private void logLeakIfNeeded()
    {
        Boolean isOpen = Futures.blockingGet( currentConnectionIsOpen() );
        if ( isOpen )
        {
            logger.error( "BigConnect Session object leaked, please ensure that your application " +
                          "fully consumes results in Sessions or explicitly calls `close` on Sessions before disposing of the objects.\n" +
                          "Session was create at:\n" + stackTrace, null );
        }
    }
    private static String captureStackTrace()
    {
        StringBuilder result = new StringBuilder();
        StackTraceElement[] elements = Thread.currentThread().getStackTrace();
        for ( StackTraceElement element : elements )
        {
            result.append( "\t" ).append( element ).append( lineSeparator() );
        }
        return result.toString();
    }
}
