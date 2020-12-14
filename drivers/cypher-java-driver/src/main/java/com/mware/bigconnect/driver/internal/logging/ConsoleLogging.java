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

import java.time.LocalDateTime;
import java.util.Objects;
import java.util.logging.*;

import static java.time.format.DateTimeFormatter.ISO_LOCAL_DATE_TIME;

/**
 * Internal implementation of the console logging.
 * <b>This class should not be used directly.</b> Please use {@link Logging#console(Level)} factory method instead.
 *
 * @see Logging#console(Level)
 */
public class ConsoleLogging implements Logging
{
    private final Level level;

    public ConsoleLogging( Level level )
    {
        this.level = Objects.requireNonNull( level );
    }

    @Override
    public Logger getLog( String name )
    {
        return new ConsoleLogger( name, level );
    }

    public static class ConsoleLogger extends JULogger
    {
        private final ConsoleHandler handler;

        public ConsoleLogger(String name, Level level )
        {
            super( name, level );
            java.util.logging.Logger logger = java.util.logging.Logger.getLogger( name );

            logger.setUseParentHandlers( false );
            // remove all other logging handlers
            Handler[] handlers = logger.getHandlers();
            for ( Handler handlerToRemove : handlers )
            {
                logger.removeHandler( handlerToRemove );
            }

            handler = new ConsoleHandler();
            handler.setFormatter( new ShortFormatter() );
            handler.setLevel( level );
            logger.addHandler( handler );
            logger.setLevel( level );
        }
    }

    private static class ShortFormatter extends Formatter
    {
        @Override
        public String format(LogRecord record )
        {
            return LocalDateTime.now().format( ISO_LOCAL_DATE_TIME ) + " " +
                   record.getLevel() + " " +
                   record.getLoggerName() + " - " +
                   formatMessage( record ) +
                   "\n";
        }
    }
}
