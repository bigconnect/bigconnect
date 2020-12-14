/*
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
package com.mware.bolt.v1.runtime;

import com.mware.bolt.runtime.BigConnectError;
import com.mware.core.util.BcLogger;
import com.mware.core.util.BcLoggerFactory;
import com.mware.ge.cypher.SyntaxException;

import static com.mware.bolt.runtime.Status.Classification.DatabaseError;
import static java.lang.String.format;

/**
 * Report received exceptions into the appropriate log (console or debug) and delivery stacktraces to debug.log.
 */
class ErrorReporter {
    private static final BcLogger LOGGER = BcLoggerFactory.getLogger(ErrorReporter.class);

    /**
     * Writes logs about database errors.
     * Short one-line message is written to both user and internal log.
     * Large message with stacktrace (if available) is written to internal log.
     *
     * @param error the error to log.
     */
    public void report(BigConnectError error) {
        if (error.status().code().classification() == DatabaseError) {
            String message = format("Client triggered an unexpected error [%s]: %s, reference %s.",
                    error.status().code().serialize(), error.message(), error.reference());

            if (!(error.cause() instanceof SyntaxException)) {
                // Writing to user log gets duplicated to the internal log
                LOGGER.error(message);

                // If cause/stacktrace is available write it to the internal log
                if (error.cause() != null) {
                    LOGGER.error(message, error.cause());
                }
            }
        }
    }
}
