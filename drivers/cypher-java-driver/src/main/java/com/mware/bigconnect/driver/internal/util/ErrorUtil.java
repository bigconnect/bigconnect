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
package com.mware.bigconnect.driver.internal.util;

import io.netty.util.internal.PlatformDependent;
import com.mware.bigconnect.driver.exceptions.*;

import java.util.concurrent.ExecutionException;
import java.util.stream.Stream;

public final class ErrorUtil
{
    private ErrorUtil()
    {
    }

    public static ServiceUnavailableException newConnectionTerminatedError( String reason )
    {
        if ( reason == null )
        {
            return newConnectionTerminatedError();
        }
        return new ServiceUnavailableException( "Connection to the database terminated. " + reason );
    }

    public static ServiceUnavailableException newConnectionTerminatedError()
    {
        return new ServiceUnavailableException( "Connection to the database terminated. " +
                                                "This can happen due to network instabilities, " +
                                                "or due to restarts of the database" );
    }

    public static BigConnectException newBigConnectError(String code, String message )
    {
        String classification = extractClassification( code );
        switch ( classification )
        {
        case "ClientError":
            if ( code.equalsIgnoreCase( "Cypher.ClientError.Security.Unauthorized" ) )
            {
                return new AuthenticationException( code, message );
            }
            else if ( code.equalsIgnoreCase( "Cypher.ClientError.Database.DatabaseNotFound" ) )
            {
                return new FatalDiscoveryException( code, message );
            }
            else
            {
                return new ClientException( code, message );
            }
        case "TransientError":
            return new TransientException( code, message );
        default:
            return new DatabaseException( code, message );
        }
    }

    public static boolean isFatal( Throwable error )
    {
        if ( error instanceof BigConnectException )
        {
            if ( isProtocolViolationError( ((BigConnectException) error) ) )
            {
                return true;
            }

            if ( isClientOrTransientError( ((BigConnectException) error) ) )
            {
                return false;
            }
        }
        return true;
    }

    public static void rethrowAsyncException( ExecutionException e )
    {
        Throwable error = e.getCause();

        InternalExceptionCause internalCause = new InternalExceptionCause( error.getStackTrace() );
        error.addSuppressed( internalCause );

        StackTraceElement[] currentStackTrace = Stream.of( Thread.currentThread().getStackTrace() )
                .skip( 2 ) // do not include Thread.currentThread() and this method in the stacktrace
                .toArray( StackTraceElement[]::new );
        error.setStackTrace( currentStackTrace );

        PlatformDependent.throwException( error );
    }

    private static boolean isProtocolViolationError( BigConnectException error )
    {
        String errorCode = error.code();
        return errorCode != null && errorCode.startsWith( "Cypher.ClientError.Request" );
    }

    private static boolean isClientOrTransientError( BigConnectException error )
    {
        String errorCode = error.code();
        return errorCode != null && (errorCode.contains( "ClientError" ) || errorCode.contains( "TransientError" ));
    }

    private static String extractClassification(String code )
    {
        String[] parts = code.split( "\\." );
        if ( parts.length < 2 )
        {
            return "";
        }
        return parts[1];
    }

    public static void addSuppressed(Throwable mainError, Throwable error )
    {
        if ( mainError != error )
        {
            mainError.addSuppressed( error );
        }
    }

    /**
     * Exception which is merely a holder of an async stacktrace, which is not the primary stacktrace users are interested in.
     * Used for blocking API calls that block on async API calls.
     */
    private static class InternalExceptionCause extends RuntimeException
    {
        InternalExceptionCause( StackTraceElement[] stackTrace )
        {
            setStackTrace( stackTrace );
        }

        @Override
        public synchronized Throwable fillInStackTrace()
        {
            // no need to fill in the stack trace
            // this exception just uses the given stack trace
            return this;
        }
    }
}
