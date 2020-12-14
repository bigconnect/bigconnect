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
package com.mware.bigconnect.driver.internal.handlers;

import com.mware.bigconnect.driver.AccessMode;
import com.mware.bigconnect.driver.Value;
import com.mware.bigconnect.driver.exceptions.ClientException;
import com.mware.bigconnect.driver.exceptions.ServiceUnavailableException;
import com.mware.bigconnect.driver.exceptions.SessionExpiredException;
import com.mware.bigconnect.driver.exceptions.TransientException;
import com.mware.bigconnect.driver.internal.BoltServerAddress;
import com.mware.bigconnect.driver.internal.RoutingErrorHandler;
import com.mware.bigconnect.driver.internal.spi.ResponseHandler;
import com.mware.bigconnect.driver.internal.util.Futures;

import java.util.Map;
import java.util.Objects;

import static java.lang.String.format;

public class RoutingResponseHandler implements ResponseHandler
{
    private final ResponseHandler delegate;
    private final BoltServerAddress address;
    private final AccessMode accessMode;
    private final RoutingErrorHandler errorHandler;

    public RoutingResponseHandler( ResponseHandler delegate, BoltServerAddress address, AccessMode accessMode,
            RoutingErrorHandler errorHandler )
    {
        this.delegate = delegate;
        this.address = address;
        this.accessMode = accessMode;
        this.errorHandler = errorHandler;
    }

    @Override
    public void onSuccess( Map<String,Value> metadata )
    {
        delegate.onSuccess( metadata );
    }

    @Override
    public void onFailure( Throwable error )
    {
        Throwable newError = handledError( error );
        delegate.onFailure( newError );
    }

    @Override
    public void onRecord( Value[] fields )
    {
        delegate.onRecord( fields );
    }

    @Override
    public boolean canManageAutoRead()
    {
        return delegate.canManageAutoRead();
    }

    @Override
    public void disableAutoReadManagement()
    {
        delegate.disableAutoReadManagement();
    }

    private Throwable handledError(Throwable receivedError )
    {
        Throwable error = Futures.completionExceptionCause( receivedError );

        if ( error instanceof ServiceUnavailableException )
        {
            return handledServiceUnavailableException( ((ServiceUnavailableException) error) );
        }
        else if ( error instanceof ClientException )
        {
            return handledClientException( ((ClientException) error) );
        }
        else if ( error instanceof TransientException )
        {
            return handledTransientException( ((TransientException) error) );
        }
        else
        {
            return error;
        }
    }

    private Throwable handledServiceUnavailableException(ServiceUnavailableException e )
    {
        errorHandler.onConnectionFailure( address );
        return new SessionExpiredException( format( "Server at %s is no longer available", address ), e );
    }

    private Throwable handledTransientException(TransientException e )
    {
        String errorCode = e.code();
        if ( Objects.equals( errorCode, "Cypher.TransientError.General.DatabaseUnavailable" ) )
        {
            errorHandler.onConnectionFailure( address );
        }
        return e;
    }

    private Throwable handledClientException(ClientException e )
    {
        if ( isFailureToWrite( e ) )
        {
            // The server is unaware of the session mode, so we have to implement this logic in the driver.
            // In the future, we might be able to move this logic to the server.
            switch ( accessMode )
            {
            case READ:
                return new ClientException( "Write queries cannot be performed in READ access mode." );
            case WRITE:
                errorHandler.onWriteFailure( address );
                return new SessionExpiredException( format( "Server at %s no longer accepts writes", address ) );
            default:
                throw new IllegalArgumentException( accessMode + " not supported." );
            }
        }
        return e;
    }

    private static boolean isFailureToWrite( ClientException e )
    {
        String errorCode = e.code();
        return Objects.equals( errorCode, "Cypher.ClientError.Cluster.NotALeader" ) ||
               Objects.equals( errorCode, "Cypher.ClientError.General.ForbiddenOnReadOnlyDatabase" );
    }
}
