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
package com.mware.bigconnect.driver.internal.async.inbound;

import io.netty.channel.Channel;
import com.mware.bigconnect.driver.Logger;
import com.mware.bigconnect.driver.Logging;
import com.mware.bigconnect.driver.Value;
import com.mware.bigconnect.driver.exceptions.ClientException;
import com.mware.bigconnect.driver.internal.handlers.ResetResponseHandler;
import com.mware.bigconnect.driver.internal.logging.ChannelActivityLogger;
import com.mware.bigconnect.driver.internal.messaging.ResponseMessageHandler;
import com.mware.bigconnect.driver.internal.spi.ResponseHandler;
import com.mware.bigconnect.driver.internal.util.ErrorUtil;

import java.util.Arrays;
import java.util.LinkedList;
import java.util.Map;
import java.util.Queue;

import static java.util.Objects.requireNonNull;
import static com.mware.bigconnect.driver.internal.messaging.request.ResetMessage.RESET;
import static com.mware.bigconnect.driver.internal.util.ErrorUtil.addSuppressed;

public class InboundMessageDispatcher implements ResponseMessageHandler
{
    private final Channel channel;
    private final Queue<ResponseHandler> handlers = new LinkedList<>();
    private final Logger log;

    private Throwable currentError;
    private boolean fatalErrorOccurred;

    private ResponseHandler autoReadManagingHandler;

    public InboundMessageDispatcher( Channel channel, Logging logging )
    {
        this.channel = requireNonNull( channel );
        this.log = new ChannelActivityLogger( channel, logging, getClass() );
    }

    public void enqueue( ResponseHandler handler )
    {
        if ( fatalErrorOccurred )
        {
            handler.onFailure( currentError );
        }
        else
        {
            handlers.add( handler );
            updateAutoReadManagingHandlerIfNeeded( handler );
        }
    }

    public int queuedHandlersCount()
    {
        return handlers.size();
    }

    @Override
    public void handleSuccessMessage( Map<String,Value> meta )
    {
        log.debug( "S: SUCCESS %s", meta );
        ResponseHandler handler = removeHandler();
        handler.onSuccess( meta );
    }

    @Override
    public void handleRecordMessage( Value[] fields )
    {
        if ( log.isDebugEnabled() )
        {
            log.debug( "S: RECORD %s", Arrays.toString( fields ) );
        }
        ResponseHandler handler = handlers.peek();
        if ( handler == null )
        {
            throw new IllegalStateException( "No handler exists to handle RECORD message with fields: " + Arrays.toString( fields ) );
        }
        handler.onRecord( fields );
    }

    @Override
    public void handleFailureMessage(String code, String message )
    {
        log.debug( "S: FAILURE %s \"%s\"", code, message );

        currentError = ErrorUtil.newBigConnectError( code, message );

        if ( ErrorUtil.isFatal( currentError ) )
        {
            // we should not continue using channel after a fatal error
            // fire error event back to the pipeline and avoid sending RESET
            channel.pipeline().fireExceptionCaught( currentError );
            return;
        }

        // write a RESET to "acknowledge" the failure
        enqueue( new ResetResponseHandler( this ) );
        channel.writeAndFlush( RESET, channel.voidPromise() );

        ResponseHandler handler = removeHandler();
        handler.onFailure( currentError );
    }

    @Override
    public void handleIgnoredMessage()
    {
        log.debug( "S: IGNORED" );

        ResponseHandler handler = removeHandler();

        Throwable error;
        if ( currentError != null )
        {
            error = currentError;
        }
        else
        {
            log.warn( "Received IGNORED message for handler %s but error is missing and RESET is not in progress. " +
                      "Current handlers %s", handler, handlers );

            error = new ClientException( "Database ignored the request" );
        }
        handler.onFailure( error );
    }

    public void handleChannelError( Throwable error )
    {
        if ( currentError != null )
        {
            // we already have an error, this new error probably is caused by the existing one, thus we chain the new error on this current error
            addSuppressed( currentError, error );
        }
        else
        {
            currentError = error;
        }
        fatalErrorOccurred = true;

        while ( !handlers.isEmpty() )
        {
            ResponseHandler handler = removeHandler();
            handler.onFailure( currentError );
        }
    }

    public void clearCurrentError()
    {
        currentError = null;
    }

    public Throwable currentError()
    {
        return currentError;
    }

    public boolean fatalErrorOccurred()
    {
        return fatalErrorOccurred;
    }

    /**
     * <b>Visible for testing</b>
     */
    ResponseHandler autoReadManagingHandler()
    {
        return autoReadManagingHandler;
    }

    private ResponseHandler removeHandler()
    {
        ResponseHandler handler = handlers.remove();
        if ( handler == autoReadManagingHandler )
        {
            // the auto-read managing handler is being removed
            // make sure this dispatcher does not hold on to a removed handler
            updateAutoReadManagingHandler( null );
        }
        return handler;
    }

    private void updateAutoReadManagingHandlerIfNeeded( ResponseHandler handler )
    {
        if ( handler.canManageAutoRead() )
        {
            updateAutoReadManagingHandler( handler );
        }
    }

    private void updateAutoReadManagingHandler( ResponseHandler newHandler )
    {
        if ( autoReadManagingHandler != null )
        {
            // there already exists a handler that manages channel's auto-read
            // make it stop because new managing handler is being added and there should only be a single such handler
            autoReadManagingHandler.disableAutoReadManagement();
            // restore the default value of auto-read
            channel.config().setAutoRead( true );
        }
        autoReadManagingHandler = newHandler;
    }
}
