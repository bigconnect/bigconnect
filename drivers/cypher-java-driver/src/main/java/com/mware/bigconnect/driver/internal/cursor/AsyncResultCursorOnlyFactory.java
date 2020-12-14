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
package com.mware.bigconnect.driver.internal.cursor;

import com.mware.bigconnect.driver.exceptions.ClientException;
import com.mware.bigconnect.driver.internal.async.AsyncStatementResultCursor;
import com.mware.bigconnect.driver.internal.handlers.PullAllResponseHandler;
import com.mware.bigconnect.driver.internal.handlers.RunResponseHandler;
import com.mware.bigconnect.driver.internal.messaging.Message;
import com.mware.bigconnect.driver.internal.spi.Connection;
import com.mware.bigconnect.driver.internal.util.Futures;

import java.util.concurrent.CompletionStage;

import static java.util.Objects.requireNonNull;
import static java.util.concurrent.CompletableFuture.completedFuture;
import static com.mware.bigconnect.driver.internal.messaging.request.PullAllMessage.PULL_ALL;

/**
 * Used by Bolt V1, V2, V3
 */
public class AsyncResultCursorOnlyFactory implements StatementResultCursorFactory
{
    protected final Connection connection;
    protected final Message runMessage;
    protected final RunResponseHandler runHandler;
    protected final PullAllResponseHandler pullAllHandler;
    private final boolean waitForRunResponse;

    public AsyncResultCursorOnlyFactory( Connection connection, Message runMessage, RunResponseHandler runHandler,
            PullAllResponseHandler pullHandler, boolean waitForRunResponse )
    {
        requireNonNull( connection );
        requireNonNull( runMessage );
        requireNonNull( runHandler );
        requireNonNull( pullHandler );

        this.connection = connection;
        this.runMessage = runMessage;
        this.runHandler = runHandler;

        this.pullAllHandler = pullHandler;
        this.waitForRunResponse = waitForRunResponse;
    }

    public CompletionStage<InternalStatementResultCursor> asyncResult()
    {
        // only write and flush messages when async result is wanted.
        connection.writeAndFlush( runMessage, runHandler, PULL_ALL, pullAllHandler );

        if ( waitForRunResponse )
        {
            // wait for response of RUN before proceeding
            return runHandler.runFuture().thenApply( ignore -> new AsyncStatementResultCursor( runHandler, pullAllHandler ) );
        }
        else
        {
            return completedFuture( new AsyncStatementResultCursor( runHandler, pullAllHandler ) );
        }
    }

    public CompletionStage<RxStatementResultCursor> rxResult()
    {
        return Futures.failedFuture( new ClientException( "Driver is connected to the database that does not support driver reactive API." ) );
    }
}
