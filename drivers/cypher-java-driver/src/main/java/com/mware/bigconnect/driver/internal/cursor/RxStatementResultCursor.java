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

import com.mware.bigconnect.driver.Record;
import com.mware.bigconnect.driver.internal.FailableCursor;
import com.mware.bigconnect.driver.internal.handlers.RunResponseHandler;
import com.mware.bigconnect.driver.internal.handlers.pulln.BasicPullResponseHandler;
import com.mware.bigconnect.driver.summary.ResultSummary;
import org.reactivestreams.Subscription;

import java.util.List;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.function.BiConsumer;

import static com.mware.bigconnect.driver.internal.handlers.pulln.AbstractBasicPullResponseHandler.DISCARD_RECORD_CONSUMER;

public class RxStatementResultCursor implements Subscription, FailableCursor
{
    private final RunResponseHandler runHandler;
    private final BasicPullResponseHandler pullHandler;
    private final Throwable runResponseError;
    private final CompletableFuture<ResultSummary> summaryFuture = new CompletableFuture<>();
    boolean isRecordHandlerInstalled = false;

    public RxStatementResultCursor( RunResponseHandler runHandler, BasicPullResponseHandler pullHandler )
    {
        this( null, runHandler, pullHandler );
    }

    public RxStatementResultCursor(Throwable runError, RunResponseHandler runHandler, BasicPullResponseHandler pullHandler )
    {
        Objects.requireNonNull( runHandler );
        Objects.requireNonNull( pullHandler );
        assertRunResponseArrived( runHandler );

        this.runResponseError = runError;
        this.runHandler = runHandler;
        this.pullHandler = pullHandler;
        installSummaryConsumer();
    }

    public List<String> keys()
    {
        return runHandler.statementKeys();
    }

    public void installRecordConsumer( BiConsumer<Record, Throwable> recordConsumer )
    {
        if ( isRecordHandlerInstalled )
        {
            return;
        }
        isRecordHandlerInstalled = true;
        pullHandler.installRecordConsumer( recordConsumer );
        assertRunCompletedSuccessfully();
    }

    public void request( long n )
    {
        pullHandler.request( n );
    }

    @Override
    public void cancel()
    {
        pullHandler.cancel();
    }

    @Override
    public CompletionStage<Throwable> failureAsync()
    {
        // calling this method will enforce discarding record stream and finish running cypher query
        return summaryAsync().thenApply( summary -> (Throwable) null ).exceptionally(error -> error );
    }

    public CompletionStage<ResultSummary> summaryAsync()
    {
        if ( !isDone() ) // the summary is called before record streaming
        {
            installRecordConsumer( DISCARD_RECORD_CONSUMER );
            cancel();
        }

        return this.summaryFuture;
    }

    public boolean isDone()
    {
        return summaryFuture.isDone();
    }

    private void assertRunCompletedSuccessfully()
    {
        if ( runResponseError != null )
        {
            pullHandler.onFailure( runResponseError );
        }
    }

    private void installSummaryConsumer()
    {
        pullHandler.installSummaryConsumer( ( summary, error ) -> {
            if ( error != null )
            {
                summaryFuture.completeExceptionally( error );
            }
            else if ( summary != null )
            {
                summaryFuture.complete( summary );
            }
            //else (null, null) to indicate a has_more success
        } );
    }

    private void assertRunResponseArrived( RunResponseHandler runHandler )
    {
        if ( !runHandler.runFuture().isDone() )
        {
            throw new IllegalStateException( "Should wait for response of RUN before allowing PULL." );
        }
    }
}
