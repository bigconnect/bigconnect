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

import com.mware.bigconnect.driver.Record;
import com.mware.bigconnect.driver.exceptions.NoSuchRecordException;
import com.mware.bigconnect.driver.internal.cursor.InternalStatementResultCursor;
import com.mware.bigconnect.driver.internal.handlers.PullAllResponseHandler;
import com.mware.bigconnect.driver.internal.handlers.RunResponseHandler;
import com.mware.bigconnect.driver.internal.util.Futures;
import com.mware.bigconnect.driver.summary.ResultSummary;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.function.Consumer;
import java.util.function.Function;

public class AsyncStatementResultCursor implements InternalStatementResultCursor
{
    private final RunResponseHandler runResponseHandler;
    private final PullAllResponseHandler pullAllHandler;

    public AsyncStatementResultCursor( RunResponseHandler runResponseHandler, PullAllResponseHandler pullAllHandler )
    {
        this.runResponseHandler = runResponseHandler;
        this.pullAllHandler = pullAllHandler;
    }

    @Override
    public List<String> keys()
    {
        return runResponseHandler.statementKeys();
    }

    @Override
    public CompletionStage<ResultSummary> summaryAsync()
    {
        return pullAllHandler.summaryAsync();
    }

    @Override
    public CompletionStage<Record> nextAsync()
    {
        return pullAllHandler.nextAsync();
    }

    @Override
    public CompletionStage<Record> peekAsync()
    {
        return pullAllHandler.peekAsync();
    }

    @Override
    public CompletionStage<Record> singleAsync()
    {
        return nextAsync().thenCompose( firstRecord ->
        {
            if ( firstRecord == null )
            {
                throw new NoSuchRecordException(
                        "Cannot retrieve a single record, because this result is empty." );
            }
            return nextAsync().thenApply( secondRecord ->
            {
                if ( secondRecord != null )
                {
                    throw new NoSuchRecordException(
                            "Expected a result with a single record, but this result " +
                            "contains at least one more. Ensure your query returns only " +
                            "one record." );
                }
                return firstRecord;
            } );
        } );
    }

    @Override
    public CompletionStage<ResultSummary> consumeAsync()
    {
        return pullAllHandler.consumeAsync();
    }

    @Override
    public CompletionStage<ResultSummary> forEachAsync(Consumer<Record> action )
    {
        CompletableFuture<Void> resultFuture = new CompletableFuture<>();
        internalForEachAsync( action, resultFuture );
        return resultFuture.thenCompose( ignore -> summaryAsync() );
    }

    @Override
    public CompletionStage<List<Record>> listAsync()
    {
        return listAsync( Function.identity() );
    }

    @Override
    public <T> CompletionStage<List<T>> listAsync(Function<Record,T> mapFunction )
    {
        return pullAllHandler.listAsync( mapFunction );
    }

    @Override
    public CompletionStage<Throwable> failureAsync()
    {
        return pullAllHandler.failureAsync();
    }

    private void internalForEachAsync(Consumer<Record> action, CompletableFuture<Void> resultFuture )
    {
        CompletionStage<Record> recordFuture = nextAsync();

        // use async completion listener because of recursion, otherwise it is possible for
        // the caller thread to get StackOverflowError when result is large and buffered
        recordFuture.whenCompleteAsync( ( record, completionError ) ->
        {
            Throwable error = Futures.completionExceptionCause( completionError );
            if ( error != null )
            {
                resultFuture.completeExceptionally( error );
            }
            else if ( record != null )
            {
                try
                {
                    action.accept( record );
                }
                catch ( Throwable actionError )
                {
                    resultFuture.completeExceptionally( actionError );
                    return;
                }
                internalForEachAsync( action, resultFuture );
            }
            else
            {
                resultFuture.complete( null );
            }
        } );
    }
}
