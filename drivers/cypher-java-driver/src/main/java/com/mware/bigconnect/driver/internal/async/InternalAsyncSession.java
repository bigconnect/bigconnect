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
import com.mware.bigconnect.driver.Statement;
import com.mware.bigconnect.driver.TransactionConfig;
import com.mware.bigconnect.driver.async.AsyncSession;
import com.mware.bigconnect.driver.async.AsyncTransaction;
import com.mware.bigconnect.driver.async.AsyncTransactionWork;
import com.mware.bigconnect.driver.async.StatementResultCursor;
import com.mware.bigconnect.driver.internal.Bookmark;
import com.mware.bigconnect.driver.internal.util.Futures;

import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

import static java.util.Collections.emptyMap;
import static com.mware.bigconnect.driver.internal.util.Futures.completedWithNull;
import static com.mware.bigconnect.driver.internal.util.Futures.failedFuture;

public class InternalAsyncSession extends AsyncAbstractStatementRunner implements AsyncSession
{
    private final NetworkSession session;

    public InternalAsyncSession( NetworkSession session )
    {
        this.session = session;
    }

    @Override
    public CompletionStage<StatementResultCursor> runAsync(Statement statement )
    {
        return runAsync( statement, TransactionConfig.empty() );
    }

    @Override
    public CompletionStage<StatementResultCursor> runAsync(String statement, TransactionConfig config )
    {
        return runAsync( statement, emptyMap(), config );
    }

    @Override
    public CompletionStage<StatementResultCursor> runAsync(String statement, Map<String, Object> parameters, TransactionConfig config )
    {
        return runAsync( new Statement( statement, parameters ), config );
    }

    @Override
    public CompletionStage<StatementResultCursor> runAsync(Statement statement, TransactionConfig config )
    {
        return session.runAsync( statement, config, true );
    }

    @Override
    public CompletionStage<Void> closeAsync()
    {
        return session.closeAsync();
    }

    @Override
    public CompletionStage<AsyncTransaction> beginTransactionAsync()
    {
        return beginTransactionAsync( TransactionConfig.empty() );
    }

    @Override
    public CompletionStage<AsyncTransaction> beginTransactionAsync(TransactionConfig config )
    {
        return session.beginTransactionAsync( config ).thenApply( InternalAsyncTransaction::new );
    }

    @Override
    public <T> CompletionStage<T> readTransactionAsync(AsyncTransactionWork<CompletionStage<T>> work )
    {
        return readTransactionAsync( work, TransactionConfig.empty() );
    }

    @Override
    public <T> CompletionStage<T> readTransactionAsync(AsyncTransactionWork<CompletionStage<T>> work, TransactionConfig config )
    {
        return transactionAsync( AccessMode.READ, work, config );
    }

    @Override
    public <T> CompletionStage<T> writeTransactionAsync(AsyncTransactionWork<CompletionStage<T>> work )
    {
        return writeTransactionAsync( work, TransactionConfig.empty() );
    }

    @Override
    public <T> CompletionStage<T> writeTransactionAsync(AsyncTransactionWork<CompletionStage<T>> work, TransactionConfig config )
    {
        return transactionAsync( AccessMode.WRITE, work, config );
    }

    @Override
    public Bookmark lastBookmark()
    {
        return session.lastBookmark();
    }

    private <T> CompletionStage<T> transactionAsync(AccessMode mode, AsyncTransactionWork<CompletionStage<T>> work, TransactionConfig config )
    {
        return session.retryLogic().retryAsync( () -> {
            CompletableFuture<T> resultFuture = new CompletableFuture<>();
            CompletionStage<ExplicitTransaction> txFuture = session.beginTransactionAsync( mode, config );

            txFuture.whenComplete( ( tx, completionError ) -> {
                Throwable error = Futures.completionExceptionCause( completionError );
                if ( error != null )
                {
                    resultFuture.completeExceptionally( error );
                }
                else
                {
                    executeWork( resultFuture, tx, work );
                }
            } );

            return resultFuture;
        } );
    }

    private <T> void executeWork(CompletableFuture<T> resultFuture, ExplicitTransaction tx, AsyncTransactionWork<CompletionStage<T>> work )
    {
        CompletionStage<T> workFuture = safeExecuteWork( tx, work );
        workFuture.whenComplete( ( result, completionError ) -> {
            Throwable error = Futures.completionExceptionCause( completionError );
            if ( error != null )
            {
                rollbackTxAfterFailedTransactionWork( tx, resultFuture, error );
            }
            else
            {
                closeTxAfterSucceededTransactionWork( tx, resultFuture, result );
            }
        } );
    }

    private <T> CompletionStage<T> safeExecuteWork(ExplicitTransaction tx, AsyncTransactionWork<CompletionStage<T>> work )
    {
        // given work might fail in both async and sync way
        // async failure will result in a failed future being returned
        // sync failure will result in an exception being thrown
        try
        {
            CompletionStage<T> result = work.execute( new InternalAsyncTransaction( tx ) );

            // protect from given transaction function returning null
            return result == null ? completedWithNull() : result;
        }
        catch ( Throwable workError )
        {
            // work threw an exception, wrap it in a future and proceed
            return failedFuture( workError );
        }
    }

    private <T> void rollbackTxAfterFailedTransactionWork(ExplicitTransaction tx, CompletableFuture<T> resultFuture, Throwable error )
    {
        if ( tx.isOpen() )
        {
            tx.rollbackAsync().whenComplete( ( ignore, rollbackError ) -> {
                if ( rollbackError != null )
                {
                    error.addSuppressed( rollbackError );
                }
                resultFuture.completeExceptionally( error );
            } );
        }
        else
        {
            resultFuture.completeExceptionally( error );
        }
    }

    private <T> void closeTxAfterSucceededTransactionWork(ExplicitTransaction tx, CompletableFuture<T> resultFuture, T result )
    {
        if ( tx.isOpen() )
        {
            tx.commitAsync().whenComplete( ( ignore, completionError ) -> {
                Throwable commitError = Futures.completionExceptionCause( completionError );
                if ( commitError != null )
                {
                    resultFuture.completeExceptionally( commitError );
                }
                else
                {
                    resultFuture.complete( result );
                }
            } );
        }
        else
        {
            resultFuture.complete( result );
        }
    }
}
