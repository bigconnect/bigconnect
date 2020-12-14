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

import com.mware.bigconnect.driver.Session;
import com.mware.bigconnect.driver.Statement;
import com.mware.bigconnect.driver.TransactionConfig;
import com.mware.bigconnect.driver.async.StatementResultCursor;
import com.mware.bigconnect.driver.exceptions.ClientException;
import com.mware.bigconnect.driver.internal.BookmarkHolder;
import com.mware.bigconnect.driver.internal.InternalBookmark;
import com.mware.bigconnect.driver.internal.cursor.InternalStatementResultCursor;
import com.mware.bigconnect.driver.internal.cursor.RxStatementResultCursor;
import com.mware.bigconnect.driver.internal.messaging.BoltProtocol;
import com.mware.bigconnect.driver.internal.spi.Connection;
import com.mware.bigconnect.driver.internal.util.Futures;

import java.util.concurrent.CompletionException;
import java.util.concurrent.CompletionStage;
import java.util.function.BiFunction;

import static com.mware.bigconnect.driver.internal.util.Futures.completedWithNull;
import static com.mware.bigconnect.driver.internal.util.Futures.failedFuture;

public class ExplicitTransaction
{
    private enum State
    {
        /** The transaction is running with no explicit success or failure marked */
        ACTIVE,

        /**
         * This transaction has been terminated either because of explicit {@link Session#reset()} or because of a
         * fatal connection error.
         */
        TERMINATED,

        /** This transaction has successfully committed */
        COMMITTED,

        /** This transaction has been rolled back */
        ROLLED_BACK
    }

    private final Connection connection;
    private final BoltProtocol protocol;
    private final BookmarkHolder bookmarkHolder;
    private final ResultCursorsHolder resultCursors;

    private volatile State state = State.ACTIVE;

    public ExplicitTransaction( Connection connection, BookmarkHolder bookmarkHolder )
    {
        this.connection = connection;
        this.protocol = connection.protocol();
        this.bookmarkHolder = bookmarkHolder;
        this.resultCursors = new ResultCursorsHolder();
    }

    public CompletionStage<ExplicitTransaction> beginAsync(InternalBookmark initialBookmark, TransactionConfig config )
    {
        return protocol.beginTransaction( connection, initialBookmark, config )
                .handle( ( ignore, beginError ) ->
                {
                    if ( beginError != null )
                    {
                        // release connection if begin failed, transaction can't be started
                        connection.release();
                        throw Futures.asCompletionException( beginError );
                    }
                    return this;
                } );
    }

    public CompletionStage<Void> closeAsync()
    {
        if ( isOpen() )
        {
            return rollbackAsync();
        }
        else
        {
            return completedWithNull();
        }
    }

    public CompletionStage<Void> commitAsync()
    {
        if ( state == State.COMMITTED )
        {
            return failedFuture( new ClientException( "Can't commit, transaction has been committed" ) );
        }
        else if ( state == State.ROLLED_BACK )
        {
            return failedFuture( new ClientException( "Can't commit, transaction has been rolled back" ) );
        }
        else
        {
            return resultCursors.retrieveNotConsumedError()
                    .thenCompose( error -> doCommitAsync().handle( handleCommitOrRollback( error ) ) )
                    .whenComplete( ( ignore, error ) -> transactionClosed( error == null ) );
        }
    }

    public CompletionStage<Void> rollbackAsync()
    {
        if ( state == State.COMMITTED )
        {
            return failedFuture( new ClientException( "Can't rollback, transaction has been committed" ) );
        }
        else if ( state == State.ROLLED_BACK )
        {
            return failedFuture( new ClientException( "Can't rollback, transaction has been rolled back" ) );
        }
        else
        {
            return resultCursors.retrieveNotConsumedError()
                    .thenCompose( error -> doRollbackAsync().handle( handleCommitOrRollback( error ) ) )
                    .whenComplete( ( ignore, error ) -> transactionClosed( false ) );
        }
    }

    public CompletionStage<StatementResultCursor> runAsync(Statement statement, boolean waitForRunResponse )
    {
        ensureCanRunQueries();
        CompletionStage<InternalStatementResultCursor> cursorStage =
                protocol.runInExplicitTransaction( connection, statement, this, waitForRunResponse ).asyncResult();
        resultCursors.add( cursorStage );
        return cursorStage.thenApply( cursor -> cursor );
    }

    public CompletionStage<RxStatementResultCursor> runRx(Statement statement )
    {
        ensureCanRunQueries();
        CompletionStage<RxStatementResultCursor> cursorStage =
                protocol.runInExplicitTransaction( connection, statement, this, false ).rxResult();
        resultCursors.add( cursorStage );
        return cursorStage;
    }

    public boolean isOpen()
    {
        return state != State.COMMITTED && state != State.ROLLED_BACK;
    }

    public void markTerminated()
    {
        state = State.TERMINATED;
    }

    public Connection connection()
    {
        return connection;
    }

    private void ensureCanRunQueries()
    {
        if ( state == State.COMMITTED )
        {
            throw new ClientException( "Cannot run more statements in this transaction, it has been committed" );
        }
        else if ( state == State.ROLLED_BACK )
        {
            throw new ClientException( "Cannot run more statements in this transaction, it has been rolled back" );
        }
        else if ( state == State.TERMINATED )
        {
            throw new ClientException( "Cannot run more statements in this transaction, " +
                    "it has either experienced an fatal error or was explicitly terminated" );
        }
    }

    private CompletionStage<Void> doCommitAsync()
    {
        if ( state == State.TERMINATED )
        {
            return failedFuture( new ClientException( "Transaction can't be committed. " +
                                                      "It has been rolled back either because of an error or explicit termination" ) );
        }
        return protocol.commitTransaction( connection ).thenAccept( bookmarkHolder::setBookmark );
    }

    private CompletionStage<Void> doRollbackAsync()
    {
        if ( state == State.TERMINATED )
        {
            return completedWithNull();
        }
        return protocol.rollbackTransaction( connection );
    }

    private static BiFunction<Void, Throwable, Void> handleCommitOrRollback(Throwable cursorFailure )
    {
        return ( ignore, commitOrRollbackError ) ->
        {
            CompletionException combinedError = Futures.combineErrors( cursorFailure, commitOrRollbackError );
            if ( combinedError != null )
            {
                throw combinedError;
            }
            return null;
        };
    }

    private void transactionClosed( boolean isCommitted )
    {
        if ( isCommitted )
        {
            state = State.COMMITTED;
        }
        else
        {
            state = State.ROLLED_BACK;
        }
        connection.release(); // release in background
    }
}
