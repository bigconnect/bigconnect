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
package com.mware.bigconnect.driver.internal.reactive;

import com.mware.bigconnect.driver.AccessMode;
import com.mware.bigconnect.driver.Statement;
import com.mware.bigconnect.driver.TransactionConfig;
import com.mware.bigconnect.driver.internal.Bookmark;
import com.mware.bigconnect.driver.internal.async.NetworkSession;
import com.mware.bigconnect.driver.internal.cursor.RxStatementResultCursor;
import com.mware.bigconnect.driver.internal.util.Futures;
import com.mware.bigconnect.driver.reactive.RxSession;
import com.mware.bigconnect.driver.reactive.RxStatementResult;
import com.mware.bigconnect.driver.reactive.RxTransaction;
import com.mware.bigconnect.driver.reactive.RxTransactionWork;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Flux;

import java.util.Map;
import java.util.concurrent.CompletableFuture;

import static com.mware.bigconnect.driver.internal.reactive.RxUtils.createEmptyPublisher;
import static com.mware.bigconnect.driver.internal.reactive.RxUtils.createMono;

public class InternalRxSession extends AbstractRxStatementRunner implements RxSession
{
    private final NetworkSession session;

    public InternalRxSession( NetworkSession session )
    {
        // RxSession accept a network session as input.
        // The network session different from async session that it provides ways to both run for Rx and Async
        // Note: Blocking result could just build on top of async result. However Rx result cannot just build on top of async result.
        this.session = session;
    }

    @Override
    public Publisher<RxTransaction> beginTransaction()
    {
        return beginTransaction( TransactionConfig.empty() );
    }

    @Override
    public Publisher<RxTransaction> beginTransaction( TransactionConfig config )
    {
        return createMono( () ->
        {
            CompletableFuture<RxTransaction> txFuture = new CompletableFuture<>();
            session.beginTransactionAsync( config ).whenComplete( ( tx, completionError ) -> {
                if ( tx != null )
                {
                    txFuture.complete( new InternalRxTransaction( tx ) );
                }
                else
                {
                    releaseConnectionBeforeReturning( txFuture, completionError );
                }
            } );
            return txFuture;
        } );
    }

    private Publisher<RxTransaction> beginTransaction( AccessMode mode, TransactionConfig config )
    {
        return createMono( () ->
        {
            CompletableFuture<RxTransaction> txFuture = new CompletableFuture<>();
            session.beginTransactionAsync( mode, config ).whenComplete( ( tx, completionError ) -> {
                if ( tx != null )
                {
                    txFuture.complete( new InternalRxTransaction( tx ) );
                }
                else
                {
                    releaseConnectionBeforeReturning( txFuture, completionError );
                }
            } );
            return txFuture;
        } );
    }

    @Override
    public <T> Publisher<T> readTransaction( RxTransactionWork<Publisher<T>> work )
    {
        return readTransaction( work, TransactionConfig.empty() );
    }

    @Override
    public <T> Publisher<T> readTransaction( RxTransactionWork<Publisher<T>> work, TransactionConfig config )
    {
        return runTransaction( AccessMode.READ, work, config );
    }

    @Override
    public <T> Publisher<T> writeTransaction( RxTransactionWork<Publisher<T>> work )
    {
        return writeTransaction( work, TransactionConfig.empty() );
    }

    @Override
    public <T> Publisher<T> writeTransaction( RxTransactionWork<Publisher<T>> work, TransactionConfig config )
    {
        return runTransaction( AccessMode.WRITE, work, config );
    }

    private <T> Publisher<T> runTransaction( AccessMode mode, RxTransactionWork<Publisher<T>> work, TransactionConfig config )
    {
        Flux<T> repeatableWork = Flux.usingWhen( beginTransaction( mode, config ), work::execute, RxTransaction::commit, RxTransaction::rollback );
        return session.retryLogic().retryRx( repeatableWork );
    }

    @Override
    public RxStatementResult run(String statement, TransactionConfig config )
    {
        return run( new Statement( statement ), config );
    }

    @Override
    public RxStatementResult run(String statement, Map<String, Object> parameters, TransactionConfig config )
    {
        return run( new Statement( statement, parameters ), config );
    }

    @Override
    public RxStatementResult run( Statement statement )
    {
        return run( statement, TransactionConfig.empty() );
    }

    @Override
    public RxStatementResult run( Statement statement, TransactionConfig config )
    {
        return new InternalRxStatementResult( () -> {
            CompletableFuture<RxStatementResultCursor> resultCursorFuture = new CompletableFuture<>();
            session.runRx( statement, config ).whenComplete( ( cursor, completionError ) -> {
                if ( cursor != null )
                {
                    resultCursorFuture.complete( cursor );
                }
                else
                {
                    releaseConnectionBeforeReturning( resultCursorFuture, completionError );
                }
            } );
            return resultCursorFuture;
        } );
    }

    private <T> void releaseConnectionBeforeReturning(CompletableFuture<T> returnFuture, Throwable completionError )
    {
        // We failed to create a result cursor so we cannot rely on result cursor to cleanup resources.
        // Therefore we will first release the connection that might have been created in the session and then notify the error.
        // The logic here shall be the same as `SessionPullResponseHandler#afterFailure`.
        // The reason we need to release connection in session is that we do not have a `rxSession.close()`;
        // Otherwise, session.close shall handle everything for us.
        Throwable error = Futures.completionExceptionCause( completionError );
        session.releaseConnectionAsync().whenComplete( ( ignored, closeError ) ->
                returnFuture.completeExceptionally( Futures.combineErrors( error, closeError ) ) );
    }

    @Override
    public Bookmark lastBookmark()
    {
        return session.lastBookmark();
    }

    public Publisher<Void> reset()
    {
        return createEmptyPublisher( session::resetAsync );
    }

    @Override
    public <T> Publisher<T> close()
    {
        return createEmptyPublisher( session::closeAsync );
    }
}
