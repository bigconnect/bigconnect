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

import com.mware.bigconnect.driver.Record;
import com.mware.bigconnect.driver.internal.cursor.RxStatementResultCursor;
import com.mware.bigconnect.driver.internal.util.Futures;
import com.mware.bigconnect.driver.reactive.RxStatementResult;
import com.mware.bigconnect.driver.summary.ResultSummary;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.concurrent.CompletionStage;
import java.util.function.Supplier;

import static reactor.core.publisher.FluxSink.OverflowStrategy.IGNORE;

public class InternalRxStatementResult implements RxStatementResult
{
    private Supplier<CompletionStage<RxStatementResultCursor>> cursorFutureSupplier;
    private volatile CompletionStage<RxStatementResultCursor> cursorFuture;

    public InternalRxStatementResult( Supplier<CompletionStage<RxStatementResultCursor>> cursorFuture )
    {
        this.cursorFutureSupplier = cursorFuture;
    }

    @Override
    public Publisher<String> keys()
    {
        return Flux.defer( () -> Mono.fromCompletionStage( getCursorFuture() )
                .flatMapIterable( RxStatementResultCursor::keys ).onErrorMap( Futures::completionExceptionCause ) );
    }

    @Override
    public Publisher<Record> records()
    {
        return Flux.create( sink -> getCursorFuture().whenComplete( ( cursor, completionError ) -> {
            if( cursor != null )
            {
                if( cursor.isDone() )
                {
                    sink.complete();
                }
                else
                {
                    cursor.installRecordConsumer( ( r, e ) -> {
                        if ( r != null )
                        {
                            sink.next( r );
                        }
                        else if ( e != null )
                        {
                            sink.error( e );
                        }
                        else
                        {
                            sink.complete();
                        }
                    } );
                    sink.onCancel( cursor::cancel );
                    sink.onRequest( cursor::request );
                }
            }
            else
            {
                Throwable error = Futures.completionExceptionCause( completionError );
                sink.error( error );
            }
        } ), IGNORE );
    }

    private CompletionStage<RxStatementResultCursor> getCursorFuture()
    {
        if ( cursorFuture != null )
        {
            return cursorFuture;
        }
        return initCursorFuture();
    }

    synchronized CompletionStage<RxStatementResultCursor> initCursorFuture()
    {
        // A quick path to return
        if ( cursorFuture != null )
        {
            return cursorFuture;
        }

        // now we obtained lock and we are going to be the one who assigns cursorFuture one and only once.
        cursorFuture = cursorFutureSupplier.get();
        cursorFutureSupplier = null; // we no longer need the reference to this object
        return this.cursorFuture;
    }

    @Override
    public Publisher<ResultSummary> summary()
    {
        return Mono.create( sink -> getCursorFuture().whenComplete( ( cursor, completionError ) -> {
            if ( cursor != null )
            {
                cursor.summaryAsync().whenComplete( ( summary, summaryCompletionError ) -> {
                    Throwable error = Futures.completionExceptionCause( summaryCompletionError );
                    if ( summary != null )
                    {
                        sink.success( summary );
                    }
                    else
                    {
                        sink.error( error );
                    }
                } );
            }
            else
            {
                Throwable error = Futures.completionExceptionCause( completionError );
                sink.error( error );
            }
        } ) );
    }

    // For testing purpose
    Supplier<CompletionStage<RxStatementResultCursor>> cursorFutureSupplier()
    {
        return this.cursorFutureSupplier;
    }
}
