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

import com.mware.bigconnect.driver.Statement;
import com.mware.bigconnect.driver.internal.async.ExplicitTransaction;
import com.mware.bigconnect.driver.internal.cursor.RxStatementResultCursor;
import com.mware.bigconnect.driver.internal.util.Futures;
import com.mware.bigconnect.driver.reactive.RxStatementResult;
import com.mware.bigconnect.driver.reactive.RxTransaction;
import org.reactivestreams.Publisher;

import java.util.concurrent.CompletableFuture;

import static com.mware.bigconnect.driver.internal.reactive.RxUtils.createEmptyPublisher;

public class InternalRxTransaction extends AbstractRxStatementRunner implements RxTransaction
{
    private final ExplicitTransaction tx;

    public InternalRxTransaction( ExplicitTransaction tx )
    {
        this.tx = tx;
    }

    @Override
    public RxStatementResult run( Statement statement )
    {
        return new InternalRxStatementResult( () -> {
            CompletableFuture<RxStatementResultCursor> cursorFuture = new CompletableFuture<>();
            tx.runRx( statement ).whenComplete( ( cursor, completionError ) -> {
                if ( cursor != null )
                {
                    cursorFuture.complete( cursor );
                }
                else
                {
                    // We failed to create a result cursor so we cannot rely on result cursor to handle failure.
                    // The logic here shall be the same as `TransactionPullResponseHandler#afterFailure` as that is where cursor handling failure
                    // This is optional as tx still holds a reference to all cursor futures and they will be clean up properly in commit
                    Throwable error = Futures.completionExceptionCause( completionError );
                    tx.markTerminated();
                    cursorFuture.completeExceptionally( error );
                }
            } );
            return cursorFuture;
        } );
    }

    @Override
    public <T> Publisher<T> commit()
    {
        return close( true );
    }

    @Override
    public <T> Publisher<T> rollback()
    {
        return close( false );
    }

    private <T> Publisher<T> close( boolean commit )
    {
        return createEmptyPublisher( () -> {
            if ( commit )
            {
                return tx.commitAsync();
            }
            else
            {
                return tx.rollbackAsync();
            }
        } );
    }
}
