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

import com.mware.bigconnect.driver.internal.FailableCursor;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

import static com.mware.bigconnect.driver.internal.util.Futures.completedWithNull;

public class ResultCursorsHolder
{
    private final List<CompletionStage<? extends FailableCursor>> cursorStages = new ArrayList<>();

    public void add( CompletionStage<? extends FailableCursor> cursorStage )
    {
        Objects.requireNonNull( cursorStage );
        cursorStages.add( cursorStage );
    }

    CompletionStage<Throwable> retrieveNotConsumedError()
    {
        CompletableFuture<Throwable>[] failures = retrieveAllFailures();

        return CompletableFuture.allOf( failures )
                .thenApply( ignore -> findFirstFailure( failures ) );
    }

    @SuppressWarnings( "unchecked" )
    private CompletableFuture<Throwable>[] retrieveAllFailures()
    {
        return cursorStages.stream()
                .map( ResultCursorsHolder::retrieveFailure )
                .map( CompletionStage::toCompletableFuture )
                .toArray( CompletableFuture[]::new );
    }

    private static Throwable findFirstFailure(CompletableFuture<Throwable>[] completedFailureFutures )
    {
        // all given futures should be completed, it is thus safe to get their values

        for ( CompletableFuture<Throwable> failureFuture : completedFailureFutures )
        {
            Throwable failure = failureFuture.getNow( null ); // does not block
            if ( failure != null )
            {
                return failure;
            }
        }
        return null;
    }

    private static CompletionStage<Throwable> retrieveFailure(CompletionStage<? extends FailableCursor> cursorStage )
    {
        return cursorStage
                .exceptionally( cursor -> null )
                .thenCompose( cursor -> cursor == null ? completedWithNull() : cursor.failureAsync() );
    }
}
