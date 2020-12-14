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

import com.mware.bigconnect.driver.internal.util.Futures;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Mono;

import java.util.concurrent.CompletionStage;
import java.util.function.Supplier;

public class RxUtils
{
    /**
     * The publisher created by this method will either succeed without publishing anything or fail with an error.
     * @param supplier supplies a {@link CompletionStage < Void >}.
     * @return A publisher that publishes nothing on completion or fails with an error.
     */
    public static <T> Publisher<T> createEmptyPublisher( Supplier<CompletionStage<Void>> supplier )
    {
        return Mono.create( sink -> supplier.get().whenComplete( ( ignore, completionError ) -> {
            Throwable error = Futures.completionExceptionCause( completionError );
            if ( error != null )
            {
                sink.error( error );
            }
            else
            {
                sink.success();
            }
        } ) );
    }

    /**
     * Create a {@link Mono<T>} publisher from the given {@link CompletionStage <T>} supplier.
     * @param supplier supplies a {@link CompletionStage <T>}.
     * @param <T> the type of the item to publish.
     * @return A {@link Mono<T>} publisher.
     */
    public static <T> Publisher<T> createMono( Supplier<CompletionStage<T>> supplier )
    {
        return Mono.create( sink -> supplier.get().whenComplete( ( item, completionError ) -> {
            Throwable error = Futures.completionExceptionCause( completionError );
            if ( item != null )
            {
                sink.success( item );
            }
            if ( error != null )
            {
                sink.error( error );
            }
            else
            {
                sink.success();
            }
        } ) );
    }
}
