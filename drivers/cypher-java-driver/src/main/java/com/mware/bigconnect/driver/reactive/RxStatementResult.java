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
package com.mware.bigconnect.driver.reactive;

import com.mware.bigconnect.driver.Record;
import com.mware.bigconnect.driver.Statement;
import com.mware.bigconnect.driver.summary.ResultSummary;
import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;

/**
 * A reactive result provides a reactive way to execute query on the server and receives records back.
 * This reactive result consists of a result key publisher, a record publisher and a result summary publisher.
 * The reactive result is created via {@link RxSession#run(Statement)} and {@link RxTransaction#run(Statement)} for example.
 * On the creation of the result, the query submitted to create this result will not be executed until one of the publishers in this class is subscribed.
 * The records or the summary stream has to be consumed and finished (completed or errored) to ensure the resources used by this result to be freed correctly.
 *
 * @see Publisher
 * @see Subscriber
 * @see Subscription
 * @since 2.0
 */
public interface RxStatementResult
{
    /**
     * Returns a cold publisher of keys.
     * <p>
     * When this publisher is {@linkplain Publisher#subscribe(Subscriber) subscribed}, the query statement is sent to the server and get executed.
     * This method does not start the record streaming nor publish query execution error.
     * To retrieve the execution result, either {@link #records()} or {@link #summary()} can be used.
     * {@link #records()} starts record streaming and reports query execution error.
     * {@link #summary()} skips record streaming and directly reports query execution error.
     * <p>
     * Consuming of execution result ensures the resources (such as network connections) used by this result is freed correctly.
     * Consuming the keys without consuming the execution result will result in resource leak.
     * To avoid the resource leak, {@link RxSession#close()} (and/or {@link RxTransaction#commit()} and {@link RxTransaction#rollback()}) shall be invoked
     * and subscribed to enforce the result resources created in the {@link RxSession} (and/or {@link RxTransaction}) to be freed correctly.
     * <p>
     * This publisher can be subscribed many times. The keys published stays the same as the keys are buffered.
     * If this publisher is subscribed after the publisher of {@link #records()} or {@link #summary()},
     * then the buffered keys will be returned.
     * @return a cold publisher of keys.
     */
    Publisher<String> keys();

    /**
     * Returns a cold unicast publisher of records.
     * <p>
     * When the record publisher is {@linkplain Publisher#subscribe(Subscriber) subscribed},
     * the query statement is executed and the query result is streamed back as a record stream followed by a result summary.
     * This record publisher publishes all records in the result and signals the completion.
     * However before completion or error reporting if any, a cleanup of result resources such as network connection will be carried out automatically.
     * <p>
     * Therefore the {@link Subscriber} of this record publisher shall wait for the termination signal (complete or error)
     * to ensure that the resources used by this result are released correctly.
     * Then the session is ready to be used to run more queries.
     * <p>
     * Cancelling of the record streaming will immediately terminate the propagation of new records.
     * But it will not cancel the query execution.
     * As a result, a termination signal (complete or error) will still be sent to the {@link Subscriber} after the query execution is finished.
     * <p>
     * The record publishing event by default runs in an Network IO thread, as a result no blocking operation is allowed in this thread.
     * Otherwise network IO might be blocked by application logic.
     * <p>
     * This publisher can only be subscribed by one {@link Subscriber} once.
     * <p>
     * If this publisher is subscribed after {@link #keys()}, then the publish of records is carried out after the arrival of keys.
     * If this publisher is subscribed after {@link #summary()}, then the publish of records is already cancelled
     * and an empty publisher of zero record will be return.
     * @return a cold unicast publisher of records.
     */
    Publisher<Record> records();

    /**
     * Returns a cold publisher of result summary which arrives after all records.
     * <p>
     * {@linkplain Publisher#subscribe(Subscriber) Subscribing} the summary publisher results in the execution of the query followed by the result summary returned.
     * The summary publisher cancels record publishing if not yet subscribed and directly streams back the summary on query execution completion.
     * As a result, the invocation of {@link #records()} after this method, would receive an empty publisher.
     * <p>
     * If subscribed after {@link #keys()}, then the result summary will be published after the query execution without streaming any record to client.
     * If subscribed after {@link #records()}, then the result summary will be published after the query execution and the streaming of records.
     * <p>
     * Usually, this method shall be chained after {@link #records()} to ensure that all records are processed before summary.
     * <p>
     * This method can be subscribed multiple times. When the {@linkplain ResultSummary summary} arrives, it will be buffered locally for all subsequent calls.
     * @return a cold publisher of result summary which only arrives after all records.
     */
    Publisher<ResultSummary> summary();
}
