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
package com.mware.bigconnect.driver.async;

import com.mware.bigconnect.driver.Record;
import com.mware.bigconnect.driver.Records;
import com.mware.bigconnect.driver.exceptions.NoSuchRecordException;
import com.mware.bigconnect.driver.summary.ResultSummary;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.Executor;
import java.util.function.Consumer;
import java.util.function.Function;

/**
 * The result of asynchronous execution of a Cypher statement, conceptually an asynchronous stream of
 * {@link Record records}.
 * <p>
 * Result can be eagerly fetched in a list using {@link #listAsync()} or navigated lazily using
 * {@link #forEachAsync(Consumer)} or {@link #nextAsync()}.
 * <p>
 * Results are valid until the next statement is run or until the end of the current transaction,
 * whichever comes first. To keep a result around while further statements are run, or to use a result outside the scope
 * of the current transaction, see {@link #listAsync()}.
 * <h2>Important note on semantics</h2>
 * <p>
 * In order to handle very large results, and to minimize memory overhead and maximize
 * performance, results are retrieved lazily. Please see {@link AsyncStatementRunner} for
 * important details on the effects of this.
 * <p>
 * The short version is that, if you want a hard guarantee that the underlying statement
 * has completed, you need to either call {@link AsyncTransaction#commitAsync()} on the {@link AsyncTransaction transaction}
 * or {@link AsyncSession#closeAsync()} on the {@link AsyncSession session} that created this result, or you need to use
 * the result.
 * <p>
 * <b>Note:</b> Every returned {@link CompletionStage} can be completed by an IO thread which should never block.
 * Otherwise IO operations on this and potentially other network connections might deadlock. Please do not chain
 * blocking operations like {@link CompletableFuture#get()} on the returned stages. Consider using asynchronous calls
 * throughout the chain or offloading blocking operation to a different {@link Executor}. This can be done using
 * methods with "Async" suffix like {@link CompletionStage#thenApplyAsync(java.util.function.Function)} or
 * {@link CompletionStage#thenApplyAsync(java.util.function.Function, Executor)}.
 *
 * @since 1.5
 */
public interface StatementResultCursor
{
    /**
     * Retrieve the keys of the records this result cursor contains.
     *
     * @return list of all keys.
     */
    List<String> keys();

    /**
     * Asynchronously retrieve the result summary.
     * <p>
     * If the records in the result is not fully consumed, then calling this method will force to pull all remaining
     * records into buffer to yield the summary.
     * <p>
     * If you want to obtain the summary but discard the records, use {@link #consumeAsync()} instead.
     *
     * @return a {@link CompletionStage} completed with a summary for the whole query result. Stage can also be
     * completed exceptionally if query execution fails.
     */
    CompletionStage<ResultSummary> summaryAsync();

    /**
     * Asynchronously navigate to and retrieve the next {@link Record} in this result. Returned stage can contain
     * {@code null} if end of records stream has been reached.
     *
     * @return a {@link CompletionStage} completed with a record or {@code null}. Stage can also be
     * completed exceptionally if query execution fails.
     */
    CompletionStage<Record> nextAsync();

    /**
     * Asynchronously investigate the next upcoming {@link Record} without moving forward in the result. Returned
     * stage can contain {@code null} if end of records stream has been reached.
     *
     * @return a {@link CompletionStage} completed with a record or {@code null}. Stage can also be
     * completed exceptionally if query execution fails.
     */
    CompletionStage<Record> peekAsync();

    /**
     * Asynchronously return the first record in the result, failing if there is not exactly
     * one record left in the stream.
     *
     * @return a {@link CompletionStage} completed with the first and only record in the stream. Stage will be
     * completed exceptionally with {@link NoSuchRecordException} if there is not exactly one record left in the
     * stream. It can also be completed exceptionally if query execution fails.
     */
    CompletionStage<Record> singleAsync();

    /**
     * Asynchronously consume the entire result, yielding a summary of it. Calling this method exhausts the result.
     *
     * @return a {@link CompletionStage} completed with a summary for the whole query result. Stage can also be
     * completed exceptionally if query execution fails.
     */
    CompletionStage<ResultSummary> consumeAsync();

    /**
     * Asynchronously apply the given {@link Consumer action} to every record in the result, yielding a summary of it.
     *
     * @param action the function to be applied to every record in the result. Provided function should not block.
     * @return a {@link CompletionStage} completed with a summary for the whole query result. Stage can also be
     * completed exceptionally if query execution or provided function fails.
     */
    CompletionStage<ResultSummary> forEachAsync(Consumer<Record> action);

    /**
     * Asynchronously retrieve and store the entire result stream.
     * This can be used if you want to iterate over the stream multiple times or to store the
     * whole result for later use.
     * <p>
     * Note that this method can only be used if you know that the statement that
     * yielded this result returns a finite stream. Some statements can yield
     * infinite results, in which case calling this method will lead to running
     * out of memory.
     * <p>
     * Calling this method exhausts the result.
     *
     * @return a {@link CompletionStage} completed with a list of all remaining immutable records. Stage can also be
     * completed exceptionally if query execution fails.
     */
    CompletionStage<List<Record>> listAsync();

    /**
     * Asynchronously retrieve and store a projection of the entire result.
     * This can be used if you want to iterate over the stream multiple times or to store the
     * whole result for later use.
     * <p>
     * Note that this method can only be used if you know that the statement that
     * yielded this result returns a finite stream. Some statements can yield
     * infinite results, in which case calling this method will lead to running
     * out of memory.
     * <p>
     * Calling this method exhausts the result.
     *
     * @param mapFunction a function to map from Record to T. See {@link Records} for some predefined functions.
     * @param <T> the type of result list elements
     * @return a {@link CompletionStage} completed with a list of all remaining immutable records. Stage can also be
     * completed exceptionally if query execution or provided function fails.
     */
    <T> CompletionStage<List<T>> listAsync(Function<Record, T> mapFunction);
}
