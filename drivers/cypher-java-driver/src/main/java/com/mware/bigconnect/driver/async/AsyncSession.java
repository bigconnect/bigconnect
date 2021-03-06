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

import com.mware.bigconnect.driver.*;
import com.mware.bigconnect.driver.internal.Bookmark;

import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.Executor;
import java.util.function.Function;

/**
 * Provides a context of work for database interactions.
 * <p>
 * A <em>AsyncSession</em> hosts a series of {@linkplain AsyncTransaction transactions}
 * carried out against a database. Within the database, all statements are
 * carried out within a transaction. Within application code, however, it is
 * not always necessary to explicitly {@link #beginTransactionAsync() begin a
 * transaction}. If a statement is {@link #runAsync} directly against a {@link
 * AsyncSession}, the server will automatically <code>BEGIN</code> and
 * <code>COMMIT</code> that statement within its own transaction. This type
 * of transaction is known as an <em>autocommit transaction</em>.
 * <p>
 * Explicit transactions allow multiple statements to be committed as part of
 * a single atomic operation and can be rolled back if necessary. They can also
 * be used to ensure <em>causal consistency</em>, meaning that an application
 * can run a series of queries on different members of a cluster, while
 * ensuring that each query sees the state of graph at least as up-to-date as
 * the graph seen by the previous query. For more on causal consistency, see
 * the BigConnect clustering manual.
 * <p>
 * Typically, a session will acquire a TCP connection to execute query or
 * transaction. Such a connection will be acquired from a connection pool
 * and released back there when query result is consumed or transaction is
 * committed or rolled back. One connection can therefore be adopted by many
 * sessions, although by only one at a time. Application code should never need
 * to deal directly with connection management.
 * <p>
 * A session inherits its destination address and permissions from its
 * underlying connection. This means that for a single query/transaction one
 * session may only ever target one machine within a cluster and does not
 * support re-authentication. To achieve otherwise requires creation of a
 * separate session.
 * <p>
 * Similarly, multiple sessions should be used when working with concurrency;
 * session implementations are not thread safe.
 *
 * @since 2.0
 */
public interface AsyncSession extends AsyncStatementRunner
{
    /**
     * Begin a new <em>explicit {@linkplain Transaction transaction}</em>. At
     * most one transaction may exist in a session at any point in time. To
     * maintain multiple concurrent transactions, use multiple concurrent
     * sessions.
     * <p>
     * This operation is asynchronous and returns a {@link CompletionStage}. This stage is completed with a new
     * {@link Transaction} object when begin operation is successful.
     * It is completed exceptionally if transaction can't be started.
     * <p>
     * Returned stage can be completed by an IO thread which should never block.
     * Otherwise IO operations on this and potentially other network connections might deadlock.
     * Please do not chain blocking operations like {@link CompletableFuture#get()} on the returned stage.
     * Consider using asynchronous calls throughout the chain or offloading blocking operation to a different {@link Executor}.
     * This can be done using methods with "Async" suffix like
     * {@link CompletionStage#thenApplyAsync(Function)} or {@link CompletionStage#thenApplyAsync(Function, Executor)}.
     *
     * @return a {@link CompletionStage completion stage} that represents the asynchronous begin of a transaction.
     */
    CompletionStage<AsyncTransaction> beginTransactionAsync();

    /**
     * Begin a new <em>explicit {@linkplain AsyncTransaction transaction}</em> with the specified {@link TransactionConfig configuration}.
     * At most one transaction may exist in a session at any point in time.
     * To maintain multiple concurrent transactions, use multiple concurrent sessions.
     * <p>
     * This operation is asynchronous and returns a {@link CompletionStage}. This stage is completed with a new
     * {@link AsyncTransaction} object when begin operation is successful. It is completed exceptionally if
     * transaction can't be started.
     * <p>
     * Returned stage can be completed by an IO thread which should never block.
     * Otherwise IO operations on this and potentially other network connections might deadlock.
     * Please do not chain blocking operations like {@link CompletableFuture#get()} on the returned stage.
     * Consider using asynchronous calls throughout the chain or offloading blocking operation to a different {@link Executor}.
     * This can be done using methods with "Async" suffix like
     * {@link CompletionStage#thenApplyAsync(Function)} or {@link CompletionStage#thenApplyAsync(Function, Executor)}.
     *
     * @param config configuration for the new transaction.
     * @return a {@link CompletionStage completion stage} that represents the asynchronous begin of a transaction.
     */
    CompletionStage<AsyncTransaction> beginTransactionAsync(TransactionConfig config);

    /**
     * Execute given unit of asynchronous work in a  {@link AccessMode#READ read} asynchronous transaction.
     * <p>
     * Transaction will automatically be committed unless given unit of work fails or
     * {@link AsyncTransaction#commitAsync() async transaction commit} fails.
     * It will also not be committed if explicitly rolled back via {@link AsyncTransaction#rollbackAsync()}.
     * <p>
     * Returned stage and given {@link AsyncTransactionWork} can be completed/executed by an IO thread which should never block.
     * Otherwise IO operations on this and potentially other network connections might deadlock.
     * Please do not chain blocking operations like {@link CompletableFuture#get()} on the returned stage and do not use them inside the
     * {@link AsyncTransactionWork}.
     * Consider using asynchronous calls throughout the chain or offloading blocking operation to a different {@link Executor}.
     * This can be done using methods with "Async" suffix like
     * {@link CompletionStage#thenApplyAsync(Function)} or {@link CompletionStage#thenApplyAsync(Function, Executor)}.
     *
     * @param work the {@link AsyncTransactionWork} to be applied to a new read transaction. Operation executed by the
     * given work must be asynchronous.
     * @param <T> the return type of the given unit of work.
     * @return a {@link CompletionStage completion stage} completed with the same result as returned by the given
     * unit of work. Stage can be completed exceptionally if given work or commit fails.
     */
    <T> CompletionStage<T> readTransactionAsync(AsyncTransactionWork<CompletionStage<T>> work);

    /**
     * Execute given unit of asynchronous work in a  {@link AccessMode#READ read} asynchronous transaction with
     * the specified {@link TransactionConfig configuration}.
     * <p>
     * Transaction will automatically be committed unless given unit of work fails or
     * {@link AsyncTransaction#commitAsync() async transaction commit} fails.
     * It will also not be committed if explicitly rolled back via {@link AsyncTransaction#rollbackAsync()}.
     * <p>
     * Returned stage and given {@link AsyncTransactionWork} can be completed/executed by an IO thread which should never block.
     * Otherwise IO operations on this and potentially other network connections might deadlock.
     * Please do not chain blocking operations like {@link CompletableFuture#get()} on the returned stage and do not use them inside the
     * {@link AsyncTransactionWork}.
     * Consider using asynchronous calls throughout the chain or offloading blocking operation to a different {@link Executor}.
     * This can be done using methods with "Async" suffix like
     * {@link CompletionStage#thenApplyAsync(Function)} or {@link CompletionStage#thenApplyAsync(Function, Executor)}.
     *
     * @param work the {@link  AsyncTransactionWork} to be applied to a new read transaction. Operation executed by the
     * given work must be asynchronous.
     * @param config configuration for all transactions started to execute the unit of work.
     * @param <T> the return type of the given unit of work.
     * @return a {@link CompletionStage completion stage} completed with the same result as returned by the given
     * unit of work. Stage can be completed exceptionally if given work or commit fails.
     */
    <T> CompletionStage<T> readTransactionAsync(AsyncTransactionWork<CompletionStage<T>> work, TransactionConfig config);

    /**
     * Execute given unit of asynchronous work in a  {@link AccessMode#WRITE write} asynchronous transaction.
     * <p>
     * Transaction will automatically be committed unless given unit of work fails or
     * {@link AsyncTransaction#commitAsync() async transaction commit} fails. It will also not be committed if explicitly
     * rolled back via {@link AsyncTransaction#rollbackAsync()}.
     * <p>
     * Returned stage and given {@link  AsyncTransactionWork} can be completed/executed by an IO thread which should never block.
     * Otherwise IO operations on this and potentially other network connections might deadlock.
     * Please do not chain blocking operations like {@link CompletableFuture#get()} on the returned stage and do not use them inside the
     * {@link AsyncTransactionWork}.
     * Consider using asynchronous calls throughout the chain or offloading blocking operation to a different {@link Executor}.
     * This can be done using methods with "Async" suffix like
     * {@link CompletionStage#thenApplyAsync(Function)} or {@link CompletionStage#thenApplyAsync(Function, Executor)}.
     *
     * @param work the {@link AsyncTransactionWork} to be applied to a new write transaction. Operation executed by the
     * given work must be asynchronous.
     * @param <T> the return type of the given unit of work.
     * @return a {@link CompletionStage completion stage} completed with the same result as returned by the given
     * unit of work. Stage can be completed exceptionally if given work or commit fails.
     */
    <T> CompletionStage<T> writeTransactionAsync(AsyncTransactionWork<CompletionStage<T>> work);

    /**
     * Execute given unit of asynchronous work in a  {@link AccessMode#WRITE write} asynchronous transaction with
     * the specified {@link TransactionConfig configuration}.
     * <p>
     * Transaction will automatically be committed unless given unit of work fails or
     * {@link AsyncTransaction#commitAsync() async transaction commit} fails. It will also not be committed if explicitly
     * rolled back via {@link AsyncTransaction#rollbackAsync()}.
     * <p>
     * Returned stage and given {@link AsyncTransactionWork} can be completed/executed by an IO thread which should never block.
     * Otherwise IO operations on this and potentially other network connections might deadlock.
     * Please do not chain blocking operations like {@link CompletableFuture#get()} on the returned stage and do not use them inside the
     * {@link AsyncTransactionWork}.
     * Consider using asynchronous calls throughout the chain or offloading blocking operation to a different {@link Executor}.
     * This can be done using methods with "Async" suffix like
     * {@link CompletionStage#thenApplyAsync(Function)} or {@link CompletionStage#thenApplyAsync(Function, Executor)}.
     *
     * @param work the {@link AsyncTransactionWork} to be applied to a new write transaction. Operation executed by the
     * given work must be asynchronous.
     * @param config configuration for all transactions started to execute the unit of work.
     * @param <T> the return type of the given unit of work.
     * @return a {@link CompletionStage completion stage} completed with the same result as returned by the given
     * unit of work. Stage can be completed exceptionally if given work or commit fails.
     */
    <T> CompletionStage<T> writeTransactionAsync(AsyncTransactionWork<CompletionStage<T>> work, TransactionConfig config);

    /**
     * Run a statement asynchronously in an auto-commit transaction with the specified {@link TransactionConfig configuration} and return a
     * {@link CompletionStage} with a result cursor.
     * <p>
     * It is not allowed to chain blocking operations on the returned {@link CompletionStage}. See class javadoc in {@link AsyncStatementRunner} for
     * more information.
     *
     * @param statement text of a BigConnect statement.
     * @param config configuration for the new transaction.
     * @return new {@link CompletionStage} that gets completed with a result cursor when query execution is successful.
     * Stage can be completed exceptionally when error happens, e.g. connection can't be acquired from the pool.
     */
    CompletionStage<StatementResultCursor> runAsync(String statement, TransactionConfig config);

    /**
     * Run a statement asynchronously in an auto-commit transaction with the specified {@link TransactionConfig configuration} and return a
     * {@link CompletionStage} with a result cursor.
     * <p>
     * This method takes a set of parameters that will be injected into the
     * statement by BigConnect. Using parameters is highly encouraged, it helps avoid
     * dangerous cypher injection attacks and improves database performance as
     * BigConnect can re-use query plans more often.
     * <p>
     * This version of runAsync takes a {@link Map} of parameters. The values in the map
     * must be values that can be converted to BigConnect types. See {@link Values#parameters(Object...)} for
     * a list of allowed types.
     * <h2>Example</h2>
     * <pre>
     * {@code
     * Map<String, Object> metadata = new HashMap<>();
     * metadata.put("type", "update name");
     *
     * TransactionConfig config = TransactionConfig.builder()
     *                 .withTimeout(Duration.ofSeconds(3))
     *                 .withMetadata(metadata)
     *                 .build();
     *
     * Map<String, Object> parameters = new HashMap<String, Object>();
     * parameters.put("myNameParam", "Bob");
     *
     * CompletionStage<StatementResultCursor> cursorStage = session.runAsync(
     *             "MATCH (n) WHERE n.name = {myNameParam} RETURN (n)",
     *             parameters,
     *             config);
     * }
     * </pre>
     * It is not allowed to chain blocking operations on the returned {@link CompletionStage}. See class javadoc in {@link AsyncStatementRunner} for
     * more information.
     *
     * @param statement text of a BigConnect statement.
     * @param parameters input data for the statement.
     * @param config configuration for the new transaction.
     * @return new {@link CompletionStage} that gets completed with a result cursor when query execution is successful.
     * Stage can be completed exceptionally when error happens, e.g. connection can't be acquired from the pool.
     */
    CompletionStage<StatementResultCursor> runAsync(String statement, Map<String, Object> parameters, TransactionConfig config);

    /**
     * Run a statement asynchronously in an auto-commit transaction with the specified {@link TransactionConfig configuration} and return a
     * {@link CompletionStage} with a result cursor.
     * <h2>Example</h2>
     * <pre>
     * {@code
     * Map<String, Object> metadata = new HashMap<>();
     * metadata.put("type", "update name");
     *
     * TransactionConfig config = TransactionConfig.builder()
     *                 .withTimeout(Duration.ofSeconds(3))
     *                 .withMetadata(metadata)
     *                 .build();
     *
     * Statement statement = new Statement( "MATCH (n) WHERE n.name=$myNameParam RETURN n.age" );
     * CompletionStage<StatementResultCursor> cursorStage = session.runAsync(statement, config);
     * }
     * </pre>
     * It is not allowed to chain blocking operations on the returned {@link CompletionStage}. See class javadoc in {@link AsyncStatementRunner} for
     * more information.
     *
     * @param statement a BigConnect statement.
     * @param config configuration for the new transaction.
     * @return new {@link CompletionStage} that gets completed with a result cursor when query execution is successful.
     * Stage can be completed exceptionally when error happens, e.g. connection can't be acquired from the pool.
     */
    CompletionStage<StatementResultCursor> runAsync(Statement statement, TransactionConfig config);

    /**
     * Return the bookmark received following the last completed
     * {@linkplain Transaction transaction}. If no bookmark was received
     * or if this transaction was rolled back, the bookmark value will
     * be null.
     *
     * @return a reference to a previous transaction
     */
    Bookmark lastBookmark();

    /**
     * Signal that you are done using this session. In the default driver usage, closing and accessing sessions is
     * very low cost.
     * <p>
     * This operation is asynchronous and returns a {@link CompletionStage}. Stage is completed when all outstanding
     * statements in the session have completed, meaning any writes you performed are guaranteed to be durably stored.
     * It might be completed exceptionally when there are unconsumed errors from previous statements or transactions.
     *
     * @return a {@link CompletionStage completion stage} that represents the asynchronous close.
     */
    CompletionStage<Void> closeAsync();
}
