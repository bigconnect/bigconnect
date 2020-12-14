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

import com.mware.bigconnect.driver.Session;
import com.mware.bigconnect.driver.Statement;
import com.mware.bigconnect.driver.StatementRunner;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.Executor;
import java.util.function.Function;

/**
 * Logical container for an atomic unit of work.
 * A driver Transaction object corresponds to a server transaction.
 * <p>
 * Transactions are typically obtained in a {@link CompletionStage} and all
 * operations chain on this stage. Explicit commit with {@link #commitAsync()}
 * or rollback with {@link #rollbackAsync()} is required. Without explicit
 * commit/rollback corresponding transaction will remain open in the database.
 * <pre>
 * {@code
 * session.beginTransactionAsync()
 *        .thenCompose(tx ->
 *               tx.runAsync("CREATE (a:Person {name: {x}})", parameters("x", "Alice"))
 *                 .exceptionally(e -> {
 *                    e.printStackTrace();
 *                    return null;
 *                 })
 *                 .thenApply(ignore -> tx)
 *        ).thenCompose(Transaction::commitAsync);
 * }
 * </pre>
 * Async calls are: {@link #commitAsync()}, {@link #rollbackAsync()} and various overloads of
 * {@link #runAsync(Statement)}.
 *
 * @see Session#run
 * @see StatementRunner
 * @since 2.0
 */
public interface AsyncTransaction extends AsyncStatementRunner
{
    /**
     * Commit this transaction in asynchronous fashion. This operation is typically executed as part of the
     * {@link CompletionStage} chain that starts with a transaction.
     * There is no need to close transaction after calling this method.
     * Transaction object should not be used after calling this method.
     * <p>
     * Returned stage can be completed by an IO thread which should never block. Otherwise IO operations on this and
     * potentially other network connections might deadlock. Please do not chain blocking operations like
     * {@link CompletableFuture#get()} on the returned stage. Consider using asynchronous calls throughout the chain or offloading blocking
     * operation to a different {@link Executor}. This can be done using methods with "Async" suffix like
     * {@link CompletionStage#thenApplyAsync(Function)} or {@link CompletionStage#thenApplyAsync(Function, Executor)}.
     *
     * @return new {@link CompletionStage} that gets completed with {@code null} when commit is successful. Stage can
     * be completed exceptionally when commit fails.
     */
    CompletionStage<Void> commitAsync();

    /**
     * Rollback this transaction in asynchronous fashion. This operation is typically executed as part of the
     * {@link CompletionStage} chain that starts with a transaction.
     * There is no need to close transaction after calling this method.
     * Transaction object should not be used after calling this method.
     * <p>
     * Returned stage can be completed by an IO thread which should never block. Otherwise IO operations on this and
     * potentially other network connections might deadlock. Please do not chain blocking operations like
     * {@link CompletableFuture#get()} on the returned stage. Consider using asynchronous calls throughout the chain or offloading blocking
     * operation to a different {@link Executor}. This can be done using methods with "Async" suffix like
     * {@link CompletionStage#thenApplyAsync(Function)} or {@link CompletionStage#thenApplyAsync(Function, Executor)}.
     *
     * @return new {@link CompletionStage} that gets completed with {@code null} when rollback is successful. Stage can
     * be completed exceptionally when rollback fails.
     */
    CompletionStage<Void> rollbackAsync();
}
