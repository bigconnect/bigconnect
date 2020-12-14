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
package com.mware.bigconnect.driver;

import com.mware.bigconnect.driver.util.Resource;

/**
 * Logical container for an atomic unit of work.
 * A driver Transaction object corresponds to a server transaction.
 * <p>
 * Transactions are typically wrapped in a try-with-resources block
 * which ensures in case of any error in try block, the transaction is
 * automatically rolled back on close. Note that <code>ROLLBACK</code> is the
 * default action unless {@link #commit()} is called before closing.
 * {@code
 * try(Transaction tx = session.beginTransaction())
 * {
 *     tx.run("CREATE (a:Person {name: {x}})", parameters("x", "Alice"));
 *     tx.commit();
 * }
 * }
 * Blocking calls are: {@link #commit()}, {@link #rollback()}, {@link #close()}
 * and various overloads of {@link #run(Statement)}.
 *
 * @see Session#run
 * @see StatementRunner
 * @since 1.0
 */
public interface Transaction extends Resource, StatementRunner
{
    /**
     * Commit this current transaction.
     * When this method returns, all outstanding statements in the transaction are guaranteed to
     * have completed, meaning any writes you performed are guaranteed to be durably stored.
     * No more statement can be executed inside this transaction once this transaction is committed.
     * After this method is called, the transaction cannot be committed or rolled back again.
     * You must call this method before calling {@link #close()} to have your transaction committed.
     * If a transaction is not committed or rolled back before close,
     * the transaction will be rolled back by default in {@link #close}.
     *
     Example:
     *
     * {@code
     * try(Transaction tx = session.beginTransaction() )
     * {
     *     tx.run("CREATE (a:Person {name: {x}})", parameters("x", "Alice"));
     *     tx.commit();
     * }
     * }
     */
    void commit();

    /**
     * Roll back this current transaction.
     * No more statement can be executed inside this transaction once this transaction is committed.
     * After this method has been called, the transaction cannot be committed or rolled back again.
     * If a transaction is not committed or rolled back before close,
     * the transaction will be rolled back by default in {@link #close}.
     *
     * Example:
     *
     * {@code
     * try(Transaction tx = session.beginTransaction() )
     * {
     *     tx.run("CREATE (a:Person {name: {x}})", parameters("x", "Alice"));
     *     tx.rollback();
     * }
     * }
     */
    void rollback();

    /**
     * Close the transaction.
     * If the transaction has been {@link #commit() committed} or {@link #rollback() rolled back},
     * the close is optional and no operation is performed inside.
     * Otherwise, the transaction will be rolled back by default by this method.
     *
     * Example:
     *
     * {@code
     * try(Transaction tx = session.beginTransaction() )
     * {
     *     tx.run("CREATE (a:Person {name: {x}})", parameters("x", "Alice"));
     * }
     * }
     */
    @Override
    void close();
}
