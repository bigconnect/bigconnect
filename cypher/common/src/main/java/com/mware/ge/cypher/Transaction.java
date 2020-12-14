/*
 * Copyright (c) 2013-2020 "BigConnect,"
 * MWARE SOLUTIONS SRL
 *
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j.
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
package com.mware.ge.cypher;

import com.mware.ge.cypher.exception.TransactionTerminatedException;
import com.mware.ge.io.ResourceIterable;

public interface Transaction extends AutoCloseable {
    enum Type {
        implicit,
        explicit
    }

    /**
     * Marks this transaction as terminated, which means that it will be, much like in the case of failure,
     * unconditionally rolled back when {@link #close()} is called. Once this method has been invoked, it doesn't matter
     * if {@link #success()} is invoked afterwards -- the transaction will still be rolled back.
     * <p>
     * Additionally, terminating a transaction causes all subsequent operations carried out within that
     * transaction to throw a {@link TransactionTerminatedException} in the owning thread.
     * <p>
     * Note that, unlike the other transaction operations, this method can be called from threads other than
     * the owning thread of the transaction. When this method is called from a different thread,
     * it signals the owning thread to terminate the transaction and returns immediately.
     * <p>
     * Calling this method on an already closed transaction has no effect.
     */
    void terminate();

    /**
     * Marks this transaction as failed, which means that it will
     * unconditionally be rolled back when {@link #close()} is called. Once
     * this method has been invoked, it doesn't matter if
     * {@link #success()} is invoked afterwards -- the transaction will still be
     * rolled back.
     */
    void failure();

    /**
     * Marks this transaction as successful, which means that it will be
     * committed upon invocation of {@link #close()} unless {@link #failure()}
     * has or will be invoked before then.
     */
    void success();

    /**
     * Commits or marks this transaction for rollback, depending on whether
     * {@link #success()} or {@link #failure()} has been previously invoked.
     * <p>
     * All {@link ResourceIterable ResourceIterables} that where returned from operations executed inside this
     * transaction will be automatically closed by this method.
     * <p>
     * This method comes from {@link AutoCloseable} so that a {@link Transaction} can participate
     * in try-with-resource statements. It will not throw any declared exception.
     * <p>
     * Invoking this method (which is unnecessary when in try-with-resource statement).
     */
    @Override
    void close();
}
