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

/**
 * Callback that executes operations against a given {@link RxTransaction}.
 * To be used with {@link RxSession#readTransaction(RxTransactionWork)} and
 * {@link RxSession#writeTransaction(RxTransactionWork)} methods.
 *
 * @param <T> the return type of this work.
 * @since 2.0
 */
public interface RxTransactionWork<T>
{
    /**
     * Executes all given operations against the same transaction.
     *
     * @param tx the transaction to use.
     * @return some result object or {@code null} if none.
     */
    T execute(RxTransaction tx);
}
