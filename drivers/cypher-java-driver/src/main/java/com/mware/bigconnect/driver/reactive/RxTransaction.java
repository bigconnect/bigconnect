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

import com.mware.bigconnect.driver.Transaction;
import org.reactivestreams.Publisher;

/**
 * Same as {@link Transaction} except this reactive transaction exposes a reactive API.
 * @see Transaction
 * @see RxSession
 * @see Publisher
 * @since 2.0
 */
public interface RxTransaction extends RxStatementRunner
{
    /**
     * Commits the transaction.
     * It completes without publishing anything if transaction is committed successfully.
     * Otherwise, errors when there is any error to commit.
     * @param <T> makes it easier to be chained after other publishers.
     * @return an empty publisher.
     */
    <T> Publisher<T> commit();

    /**
     * Rolls back the transaction.
     * It completes without publishing anything if transaction is rolled back successfully.
     * Otherwise, errors when there is any error to roll back.
     * @param <T> makes it easier to be chained after other publishers.
     * @return an empty publisher.
     */
    <T> Publisher<T> rollback();
}
