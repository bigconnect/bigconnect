/*
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
package com.mware.bolt.runtime;

import com.mware.bolt.v1.bookmarking.Bookmark;
import com.mware.core.exception.BcException;
import com.mware.ge.function.ThrowingConsumer;
import com.mware.ge.values.virtual.MapValue;

import java.time.Duration;
import java.util.Map;

public interface StatementProcessor {
    void beginTransaction(Bookmark bookmark) throws BcException;

    void beginTransaction(Bookmark bookmark, Duration txTimeout, Map<String, Object> txMetadata) throws BcException;

    StatementMetadata run(String statement, MapValue params) throws BcException;

    StatementMetadata run(String statement, MapValue params, Bookmark bookmark, Duration txTimeout, Map<String, Object> txMetaData) throws BcException;

    Bookmark streamResult(ThrowingConsumer<BoltResult, Exception> resultConsumer) throws Exception;

    Bookmark commitTransaction() throws BcException;

    void rollbackTransaction() throws BcException;

    void reset() throws BcException;

    void markCurrentTransactionForTermination();

    boolean hasTransaction();

    boolean hasOpenStatement();

    void validateTransaction() throws BcException;

    StatementProcessor EMPTY = new StatementProcessor() {
        @Override
        public void beginTransaction(Bookmark bookmark) throws BcException {
            throw new UnsupportedOperationException("Unable to run statements");
        }

        @Override
        public void beginTransaction(Bookmark bookmark, Duration txTimeout, Map<String, Object> txMetadata) throws BcException {
            throw new UnsupportedOperationException("Unable to begin a transaction");
        }

        @Override
        public StatementMetadata run(String statement, MapValue params) throws BcException {
            throw new UnsupportedOperationException("Unable to run statements");
        }

        @Override
        public StatementMetadata run(String statement, MapValue params, Bookmark bookmark, Duration txTimeout, Map<String, Object> txMetaData)
                throws BcException {
            throw new UnsupportedOperationException("Unable to run statements");
        }

        @Override
        public Bookmark streamResult(ThrowingConsumer<BoltResult, Exception> resultConsumer) throws Exception {
            throw new UnsupportedOperationException("Unable to stream results");
        }

        @Override
        public Bookmark commitTransaction() throws BcException {
            throw new UnsupportedOperationException("Unable to commit a transaction");
        }

        @Override
        public void rollbackTransaction() throws BcException {
            throw new UnsupportedOperationException("Unable to rollback a transaction");
        }

        @Override
        public void reset() throws BcException {
        }

        @Override
        public void markCurrentTransactionForTermination() {
        }

        @Override
        public boolean hasTransaction() {
            return false;
        }

        @Override
        public boolean hasOpenStatement() {
            return false;
        }

        @Override
        public void validateTransaction() throws BcException {
        }
    };
}
