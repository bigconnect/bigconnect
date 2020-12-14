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
package com.mware.bolt.v1.runtime;

import com.mware.bolt.runtime.*;
import com.mware.bolt.security.auth.AuthenticationResult;
import com.mware.bolt.v1.bookmarking.Bookmark;
import com.mware.bolt.v1.runtime.spi.BookmarkResult;
import com.mware.core.exception.BcException;
import com.mware.ge.function.ThrowingConsumer;
import com.mware.ge.values.virtual.MapValue;

import java.time.Clock;
import java.time.Duration;
import java.util.Map;

import static com.mware.ge.util.Preconditions.checkState;

public class TransactionStateMachine implements StatementProcessor {
    final TransactionStateMachineSPI spi;
    final MutableTransactionState ctx;
    State state = State.AUTO_COMMIT;

    TransactionStateMachine(TransactionStateMachineSPI spi, AuthenticationResult authorizations, Clock clock) {
        this.spi = spi;
        ctx = new MutableTransactionState(authorizations, clock);
    }

    public State state() {
        return state;
    }

    @Override
    public void beginTransaction(Bookmark bookmark) throws BcException {
        beginTransaction(bookmark, null, null);
    }

    @Override
    public void beginTransaction(Bookmark bookmark, Duration txTimeout, Map<String, Object> txMetadata) throws BcException {
        state = state.beginTransaction(ctx, spi, bookmark, txTimeout, txMetadata);
    }

    @Override
    public StatementMetadata run(String statement, MapValue params) throws BcException {
        return run(statement, params, null, null, null);
    }

    @Override
    public StatementMetadata run(String statement, MapValue params, Bookmark bookmark, Duration txTimeout, Map<String, Object> txMetaData)
            throws BcException {
        state = state.run(ctx, spi, statement, params, bookmark, txTimeout, txMetaData);

        return ctx.currentStatementMetadata;
    }

    @Override
    public Bookmark streamResult(ThrowingConsumer<BoltResult, Exception> resultConsumer) throws Exception {
        return state.streamResult(ctx, spi, resultConsumer);
    }

    @Override
    public Bookmark commitTransaction() throws BcException {
        try {
            state = state.commitTransaction(ctx, spi);
            return newestBookmark(spi);
        } catch (BcException ex) {
            state = State.AUTO_COMMIT;
            throw ex;
        }
    }

    @Override
    public void rollbackTransaction() throws BcException {
        state = state.rollbackTransaction(ctx, spi);
    }

    @Override
    public boolean hasOpenStatement() {
        return ctx.currentResultHandle != null;
    }

    /**
     * Rollback and close transaction. Move back to {@link State#AUTO_COMMIT}.
     * <p>
     * <b>Warning:</b>This method should only be called by the bolt worker thread during it's regular message
     * processing. It is wrong to call it from a different thread because kernel transactions are not thread-safe.
     *
     * @throws BcException when transaction fails to close.
     */
    @Override
    public void reset() throws BcException {
        state.terminateQueryAndRollbackTransaction(ctx);
        state = State.AUTO_COMMIT;
    }

    @Override
    public void markCurrentTransactionForTermination() {
        // can we do something here ?
    }

    @Override
    public void validateTransaction() throws BcException {
    }

    @Override
    public boolean hasTransaction() {
        return state == State.EXPLICIT_TRANSACTION;
    }

    enum State {
        AUTO_COMMIT {
            @Override
            State beginTransaction(MutableTransactionState ctx, TransactionStateMachineSPI spi, Bookmark bookmark, Duration txTimeout,
                                   Map<String, Object> txMetadata) throws BcException {
                ctx.currentResult = BoltResult.EMPTY;
                return EXPLICIT_TRANSACTION;
            }

            @Override
            State run(MutableTransactionState ctx, TransactionStateMachineSPI spi, String statement, MapValue params, Bookmark bookmark,
                      Duration txTimeout, Map<String, Object> txMetadata)
                    throws BcException {
                statement = parseStatement(ctx, statement);
                execute(ctx, spi, statement, params, txTimeout, txMetadata);
                return AUTO_COMMIT;
            }

            private String parseStatement(MutableTransactionState ctx, String statement) {
                if (statement.isEmpty()) {
                    statement = ctx.lastStatement;
                } else {
                    ctx.lastStatement = statement;
                }
                return statement;
            }

            void execute(MutableTransactionState ctx, TransactionStateMachineSPI spi, String statement, MapValue params,
                         Duration txTimeout, Map<String, Object> txMetadata)
                    throws BcException {

                BoltResultHandle resultHandle = spi.executeQuery(ctx.authenticationResult, statement, params, txTimeout, txMetadata);
                startExecution(ctx, resultHandle);
            }

            @Override
            Bookmark streamResult(MutableTransactionState ctx, TransactionStateMachineSPI spi, ThrowingConsumer<BoltResult, Exception> resultConsumer)
                    throws Exception {
                assert ctx.currentResult != null;

                consumeResult(ctx, resultConsumer);
                return newestBookmark(spi);
            }

            @Override
            State commitTransaction(MutableTransactionState ctx, TransactionStateMachineSPI spi) throws BcException {
                throw new BcException("No current transaction to commit.");
            }

            @Override
            State rollbackTransaction(MutableTransactionState ctx, TransactionStateMachineSPI spi) {
                ctx.currentResult = BoltResult.EMPTY;
                return AUTO_COMMIT;
            }
        },
        EXPLICIT_TRANSACTION {
            @Override
            State beginTransaction(MutableTransactionState ctx, TransactionStateMachineSPI spi, Bookmark bookmark, Duration txTimeout,
                                   Map<String, Object> txMetadata) throws BcException {
                throw new BcException("Nested transactions are not supported.");
            }

            @Override
            State run(MutableTransactionState ctx, TransactionStateMachineSPI spi, String statement, MapValue params, Bookmark bookmark,
                      Duration ignored1, Map<String, Object> ignored2)
                    throws BcException {
                checkState(ignored1 == null, "Explicit Transaction should not run with tx_timeout");
                checkState(ignored2 == null, "Explicit Transaction should not run with tx_metadata");

                if (statement.isEmpty()) {
                    statement = ctx.lastStatement;
                } else {
                    ctx.lastStatement = statement;
                }
                BoltResultHandle resultHandle = spi.executeQuery(ctx.authenticationResult, statement, params, null, null /*ignored in explict tx run*/);
                startExecution(ctx, resultHandle);
                return EXPLICIT_TRANSACTION;
            }

            @Override
            Bookmark streamResult(MutableTransactionState ctx, TransactionStateMachineSPI spi, ThrowingConsumer<BoltResult, Exception> resultConsumer)
                    throws Exception {
                assert ctx.currentResult != null;
                consumeResult(ctx, resultConsumer);
                return null; // Explict tx shall not get a bookmark in PULL_ALL or DISCARD_ALL
            }

            @Override
            State commitTransaction(MutableTransactionState ctx, TransactionStateMachineSPI spi) throws BcException {
                Bookmark bookmark = newestBookmark(spi);
                ctx.currentResult = new BookmarkResult(bookmark);
                return AUTO_COMMIT;
            }

            @Override
            State rollbackTransaction(MutableTransactionState ctx, TransactionStateMachineSPI spi) throws BcException {
                ctx.currentResult = BoltResult.EMPTY;
                return AUTO_COMMIT;
            }
        };

        abstract State beginTransaction(MutableTransactionState ctx, TransactionStateMachineSPI spi, Bookmark bookmark, Duration txTimeout,
                                        Map<String, Object> txMetadata) throws BcException;

        abstract State run(MutableTransactionState ctx, TransactionStateMachineSPI spi, String statement, MapValue params, Bookmark bookmark,
                           Duration txTimeout, Map<String, Object> txMetadata)
                throws BcException;

        abstract Bookmark streamResult(MutableTransactionState ctx, TransactionStateMachineSPI spi, ThrowingConsumer<BoltResult, Exception> resultConsumer)
                throws Exception;

        abstract State commitTransaction(MutableTransactionState ctx, TransactionStateMachineSPI spi) throws BcException;

        abstract State rollbackTransaction(MutableTransactionState ctx, TransactionStateMachineSPI spi) throws BcException;

        void terminateQueryAndRollbackTransaction(MutableTransactionState ctx) throws BcException {
            if (ctx.currentResultHandle != null) {
                ctx.currentResultHandle.terminate();
                ctx.currentResultHandle = null;
            }
            if (ctx.currentResult != null) {
                ctx.currentResult.close();
                ctx.currentResult = null;
            }
        }

        boolean consumeResult(MutableTransactionState ctx, ThrowingConsumer<BoltResult, Exception> resultConsumer) throws Exception {
            boolean success = false;
            try {
                resultConsumer.accept(ctx.currentResult);
                success = true;
            } finally {
                ctx.currentResult.close();
                ctx.currentResult = null;

                if (ctx.currentResultHandle != null) {
                    ctx.currentResultHandle.close(success);
                    ctx.currentResultHandle = null;
                }
            }
            return success;
        }

        void startExecution(MutableTransactionState ctx, BoltResultHandle resultHandle) throws BcException {
            ctx.currentResultHandle = resultHandle;
            try {
                ctx.currentResult = resultHandle.start();
            } catch (Throwable t) {
                ctx.currentResultHandle.close(false);
                ctx.currentResultHandle = null;
                throw t;
            }
        }

    }

    private static Bookmark newestBookmark(TransactionStateMachineSPI spi) {
        long txId = spi.newestEncounteredTxId();
        return new Bookmark(txId);
    }

    static class MutableTransactionState {
        /**
         * The current session security context to be used for starting transactions
         */
        final AuthenticationResult authenticationResult;

        /**
         * Last Cypher statement executed
         */
        String lastStatement = "";

        /**
         * The current pending result, if present
         */
        BoltResult currentResult;

        BoltResultHandle currentResultHandle;

        final Clock clock;

        /**
         * A re-usable statement metadata instance that always represents the currently running statement
         */
        private final StatementMetadata currentStatementMetadata = new StatementMetadata() {
            @Override
            public String[] fieldNames() {
                return currentResult.fieldNames();
            }
        };

        private MutableTransactionState(AuthenticationResult authenticationResult, Clock clock) {
            this.clock = clock;
            this.authenticationResult = authenticationResult;
        }
    }
}
