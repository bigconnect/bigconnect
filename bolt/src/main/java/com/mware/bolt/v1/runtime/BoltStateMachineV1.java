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

import com.mware.bolt.BoltChannel;
import com.mware.bolt.messaging.RequestMessage;
import com.mware.bolt.runtime.*;
import com.mware.bolt.security.auth.AuthenticationException;
import com.mware.bolt.v1.messaging.BoltStateMachineV1Context;
import com.mware.bolt.v1.messaging.request.InterruptSignal;
import com.mware.core.exception.BcException;
import org.apache.commons.lang3.exception.ExceptionUtils;

import java.time.Clock;

/**
 * This state machine oversees the exchange of messages for the Bolt protocol.
 * Central to this are the five active states -- CONNECTED, READY, STREAMING,
 * FAILED and INTERRUPTED -- as well as the transitions between them which
 * correspond to the Bolt protocol request messages INIT, ACK_FAILURE, RESET,
 * RUN, DISCARD_ALL and PULL_ALL. Of particular note is RESET which exhibits
 * dual behaviour in both marking the current query for termination and clearing
 * down the current connection state.
 * <p>
 * To help ensure a secure protocol, any transition not explicitly defined here
 * (i.e. a message sent out of sequence) will result in an immediate failure
 * response and a closed connection.
 */
public class BoltStateMachineV1 implements BoltStateMachine {
    private final String id;
    private final BoltChannel boltChannel;
    private final BoltStateMachineSPI spi;
    protected final MutableConnectionState connectionState;
    private final StateMachineContext context;

    private BoltStateMachineState state;
    private final BoltStateMachineState failedState;

    public BoltStateMachineV1(BoltStateMachineSPI spi, BoltChannel boltChannel, Clock clock) {
        this.id = boltChannel.id();
        this.boltChannel = boltChannel;
        this.spi = spi;
        this.connectionState = new MutableConnectionState();
        this.context = new BoltStateMachineV1Context(this, boltChannel, spi, connectionState, clock);

        States states = buildStates();
        this.state = states.initial;
        this.failedState = states.failed;
    }

    @Override
    public void process(RequestMessage message, BoltResponseHandler handler) throws BoltConnectionFatality {
        before(handler);
        try {
            if (message.safeToProcessInAnyState() || connectionState.canProcessMessage()) {
                nextState(message, context);
            }
        } finally {
            after();
        }
    }

    private void before(BoltResponseHandler handler) throws BoltConnectionFatality {
        if (connectionState.isTerminated()) {
            close();
        } else if (connectionState.isInterrupted()) {
            nextState(InterruptSignal.INSTANCE, context);
        }
        connectionState.setResponseHandler(handler);
    }

    protected void after() {
        if (connectionState.getResponseHandler() != null) {
            try {
                BigConnectError pendingError = connectionState.getPendingError();
                if (pendingError != null) {
                    connectionState.markFailed(pendingError);
                }

                if (connectionState.hasPendingIgnore()) {
                    connectionState.markIgnored();
                }

                connectionState.resetPendingFailedAndIgnored();
                connectionState.getResponseHandler().onFinish();
            } finally {
                connectionState.setResponseHandler(null);
            }
        }
    }

    private void nextState(RequestMessage message, StateMachineContext context) throws BoltConnectionFatality {
        BoltStateMachineState preState = state;
        state = state.process(message, context);
        if (state == null) {
            String msg = "Message '" + message + "' cannot be handled by a session in the " + preState.name() + " state.";
            fail(BigConnectError.fatalFrom(Status.Request.Invalid, msg));
            throw new BoltProtocolBreachFatality(msg);
        }
    }

    @Override
    public void markFailed(BigConnectError error) {
        fail(error);
        state = failedState;
    }

    /**
     * When this is invoked, the machine will make attempts
     * at interrupting any currently running action,
     * and will then ignore all inbound messages until a {@code RESET}
     * message is received. If this is called multiple times, an equivalent number
     * of reset messages must be received before the SSM goes back to a good state.
     * <p>
     * You can imagine this is as a "call ahead" mechanism used by RESET to
     * cancel any statements ahead of it in line, without compromising the single-
     * threaded processing of messages that the state machine does.
     * <p>
     * This can be used to cancel a long-running statement or transaction.
     */
    @Override
    public void interrupt() {
        connectionState.incrementInterruptCounter();
        statementProcessor().markCurrentTransactionForTermination();
    }

    /**
     * When this is invoked, the machine will check whether the related transaction is
     * marked for termination and will reset the TransactionStateMachine to AUTO_COMMIT mode
     * while releasing the related transactional resources.
     */
    @Override
    public void validateTransaction() throws BcException {
        statementProcessor().validateTransaction();
    }

    @Override
    public void handleExternalFailure(BigConnectError error, BoltResponseHandler handler) throws BoltConnectionFatality {
        before(handler);
        try {
            if (connectionState.canProcessMessage()) {
                fail(error);
                state = failedState;
            }
        } finally {
            after();
        }
    }

    @Override
    public boolean isClosed() {
        return connectionState.isClosed();
    }

    @Override
    public void close() {
        try {
            boltChannel.close();
        } finally {
            connectionState.markClosed();
            // However a new transaction may have been created so we must always to reset
            resetStatementProcessor();
        }
    }

    @Override
    public String id() {
        return id;
    }

    @Override
    public void markForTermination() {
        /*
         * This is a side-channel call and we should not close anything directly.
         * Just mark the transaction and set isTerminated to true and then the session
         * thread will close down the connection eventually.
         */
        connectionState.markTerminated();
        statementProcessor().markCurrentTransactionForTermination();
    }

    @Override
    public boolean shouldStickOnThread() {
        // Currently, we're doing our best to keep things together
        // We should not switch threads when there's an active statement (executing/streaming)
        // Also, we're currently sticking to the thread when there's an open transaction due to
        // cursor errors we receive when a transaction is picked up by another thread linearly.
        return statementProcessor().hasTransaction() || statementProcessor().hasOpenStatement();
    }

    @Override
    public boolean hasOpenStatement() {
        return statementProcessor().hasOpenStatement();
    }

    @Override
    public boolean reset() throws BoltConnectionFatality {
        try {
            resetStatementProcessor();
            return true;
        } catch (Throwable t) {
            handleFailure(t, true);
            return false;
        }
    }

    @Override
    public void handleFailure(Throwable cause, boolean fatal) throws BoltConnectionFatality {
        if (ExceptionUtils.indexOfType(cause, BoltConnectionFatality.class) != -1) {
            fatal = true;
        }

        BigConnectError error = fatal ? BigConnectError.fatalFrom(cause) : BigConnectError.from(cause);
        fail(error);

        if (error.isFatal()) {
            if (cause instanceof AuthenticationException) {
                throw new BoltConnectionAuthFatality((AuthenticationException) cause);
            }

            throw new BoltConnectionFatality("Failed to process a bolt message", cause);
        }
    }

    public BoltStateMachineState state() {
        return state;
    }

    public StatementProcessor statementProcessor() {
        return connectionState.getStatementProcessor();
    }

    public MutableConnectionState connectionState() {
        return connectionState;
    }

    private void fail(BigConnectError bigConnectError) {
        spi.reportError(bigConnectError);
        if (state == failedState) {
            connectionState.markIgnored();
        } else {
            connectionState.markFailed(bigConnectError);
        }
    }

    private void resetStatementProcessor() {
        try {
            statementProcessor().reset();
        } catch (BcException e) {
            throw new RuntimeException(e);
        }
    }

    protected States buildStates() {
        ConnectedState connected = new ConnectedState();
        ReadyState ready = new ReadyState();
        StreamingState streaming = new StreamingState();
        FailedState failed = new FailedState();
        InterruptedState interrupted = new InterruptedState();

        connected.setReadyState(ready);
        connected.setFailedState(failed);

        ready.setStreamingState(streaming);
        ready.setInterruptedState(interrupted);
        ready.setFailedState(failed);

        streaming.setReadyState(ready);
        streaming.setInterruptedState(interrupted);
        streaming.setFailedState(failed);

        failed.setReadyState(ready);
        failed.setInterruptedState(interrupted);

        interrupted.setReadyState(ready);
        interrupted.setFailedState(failed);

        return new States(connected, failed);
    }

    public static class States {
        final BoltStateMachineState initial;
        final BoltStateMachineState failed;

        public States(BoltStateMachineState initial, BoltStateMachineState failed) {
            this.initial = initial;
            this.failed = failed;
        }
    }
}
