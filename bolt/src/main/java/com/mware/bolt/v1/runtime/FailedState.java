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

import com.mware.bolt.messaging.RequestMessage;
import com.mware.bolt.runtime.BoltConnectionFatality;
import com.mware.bolt.runtime.BoltStateMachineState;
import com.mware.bolt.runtime.StateMachineContext;
import com.mware.bolt.v1.messaging.request.*;

import static com.mware.ge.util.Preconditions.checkState;

/**
 * The FAILED state occurs when a recoverable error is encountered.
 * This might be something like a Cypher SyntaxError or
 * ConstraintViolation. To exit the FAILED state, either a RESET
 * or and ACK_FAILURE must be issued. All stream will be IGNORED
 * until this is done.
 */
public class FailedState implements BoltStateMachineState {
    private BoltStateMachineState readyState;
    private BoltStateMachineState interruptedState;

    @Override
    public BoltStateMachineState process(RequestMessage message, StateMachineContext context) throws BoltConnectionFatality {
        assertInitialized();
        if (shouldIgnore(message)) {
            context.connectionState().markIgnored();
            return this;
        }
        if (message instanceof AckFailureMessage) {
            context.connectionState().resetPendingFailedAndIgnored();
            return readyState;
        }
        if (message instanceof ResetMessage) {
            return processResetMessage(context);
        }
        if (message instanceof InterruptSignal) {
            return interruptedState;
        }
        return null;
    }

    @Override
    public String name() {
        return "FAILED";
    }

    public void setReadyState(BoltStateMachineState readyState) {
        this.readyState = readyState;
    }

    public void setInterruptedState(BoltStateMachineState interruptedState) {
        this.interruptedState = interruptedState;
    }

    private BoltStateMachineState processResetMessage(StateMachineContext context) throws BoltConnectionFatality {
        boolean success = context.resetMachine();
        if (success) {
            context.connectionState().resetPendingFailedAndIgnored();
            return readyState;
        }
        return this;
    }

    private void assertInitialized() {
        checkState(readyState != null, "Ready state not set");
        checkState(interruptedState != null, "Interrupted state not set");
    }

    private static boolean shouldIgnore(RequestMessage message) {
        return message instanceof RunMessage ||
                message instanceof PullAllMessage ||
                message instanceof DiscardAllMessage;
    }
}
