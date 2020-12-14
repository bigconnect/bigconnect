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
package com.mware.bolt.v3.runtime;

import com.mware.bolt.messaging.RequestMessage;
import com.mware.bolt.runtime.BoltConnectionFatality;
import com.mware.bolt.runtime.BoltStateMachineState;
import com.mware.bolt.runtime.StateMachineContext;
import com.mware.bolt.v1.messaging.request.DiscardAllMessage;
import com.mware.bolt.v1.messaging.request.InterruptSignal;
import com.mware.bolt.v1.messaging.request.PullAllMessage;
import com.mware.bolt.v3.messaging.request.CommitMessage;
import com.mware.bolt.v3.messaging.request.RollbackMessage;
import com.mware.bolt.v3.messaging.request.RunMessage;

import static com.mware.ge.util.Preconditions.checkState;

/**
 * The FAILED state occurs when a recoverable error is encountered.
 * This might be something like a Cypher SyntaxError or
 * ConstraintViolation. To exit the FAILED state, either a RESET
 * or and ACK_FAILURE must be issued. All stream will be IGNORED
 * until this is done.
 */
public class FailedState implements BoltStateMachineState {
    private BoltStateMachineState interruptedState;

    @Override
    public BoltStateMachineState process(RequestMessage message, StateMachineContext context) throws BoltConnectionFatality {
        assertInitialized();
        if (shouldIgnore(message)) {
            context.connectionState().markIgnored();
            return this;
        }
        if (message instanceof InterruptSignal) {
            return interruptedState;
        }
        return null;
    }

    public void setInterruptedState(BoltStateMachineState interruptedState) {
        this.interruptedState = interruptedState;
    }

    protected void assertInitialized() {
        checkState(interruptedState != null, "Interrupted state not set");
    }

    @Override
    public String name() {
        return "FAILED";
    }

    private static boolean shouldIgnore(RequestMessage message) {
        return message instanceof RunMessage || message instanceof PullAllMessage || message instanceof DiscardAllMessage
                || message instanceof CommitMessage || message instanceof RollbackMessage;
    }
}
