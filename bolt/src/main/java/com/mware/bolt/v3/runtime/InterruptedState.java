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
import com.mware.bolt.v1.messaging.request.InterruptSignal;
import com.mware.bolt.v1.messaging.request.ResetMessage;

import static com.mware.ge.util.Preconditions.checkState;

/**
 * If the state machine has been INTERRUPTED then a RESET message
 * has entered the queue and is waiting to be processed. The initial
 * interrupt forces the current statement to stop and all subsequent
 * requests to be IGNORED until the RESET itself is processed.
 */
public class InterruptedState implements BoltStateMachineState {
    private BoltStateMachineState readyState;

    @Override
    public BoltStateMachineState process(RequestMessage message, StateMachineContext context) throws BoltConnectionFatality {
        assertInitialized();
        if (message instanceof InterruptSignal) {
            return this;
        }
        if (message instanceof ResetMessage) {
            if (context.connectionState().decrementInterruptCounter() > 0) {
                context.connectionState().markIgnored();
                return this;
            }

            if (context.resetMachine()) {
                context.connectionState().resetPendingFailedAndIgnored();
                return readyState;
            }
            return null;
        } else {
            context.connectionState().markIgnored();
            return this;
        }
    }

    @Override
    public String name() {
        return "INTERRUPTED";
    }

    public void setReadyState(BoltStateMachineState readyState) {
        this.readyState = readyState;
    }

    private void assertInitialized() {
        checkState(readyState != null, "Ready state not set");
    }
}
