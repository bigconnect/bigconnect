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

import static com.mware.ge.util.Preconditions.checkState;

public abstract class FailSafeBoltStateMachineState implements BoltStateMachineState {
    private BoltStateMachineState failedState;
    private BoltStateMachineState interruptedState;

    @Override
    public BoltStateMachineState process(RequestMessage message, StateMachineContext context) throws BoltConnectionFatality {
        assertInitialized();

        if (message instanceof InterruptSignal) {
            return interruptedState;
        }

        try {
            return processUnsafe(message, context);
        } catch (Throwable t) {
            context.handleFailure(t, false);
            return failedState;
        }
    }

    public void setFailedState(BoltStateMachineState failedState) {
        this.failedState = failedState;
    }

    public void setInterruptedState(BoltStateMachineState interruptedState) {
        this.interruptedState = interruptedState;
    }

    protected void assertInitialized() {
        checkState(failedState != null, "Failed state not set");
        checkState(interruptedState != null, "Interrupted state not set");
    }

    protected abstract BoltStateMachineState processUnsafe(RequestMessage message, StateMachineContext context) throws Throwable;
}
