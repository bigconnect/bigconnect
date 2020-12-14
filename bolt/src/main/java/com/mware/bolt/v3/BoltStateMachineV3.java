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
package com.mware.bolt.v3;


import com.mware.bolt.BoltChannel;
import com.mware.bolt.runtime.BoltStateMachineSPI;
import com.mware.bolt.v1.runtime.BoltStateMachineV1;
import com.mware.bolt.v3.runtime.*;

import java.time.Clock;

public class BoltStateMachineV3 extends BoltStateMachineV1 {
    public BoltStateMachineV3(BoltStateMachineSPI boltSPI, BoltChannel boltChannel, Clock clock) {
        super(boltSPI, boltChannel, clock);
    }

    @Override
    protected States buildStates() {
        ConnectedState connected = new ConnectedState();
        ReadyState ready = new ReadyState();
        StreamingState streaming = new StreamingState();
        TransactionReadyState txReady = new TransactionReadyState();
        TransactionStreamingState txStreaming = new TransactionStreamingState();
        FailedState failed = new FailedState();
        InterruptedState interrupted = new InterruptedState();

        connected.setReadyState(ready);

        ready.setTransactionReadyState(txReady);
        ready.setStreamingState(streaming);
        ready.setFailedState(failed);
        ready.setInterruptedState(interrupted);

        streaming.setReadyState(ready);
        streaming.setFailedState(failed);
        streaming.setInterruptedState(interrupted);

        txReady.setReadyState(ready);
        txReady.setTransactionStreamingState(txStreaming);
        txReady.setFailedState(failed);
        txReady.setInterruptedState(interrupted);

        txStreaming.setReadyState(txReady);
        txStreaming.setFailedState(failed);
        txStreaming.setInterruptedState(interrupted);

        failed.setInterruptedState(interrupted);

        interrupted.setReadyState(ready);

        return new States(connected, failed);
    }

    @Override
    protected void after() {
        if (connectionState.isTerminated()) {
            close();
        } else {
            super.after();
        }
    }
}
