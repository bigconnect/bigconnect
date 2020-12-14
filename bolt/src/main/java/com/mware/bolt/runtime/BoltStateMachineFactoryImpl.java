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

import com.mware.bolt.BoltChannel;
import com.mware.bolt.v1.BoltProtocolV1;
import com.mware.bolt.v1.runtime.BoltStateMachineV1;
import com.mware.bolt.v1.runtime.BoltStateMachineV1SPI;
import com.mware.bolt.v1.runtime.TransactionStateMachineV1SPI;
import com.mware.bolt.v2.BoltProtocolV2;
import com.mware.bolt.v3.BoltProtocolV3;
import com.mware.bolt.v3.BoltStateMachineV3;
import com.mware.bolt.v3.runtime.TransactionStateMachineV3SPI;
import com.mware.ge.cypher.GeCypherExecutionEngine;

import java.time.Clock;
import java.time.Duration;

public class BoltStateMachineFactoryImpl implements BoltStateMachineFactory {
    private final GeCypherExecutionEngine executionEngine;
    private final Clock clock = Clock.systemUTC();

    public BoltStateMachineFactoryImpl(GeCypherExecutionEngine executionEngine) {
        this.executionEngine = executionEngine;
    }

    @Override
    public BoltStateMachine newStateMachine(long protocolVersion, BoltChannel boltChannel) {
        if (protocolVersion == BoltProtocolV1.VERSION || protocolVersion == BoltProtocolV2.VERSION) {
            return newStateMachineV1(boltChannel);
        } else if (protocolVersion == BoltProtocolV3.VERSION) {
            return newStateMachineV3(boltChannel);
        } else {
            throw new IllegalArgumentException("Failed to create a state machine for protocol version " + protocolVersion);
        }
    }

    private BoltStateMachine newStateMachineV1(BoltChannel boltChannel) {
        TransactionStateMachineSPI transactionSPI = new TransactionStateMachineV1SPI( executionEngine, boltChannel, getAwaitDuration(), clock );
        BoltStateMachineSPI boltSPI = new BoltStateMachineV1SPI(executionEngine, transactionSPI);
        return new BoltStateMachineV1(boltSPI, boltChannel, clock);
    }

    private BoltStateMachine newStateMachineV3(BoltChannel boltChannel) {
        TransactionStateMachineSPI transactionSPI = new TransactionStateMachineV3SPI( executionEngine, boltChannel, getAwaitDuration(), clock );
        BoltStateMachineSPI boltSPI = new BoltStateMachineV1SPI(executionEngine, transactionSPI);
        return new BoltStateMachineV3(boltSPI, boltChannel, clock);
    }

    private Duration getAwaitDuration() {
        long bookmarkReadyTimeout = Duration.ofSeconds(30).toMillis();
        return Duration.ofMillis(bookmarkReadyTimeout);
    }
}
