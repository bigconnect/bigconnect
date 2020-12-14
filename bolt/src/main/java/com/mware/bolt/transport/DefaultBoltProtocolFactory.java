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
package com.mware.bolt.transport;

import com.mware.bolt.BoltChannel;
import com.mware.bolt.runtime.BoltConnectionFactory;
import com.mware.bolt.runtime.BoltStateMachineFactory;
import com.mware.bolt.v1.BoltProtocolV1;
import com.mware.bolt.v2.BoltProtocolV2;
import com.mware.bolt.v3.BoltProtocolV3;

public class DefaultBoltProtocolFactory implements BoltProtocolFactory {
    private final BoltConnectionFactory connectionFactory;
    private final BoltStateMachineFactory stateMachineFactory;

    public DefaultBoltProtocolFactory(BoltConnectionFactory connectionFactory, BoltStateMachineFactory stateMachineFactory) {
        this.connectionFactory = connectionFactory;
        this.stateMachineFactory = stateMachineFactory;
    }

    @Override
    public BoltProtocol create(long protocolVersion, BoltChannel channel) {
        if (protocolVersion == BoltProtocolV1.VERSION) {
            return new BoltProtocolV1(channel, connectionFactory, stateMachineFactory);
        } else if (protocolVersion == BoltProtocolV2.VERSION) {
            return new BoltProtocolV2(channel, connectionFactory, stateMachineFactory);
        } else if (protocolVersion == BoltProtocolV3.VERSION) {
            return new BoltProtocolV3(channel, connectionFactory, stateMachineFactory);
        } else {
            return null;
        }
    }
}
