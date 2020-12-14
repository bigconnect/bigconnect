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
import com.mware.bolt.transport.TransportThrottleGroup;
import com.mware.bolt.v1.transport.ChunkedOutput;

import static java.util.Objects.requireNonNull;

public class DefaultBoltConnectionFactory implements BoltConnectionFactory {
    private final BoltSchedulerProvider schedulerProvider;
    private final TransportThrottleGroup throttleGroup;

    public DefaultBoltConnectionFactory(BoltSchedulerProvider schedulerProvider, TransportThrottleGroup throttleGroup) {
        this.schedulerProvider = schedulerProvider;
        this.throttleGroup = throttleGroup;
    }

    @Override
    public BoltConnection newConnection(BoltChannel channel, BoltStateMachine stateMachine) {
        requireNonNull(channel);
        requireNonNull(stateMachine);

        BoltScheduler scheduler = schedulerProvider.get(channel);
        BoltConnectionReadLimiter readLimiter = createReadLimiter();
        BoltConnectionQueueMonitor connectionQueueMonitor = new BoltConnectionQueueMonitorAggregate(scheduler, readLimiter);
        ChunkedOutput chunkedOutput = new ChunkedOutput(channel.rawChannel(), throttleGroup);

        BoltConnection connection = new DefaultBoltConnection(channel, chunkedOutput, stateMachine, scheduler,
                connectionQueueMonitor);

        connection.start();

        return connection;
    }

    private static BoltConnectionReadLimiter createReadLimiter() {
        /**
         * When the number of queued inbound messages, previously reached configured high watermark value, " +
         *            "drops below this value, reading from underlying channel will be enabled and any pending messages " +
         *            "will start queuing again.
         */
        int lowWatermark = 100;


        /**
         * When the number of queued inbound messages grows beyond this value, reading from underlying " +
         *             "channel will be paused (no more inbound messages will be available) until queued number of " +
         *             "messages drops below the configured low watermark value.
         */
        int highWatermark = 300;

        return new BoltConnectionReadLimiter(lowWatermark, highWatermark);
    }
}
