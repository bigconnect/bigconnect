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

import io.netty.channel.Channel;

import java.time.Clock;
import java.time.Duration;

/**
 * Serves as an entry point for throttling of transport related resources. Currently only
 * write operations are throttled based on pending buffered data. If there will be new types
 * of throttles (number of active channels, reads, etc.) they should be added and registered
 * through this class.
 */
public class TransportThrottleGroup {
    public static final TransportThrottleGroup NO_THROTTLE = new TransportThrottleGroup(false);
    public static final TransportThrottleGroup WRITE_THROTTLE = new TransportThrottleGroup(true);

    private final TransportThrottle writeThrottle;

    private TransportThrottleGroup(boolean withWriteThrottle) {
        this.writeThrottle = withWriteThrottle ? createWriteThrottle() : NoOpTransportThrottle.INSTANCE;

    }

    public TransportThrottle writeThrottle()
    {
        return writeThrottle;
    }

    public void install(Channel channel) {
        writeThrottle.install(channel);
    }

    public void uninstall(Channel channel) {
        writeThrottle.uninstall(channel);
    }

    private static TransportThrottle createWriteThrottle() {
        return new TransportWriteThrottle(128 * 1024, 512 * 1024, Clock.systemUTC(), Duration.ofMinutes(15));
    }

    private static class NoOpTransportThrottle implements TransportThrottle {
        private static final TransportThrottle INSTANCE = new NoOpTransportThrottle();

        @Override
        public void install(Channel channel) {

        }

        @Override
        public void acquire(Channel channel) {

        }

        @Override
        public void release(Channel channel) {

        }

        @Override
        public void uninstall(Channel channel) {

        }
    }
}
