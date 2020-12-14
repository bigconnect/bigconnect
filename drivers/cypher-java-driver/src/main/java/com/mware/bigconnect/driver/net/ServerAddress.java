/*
 * Copyright (c) 2013-2020 "BigConnect,"
 * MWARE SOLUTIONS SRL
 *
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
package com.mware.bigconnect.driver.net;

import com.mware.bigconnect.driver.internal.BoltServerAddress;

/**
 * Represents a host and port. Host can either be an IP address or a DNS name.
 * Both IPv4 and IPv6 hosts are supported.
 */
public interface ServerAddress
{
    /**
     * Retrieve the host portion of this {@link ServerAddress}.
     *
     * @return the host, never {@code null}.
     */
    String host();

    /**
     * Retrieve the port portion of this {@link ServerAddress}.
     *
     * @return the port, always in range [0, 65535].
     */
    int port();

    /**
     * Create a new address with the given host and port.
     *
     * @param host the host portion. Should not be {@code null}.
     * @param port the port portion. Should be in range [0, 65535].
     * @return new server address with the specified host and port.
     */
    static ServerAddress of(String host, int port)
    {
        return new BoltServerAddress( host, port );
    }
}
