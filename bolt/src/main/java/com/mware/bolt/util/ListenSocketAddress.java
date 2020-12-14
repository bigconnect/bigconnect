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
package com.mware.bolt.util;

import com.google.common.collect.Sets;

import java.net.InetSocketAddress;
import java.util.Collection;
import java.util.Objects;

/**
 * Socket address derived from configuration.
 * There is no network awareness at all, just stores the raw configuration exactly as it comes.
 */
public class ListenSocketAddress {
    private static final Collection<String> WILDCARDS = Sets.newHashSet("0.0.0.0", "::");

    private final String hostname;
    private final int port;

    public ListenSocketAddress(String hostname, int port) {
        if (hostname == null) {
            throw new IllegalArgumentException("hostname cannot be null");
        }
        if (hostname.contains("[") || hostname.contains("]")) {
            throw new IllegalArgumentException("hostname cannot contain '[' or ']'");
        }

        this.hostname = hostname;
        this.port = port;
    }

    /**
     * @return hostname or IP address; we don't care.
     */
    public String getHostname() {
        return hostname;
    }

    public int getPort() {
        return port;
    }

    public InetSocketAddress socketAddress() {
        return new InetSocketAddress(hostname, port);
    }

    public boolean isWildcard() {
        return WILDCARDS.contains(hostname);
    }

    public boolean isIPv6() {
        return isIPv6(hostname);
    }

    @Override
    public String toString() {
        return format(hostname, port);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        ListenSocketAddress that = (ListenSocketAddress) o;
        return port == that.port && Objects.equals(hostname, that.hostname);
    }

    @Override
    public int hashCode() {
        return Objects.hash(hostname, port);
    }

    public static String format(java.net.SocketAddress address) {
        if (address instanceof InetSocketAddress) {
            InetSocketAddress inetSocketAddress = (InetSocketAddress) address;
            return format(inetSocketAddress.getHostString(), inetSocketAddress.getPort());
        }
        return address.toString();
    }

    public static String format(String hostname, int port) {
        return String.format(isIPv6(hostname) ? "[%s]:%s" : "%s:%s", hostname, port);
    }

    private static boolean isIPv6(String hostname) {
        return hostname.contains(":");
    }

    /**
     * Textual representation format for a listen socket address.
     *
     * @param hostname of the address.
     * @param port     of the address.
     * @return a string representing the address.
     */
    public static String listenAddress(String hostname, int port) {
        return new ListenSocketAddress(hostname, port).toString();
    }
}
