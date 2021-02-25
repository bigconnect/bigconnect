/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j.
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
package com.mware.ge.cypher.builtin.proc.dbms;


import com.mware.ge.cypher.connection.TrackedNetworkConnection;

import java.net.InetSocketAddress;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.ZoneId;

import static java.time.format.DateTimeFormatter.ISO_OFFSET_DATE_TIME;
import static org.apache.commons.lang3.StringUtils.EMPTY;

public class ListConnectionResult {
    public final String connectionId;
    public final String connectTime;
    public final String connector;
    public final String username;
    public final String userAgent;
    public final String serverAddress;
    public final String clientAddress;

    ListConnectionResult(TrackedNetworkConnection connection, ZoneId timeZone) {
        connectionId = connection.id();
        connectTime = formatTime(connection.connectTime(), timeZone);
        connector = connection.connector();
        username = connection.username();
        userAgent = connection.userAgent();
        serverAddress = format(connection.serverAddress());
        clientAddress = format(connection.clientAddress());
    }

    public static String format(java.net.SocketAddress address) {
        if (address instanceof InetSocketAddress) {
            InetSocketAddress inetSocketAddress = (InetSocketAddress) address;
            return format(inetSocketAddress.getHostString(), inetSocketAddress.getPort());
        }
        return address == null ? EMPTY : address.toString();
    }

    public static String format(String hostname, int port) {
        String portStr = port >= 0 ? String.format(":%s", port) : "";
        String hostnameStr = "";
        if (hostname != null) {
            hostnameStr = isHostnameIPv6(hostname) ? String.format("[%s]", hostname) : hostname;
        }
        return hostnameStr + portStr;
    }

    private static boolean isHostnameIPv6(String hostname) {
        return hostname.contains(":");
    }

    public static String formatTime(final long startTime, ZoneId zoneId) {
        return OffsetDateTime
                .ofInstant(Instant.ofEpochMilli(startTime), zoneId)
                .format(ISO_OFFSET_DATE_TIME);
    }
}
