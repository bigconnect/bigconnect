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
package com.mware.bolt;

import java.net.InetSocketAddress;
import java.net.SocketAddress;

public class BoltConnectionInfo {
    private final String connectionId;
    private final String principalName;
    private final String clientName;
    private final SocketAddress clientAddress;
    private final SocketAddress serverAddress;

    public BoltConnectionInfo(
            String connectionId,
            String principalName,
            String clientName,
            SocketAddress clientAddress,
            SocketAddress serverAddress) {
        this.connectionId = connectionId;
        this.principalName = principalName;
        this.clientName = clientName;
        this.clientAddress = clientAddress;
        this.serverAddress = serverAddress;
    }

    public String asConnectionDetails() {
        return String.format(
                "bolt-session\tbolt\t%s\t%s\t\tclient%s\tserver%s>",
                principalName,
                clientName,
                clientAddress,
                serverAddress);
    }

    public String protocol() {
        return "bolt";
    }

    public String connectionId() {
        return connectionId;
    }

    public String clientAddress() {
        return format(clientAddress);
    }

    public String requestURI() {
        return format(serverAddress);
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

}
