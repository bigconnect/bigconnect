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

import com.google.inject.Inject;
import com.google.inject.Singleton;
import com.mware.bolt.util.ListenSocketAddress;
import com.mware.core.config.Configuration;

import java.time.Duration;
import java.util.List;

import static java.util.Collections.singletonList;

@Singleton
public class BoltConnector {
    public static final List<String> TLS_VERSION_DEFAULTS = singletonList("TLSv1.2");
    public static final List<String> CIPHER_SUITES_DEFAULTS = null;

    public static final String ID = "bolt";

    private Configuration config;

    @Inject
    @SuppressWarnings("unchecked")
    public BoltConnector(Configuration config) {
        this.config = config;
    }

    public String getHost() {
        return config.get(BoltOptions.BOLT_HOST);
    }

    public int getPort() {
        return config.get(BoltOptions.BOLT_PORT);
    }

    public int getThreadPoolMinSize() {
        return config.get(BoltOptions.THREAD_POOL_MIN_SIZE);
    }

    public int getThreadPoolMaxSize() {
        return config.get(BoltOptions.THREAD_POOL_MAX_SIZE);
    }

    public Duration getThreadPoolKeepalive() {
        return config.get(BoltOptions.THREAD_POOL_KEEPALIVE);
    }

    public EncryptionLevel getEncryptionLevel() {
        return EncryptionLevel.valueOf(config.get(BoltOptions.ENCRYPTION_LEVEL));
    }

    public String getSslCertificateFile() {
        return config.get(BoltOptions.SSL_CERTIFICATE_FILE);
    }

    public String getSslKeyFile() {
        return config.get(BoltOptions.SSL_KEY_FILE);
    }

    // The queue size of the thread pool bound to this connector (-1 for unbounded, 0 for direct handoff, > 0 for bounded)
    public int getUnsupportedThreadPoolQueueSize() {
        return 0;
    }

    public ListenSocketAddress getListenSocketAddress() {
        return new ListenSocketAddress(getHost(), getPort());
    }

    public enum EncryptionLevel {
        REQUIRED,
        OPTIONAL,
        DISABLED
    }
}
