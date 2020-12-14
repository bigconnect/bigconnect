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
import com.mware.ge.util.ConfigurationUtils;

import java.time.Duration;
import java.util.List;
import java.util.Map;

import static java.util.Collections.singletonList;

@Singleton
public class BoltConnector {
    public static final List<String> TLS_VERSION_DEFAULTS = singletonList( "TLSv1.2" );
    public static final List<String> CIPHER_SUITES_DEFAULTS = null;

    public static final String ID = "bolt";

    // Address the connector should bind to
    public static final String HOST = "host";
    public static final String PORT = "port";
    // The number of threads to keep in the thread pool bound to this connector, even if they are idle.
    public static final String THREAD_POOL_MIN_SIZE = "threadPoolMinSize";
    // The maximum number of threads allowed in the thread pool bound to this connector.
    public static final String THREAD_POOL_MAX_SIZE = "threadPoolMaxSize";
    // The maximum time an idle thread in the thread pool bound to this connector will wait for new tasks.
    public static final String THREAD_POOL_KEEPALIVE = "threadPoolKeepalive";
    public static final String GRAPH_FETCH_BATCH_SIZE = "graphFetchBatchSize";

    // Encryption level to require this connector to use
    public static final String ENCRYPTION_LEVEL = "encryptionLevel";
    public static final String SSL_PREFIX = "ssl";
    // Directory for storing certificates
    // Path to the X.509 public certificate file
    public static final String SSL_CERTIFICATE_FILE = SSL_PREFIX+".tls_certificate_file";
    // "Path to the X.509 private key file
    public static final String SSL_KEY_FILE = SSL_PREFIX+".tls_key_file";

    public static final String DEFAULT_HOST = "localhost";
    public static final int DEFAULT_PORT = 10242;
    public static final int DEFAULT_THREAD_POOL_MIN_SIZE = 5;
    public static final int DEFAULT_THREAD_POOL_MAX_SIZE = 400;
    public static final Duration DEFAULT_THREAD_POOL_KEEPALIVE = Duration.ofMinutes(5);
    public static final int DEFAULT_GRAPH_FETCH_BATCH_SIZE = 1000;
    public static final String DEFAULT_ENCRYPTION_LEVEL = EncryptionLevel.OPTIONAL.name();
    public static final String DEFAULT_SSL_CERTIFICATE_FILE = "bolt.cert";
    public static final String DEFAULT_SSL_KEY_FILE = "bolt.key";

    private Map<String, Object> config;

    @Inject
    @SuppressWarnings("unchecked")
    public BoltConnector(Configuration configuration) {
        this.config = (Map) configuration.getSubset("bolt");
    }

    public String getHost() {
        return ConfigurationUtils.getString(config, HOST, DEFAULT_HOST);
    }

    public int getPort() {
        return ConfigurationUtils.getInt(config, PORT, DEFAULT_PORT);
    }

    public int getGraphFetchBatchSize() {
        return ConfigurationUtils.getInt(config, GRAPH_FETCH_BATCH_SIZE, DEFAULT_GRAPH_FETCH_BATCH_SIZE);
    }

    public int getThreadPoolMinSize() {
        return ConfigurationUtils.getInt(config, THREAD_POOL_MIN_SIZE, DEFAULT_THREAD_POOL_MIN_SIZE);
    }

    public int getThreadPoolMaxSize() {
        return ConfigurationUtils.getInt(config, THREAD_POOL_MAX_SIZE, DEFAULT_THREAD_POOL_MAX_SIZE);
    }

    public Duration getThreadPoolKeepalive() {
        return ConfigurationUtils.getDuration(config, THREAD_POOL_KEEPALIVE, DEFAULT_THREAD_POOL_KEEPALIVE);
    }

    public EncryptionLevel getEncryptionLevel() {
        String encryptionLevel = ConfigurationUtils.getString(config, ENCRYPTION_LEVEL, DEFAULT_ENCRYPTION_LEVEL);
        return EncryptionLevel.valueOf(encryptionLevel);
    }

    public String getSslCertificateFile() {
        return ConfigurationUtils.getString(config, SSL_CERTIFICATE_FILE, DEFAULT_SSL_CERTIFICATE_FILE);
    }

    public String getSslKeyFile() {
        return ConfigurationUtils.getString(config, SSL_KEY_FILE, DEFAULT_SSL_KEY_FILE);
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
