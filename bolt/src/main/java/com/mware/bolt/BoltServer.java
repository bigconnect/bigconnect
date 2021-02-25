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
package com.mware.bolt;

import com.google.inject.Inject;
import com.google.inject.Singleton;
import com.mware.bolt.runtime.*;
import com.mware.bolt.ssl.SslPolicyLoader;
import com.mware.bolt.transport.*;
import com.mware.bolt.util.BoltThreadFactory;
import com.mware.core.exception.BcException;
import com.mware.core.lifecycle.LifeSupportService;
import com.mware.core.lifecycle.LifecycleAdapter;
import com.mware.core.util.BcLogger;
import com.mware.core.util.BcLoggerFactory;
import com.mware.ge.cypher.GeCypherExecutionEngine;
import com.mware.ge.cypher.connection.NetworkConnectionTracker;
import io.netty.handler.ssl.SslContext;

@Singleton
public class BoltServer extends LifecycleAdapter {
    private static BcLogger LOGGER = BcLoggerFactory.getLogger(BoltServer.class);
    private final LifeSupportService lifeSupportService;
    private BoltConnector boltConnector;
    private final NetworkConnectionTracker connectionTracker;
    private GeCypherExecutionEngine executionEngine;
    private ExecutorBoltSchedulerProvider boltSchedulerProvider;
    private NettyServer server;
    private SslPolicyLoader sslPolicyLoader;

    @Inject
    public BoltServer(
            GeCypherExecutionEngine executionEngine,
            LifeSupportService lifeSupportService,
            BoltConnector boltConnector,
            NetworkConnectionTracker connectionTracker
    ) {
        this.executionEngine = executionEngine;
        this.lifeSupportService = lifeSupportService;
        this.boltConnector = boltConnector;
        this.connectionTracker = connectionTracker;
        lifeSupportService.add(this);
    }

    @Override
    public void start() throws Throwable {
        try {
            sslPolicyLoader = SslPolicyLoader.create(boltConnector);
            TransportThrottleGroup throttleGroup = TransportThrottleGroup.NO_THROTTLE;
            boltSchedulerProvider = new ExecutorBoltSchedulerProvider(boltConnector, new CachedThreadPoolExecutorFactory());
            boltSchedulerProvider.start();
            BoltConnectionFactory boltConnectionFactory = new DefaultBoltConnectionFactory(boltSchedulerProvider, throttleGroup);
            BoltStateMachineFactory boltStateMachineFactory = new BoltStateMachineFactoryImpl(executionEngine);
            BoltProtocolFactory boltProtocolFactory = new DefaultBoltProtocolFactory(boltConnectionFactory, boltStateMachineFactory);

            server = new NettyServer(new BoltThreadFactory("Bolt"), createProtocolInitializer(boltProtocolFactory, throttleGroup));
            server.start();
        } catch (Throwable t) {
            throw new BcException("Could not start Bolt server", t);
        }
    }

    private NettyServer.ProtocolInitializer createProtocolInitializer(
            BoltProtocolFactory boltProtocolFactory,
            TransportThrottleGroup throttleGroup
    ) {
        SslContext sslCtx = null;
        boolean requireEncryption;

        BoltConnector.EncryptionLevel encryptionLevel = boltConnector.getEncryptionLevel();

        switch (encryptionLevel) {
            case REQUIRED:
                // Encrypted connections are mandatory, a self-signed certificate may be generated.
                requireEncryption = true;
                sslCtx = createSslContext(sslPolicyLoader, boltConnector);
                break;
            case OPTIONAL:
                // Encrypted connections are optional, a self-signed certificate may be generated.
                requireEncryption = false;
                sslCtx = createSslContext(sslPolicyLoader, boltConnector);
                break;
            case DISABLED:
                // Encryption is turned off, no self-signed certificate will be generated.
                requireEncryption = false;
                sslCtx = null;
                break;
            default:
                // In the unlikely event that we happen to fall through to the default option here,
                // there is a mismatch between the BoltConnector.EncryptionLevel enum and the options
                // handled in this switch statement. In this case, we'll log a warning and default to
                // disabling encryption, since this mirrors the functionality introduced in 3.0.
                LOGGER.warn("Unhandled encryption level %s - assuming DISABLED.", encryptionLevel.name());
                requireEncryption = false;
                sslCtx = null;
                break;
        }

        return new SocketTransport(boltConnector.getListenSocketAddress(),
                sslCtx,
                requireEncryption,
                throttleGroup,
                boltProtocolFactory,
                connectionTracker
        );
    }

    private static SslContext createSslContext(SslPolicyLoader sslPolicyFactory, BoltConnector config) {
        try {
            return sslPolicyFactory.getPolicy().nettyServerContext();
        } catch (Exception e) {
            throw new RuntimeException("Failed to initialize SSL encryption support, which is required to start this connector. " +
                    "Error was: " + e.getMessage(), e);
        }
    }

    public static String getVersion() {
        return "4.1.0";
    }

    @Override
    public void stop() {
        try {
            server.stop();
            boltSchedulerProvider.stop();
        } catch (Throwable ex) {
            ex.printStackTrace();
        }
    }
}
