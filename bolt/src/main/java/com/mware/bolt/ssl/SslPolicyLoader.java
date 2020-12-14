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
package com.mware.bolt.ssl;

import com.mware.bolt.BoltConnector;
import com.mware.core.util.BcLogger;
import com.mware.core.util.BcLoggerFactory;
import io.netty.handler.ssl.ClientAuth;
import io.netty.handler.ssl.SslProvider;
import io.netty.handler.ssl.util.InsecureTrustManagerFactory;

import java.io.File;
import java.security.PrivateKey;
import java.security.cert.X509Certificate;

public class SslPolicyLoader {
    private static BcLogger LOGGER = BcLoggerFactory.getLogger(SslPolicyLoader.class);
    private BoltConnector boltConnector;
    private SslPolicy defaultPolicy;
    private final PkiUtils pkiUtils = new PkiUtils();
    private final SslProvider sslProvider = SslProvider.JDK;

    public SslPolicyLoader(BoltConnector boltConnector) {
        this.boltConnector = boltConnector;
        this.getOrCreateLegacyPolicy();
    }

    public static SslPolicyLoader create(BoltConnector boltConnector) {
        SslPolicyLoader policyFactory = new SslPolicyLoader(boltConnector);
        return policyFactory;
    }

    public SslPolicy getPolicy() {
        return defaultPolicy;
    }

    private synchronized SslPolicy getOrCreateLegacyPolicy() {
        if (defaultPolicy != null) {
            return defaultPolicy;
        }
        defaultPolicy = loadOrCreateLegacyPolicy();
        return defaultPolicy;
    }

    private SslPolicy loadOrCreateLegacyPolicy() {
        File privateKeyFile = new File(boltConnector.getSslKeyFile()).getAbsoluteFile();
        File certificateFile = new File(boltConnector.getSslCertificateFile()).getAbsoluteFile();
        if (!privateKeyFile.exists() && !certificateFile.exists()) {
            LOGGER.warn("Cannot find valid certificate and/or private key for SSL configuration. Generating a self-signed certificate");
            String hostname = boltConnector.getListenSocketAddress().getHostname();
            try {
                pkiUtils.createSelfSignedCertificate(certificateFile, privateKeyFile, hostname);
            } catch (Exception e) {
                throw new RuntimeException("Failed to generate private key and certificate", e);
            }
        }

        PrivateKey privateKey = loadPrivateKey(privateKeyFile, null);
        X509Certificate[] keyCertChain = loadCertificateChain(certificateFile);

        return new SslPolicy(privateKey, keyCertChain, BoltConnector.TLS_VERSION_DEFAULTS, BoltConnector.CIPHER_SUITES_DEFAULTS,
                ClientAuth.NONE, InsecureTrustManagerFactory.INSTANCE, sslProvider, false);
    }

    private PrivateKey loadPrivateKey(File privateKeyFile, String privateKeyPassword) {
        if (privateKeyPassword != null) {
            // TODO: Support loading of private keys with passwords.
            throw new UnsupportedOperationException("Loading private keys with passwords is not yet supported");
        }

        try {
            return pkiUtils.loadPrivateKey(privateKeyFile);
        } catch (Exception e) {
            throw new RuntimeException("Failed to load private key: " + privateKeyFile +
                    (privateKeyPassword == null ? "" : " (using configured password)"), e);
        }
    }

    private X509Certificate[] loadCertificateChain(File keyCertChainFile) {
        try {
            return pkiUtils.loadCertificates(keyCertChainFile);
        } catch (Exception e) {
            throw new RuntimeException("Failed to load public certificate chain: " + keyCertChainFile, e);
        }
    }
}
