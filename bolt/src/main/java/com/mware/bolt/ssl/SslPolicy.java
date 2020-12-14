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

import io.netty.handler.ssl.ClientAuth;
import io.netty.handler.ssl.SslContext;
import io.netty.handler.ssl.SslContextBuilder;
import io.netty.handler.ssl.SslProvider;

import javax.net.ssl.SSLException;
import javax.net.ssl.TrustManagerFactory;
import java.security.PrivateKey;
import java.security.cert.X509Certificate;
import java.util.List;

public class SslPolicy {
    /* cryptographic objects */
    private final PrivateKey privateKey;
    private final X509Certificate[] keyCertChain;

    /* cryptographic parameters */
    private final List<String> ciphers;
    private final String[] tlsVersions;
    private final ClientAuth clientAuth;

    private final TrustManagerFactory trustManagerFactory;
    private final SslProvider sslProvider;

    private final boolean verifyHostname;

    public SslPolicy(PrivateKey privateKey, X509Certificate[] keyCertChain, List<String> tlsVersions, List<String> ciphers, ClientAuth clientAuth,
                     TrustManagerFactory trustManagerFactory, SslProvider sslProvider, boolean verifyHostname) {
        this.privateKey = privateKey;
        this.keyCertChain = keyCertChain;
        this.tlsVersions = tlsVersions == null ? null : tlsVersions.toArray(new String[tlsVersions.size()]);
        this.ciphers = ciphers;
        this.clientAuth = clientAuth;
        this.trustManagerFactory = trustManagerFactory;
        this.sslProvider = sslProvider;
        this.verifyHostname = verifyHostname;
    }

    public SslContext nettyServerContext() throws SSLException {
        return SslContextBuilder.forServer(privateKey, keyCertChain)
                .sslProvider(sslProvider)
                .clientAuth(forNetty(clientAuth))
                .protocols(tlsVersions)
                .ciphers(ciphers)
                .trustManager(trustManagerFactory)
                .build();
    }

    public SslContext nettyClientContext() throws SSLException {
        return SslContextBuilder.forClient()
                .sslProvider(sslProvider)
                .keyManager(privateKey, keyCertChain)
                .protocols(tlsVersions)
                .ciphers(ciphers)
                .trustManager(trustManagerFactory)
                .build();
    }

    private io.netty.handler.ssl.ClientAuth forNetty(ClientAuth clientAuth) {
        switch (clientAuth) {
            case NONE:
                return io.netty.handler.ssl.ClientAuth.NONE;
            case OPTIONAL:
                return io.netty.handler.ssl.ClientAuth.OPTIONAL;
            case REQUIRE:
                return io.netty.handler.ssl.ClientAuth.REQUIRE;
            default:
                throw new IllegalArgumentException("Cannot translate to netty equivalent: " + clientAuth);
        }
    }
}
