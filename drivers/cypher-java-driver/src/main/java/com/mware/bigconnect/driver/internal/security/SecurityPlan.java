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
package com.mware.bigconnect.driver.internal.security;

import javax.net.ssl.*;
import java.io.File;
import java.io.IOException;
import java.security.GeneralSecurityException;
import java.security.KeyStore;
import java.security.NoSuchAlgorithmException;
import java.security.cert.CertificateException;
import java.security.cert.X509Certificate;

import static com.mware.bigconnect.driver.internal.util.CertificateTool.loadX509Cert;

/**
 * A SecurityPlan consists of encryption and trust details.
 */
public class SecurityPlan
{
    public static SecurityPlan forAllCertificates( boolean requiresHostnameVerification ) throws GeneralSecurityException
    {
        SSLContext sslContext = SSLContext.getInstance( "TLS" );
        sslContext.init( new KeyManager[0], new TrustManager[]{new TrustAllTrustManager()}, null );

        return new SecurityPlan( true, sslContext, requiresHostnameVerification );
    }

    public static SecurityPlan forCustomCASignedCertificates(File certFile, boolean requiresHostnameVerification )
            throws GeneralSecurityException, IOException
    {
        // A certificate file is specified so we will load the certificates in the file
        // Init a in memory TrustedKeyStore
        KeyStore trustedKeyStore = KeyStore.getInstance( "JKS" );
        trustedKeyStore.load( null, null );

        // Load the certs from the file
        loadX509Cert( certFile, trustedKeyStore );

        // Create TrustManager from TrustedKeyStore
        TrustManagerFactory trustManagerFactory = TrustManagerFactory.getInstance( "SunX509" );
        trustManagerFactory.init( trustedKeyStore );

        SSLContext sslContext = SSLContext.getInstance( "TLS" );
        sslContext.init( new KeyManager[0], trustManagerFactory.getTrustManagers(), null );

        return new SecurityPlan( true, sslContext, requiresHostnameVerification );
    }

    public static SecurityPlan forSystemCASignedCertificates( boolean requiresHostnameVerification ) throws NoSuchAlgorithmException
    {
        return new SecurityPlan( true, SSLContext.getDefault(), requiresHostnameVerification );
    }

    public static SecurityPlan insecure()
    {
        return new SecurityPlan( false, null, false );
    }

    private final boolean requiresEncryption;
    private final SSLContext sslContext;
    private final boolean requiresHostnameVerification;

    private SecurityPlan(boolean requiresEncryption, SSLContext sslContext, boolean requiresHostnameVerification )
    {
        this.requiresEncryption = requiresEncryption;
        this.sslContext = sslContext;
        this.requiresHostnameVerification = requiresHostnameVerification;
    }

    public boolean requiresEncryption()
    {
        return requiresEncryption;
    }

    public SSLContext sslContext()
    {
        return sslContext;
    }

    public boolean requiresHostnameVerification()
    {
        return requiresHostnameVerification;
    }

    private static class TrustAllTrustManager implements X509TrustManager
    {
        public void checkClientTrusted(X509Certificate[] chain, String authType ) throws CertificateException
        {
            throw new CertificateException( "All client connections to this client are forbidden." );
        }

        public void checkServerTrusted(X509Certificate[] chain, String authType ) throws CertificateException
        {
            // all fine, pass through
        }

        public X509Certificate[] getAcceptedIssuers()
        {
            return new X509Certificate[0];
        }
    }
}
