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
package com.mware.bigconnect.driver.internal.util;

import java.io.*;
import java.security.GeneralSecurityException;
import java.security.KeyStore;
import java.security.KeyStoreException;
import java.security.cert.Certificate;
import java.security.cert.CertificateException;
import java.security.cert.CertificateFactory;
import java.util.Base64;

/**
 * A tool used to save, load certs, etc.
 */
public class CertificateTool
{
    private static final String BEGIN_CERT = "-----BEGIN CERTIFICATE-----";
    private static final String END_CERT = "-----END CERTIFICATE-----";

    /**
     * Save a certificate to a file in base 64 binary format with BEGIN and END strings
     * @param certStr
     * @param certFile
     * @throws IOException
     */
    public static void saveX509Cert(String certStr, File certFile ) throws IOException
    {
        try ( BufferedWriter writer = new BufferedWriter( new FileWriter( certFile ) ) )
        {
            writer.write( BEGIN_CERT );
            writer.newLine();

            writer.write( certStr );
            writer.newLine();

            writer.write( END_CERT );
            writer.newLine();
        }
    }

    /**
     * Save a certificate to a file. Remove all the content in the file if there is any before.
     *
     * @param cert
     * @param certFile
     * @throws GeneralSecurityException
     * @throws IOException
     */
    public static void saveX509Cert(Certificate cert, File certFile ) throws GeneralSecurityException, IOException
    {
        saveX509Cert( new Certificate[]{cert}, certFile );
    }

    /**
     * Save a list of certificates into a file
     *
     * @param certs
     * @param certFile
     * @throws GeneralSecurityException
     * @throws IOException
     */
    public static void saveX509Cert(Certificate[] certs, File certFile ) throws GeneralSecurityException, IOException
    {
        try ( BufferedWriter writer = new BufferedWriter( new FileWriter( certFile ) ) )
        {
            for ( Certificate cert : certs )
            {
                String certStr = Base64.getEncoder().encodeToString( cert.getEncoded() ).replaceAll( "(.{64})", "$1\n" );

                writer.write( BEGIN_CERT );
                writer.newLine();

                writer.write( certStr );
                writer.newLine();

                writer.write( END_CERT );
                writer.newLine();
            }
        }
    }

    /**
     * Load the certificates written in X.509 format in a file to a key store.
     *
     * @param certFile
     * @param keyStore
     * @throws GeneralSecurityException
     * @throws IOException
     */
    public static void loadX509Cert(File certFile, KeyStore keyStore ) throws GeneralSecurityException, IOException
    {
        try ( BufferedInputStream inputStream = new BufferedInputStream( new FileInputStream( certFile ) ) )
        {
            CertificateFactory certFactory = CertificateFactory.getInstance( "X.509" );

            int certCount = 0; // The file might contain multiple certs
            while ( inputStream.available() > 0 )
            {
                try
                {
                    Certificate cert = certFactory.generateCertificate( inputStream );
                    certCount++;
                    loadX509Cert( cert, "bc.javadriver.trustedcert." + certCount, keyStore );
                }
                catch ( CertificateException e )
                {
                    if ( e.getCause() != null && e.getCause().getMessage().equals( "Empty input" ) )
                    {
                        // This happens if there is whitespace at the end of the certificate - we load one cert, and then try and load a
                        // second cert, at which point we fail
                        return;
                    }
                    throw new IOException( "Failed to load certificate from `" + certFile.getAbsolutePath() + "`: " + certCount + " : " + e.getMessage(), e );
                }
            }
        }
    }

    /**
     * Load a certificate to a key store with a name
     *
     * @param certAlias a name to identify different certificates
     * @param cert
     * @param keyStore
     */
    public static void loadX509Cert(Certificate cert, String certAlias, KeyStore keyStore ) throws KeyStoreException
    {
        keyStore.setCertificateEntry( certAlias, cert );
    }

    /**
     * Convert a certificate in base 64 binary format with BEGIN and END strings
     * @param cert encoded cert string
     * @return
     */
    public static String X509CertToString(String cert )
    {
        String cert64CharPerLine = cert.replaceAll( "(.{64})", "$1\n" );
        return BEGIN_CERT + "\n" + cert64CharPerLine + "\n"+ END_CERT + "\n";
    }
}



