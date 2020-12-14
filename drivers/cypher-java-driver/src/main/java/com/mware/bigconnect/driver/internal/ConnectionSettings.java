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
package com.mware.bigconnect.driver.internal;

import com.mware.bigconnect.driver.AuthToken;
import com.mware.bigconnect.driver.Session;

import static java.lang.String.format;

/**
 * The connection settings are used whenever a new connection is
 * established to a server, specifically as part of the INIT request.
 */
public class ConnectionSettings
{
    private static final String DEFAULT_USER_AGENT = format( "bc-java/%s", driverVersion() );

    /**
     * Extracts the driver version from the driver jar MANIFEST.MF file.
     */
    private static String driverVersion()
    {
        // "Session" is arbitrary - the only thing that matters is that the class we use here is in the
        // 'com.mware.bigconnect.driver' package, because that is where the jar manifest specifies the version.
        // This is done as part of the build, adding a MANIFEST.MF file to the generated jarfile.
        Package pkg = Session.class.getPackage();
        if ( pkg != null && pkg.getImplementationVersion() != null )
        {
            return pkg.getImplementationVersion();
        }

        // If there is no version, we're not running from a jar file, but from raw compiled class files.
        // This should only happen during development, so call the version 'dev'.
        return "dev";
    }

    private final AuthToken authToken;
    private final String userAgent;
    private final int connectTimeoutMillis;

    public ConnectionSettings(AuthToken authToken, String userAgent, int connectTimeoutMillis )
    {
        this.authToken = authToken;
        this.userAgent = userAgent;
        this.connectTimeoutMillis = connectTimeoutMillis;
    }

    public ConnectionSettings( AuthToken authToken, int connectTimeoutMillis )
    {
        this( authToken, DEFAULT_USER_AGENT, connectTimeoutMillis );
    }

    public AuthToken authToken()
    {
        return authToken;
    }

    public String userAgent()
    {
        return userAgent;
    }

    public int connectTimeoutMillis()
    {
        return connectTimeoutMillis;
    }
}
