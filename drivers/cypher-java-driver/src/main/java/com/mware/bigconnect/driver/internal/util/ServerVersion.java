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

import com.mware.bigconnect.driver.Driver;
import com.mware.bigconnect.driver.Session;

import java.util.Objects;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static java.lang.Integer.compare;

public class ServerVersion
{
    public static final String BIGCONNECT_PRODUCT = "Neo4j";

    public static final ServerVersion v4_0_0 = new ServerVersion(BIGCONNECT_PRODUCT, 4, 0, 0 );
    public static final ServerVersion v3_5_0 = new ServerVersion(BIGCONNECT_PRODUCT, 3, 5, 0 );
    public static final ServerVersion v3_4_0 = new ServerVersion(BIGCONNECT_PRODUCT, 3, 4, 0 );
    public static final ServerVersion vInDev = new ServerVersion(BIGCONNECT_PRODUCT, Integer.MAX_VALUE, Integer.MAX_VALUE, Integer.MAX_VALUE );

    private static final String NEO4J_IN_DEV_VERSION_STRING = BIGCONNECT_PRODUCT + "/dev";
    private static final Pattern PATTERN =
            Pattern.compile( "([^/]+)/(\\d+)\\.(\\d+)(?:\\.)?(\\d*)(\\.|-|\\+)?([0-9A-Za-z-.]*)?" );

    private final String product;
    private final int major;
    private final int minor;
    private final int patch;
    private final String stringValue;

    private ServerVersion(String product, int major, int minor, int patch )
    {
        this.product = product;
        this.major = major;
        this.minor = minor;
        this.patch = patch;
        this.stringValue = stringValue( product, major, minor, patch );
    }

    public String product()
    {
        return product;
    }

    public static ServerVersion version( Driver driver )
    {
        try ( Session session = driver.session() )
        {
            String versionString = session.readTransaction(tx -> tx.run( "RETURN 1" ).consume().server().version() );
            return version( versionString );
        }
    }

    public static ServerVersion version( String server )
    {
        Matcher matcher = PATTERN.matcher( server );
        if ( matcher.matches() )
        {
            String product = matcher.group( 1 );
            int major = Integer.valueOf( matcher.group( 2 ) );
            int minor = Integer.valueOf( matcher.group( 3 ) );
            String patchString = matcher.group( 4 );
            int patch = 0;
            if ( patchString != null && !patchString.isEmpty() )
            {
                patch = Integer.valueOf( patchString );
            }
            return new ServerVersion( product, major, minor, patch );
        }
        else if ( server.equalsIgnoreCase( NEO4J_IN_DEV_VERSION_STRING ) )
        {
            return vInDev;
        }
        else
        {
            throw new IllegalArgumentException( "Cannot parse " + server );
        }
    }

    @Override
    public boolean equals( Object o )
    {
        if ( this == o )
        { return true; }
        if ( o == null || getClass() != o.getClass() )
        { return false; }

        ServerVersion that = (ServerVersion) o;

        if ( !product.equals( that.product ) )
        { return false; }
        if ( major != that.major )
        { return false; }
        if ( minor != that.minor )
        { return false; }
        return patch == that.patch;
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(product, major, minor, patch);
    }

    public boolean greaterThan(ServerVersion other)
    {
        return compareTo( other ) > 0;
    }

    public boolean greaterThanOrEqual(ServerVersion other)
    {
        return compareTo( other ) >= 0;
    }

    public boolean lessThan(ServerVersion other)
    {
        return compareTo( other ) < 0;
    }

    public boolean lessThanOrEqual(ServerVersion other)
    {
        return compareTo( other ) <= 0;
    }

    private int compareTo( ServerVersion o )
    {
        if ( !product.equals( o.product ) )
        {
            throw new IllegalArgumentException( "Comparing different products '" + product + "' with '" + o.product + "'" );
        }
        int c = compare( major, o.major );
        if (c == 0)
        {
            c = compare( minor, o.minor );
            if (c == 0)
            {
                c = compare( patch, o.patch );
            }
        }

        return c;
    }

    @Override
    public String toString()
    {
        return stringValue;
    }

    private static String stringValue(String product, int major, int minor, int patch )
    {
        if ( major == Integer.MAX_VALUE && minor == Integer.MAX_VALUE && patch == Integer.MAX_VALUE )
        {
            return NEO4J_IN_DEV_VERSION_STRING;
        }
        return String.format( "%s/%s.%s.%s", product, major, minor, patch );
    }
}
