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

import com.mware.bigconnect.driver.net.ServerAddress;

import java.net.*;
import java.util.List;
import java.util.Objects;
import java.util.stream.Stream;

import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.toList;

/**
 * Holds a host and port pair that denotes a Bolt server address.
 */
public class BoltServerAddress implements ServerAddress
{
    public static final int DEFAULT_PORT = 7687;
    public static final BoltServerAddress LOCAL_DEFAULT = new BoltServerAddress( "localhost", DEFAULT_PORT );

    private final String host; // This could either be the same as originalHost or it is an IP address resolved from the original host.
    private final int port;
    private final String stringValue;

    private InetAddress resolved;

    public BoltServerAddress( String address )
    {
        this( uriFrom( address ) );
    }

    public BoltServerAddress( URI uri )
    {
        this( hostFrom( uri ), portFrom( uri ) );
    }

    public BoltServerAddress(String host, int port )
    {
        this( host, null, port );
    }

    private BoltServerAddress(String host, InetAddress resolved, int port )
    {
        this.host = requireNonNull( host, "host" );
        this.resolved = resolved;
        this.port = requireValidPort( port );
        this.stringValue = resolved != null ? String.format( "%s(%s):%d", host, resolved.getHostAddress(), port ) : String.format( "%s:%d", host, port );
    }

    public static BoltServerAddress from( ServerAddress address )
    {
        return address instanceof BoltServerAddress
               ? (BoltServerAddress) address
               : new BoltServerAddress( address.host(), address.port() );
    }

    @Override
    public boolean equals( Object o )
    {
        if ( this == o )
        {
            return true;
        }
        if ( o == null || getClass() != o.getClass() )
        {
            return false;
        }
        BoltServerAddress that = (BoltServerAddress) o;
        return port == that.port && host.equals( that.host );
    }

    @Override
    public int hashCode()
    {
        return Objects.hash( host, port );
    }

    @Override
    public String toString()
    {
        return stringValue;
    }

    /**
     * Create a {@link SocketAddress} from this bolt address. This method always attempts to resolve the hostname into
     * an {@link InetAddress}.
     *
     * @return new socket address.
     * @see InetSocketAddress
     */
    public SocketAddress toSocketAddress()
    {
        return resolved == null ? new InetSocketAddress( host, port ) : new InetSocketAddress( resolved, port );
    }

    /**
     * Resolve the host name down to an IP address
     *
     * @return a new address instance
     * @throws UnknownHostException if no IP address for the host could be found
     * @see InetAddress#getByName(String)
     */
    public BoltServerAddress resolve() throws UnknownHostException
    {
        return new BoltServerAddress( host, InetAddress.getByName( host ), port );
    }

    /**
     * Resolve the host name down to all IP addresses that can be resolved to
     *
     * @return an array of new address instances that holds resolved addresses
     * @throws UnknownHostException if no IP address for the host could be found
     * @see InetAddress#getAllByName(String)
     */
    public List<BoltServerAddress> resolveAll() throws UnknownHostException
    {
        return Stream.of( InetAddress.getAllByName( host ) )
                .map( address -> new BoltServerAddress( host, address, port ) )
                .collect( toList() );
    }

    @Override
    public String host()
    {
        return host;
    }

    @Override
    public int port()
    {
        return port;
    }

    private static String hostFrom(URI uri )
    {
        String host = uri.getHost();
        if ( host == null )
        {
            throw invalidAddressFormat( uri );
        }
        return host;
    }

    private static int portFrom( URI uri )
    {
        int port = uri.getPort();
        return port == -1 ? DEFAULT_PORT : port;
    }

    private static URI uriFrom(String address )
    {
        String scheme;
        String hostPort;

        String[] schemeSplit = address.split( "://" );
        if ( schemeSplit.length == 1 )
        {
            // URI can't parse addresses without scheme, prepend fake "bolt://" to reuse the parsing facility
            scheme = "bolt://";
            hostPort = hostPortFrom( schemeSplit[0] );
        }
        else if ( schemeSplit.length == 2 )
        {
            scheme = schemeSplit[0] + "://";
            hostPort = hostPortFrom( schemeSplit[1] );
        }
        else
        {
            throw invalidAddressFormat( address );
        }

        return URI.create( scheme + hostPort );
    }

    private static String hostPortFrom(String address )
    {
        if ( address.startsWith( "[" ) )
        {
            // expected to be an IPv6 address like [::1] or [::1]:7687
            return address;
        }

        boolean containsSingleColon = address.indexOf( ":" ) == address.lastIndexOf( ":" );
        if ( containsSingleColon )
        {
            // expected to be an IPv4 address with or without port like 127.0.0.1 or 127.0.0.1:7687
            return address;
        }

        // address contains multiple colons and does not start with '['
        // expected to be an IPv6 address without brackets
        return "[" + address + "]";
    }

    private static RuntimeException invalidAddressFormat(URI uri )
    {
        return invalidAddressFormat( uri.toString() );
    }

    private static RuntimeException invalidAddressFormat(String address )
    {
        return new IllegalArgumentException( "Invalid address format `" + address + "`" );
    }

    private static int requireValidPort( int port )
    {
        if ( port >= 0 && port <= 65_535 )
        {
            return port;
        }
        throw new IllegalArgumentException( "Illegal port: " + port );
    }
}
