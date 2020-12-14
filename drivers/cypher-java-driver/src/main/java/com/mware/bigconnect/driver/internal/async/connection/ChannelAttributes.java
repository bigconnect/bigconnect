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
package com.mware.bigconnect.driver.internal.async.connection;

import io.netty.channel.Channel;
import io.netty.util.AttributeKey;
import com.mware.bigconnect.driver.internal.BoltServerAddress;
import com.mware.bigconnect.driver.internal.async.inbound.InboundMessageDispatcher;
import com.mware.bigconnect.driver.internal.util.ServerVersion;

import static io.netty.util.AttributeKey.newInstance;

public final class ChannelAttributes
{
    private static final AttributeKey<String> CONNECTION_ID = newInstance( "connectionId" );
    private static final AttributeKey<String> POOL_ID = newInstance( "poolId" );
    private static final AttributeKey<Integer> PROTOCOL_VERSION = newInstance( "protocolVersion" );
    private static final AttributeKey<BoltServerAddress> ADDRESS = newInstance( "serverAddress" );
    private static final AttributeKey<ServerVersion> SERVER_VERSION = newInstance( "serverVersion" );
    private static final AttributeKey<Long> CREATION_TIMESTAMP = newInstance( "creationTimestamp" );
    private static final AttributeKey<Long> LAST_USED_TIMESTAMP = newInstance( "lastUsedTimestamp" );
    private static final AttributeKey<InboundMessageDispatcher> MESSAGE_DISPATCHER = newInstance( "messageDispatcher" );
    private static final AttributeKey<String> TERMINATION_REASON = newInstance( "terminationReason" );

    private ChannelAttributes()
    {
    }

    public static String connectionId(Channel channel )
    {
        return get( channel, CONNECTION_ID );
    }

    public static void setConnectionId( Channel channel, String id )
    {
        setOnce( channel, CONNECTION_ID, id );
    }

    public static String poolId(Channel channel )
    {
        return get( channel, POOL_ID );
    }

    public static void setPoolId( Channel channel, String id )
    {
        setOnce( channel, POOL_ID, id );
    }

    public static int protocolVersion( Channel channel )
    {
        return get( channel, PROTOCOL_VERSION );
    }

    public static void setProtocolVersion( Channel channel, int version )
    {
        setOnce( channel, PROTOCOL_VERSION, version );
    }

    public static BoltServerAddress serverAddress( Channel channel )
    {
        return get( channel, ADDRESS );
    }

    public static void setServerAddress( Channel channel, BoltServerAddress address )
    {
        setOnce( channel, ADDRESS, address );
    }

    public static ServerVersion serverVersion( Channel channel )
    {
        return get( channel, SERVER_VERSION );
    }

    public static void setServerVersion( Channel channel, ServerVersion version )
    {
        setOnce( channel, SERVER_VERSION, version );
    }

    public static long creationTimestamp( Channel channel )
    {
        return get( channel, CREATION_TIMESTAMP );
    }

    public static void setCreationTimestamp( Channel channel, long creationTimestamp )
    {
        setOnce( channel, CREATION_TIMESTAMP, creationTimestamp );
    }

    public static Long lastUsedTimestamp(Channel channel )
    {
        return get( channel, LAST_USED_TIMESTAMP );
    }

    public static void setLastUsedTimestamp( Channel channel, long lastUsedTimestamp )
    {
        set( channel, LAST_USED_TIMESTAMP, lastUsedTimestamp );
    }

    public static InboundMessageDispatcher messageDispatcher( Channel channel )
    {
        return get( channel, MESSAGE_DISPATCHER );
    }

    public static void setMessageDispatcher( Channel channel, InboundMessageDispatcher messageDispatcher )
    {
        setOnce( channel, MESSAGE_DISPATCHER, messageDispatcher );
    }

    public static String terminationReason(Channel channel )
    {
        return get( channel, TERMINATION_REASON );
    }

    public static void setTerminationReason( Channel channel, String reason )
    {
        setOnce( channel, TERMINATION_REASON, reason );
    }

    private static <T> T get( Channel channel, AttributeKey<T> key )
    {
        return channel.attr( key ).get();
    }

    private static <T> void set( Channel channel, AttributeKey<T> key, T value )
    {
        channel.attr( key ).set( value );
    }

    private static <T> void setOnce( Channel channel, AttributeKey<T> key, T value )
    {
        T existingValue = channel.attr( key ).setIfAbsent( value );
        if ( existingValue != null )
        {
            throw new IllegalStateException(
                    "Unable to set " + key.name() + " because it is already set to " + existingValue );
        }
    }
}
