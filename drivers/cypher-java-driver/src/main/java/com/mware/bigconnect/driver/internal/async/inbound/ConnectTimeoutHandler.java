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
package com.mware.bigconnect.driver.internal.async.inbound;

import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.timeout.ReadTimeoutHandler;
import com.mware.bigconnect.driver.exceptions.ServiceUnavailableException;

import java.util.concurrent.TimeUnit;

/**
 * Handler needed to limit amount of time connection performs TLS and Bolt handshakes.
 * It should only be used when connection is established and removed from the pipeline afterwards.
 * Otherwise it will make long running queries fail.
 */
public class ConnectTimeoutHandler extends ReadTimeoutHandler
{
    private final long timeoutMillis;
    private boolean triggered;

    public ConnectTimeoutHandler( long timeoutMillis )
    {
        super( timeoutMillis, TimeUnit.MILLISECONDS );
        this.timeoutMillis = timeoutMillis;
    }

    @Override
    protected void readTimedOut( ChannelHandlerContext ctx )
    {
        if ( !triggered )
        {
            triggered = true;
            ctx.fireExceptionCaught( unableToConnectError() );
        }
    }

    private ServiceUnavailableException unableToConnectError()
    {
        return new ServiceUnavailableException( "Unable to establish connection in " + timeoutMillis + "ms" );
    }
}
