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
package com.mware.bigconnect.driver.internal.handlers;

import io.netty.channel.Channel;
import io.netty.channel.pool.ChannelPool;
import io.netty.util.concurrent.Future;
import com.mware.bigconnect.driver.internal.async.inbound.InboundMessageDispatcher;
import com.mware.bigconnect.driver.internal.util.Clock;

import java.util.concurrent.CompletableFuture;

import static com.mware.bigconnect.driver.internal.async.connection.ChannelAttributes.setLastUsedTimestamp;

public class ChannelReleasingResetResponseHandler extends ResetResponseHandler
{
    private final Channel channel;
    private final ChannelPool pool;
    private final Clock clock;

    public ChannelReleasingResetResponseHandler( Channel channel, ChannelPool pool,
            InboundMessageDispatcher messageDispatcher, Clock clock, CompletableFuture<Void> releaseFuture )
    {
        super( messageDispatcher, releaseFuture );
        this.channel = channel;
        this.pool = pool;
        this.clock = clock;
    }

    @Override
    protected void resetCompleted(CompletableFuture<Void> completionFuture, boolean success )
    {
        if ( success )
        {
            // update the last-used timestamp before returning the channel back to the pool
            setLastUsedTimestamp( channel, clock.millis() );
        }
        else
        {
            // close the channel before returning it back to the pool if RESET failed
            channel.close();
        }

        Future<Void> released = pool.release( channel );
        released.addListener( ignore -> completionFuture.complete( null ) );
    }
}
