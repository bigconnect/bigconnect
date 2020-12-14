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
package com.mware.bigconnect.driver.internal.async.pool;

import io.netty.channel.Channel;
import io.netty.channel.pool.ChannelHealthChecker;
import io.netty.util.concurrent.Future;
import io.netty.util.concurrent.Promise;
import com.mware.bigconnect.driver.Logger;
import com.mware.bigconnect.driver.Logging;
import com.mware.bigconnect.driver.internal.handlers.PingResponseHandler;
import com.mware.bigconnect.driver.internal.messaging.request.ResetMessage;
import com.mware.bigconnect.driver.internal.util.Clock;

import static com.mware.bigconnect.driver.internal.async.connection.ChannelAttributes.*;

public class NettyChannelHealthChecker implements ChannelHealthChecker
{
    private final PoolSettings poolSettings;
    private final Clock clock;
    private final Logger log;

    public NettyChannelHealthChecker( PoolSettings poolSettings, Clock clock, Logging logging )
    {
        this.poolSettings = poolSettings;
        this.clock = clock;
        this.log = logging.getLog( getClass().getSimpleName() );
    }

    @Override
    public Future<Boolean> isHealthy(Channel channel )
    {
        if ( isTooOld( channel ) )
        {
            return channel.eventLoop().newSucceededFuture( Boolean.FALSE );
        }
        if ( hasBeenIdleForTooLong( channel ) )
        {
            return ping( channel );
        }
        return ACTIVE.isHealthy( channel );
    }

    private boolean isTooOld( Channel channel )
    {
        if ( poolSettings.maxConnectionLifetimeEnabled() )
        {
            long creationTimestampMillis = creationTimestamp( channel );
            long currentTimestampMillis = clock.millis();

            long ageMillis = currentTimestampMillis - creationTimestampMillis;
            long maxAgeMillis = poolSettings.maxConnectionLifetime();

            boolean tooOld = ageMillis > maxAgeMillis;
            if ( tooOld )
            {
                log.trace( "Failed acquire channel %s from the pool because it is too old: %s > %s",
                        channel, ageMillis, maxAgeMillis );
            }

            return tooOld;
        }
        return false;
    }

    private boolean hasBeenIdleForTooLong( Channel channel )
    {
        if ( poolSettings.idleTimeBeforeConnectionTestEnabled() )
        {
            Long lastUsedTimestamp = lastUsedTimestamp( channel );
            if ( lastUsedTimestamp != null )
            {
                long idleTime = clock.millis() - lastUsedTimestamp;
                boolean idleTooLong = idleTime > poolSettings.idleTimeBeforeConnectionTest();

                if ( idleTooLong )
                {
                    log.trace( "Channel %s has been idle for %s and needs a ping", channel, idleTime );
                }

                return idleTooLong;
            }
        }
        return false;
    }

    private Future<Boolean> ping(Channel channel )
    {
        Promise<Boolean> result = channel.eventLoop().newPromise();
        messageDispatcher( channel ).enqueue( new PingResponseHandler( result, channel, log ) );
        channel.writeAndFlush( ResetMessage.RESET, channel.voidPromise() );
        return result;
    }
}
