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
import com.mware.bigconnect.driver.internal.async.NetworkConnection;
import com.mware.bigconnect.driver.internal.metrics.MetricsListener;
import com.mware.bigconnect.driver.internal.spi.Connection;
import com.mware.bigconnect.driver.internal.util.Clock;

public class NetworkConnectionFactory implements ConnectionFactory
{
    private final Clock clock;
    private final MetricsListener metricsListener;

    public NetworkConnectionFactory( Clock clock, MetricsListener metricsListener )
    {
        this.clock = clock;
        this.metricsListener = metricsListener;
    }

    @Override
    public Connection createConnection( Channel channel, ExtendedChannelPool pool )
    {
        return new NetworkConnection( channel, pool, clock, metricsListener );
    }
}
