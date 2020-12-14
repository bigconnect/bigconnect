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
package com.mware.bigconnect.driver.internal.cluster;

import static java.util.concurrent.TimeUnit.SECONDS;

public class RoutingSettings
{
    public static final long STALE_ROUTING_TABLE_PURGE_DELAY_MS = SECONDS.toMillis( 30 );
    public static final RoutingSettings DEFAULT = new RoutingSettings( 1, SECONDS.toMillis( 5 ), STALE_ROUTING_TABLE_PURGE_DELAY_MS );

    private final int maxRoutingFailures;
    private final long retryTimeoutDelay;
    private final RoutingContext routingContext;
    private final long routingTablePurgeDelayMs;

    public RoutingSettings( int maxRoutingFailures, long retryTimeoutDelay, long routingTablePurgeDelayMs )
    {
        this( maxRoutingFailures, retryTimeoutDelay, routingTablePurgeDelayMs, RoutingContext.EMPTY );
    }

    public RoutingSettings( int maxRoutingFailures, long retryTimeoutDelay, long routingTablePurgeDelayMs, RoutingContext routingContext )
    {
        this.maxRoutingFailures = maxRoutingFailures;
        this.retryTimeoutDelay = retryTimeoutDelay;
        this.routingContext = routingContext;
        this.routingTablePurgeDelayMs = routingTablePurgeDelayMs;
    }

    public RoutingSettings withRoutingContext( RoutingContext newRoutingContext )
    {
        return new RoutingSettings( maxRoutingFailures, retryTimeoutDelay, routingTablePurgeDelayMs, newRoutingContext );
    }

    public int maxRoutingFailures()
    {
        return maxRoutingFailures;
    }

    public long retryTimeoutDelay()
    {
        return retryTimeoutDelay;
    }

    public RoutingContext routingContext()
    {
        return routingContext;
    }

    public long routingTablePurgeDelayMs()
    {
        return routingTablePurgeDelayMs;
    }
}
