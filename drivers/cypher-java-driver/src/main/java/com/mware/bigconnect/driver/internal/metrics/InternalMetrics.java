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
package com.mware.bigconnect.driver.internal.metrics;

import com.mware.bigconnect.driver.ConnectionPoolMetrics;
import com.mware.bigconnect.driver.Logger;
import com.mware.bigconnect.driver.Logging;
import com.mware.bigconnect.driver.internal.BoltServerAddress;
import com.mware.bigconnect.driver.internal.async.pool.ConnectionPoolImpl;
import com.mware.bigconnect.driver.internal.util.Clock;

import java.util.Collection;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;

import static java.lang.String.format;
import static java.util.Collections.unmodifiableCollection;
import static com.mware.bigconnect.driver.internal.metrics.ConnectionPoolMetricsListener.DEV_NULL_POOL_METRICS_LISTENER;

public class InternalMetrics extends InternalAbstractMetrics
{
    private final Map<String,ConnectionPoolMetrics> connectionPoolMetrics;
    private final Clock clock;
    private final Logger log;

    InternalMetrics( Clock clock, Logging logging )
    {
        Objects.requireNonNull( clock );
        this.connectionPoolMetrics = new ConcurrentHashMap<>();
        this.clock = clock;
        this.log = logging.getLog( getClass().getSimpleName() );
    }

    @Override
    public void putPoolMetrics(String poolId, BoltServerAddress serverAddress, ConnectionPoolImpl pool )
    {
        this.connectionPoolMetrics.put( poolId, new InternalConnectionPoolMetrics( poolId, serverAddress, pool ) );
    }

    @Override
    public void removePoolMetrics( String id )
    {
        this.connectionPoolMetrics.remove( id );
    }

    @Override
    public void beforeCreating(String poolId, ListenerEvent creatingEvent )
    {
        poolMetrics( poolId ).beforeCreating( creatingEvent );
    }

    @Override
    public void afterCreated(String poolId, ListenerEvent creatingEvent )
    {
        poolMetrics( poolId ).afterCreated( creatingEvent );
    }

    @Override
    public void afterFailedToCreate( String poolId )
    {
        poolMetrics( poolId ).afterFailedToCreate();
    }

    @Override
    public void afterClosed( String poolId )
    {
        poolMetrics( poolId ).afterClosed();
    }

    @Override
    public void beforeAcquiringOrCreating(String poolId, ListenerEvent acquireEvent )
    {
        poolMetrics( poolId ).beforeAcquiringOrCreating( acquireEvent );
    }

    @Override
    public void afterAcquiringOrCreating( String poolId )
    {
        poolMetrics( poolId ).afterAcquiringOrCreating();
    }

    @Override
    public void afterAcquiredOrCreated(String poolId, ListenerEvent acquireEvent )
    {
        poolMetrics( poolId ).afterAcquiredOrCreated( acquireEvent );
    }

    @Override
    public void afterConnectionCreated(String poolId, ListenerEvent inUseEvent )
    {
        poolMetrics( poolId ).acquired( inUseEvent );
    }

    @Override
    public void afterConnectionReleased(String poolId, ListenerEvent inUseEvent )
    {
        poolMetrics( poolId ).released( inUseEvent );
    }

    @Override
    public void afterTimedOutToAcquireOrCreate( String poolId )
    {
        poolMetrics( poolId ).afterTimedOutToAcquireOrCreate();
    }

    @Override
    public ListenerEvent createListenerEvent()
    {
        return new TimeRecorderListenerEvent( clock );
    }

    @Override
    public Collection<ConnectionPoolMetrics> connectionPoolMetrics()
    {
        return unmodifiableCollection( this.connectionPoolMetrics.values() );
    }

    @Override
    public String toString()
    {
        return format( "PoolMetrics=%s", connectionPoolMetrics );
    }

    private ConnectionPoolMetricsListener poolMetrics( String poolId )
    {
        InternalConnectionPoolMetrics poolMetrics = (InternalConnectionPoolMetrics) this.connectionPoolMetrics.get( poolId );
        if ( poolMetrics == null )
        {
            log.warn( format( "Failed to find pool metrics with id `%s` in %s.", poolId, this.connectionPoolMetrics ) );
            return DEV_NULL_POOL_METRICS_LISTENER;
        }
        return poolMetrics;
    }
}
