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
import com.mware.bigconnect.driver.Metrics;
import com.mware.bigconnect.driver.internal.BoltServerAddress;
import com.mware.bigconnect.driver.internal.async.pool.ConnectionPoolImpl;

import java.util.Collection;
import java.util.Collections;

public abstract class InternalAbstractMetrics implements Metrics, MetricsListener
{
    public static final InternalAbstractMetrics DEV_NULL_METRICS = new InternalAbstractMetrics()
    {

        @Override
        public void beforeCreating(String poolId, ListenerEvent creatingEvent )
        {

        }

        @Override
        public void afterCreated(String poolId, ListenerEvent creatingEvent )
        {

        }

        @Override
        public void afterFailedToCreate( String poolId )
        {

        }

        @Override
        public void afterClosed( String poolId )
        {

        }

        @Override
        public void beforeAcquiringOrCreating(String poolId, ListenerEvent acquireEvent )
        {

        }

        @Override
        public void afterAcquiringOrCreating( String poolId )
        {

        }

        @Override
        public void afterAcquiredOrCreated(String poolId, ListenerEvent acquireEvent )
        {

        }

        @Override
        public void afterTimedOutToAcquireOrCreate( String poolId )
        {

        }

        @Override
        public void afterConnectionCreated(String poolId, ListenerEvent inUseEvent )
        {

        }

        @Override
        public void afterConnectionReleased(String poolId, ListenerEvent inUseEvent )
        {

        }

        @Override
        public ListenerEvent createListenerEvent()
        {
            return ListenerEvent.DEV_NULL_LISTENER_EVENT;
        }

        @Override
        public void putPoolMetrics(String id, BoltServerAddress address, ConnectionPoolImpl connectionPool )
        {

        }

        @Override
        public void removePoolMetrics( String poolId )
        {

        }

        @Override
        public Collection<ConnectionPoolMetrics> connectionPoolMetrics()
        {
            return Collections.emptySet();
        }

        @Override
        public String toString()
        {
            return "Driver metrics not available while driver metrics is not enabled.";
        }
    };
}
