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

public interface ConnectionPoolMetricsListener
{
    /**
     * Invoked before a connection is creating.
     */
    void beforeCreating(ListenerEvent listenerEvent);

    /**
     * Invoked after a connection is created successfully.
     */
    void afterCreated(ListenerEvent listenerEvent);

    /**
     * Invoked after a connection is failed to create due to timeout, any kind of error.
     */
    void afterFailedToCreate();

    /**
     * Invoked after a connection is closed.
     */
    void afterClosed();

    /**
     * Invoked before acquiring or creating a connection.
     * @param acquireEvent
     */
    void beforeAcquiringOrCreating(ListenerEvent acquireEvent);

    /**
     * Invoked after a connection is being acquired or created regardless weather it is successful or not.
     */
    void afterAcquiringOrCreating();

    /**
     * Invoked after a connection is acquired or created successfully.
     * @param acquireEvent
     */
    void afterAcquiredOrCreated(ListenerEvent acquireEvent);

    /**
     * Invoked after it is timed out to acquire or create a connection.
     */
    void afterTimedOutToAcquireOrCreate();

    /**
     * After a connection is acquired from the pool.
     * @param inUseEvent
     */
    void acquired(ListenerEvent inUseEvent);

    /**
     * After a connection is released back to pool.
     * @param inUseEvent
     */
    void released(ListenerEvent inUseEvent);

    ConnectionPoolMetricsListener DEV_NULL_POOL_METRICS_LISTENER = new ConnectionPoolMetricsListener()
    {
        @Override
        public void beforeCreating( ListenerEvent listenerEvent )
        {

        }

        @Override
        public void afterCreated( ListenerEvent listenerEvent )
        {

        }

        @Override
        public void afterFailedToCreate()
        {

        }

        @Override
        public void afterClosed()
        {

        }

        @Override
        public void beforeAcquiringOrCreating( ListenerEvent acquireEvent )
        {

        }

        @Override
        public void afterAcquiringOrCreating()
        {

        }

        @Override
        public void afterAcquiredOrCreated( ListenerEvent acquireEvent )
        {

        }

        @Override
        public void afterTimedOutToAcquireOrCreate()
        {

        }

        @Override
        public void acquired( ListenerEvent inUseEvent )
        {

        }

        @Override
        public void released( ListenerEvent inUseEvent )
        {

        }
    };
}

