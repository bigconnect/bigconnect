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

import com.mware.bigconnect.driver.Config;
import com.mware.bigconnect.driver.internal.BoltServerAddress;
import com.mware.bigconnect.driver.internal.async.NetworkConnection;
import com.mware.bigconnect.driver.internal.async.pool.ConnectionPoolImpl;

import java.util.concurrent.TimeUnit;

public interface MetricsListener
{
    /**
     * Before creating a netty channel.
     * @param poolId the id of the pool where the netty channel lives.
     * @param creatingEvent a connection listener event registered when a connection is creating.
     */
    void beforeCreating(String poolId, ListenerEvent creatingEvent);

    /**
     * After a netty channel is created successfully.
     * @param poolId the id of the pool where the netty channel lives.
     */
    void afterCreated(String poolId, ListenerEvent creatingEvent);

    /**
     * After a netty channel is created with a failure.
     * @param poolId the id of the pool where the netty channel lives.
     */
    void afterFailedToCreate(String poolId);

    /**
     * After a netty channel is closed successfully.
     * @param poolId the id of the pool where the netty channel lives.
     */
    void afterClosed(String poolId);

    /**
     * Before acquiring or creating a new netty channel from pool.
     * @param poolId the id of the pool where the netty channel lives.
     * @param acquireEvent a pool listener event registered in pool for this acquire event.
     */
    void beforeAcquiringOrCreating(String poolId, ListenerEvent acquireEvent);

    /**
     * After acquiring or creating a new netty channel from pool regardless it is successful or not.
     * @param poolId the id of the pool where the netty channel lives.
     */
    void afterAcquiringOrCreating(String poolId);

    /**
     * After acquiring or creating a new netty channel from pool successfully.
     * @param poolId the id of the pool where the netty channel lives.
     * @param acquireEvent a pool listener event registered in pool for this acquire event.
     */
    void afterAcquiredOrCreated(String poolId, ListenerEvent acquireEvent);

    /**
     * After we failed to acquire a connection from pool within maximum connection acquisition timeout set by
     * {@link Config.ConfigBuilder#withConnectionAcquisitionTimeout(long, TimeUnit)}.
     * @param poolId the id of the pool where the netty channel lives.
     */
    void afterTimedOutToAcquireOrCreate(String poolId);

    /**
     * After acquiring or creating a new netty channel from pool successfully.
     * @param poolId the id of the pool where the netty channel lives.
     * @param inUseEvent a connection listener registered with a {@link NetworkConnection} when created.
     */
    void afterConnectionCreated(String poolId, ListenerEvent inUseEvent);

    /**
     * After releasing a netty channel back to pool successfully.
     * @param poolId the id of the pool where the netty channel lives.
     * @param inUseEvent a connection listener registered with a {@link NetworkConnection} when destroyed.
     */
    void afterConnectionReleased(String poolId, ListenerEvent inUseEvent);

    ListenerEvent createListenerEvent();

    void putPoolMetrics(String poolId, BoltServerAddress address, ConnectionPoolImpl connectionPool);

    void removePoolMetrics(String poolId);
}
