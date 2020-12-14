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

import java.util.concurrent.TimeUnit;

public class PoolSettings
{
    public static final int NOT_CONFIGURED = -1;

    public static final int DEFAULT_MAX_CONNECTION_POOL_SIZE = 100;
    public static final long DEFAULT_IDLE_TIME_BEFORE_CONNECTION_TEST = NOT_CONFIGURED;
    public static final long DEFAULT_MAX_CONNECTION_LIFETIME = TimeUnit.HOURS.toMillis( 1 );
    public static final long DEFAULT_CONNECTION_ACQUISITION_TIMEOUT = TimeUnit.SECONDS.toMillis( 60 );

    private final int maxConnectionPoolSize;
    private final long connectionAcquisitionTimeout;
    private final long maxConnectionLifetime;
    private final long idleTimeBeforeConnectionTest;

    public PoolSettings( int maxConnectionPoolSize, long connectionAcquisitionTimeout,
            long maxConnectionLifetime, long idleTimeBeforeConnectionTest )
    {
        this.maxConnectionPoolSize = maxConnectionPoolSize;
        this.connectionAcquisitionTimeout = connectionAcquisitionTimeout;
        this.maxConnectionLifetime = maxConnectionLifetime;
        this.idleTimeBeforeConnectionTest = idleTimeBeforeConnectionTest;
    }

    public long idleTimeBeforeConnectionTest()
    {
        return idleTimeBeforeConnectionTest;
    }

    public boolean idleTimeBeforeConnectionTestEnabled()
    {
        return idleTimeBeforeConnectionTest >= 0;
    }

    public long maxConnectionLifetime()
    {
        return maxConnectionLifetime;
    }

    public boolean maxConnectionLifetimeEnabled()
    {
        return maxConnectionLifetime > 0;
    }

    public int maxConnectionPoolSize()
    {
        return maxConnectionPoolSize;
    }

    public long connectionAcquisitionTimeout()
    {
        return connectionAcquisitionTimeout;
    }
}
