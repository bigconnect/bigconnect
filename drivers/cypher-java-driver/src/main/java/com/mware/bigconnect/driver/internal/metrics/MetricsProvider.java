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

import com.mware.bigconnect.driver.Metrics;
import com.mware.bigconnect.driver.exceptions.ClientException;

import static com.mware.bigconnect.driver.internal.metrics.InternalAbstractMetrics.DEV_NULL_METRICS;

public interface MetricsProvider
{
    MetricsProvider METRICS_DISABLED_PROVIDER = new MetricsProvider()
    {
        @Override
        public Metrics metrics()
        {
            // To outside users, we forbidden their access to the metrics API
            throw new ClientException( "Driver metrics not enabled. To access driver metrics, " +
                    "you need to enabled driver metrics in the driver's configuration." );
        }

        @Override
        public MetricsListener metricsListener()
        {
            // Internally we can still register callbacks to this empty metrics listener.
            return DEV_NULL_METRICS;
        }

        @Override
        public boolean isMetricsEnabled()
        {
            return false;
        }
    };

    Metrics metrics();

    MetricsListener metricsListener();

    boolean isMetricsEnabled();
}
