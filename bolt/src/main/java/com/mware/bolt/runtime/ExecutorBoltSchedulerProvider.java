/*
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
package com.mware.bolt.runtime;

import com.mware.bolt.BoltChannel;
import com.mware.bolt.BoltConnector;
import com.mware.core.util.BcLogger;
import com.mware.core.util.BcLoggerFactory;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.ForkJoinPool;

public class ExecutorBoltSchedulerProvider implements BoltSchedulerProvider {
    private static final BcLogger LOGGER = BcLoggerFactory.getLogger(ExecutorBoltSchedulerProvider.class);

    private BoltConnector connector;
    private ExecutorFactory executorFactory;
    private ExecutorService forkJoinThreadPool;
    private BoltScheduler boltScheduler;

    public ExecutorBoltSchedulerProvider(BoltConnector connector, ExecutorFactory executorFactory) {
        this.connector = connector;
        this.executorFactory = executorFactory;
    }

    @Override
    public void start() throws Throwable {
        forkJoinThreadPool = new ForkJoinPool();
        boltScheduler = new ExecutorBoltScheduler(executorFactory, connector.getThreadPoolMinSize(),
                connector.getThreadPoolMaxSize(), connector.getThreadPoolKeepalive(), connector.getUnsupportedThreadPoolQueueSize(),
                forkJoinThreadPool);
        boltScheduler.start();
    }

    @Override
    public void stop() throws Throwable {
        stopScheduler(boltScheduler);
        forkJoinThreadPool.shutdown();
        forkJoinThreadPool = null;
    }

    @Override
    public BoltScheduler get(BoltChannel channel) {
        if (boltScheduler == null) {
            throw new IllegalArgumentException(
                    String.format("Provided channel instance [local: %s, remote: %s] is not bound to any known bolt listen addresses.",
                            channel.serverAddress(), channel.clientAddress()));
        }
        return boltScheduler;
    }

    private void stopScheduler(BoltScheduler scheduler) {
        try {
            scheduler.stop();
        } catch (Throwable t) {
            LOGGER.warn("An unexpected error occurred while stopping BoltScheduler", t);
        }
    }
}
