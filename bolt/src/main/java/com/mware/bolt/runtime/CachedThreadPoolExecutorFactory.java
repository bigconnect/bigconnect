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

import java.time.Duration;
import java.util.concurrent.*;

import static java.util.concurrent.TimeUnit.MILLISECONDS;

public class CachedThreadPoolExecutorFactory implements ExecutorFactory {
    public static final int UNBOUNDED_QUEUE = -1;
    public static final int SYNCHRONOUS_QUEUE = 0;

    private final RejectedExecutionHandler rejectionHandler;

    public CachedThreadPoolExecutorFactory() {
        this(new ThreadPoolExecutor.AbortPolicy());
    }

    public CachedThreadPoolExecutorFactory(RejectedExecutionHandler rejectionHandler) {
        this.rejectionHandler = rejectionHandler;
    }

    @Override
    public ExecutorService create(int corePoolSize, int maxPoolSize, Duration keepAlive, int queueSize, boolean startCoreThreads, ThreadFactory threadFactory) {
        ThreadPool result = new ThreadPool(corePoolSize, maxPoolSize, keepAlive, createTaskQueue(queueSize), threadFactory, rejectionHandler);

        if (startCoreThreads) {
            result.prestartAllCoreThreads();
        }

        return result;
    }

    private static BlockingQueue<Runnable> createTaskQueue(int queueSize) {
        if (queueSize == UNBOUNDED_QUEUE) {
            return new LinkedBlockingQueue<>();
        } else if (queueSize == SYNCHRONOUS_QUEUE) {
            return new SynchronousQueue<>();
        } else if (queueSize > 0) {
            return new ArrayBlockingQueue<>(queueSize);
        }

        throw new IllegalArgumentException(String.format("Unsupported queue size %d for thread pool creation.", queueSize));
    }

    private class ThreadPool extends ThreadPoolExecutor {

        private ThreadPool(int corePoolSize, int maxPoolSize, Duration keepAlive, BlockingQueue<Runnable> workQueue, ThreadFactory threadFactory,
                           RejectedExecutionHandler rejectionHandler) {
            super(corePoolSize, maxPoolSize, keepAlive.toMillis(), MILLISECONDS, workQueue, threadFactory, rejectionHandler);
        }

    }

}
