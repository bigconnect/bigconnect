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

import com.mware.bolt.BoltConnector;
import com.mware.bolt.util.BoltThreadFactory;
import com.mware.bolt.v1.runtime.Job;
import com.mware.core.util.BcLogger;
import com.mware.core.util.BcLoggerFactory;
import org.apache.commons.lang3.exception.ExceptionUtils;

import java.time.Duration;
import java.util.Collection;
import java.util.concurrent.*;

public class ExecutorBoltScheduler implements BoltScheduler, BoltConnectionLifetimeListener, BoltConnectionQueueMonitor {
    private static final BcLogger LOGGER = BcLoggerFactory.getLogger(ExecutorBoltScheduler.class);

    private final ExecutorFactory executorFactory;
    private final ConcurrentHashMap<String, BoltConnection> activeConnections = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, CompletableFuture<Boolean>> activeWorkItems = new ConcurrentHashMap<>();
    private final int corePoolSize;
    private final int maxPoolSize;
    private final Duration keepAlive;
    private final int queueSize;
    private final ExecutorService forkJoinPool;

    private ExecutorService threadPool;

    public ExecutorBoltScheduler(ExecutorFactory executorFactory, int corePoolSize,
                                 int maxPoolSize, Duration keepAlive, int queueSize, ExecutorService forkJoinPool) {
        this.executorFactory = executorFactory;
        this.corePoolSize = corePoolSize;
        this.maxPoolSize = maxPoolSize;
        this.keepAlive = keepAlive;
        this.queueSize = queueSize;
        this.forkJoinPool = forkJoinPool;
    }

    boolean isRegistered(BoltConnection connection) {
        return activeConnections.containsKey(connection.id());
    }

    boolean isActive(BoltConnection connection) {
        return activeWorkItems.containsKey(connection.id());
    }

    @Override
    public void start() {
        threadPool = executorFactory.create(corePoolSize, maxPoolSize, keepAlive, queueSize, true,
                new NameAppendingThreadFactory(BoltConnector.ID, new BoltThreadFactory("BoltWorker")));
    }

    @Override
    public void stop() {
        if (threadPool != null) {
            activeConnections.values().forEach(this::stopConnection);

            threadPool.shutdown();
        }
    }

    @Override
    public void created(BoltConnection connection) {
        BoltConnection previous = activeConnections.put(connection.id(), connection);
        // We do not expect the same (keyed) connection twice
        assert previous == null;
    }

    @Override
    public void closed(BoltConnection connection) {
        String id = connection.id();

        try {
            CompletableFuture<Boolean> currentFuture = activeWorkItems.remove(id);
            if (currentFuture != null) {
                currentFuture.cancel(false);
            }
        } finally {
            activeConnections.remove(id);
        }
    }

    @Override
    public void enqueued(BoltConnection to, Job job) {
        handleSubmission(to);
    }

    @Override
    public void drained(BoltConnection from, Collection<Job> batch) {

    }

    private void handleSubmission(BoltConnection connection) {
        activeWorkItems.computeIfAbsent(connection.id(),
                key -> scheduleBatchOrHandleError(connection).whenCompleteAsync((result, error) -> handleCompletion(connection, result, error),
                        forkJoinPool));
    }

    private CompletableFuture<Boolean> scheduleBatchOrHandleError(BoltConnection connection) {
        try {
            return CompletableFuture.supplyAsync(() -> executeBatch(connection), threadPool);
        } catch (RejectedExecutionException ex) {
            return failedFuture(ex);
        }
    }

    private boolean executeBatch(BoltConnection connection) {
        Thread currentThread = Thread.currentThread();
        String originalName = currentThread.getName();
        String newName = String.format("%s [%s] ", originalName, connection.remoteAddress());

        currentThread.setName(newName);
        try {
            return connection.processNextBatch();
        } finally {
            currentThread.setName(originalName);
        }
    }

    private void handleCompletion(BoltConnection connection, Boolean shouldContinueScheduling, Throwable error) {
        try {
            if (error != null && ExceptionUtils.hasCause(error, RejectedExecutionException.class)) {
                connection.handleSchedulingError(error);
                return;
            }
        } finally {
            // we need to ensure that the entry is removed only after any possible handleSchedulingError
            // call is completed. Otherwise, we can end up having different threads executing against
            // bolt state machine.
            activeWorkItems.remove(connection.id());
        }

        if (error != null) {
            LOGGER.error(String.format("Unexpected error during job scheduling for session '%s'.", connection.id()), error);
            stopConnection(connection);
        } else {
            if (shouldContinueScheduling && connection.hasPendingJobs()) {
                handleSubmission(connection);
            }
        }
    }

    private void stopConnection(BoltConnection connection) {
        try {
            connection.stop();
        } catch (Throwable t) {
            LOGGER.warn(String.format("An unexpected error occurred while stopping BoltConnection [%s]", connection.id()), t);
        }
    }

    private static class NameAppendingThreadFactory implements ThreadFactory {
        private final String nameToAppend;
        private final ThreadFactory factory;

        private NameAppendingThreadFactory(String nameToAppend, ThreadFactory factory) {
            this.nameToAppend = nameToAppend;
            this.factory = factory;
        }

        @Override
        public Thread newThread(Runnable r) {
            Thread newThread = factory.newThread(r);
            newThread.setName(String.format("%s [%s]", newThread.getName(), nameToAppend));
            return newThread;
        }
    }

    /**
     * Returns a exceptionally completed @{link CompletableFuture} instance
     *
     * @param ex  the @{link Throwable} that would be set on the future
     * @param <T> the result type returned by this Future's get method
     * @return An exceptionally completed @{link CompletableFuture} with the given exception
     */
    public static <T> CompletableFuture<T> failedFuture(Throwable ex) {
        CompletableFuture<T> future = new CompletableFuture<>();
        future.completeExceptionally(ex);
        return future;
    }
}
