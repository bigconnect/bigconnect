/*
 * Copyright (c) 2013-2020 "BigConnect,"
 * MWARE SOLUTIONS SRL
 *
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j.
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
package com.mware.ge.cypher.query;

import com.mware.ge.cypher.ExecutionPlanDescription;
import com.mware.ge.io.CpuClock;
import com.mware.ge.io.HeapAllocation;
import com.mware.ge.time.SystemNanoClock;
import com.mware.ge.values.virtual.MapValue;
import org.apache.commons.lang3.builder.ToStringBuilder;

import java.util.Map;
import java.util.concurrent.atomic.AtomicLongFieldUpdater;
import java.util.function.LongSupplier;
import java.util.function.Supplier;

import static java.util.concurrent.atomic.AtomicLongFieldUpdater.newUpdater;

/**
 * Represents a currently running query.
 */
public class ExecutingQuery {
    private static final AtomicLongFieldUpdater<ExecutingQuery> WAIT_TIME =
            newUpdater(ExecutingQuery.class, "waitTimeNanos");
    private final long queryId;
    private final String username;
    private final ClientConnectionInfo clientConnection;
    private final String queryText;
    private final MapValue queryParameters;
    private final long startTimeNanos;
    private final long startTimestampMillis;
    /**
     * Uses write barrier of {@link #status}.
     */
    private long compilationCompletedNanos;
    private Supplier<ExecutionPlanDescription> planDescriptionSupplier;
    private final long threadExecutingTheQueryId;
    @SuppressWarnings({"unused", "FieldCanBeLocal"})
    private final String threadExecutingTheQueryName;
    private final LongSupplier activeLockCount;
    private final long initialActiveLocks;
    private final SystemNanoClock clock;
    private final CpuClock cpuClock;
    private final HeapAllocation heapAllocation;
    private final long cpuTimeNanosWhenQueryStarted;
    private final long heapAllocatedBytesWhenQueryStarted;
    private final Map<String, Object> transactionAnnotationData;
    /**
     * Uses write barrier of {@link #status}.
     */
    private CompilerInfo compilerInfo;
    private volatile ExecutingQueryStatus status = SimpleState.planning();
    /**
     * Updated through {@link #WAIT_TIME}
     */
    @SuppressWarnings("unused")
    private volatile long waitTimeNanos;

    public ExecutingQuery(
            long queryId,
            ClientConnectionInfo clientConnection,
            String username,
            String queryText,
            MapValue queryParameters,
            Map<String, Object> transactionAnnotationData,
            LongSupplier activeLockCount,
            long threadExecutingTheQueryId,
            String threadExecutingTheQueryName,
            SystemNanoClock clock,
            CpuClock cpuClock,
            HeapAllocation heapAllocation) {
        // Capture timestamps first
        this.cpuTimeNanosWhenQueryStarted = cpuClock.cpuTimeNanos(threadExecutingTheQueryId);
        this.startTimeNanos = clock.nanos();
        this.startTimestampMillis = clock.millis();
        // then continue with assigning fields
        this.queryId = queryId;
        this.clientConnection = clientConnection;
        this.username = username;
        this.queryText = queryText;
        this.queryParameters = queryParameters;
        this.transactionAnnotationData = transactionAnnotationData;
        this.activeLockCount = activeLockCount;
        this.initialActiveLocks = activeLockCount.getAsLong();
        this.threadExecutingTheQueryId = threadExecutingTheQueryId;
        this.threadExecutingTheQueryName = threadExecutingTheQueryName;
        this.cpuClock = cpuClock;
        this.heapAllocation = heapAllocation;
        this.clock = clock;
        this.heapAllocatedBytesWhenQueryStarted = heapAllocation.allocatedBytes(this.threadExecutingTheQueryId);
    }

    // update state

    public void compilationCompleted(CompilerInfo compilerInfo, Supplier<ExecutionPlanDescription> planDescriptionSupplier) {
        this.compilerInfo = compilerInfo;
        this.compilationCompletedNanos = clock.nanos();
        this.planDescriptionSupplier = planDescriptionSupplier;
        this.status = SimpleState.running(); // write barrier - must be last
    }

    public void waitsForQuery(ExecutingQuery child) {
        if (child == null) {
            WAIT_TIME.addAndGet(this, status.waitTimeNanos(clock.nanos()));
            this.status = SimpleState.running();
        } else {
            this.status = new WaitingOnQuery(child, clock.nanos());
        }
    }

    // basic methods

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        ExecutingQuery that = (ExecutingQuery) o;

        return queryId == that.queryId;
    }

    @Override
    public int hashCode() {
        return (int) (queryId ^ (queryId >>> 32));
    }

    @Override
    public String toString() {
        return ToStringBuilder.reflectionToString(this);
    }

    // access stable state

    public long internalQueryId() {
        return queryId;
    }

    public String username() {
        return username;
    }

    public String queryText() {
        return queryText;
    }

    public ExecutionPlanDescription planDescription() {
        return planDescriptionSupplier.get();
    }

    public MapValue queryParameters() {
        return queryParameters;
    }

    public long startTimestampMillis() {
        return startTimestampMillis;
    }

    public long elapsedNanos() {
        return clock.nanos() - startTimeNanos;
    }

    public Map<String, Object> transactionAnnotationData() {
        return transactionAnnotationData;
    }

    public long reportedWaitingTimeNanos() {
        return waitTimeNanos;
    }

    public long totalWaitingTimeNanos(long currentTimeNanos) {
        return waitTimeNanos + status.waitTimeNanos(currentTimeNanos);
    }

    ClientConnectionInfo clientConnection() {
        return clientConnection;
    }
}
