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
package com.mware.ge.cypher;

import com.mware.core.user.SystemUser;
import com.mware.ge.cypher.query.KernelStatement;
import com.mware.ge.cypher.query.ExecutingQuery;
import com.mware.ge.cypher.query.ClientConnectionInfo;
import com.mware.ge.cypher.util.MonotonicCounter;
import com.mware.ge.io.CpuClock;
import com.mware.ge.io.HeapAllocation;
import com.mware.ge.time.SystemNanoClock;
import com.mware.ge.values.virtual.MapValue;

import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Stream;

public class StackingQueryRegistrationOperations implements QueryRegistrationOperations {
    private final MonotonicCounter lastQueryId = MonotonicCounter.newAtomicMonotonicCounter();
    private final SystemNanoClock clock;
    private final AtomicReference<CpuClock> cpuClockRef;
    private final AtomicReference<HeapAllocation> heapAllocationRef;

    public StackingQueryRegistrationOperations(
            SystemNanoClock clock,
            AtomicReference<CpuClock> cpuClockRef,
            AtomicReference<HeapAllocation> heapAllocationRef) {
        this.clock = clock;
        this.cpuClockRef = cpuClockRef;
        this.heapAllocationRef = heapAllocationRef;
    }

    @Override
    public Stream<ExecutingQuery> executingQueries(KernelStatement statement) {
        return statement.executingQueryList().queries();
    }

    @Override
    public void registerExecutingQuery(KernelStatement statement, ExecutingQuery executingQuery) {
        statement.startQueryExecution(executingQuery);
    }

    @Override
    public ExecutingQuery startQueryExecution(
            KernelStatement statement,
            ClientConnectionInfo clientConnection,
            String queryText,
            MapValue queryParameters
    ) {
        long queryId = lastQueryId.incrementAndGet();
        Thread thread = Thread.currentThread();
        long threadId = thread.getId();
        String threadName = thread.getName();
        ExecutingQuery executingQuery =
                new ExecutingQuery(queryId, clientConnection, new SystemUser().getUsername(), queryText, queryParameters,
                        statement.getTransaction().getMetaData(), () -> 0L,
                        threadId, threadName, clock, cpuClockRef.get(), heapAllocationRef.get());
        registerExecutingQuery(statement, executingQuery);
        return executingQuery;
    }

    @Override
    public void unregisterExecutingQuery(KernelStatement statement, ExecutingQuery executingQuery) {
        statement.stopQueryExecution(executingQuery);
    }
}

