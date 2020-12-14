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
package com.mware.bigconnect.driver.summary;

import com.mware.bigconnect.driver.Statement;
import com.mware.bigconnect.driver.util.Immutable;

import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * The result summary of running a statement. The result summary interface can be used to investigate
 * details about the result, like the type of query run, how many and which kinds of updates have been executed,
 * and query plan and profiling information if available.
 *
 * The result summary is only available after all result records have been consumed.
 *
 * Keeping the result summary around does not influence the lifecycle of any associated session and/or transaction.
 * @since 1.0
 */
@Immutable
public interface ResultSummary
{
    /**
     * @return statement that has been executed
     */
    Statement statement();

    /**
     * @return counters for operations the statement triggered
     */
    SummaryCounters counters();

    /**
     * @return type of statement that has been executed
     */
    StatementType statementType();

    /**
     * @return true if the result contained a statement plan, i.e. is the summary of a Cypher "PROFILE" or "EXPLAIN" statement
     */
    boolean hasPlan();

    /**
     * @return true if the result contained profiling information, i.e. is the summary of a Cypher "PROFILE" statement
     */
    boolean hasProfile();

    /**
     * This describes how the database will execute your statement.
     *
     * @return statement plan for the executed statement if available, otherwise null
     */
    Plan plan();

    /**
     * This describes how the database did execute your statement.
     *
     * If the statement you executed {@link #hasProfile() was profiled}, the statement plan will contain detailed
     * information about what each step of the plan did. That more in-depth version of the statement plan becomes
     * available here.
     *
     * @return profiled statement plan for the executed statement if available, otherwise null
     */
    ProfiledPlan profile();

    /**
     * A list of notifications that might arise when executing the statement.
     * Notifications can be warnings about problematic statements or other valuable information that can be presented
     * in a client.
     *
     * Unlike failures or errors, notifications do not affect the execution of a statement.
     *
     * @return a list of notifications produced while executing the statement. The list will be empty if no
     * notifications produced while executing the statement.
     */
    List<Notification> notifications();

    /**
     * The time it took the server to make the result available for consumption.
     *
     * @param unit The unit of the duration.
     * @return The time it took for the server to have the result available in the provided time unit.
     */
    long resultAvailableAfter(TimeUnit unit);

    /**
     * The time it took the server to consume the result.
     *
     * @param unit The unit of the duration.
     * @return The time it took for the server to consume the result in the provided time unit.
     */
    long resultConsumedAfter(TimeUnit unit);

    /**
     * The basic information of the server where the result is obtained from
     * @return basic information of the server where the result is obtained from
     */
    ServerInfo server();

    /**
     * The basic information of the database where the result is obtained from
     * @return the basic information of the database where the result is obtained from
     */
    DatabaseInfo database();
}
