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
package com.mware.bigconnect.driver.internal.summary;

import com.mware.bigconnect.driver.Statement;
import com.mware.bigconnect.driver.summary.*;

import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.TimeUnit;

public class InternalResultSummary implements ResultSummary
{
    private final Statement statement;
    private final ServerInfo serverInfo;
    private final StatementType statementType;
    private final SummaryCounters counters;
    private final Plan plan;
    private final ProfiledPlan profile;
    private final List<Notification> notifications;
    private final long resultAvailableAfter;
    private final long resultConsumedAfter;
    private final DatabaseInfo databaseInfo;

    public InternalResultSummary(Statement statement, ServerInfo serverInfo, DatabaseInfo databaseInfo, StatementType statementType,
                                 SummaryCounters counters, Plan plan, ProfiledPlan profile, List<Notification> notifications, long resultAvailableAfter, long resultConsumedAfter )
    {
        this.statement = statement;
        this.serverInfo = serverInfo;
        this.databaseInfo = databaseInfo;
        this.statementType = statementType;
        this.counters = counters;
        this.plan = resolvePlan( plan, profile );
        this.profile = profile;
        this.notifications = notifications;
        this.resultAvailableAfter = resultAvailableAfter;
        this.resultConsumedAfter = resultConsumedAfter;
    }

    @Override
    public Statement statement()
    {
        return statement;
    }

    @Override
    public SummaryCounters counters()
    {
        return counters == null ? InternalSummaryCounters.EMPTY_STATS : counters;
    }

    @Override
    public StatementType statementType()
    {
        return statementType;
    }

    @Override
    public boolean hasPlan()
    {
        return plan != null;
    }

    @Override
    public boolean hasProfile()
    {
        return profile != null;
    }

    @Override
    public Plan plan()
    {
        return plan;
    }

    @Override
    public ProfiledPlan profile()
    {
        return profile;
    }

    @Override
    public List<Notification> notifications()
    {
        return notifications == null ? Collections.<Notification>emptyList() : notifications;
    }

    @Override
    public long resultAvailableAfter( TimeUnit unit )
    {
        return resultAvailableAfter == -1 ? resultAvailableAfter
                                          : unit.convert( resultAvailableAfter, TimeUnit.MILLISECONDS );
    }

    @Override
    public long resultConsumedAfter( TimeUnit unit )
    {
        return resultConsumedAfter == -1 ? resultConsumedAfter
                                         : unit.convert( resultConsumedAfter, TimeUnit.MILLISECONDS );
    }

    @Override
    public ServerInfo server()
    {
        return serverInfo;
    }

    @Override
    public DatabaseInfo database()
    {
        return databaseInfo;
    }

    @Override
    public boolean equals( Object o )
    {
        if ( this == o )
        {
            return true;
        }
        if ( o == null || getClass() != o.getClass() )
        {
            return false;
        }
        InternalResultSummary that = (InternalResultSummary) o;
        return resultAvailableAfter == that.resultAvailableAfter &&
               resultConsumedAfter == that.resultConsumedAfter &&
               Objects.equals( statement, that.statement ) &&
               Objects.equals( serverInfo, that.serverInfo ) &&
               statementType == that.statementType &&
               Objects.equals( counters, that.counters ) &&
               Objects.equals( plan, that.plan ) &&
               Objects.equals( profile, that.profile ) &&
               Objects.equals( notifications, that.notifications );
    }

    @Override
    public int hashCode()
    {
        return Objects.hash( statement, serverInfo, statementType, counters, plan, profile, notifications,
                resultAvailableAfter, resultConsumedAfter );
    }

    @Override
    public String toString()
    {
        return "InternalResultSummary{" +
               "statement=" + statement +
               ", serverInfo=" + serverInfo +
               ", databaseInfo=" + databaseInfo +
               ", statementType=" + statementType +
               ", counters=" + counters +
               ", plan=" + plan +
               ", profile=" + profile +
               ", notifications=" + notifications +
               ", resultAvailableAfter=" + resultAvailableAfter +
               ", resultConsumedAfter=" + resultConsumedAfter +
               '}';
    }

    /**
     * Profiled plan is a superset of plan. This method returns profiled plan if plan is {@code null}.
     *
     * @param plan the given plan, possibly {@code null}.
     * @param profiledPlan the given profiled plan, possibly {@code null}.
     * @return available plan.
     */
    private static Plan resolvePlan( Plan plan, ProfiledPlan profiledPlan )
    {
        return plan == null ? profiledPlan : plan;
    }
}
