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

import com.mware.bigconnect.driver.Value;
import com.mware.bigconnect.driver.summary.ProfiledPlan;

import java.util.List;
import java.util.Map;
import java.util.function.Function;

public class InternalProfiledPlan extends InternalPlan<ProfiledPlan> implements ProfiledPlan
{
    private final long dbHits;
    private final long records;

    protected InternalProfiledPlan(String operatorType, Map<String, Value> arguments,
                                   List<String> identifiers, List<ProfiledPlan> children, long dbHits, long records )
    {
        super( operatorType, arguments, identifiers, children );
        this.dbHits = dbHits;
        this.records = records;
    }

    @Override
    public long dbHits()
    {
        return dbHits;
    }

    @Override
    public long records()
    {
        return records;
    }

    public static final PlanCreator<ProfiledPlan> PROFILED_PLAN = new PlanCreator<ProfiledPlan>()
    {
        @Override
        public ProfiledPlan create(String operatorType, Map<String,Value> arguments, List<String> identifiers, List<ProfiledPlan> children, Value originalPlanValue )
        {
            return new InternalProfiledPlan( operatorType, arguments, identifiers, children,
                    originalPlanValue.get( "dbHits" ).asLong(),
                    originalPlanValue.get( "rows" ).asLong() );
        }
    };

    /** Builds a regular plan without profiling information - eg. a plan that came as a result of an `EXPLAIN` statement */
    public static final Function<Value, ProfiledPlan> PROFILED_PLAN_FROM_VALUE = new Converter<>(PROFILED_PLAN);
}
