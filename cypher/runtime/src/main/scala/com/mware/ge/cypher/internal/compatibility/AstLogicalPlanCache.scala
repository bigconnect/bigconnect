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
package com.mware.ge.cypher.internal.compatibility

import java.time.Clock

import com.mware.ge.collection.Pair
import com.mware.ge.cypher.GeGraphStatistics.BaseGeGraphStatistics
import com.mware.ge.cypher.internal.QueryCache.ParameterTypeMap
import com.mware.ge.cypher.internal._
import com.mware.ge.cypher.internal.planner.spi.GraphStatistics
import com.mware.ge.cypher.internal.compiler.StatsDivergenceCalculator
import com.mware.ge.cypher.internal.compiler.phases.LogicalPlanState
import com.mware.ge.cypher.internal.util.InternalNotification

/**
  * Cache which stores logical plans indexed by an AST statement.
  *
  * @param maximumSize Maximum size of this cache
  * @param tracer Traces cache activity
  * @param clock Clock used to compute logical plan staleness
  * @param divergence Statistics divergence calculator used to compute logical plan staleness
  * @tparam STATEMENT Type of AST statement used as key
  */
class AstLogicalPlanCache[STATEMENT <: AnyRef](override val maximumSize: Int,
                                               override val tracer: CacheTracer[Pair[STATEMENT, ParameterTypeMap]],
                                               clock: Clock,
                                               divergence: StatsDivergenceCalculator
) extends QueryCache[STATEMENT,Pair[STATEMENT,ParameterTypeMap], CacheableLogicalPlan](maximumSize,
                                                  AstLogicalPlanCache.stalenessCaller(clock, divergence),
                                                  tracer)
object AstLogicalPlanCache {
  def stalenessCaller(clock: Clock,
                      divergence: StatsDivergenceCalculator): PlanStalenessCaller[CacheableLogicalPlan] = {
    new PlanStalenessCaller[CacheableLogicalPlan](clock, divergence, (state, _) => state.reusability)
  }
}

case class CacheableLogicalPlan(logicalPlanState: LogicalPlanState,
                                reusability: ReusabilityState, notifications: Set[InternalNotification])
