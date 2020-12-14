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

import com.mware.ge.cypher.ge.GeCypherQueryContext
import com.mware.ge.cypher.CypherException
import com.mware.ge.cypher.internal.compiler.phases.{LogicalPlanState, PlannerContext}
import com.mware.ge.cypher.internal.frontend.PlannerName
import com.mware.ge.cypher.internal.frontend.phases.CompilationPhaseTracer
import com.mware.ge.cypher.internal.util.InternalNotification
import com.mware.ge.cypher.internal.{PreParsedQuery, ReusabilityState}
import com.mware.ge.values.virtual.MapValue

/**
  * Cypher planner, which parses and plans a [[PreParsedQuery]] into a [[LogicalPlanResult]].
  */
trait CypherPlanner {

  /**
    * Compile pre-parsed query into a logical plan.
    *
    * @param preParsedQuery       pre-parsed query to convert
    * @param tracer               tracer to which events of the parsing and planning are reported
    * @param cypherQueryContext   context to use during parsing and planning
    * @throws CypherException public cypher exceptions on compilation problems
    * @return a logical plan result
    */
  @throws[com.mware.ge.cypher.CypherException]
  def parseAndPlan(preParsedQuery: PreParsedQuery,
                   tracer: CompilationPhaseTracer,
                   cypherQueryContext: GeCypherQueryContext,
                   params: MapValue
                  ): LogicalPlanResult

  def name: PlannerName
}

case class LogicalPlanResult(logicalPlanState: LogicalPlanState,
                             paramNames: Seq[String],
                             extractedParams: MapValue,
                             reusability: ReusabilityState,
                             plannerContext: PlannerContext,
                             notifications: Set[InternalNotification])

