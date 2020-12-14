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
package com.mware.ge.cypher.internal.compiler

import java.time.Clock

import com.mware.ge.cypher.internal.planner.spi.PlanContext
import com.mware.ge.cypher.internal.util.InputPosition
import com.mware.ge.cypher.internal.compiler.planner.logical.{ExpressionEvaluator, MetricsFactory, QueryGraphSolver}
import com.mware.ge.cypher.internal.frontend.phases.{BaseContext, InternalNotificationLogger, Monitors}
import com.mware.ge.cypher.internal.frontend.phases.CompilationPhaseTracer
import com.mware.ge.cypher.internal.util.attribution.IdGen

trait ContextCreator[Context <: BaseContext] {
  def create(tracer: CompilationPhaseTracer,
             notificationLogger: InternalNotificationLogger,
             planContext: PlanContext,
             queryText: String,
             debugOptions: Set[String],
             offset: Option[InputPosition],
             monitors: Monitors,
             metricsFactory: MetricsFactory,
             queryGraphSolver: QueryGraphSolver,
             config: CypherPlannerConfiguration,
             updateStrategy: UpdateStrategy,
             clock: Clock,
             logicalPlanIdGen: IdGen,
             evaluator: ExpressionEvaluator): Context
}
