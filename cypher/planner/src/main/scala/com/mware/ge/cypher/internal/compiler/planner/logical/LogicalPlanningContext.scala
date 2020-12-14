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
package com.mware.ge.cypher.internal.compiler.planner.logical

import com.mware.ge.cypher.internal.planner.spi.{GraphStatistics, PlanContext, PlanningAttributes}
import com.mware.ge.cypher.internal.compiler.planner.logical.Metrics.{CardinalityModel, CostModel, QueryGraphSolverInput}
import com.mware.ge.cypher.internal.compiler.planner.logical.steps.{CostComparisonListener, LogicalPlanProducer}
import com.mware.ge.cypher.internal.ir.StrictnessMode
import com.mware.ge.cypher.internal.logical.plans.LogicalPlan
import com.mware.ge.cypher.internal.ast.semantics.SemanticTable
import com.mware.ge.cypher.internal.expressions.Variable
import com.mware.ge.cypher.internal.frontend.phases.InternalNotificationLogger
import com.mware.ge.cypher.internal.util.Cardinality

case class LogicalPlanningContext(planContext: PlanContext,
                                  logicalPlanProducer: LogicalPlanProducer,
                                  metrics: Metrics,
                                  semanticTable: SemanticTable,
                                  strategy: QueryGraphSolver,
                                  input: QueryGraphSolverInput = QueryGraphSolverInput.empty,
                                  notificationLogger: InternalNotificationLogger,
                                  useErrorsOverWarnings: Boolean = false,
                                  errorIfShortestPathFallbackUsedAtRuntime: Boolean = false,
                                  errorIfShortestPathHasCommonNodesAtRuntime: Boolean = true,
                                  legacyCsvQuoteEscaping: Boolean = true,
                                  csvBufferSize: Int = 2 * 1024 * 1024,
                                  config: QueryPlannerConfiguration = QueryPlannerConfiguration.default,
                                  leafPlanUpdater: LeafPlanUpdater = EmptyUpdater,
                                  costComparisonListener: CostComparisonListener,
                                  planningAttributes: PlanningAttributes) {
  def withStrictness(strictness: StrictnessMode): LogicalPlanningContext =
    copy(input = input.withPreferredStrictness(strictness))

  def withUpdatedCardinalityInformation(plan: LogicalPlan): LogicalPlanningContext =
    copy(input = input.recurse(plan, planningAttributes.solveds, planningAttributes.cardinalities))

  def withUpdatedSemanticTable(semanticTable: SemanticTable): LogicalPlanningContext =
    if(semanticTable == this.semanticTable) this else copy(semanticTable = semanticTable)

  def forExpressionPlanning(nodes: Iterable[Variable], rels: Iterable[Variable]): LogicalPlanningContext = {
    val tableWithNodes = nodes.foldLeft(semanticTable) { case (table, node) => table.addNode(node) }
    val tableWithRels = rels.foldLeft(tableWithNodes) { case (table, rel) => table.addRelationship(rel) }
    copy(
      input = input.copy(inboundCardinality = input.inboundCardinality.max(Cardinality(1))),
      semanticTable = tableWithRels
    )
  }

  def withAddedLeafPlanUpdater(newUpdater: LeafPlanUpdater): LogicalPlanningContext = {
    copy(leafPlanUpdater = ChainedUpdater(leafPlanUpdater, newUpdater))
  }

  def withLeafPlanUpdater(newUpdater: LeafPlanUpdater): LogicalPlanningContext = {
    copy(leafPlanUpdater = newUpdater)
  }

  def statistics: GraphStatistics = planContext.statistics

  def cost: CostModel = metrics.cost

  def cardinality: CardinalityModel = metrics.cardinality
}

object NodeIdName {
  def unapply(v: Any, context: LogicalPlanningContext): Option[String] = v match {
    case variable@Variable(name) if context.semanticTable.isNode(variable) => Some(variable.name)
    case _ => None
  }
}

object RelationshipIdName {
  def unapply(v: Any, context: LogicalPlanningContext): Option[String] = v match {
    case variable@Variable(name) if context.semanticTable.isRelationship(variable) => Some(variable.name)
    case _ => None
  }
}
