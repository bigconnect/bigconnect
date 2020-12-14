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

import com.mware.ge.cypher.internal.planner.spi.{GraphStatistics, PlanContext}
import com.mware.ge.cypher.internal.compiler.CypherPlannerConfiguration
import com.mware.ge.cypher.internal.compiler.helpers.CachedFunction
import com.mware.ge.cypher.internal.compiler.planner.logical.Metrics.{CardinalityModel, QueryGraphCardinalityModel}
import com.mware.ge.cypher.internal.compiler.planner.logical.cardinality.ExpressionSelectivityCalculator
import com.mware.ge.cypher.internal.ir.{PlannerQuery, QueryGraph}
import com.mware.ge.cypher.internal.ast.semantics.SemanticTable
import com.mware.ge.cypher.internal.util.Cardinality

case class CachedMetricsFactory(metricsFactory: MetricsFactory) extends MetricsFactory {
  def newCardinalityEstimator(queryGraphCardinalityModel: QueryGraphCardinalityModel, evaluator: ExpressionEvaluator): CardinalityModel = {
    val wrapped: CardinalityModel = metricsFactory.newCardinalityEstimator(queryGraphCardinalityModel, evaluator)
    val cached = CachedFunction[PlannerQuery, Metrics.QueryGraphSolverInput, SemanticTable, PlanContext, Cardinality] { (a, b, c, d) => wrapped(a, b, c, d) }
    new CardinalityModel {
      override def apply(query: PlannerQuery, input: Metrics.QueryGraphSolverInput, semanticTable: SemanticTable, planContext: PlanContext): Cardinality = {
        cached.apply(query, input, semanticTable, planContext)
      }
    }
  }

  def newCostModel(config: CypherPlannerConfiguration) =
    CachedFunction(metricsFactory.newCostModel(config: CypherPlannerConfiguration))

  def newQueryGraphCardinalityModel(statistics: GraphStatistics): QueryGraphCardinalityModel = {
    val wrapped: QueryGraphCardinalityModel = metricsFactory.newQueryGraphCardinalityModel(statistics)
    val cached = CachedFunction[QueryGraph, Metrics.QueryGraphSolverInput, SemanticTable, PlanContext, Cardinality] { (a, b, c, d) => wrapped(a, b, c, d) }
    new QueryGraphCardinalityModel {
      override def apply(queryGraph: QueryGraph, input: Metrics.QueryGraphSolverInput, semanticTable: SemanticTable, planContext: PlanContext): Cardinality = {
        cached.apply(queryGraph, input, semanticTable, planContext)
      }

      override val expressionSelectivityCalculator: ExpressionSelectivityCalculator = wrapped.expressionSelectivityCalculator
    }
  }

}
