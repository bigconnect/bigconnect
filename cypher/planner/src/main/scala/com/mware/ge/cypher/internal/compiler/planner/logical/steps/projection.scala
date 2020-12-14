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
package com.mware.ge.cypher.internal.compiler.planner.logical.steps

import com.mware.ge.cypher.internal.compiler.planner.logical.LogicalPlanningContext
import com.mware.ge.cypher.internal.ir.{QueryProjection, InterestingOrder}
import com.mware.ge.cypher.internal.planner.spi.PlanningAttributes.Solveds
import com.mware.ge.cypher.internal.logical.plans.LogicalPlan
import com.mware.ge.cypher.internal.expressions._

object projection {

  def apply(in: LogicalPlan,
            projectionsToPlan: Map[String, Expression],
            projectionsToMarkSolved: Map[String, Expression],
            interestingOrder: InterestingOrder,
            context: LogicalPlanningContext): LogicalPlan = {
    val stillToSolveProjection = projectionsLeft(in, projectionsToPlan, context.planningAttributes.solveds)
    val (plan, projectionsMap) = PatternExpressionSolver()(in, stillToSolveProjection, interestingOrder, context)

    val ids = plan.availableSymbols

    val projectAllCoveredIds: Set[(String, Expression)] = ids.map(id => id -> Variable(id)(null))
    val projections: Seq[(String, Expression)] = projectionsMap.toIndexedSeq

    // The projections that are not covered yet
    val projectionsDiff =
      projections.filter({
        case (x, Variable(y)) if x == y => !ids.contains(x)
        case _ => true
      }).toMap

    if (projectionsDiff.isEmpty) {
      context.logicalPlanProducer.planStarProjection(plan, projectionsToMarkSolved, context)
    } else {
      context.logicalPlanProducer.planRegularProjection(plan, projectionsDiff, projectionsToMarkSolved, context)
    }
  }

  /**
    * Computes the projections that are not yet marked as solved.
    */
  private def projectionsLeft(in: LogicalPlan, projectionsToPlan: Map[String, Expression], solveds: Solveds): Map[String, Expression] = {
    // if we had a previous projection it might have projected something already
    // we only want to project what's left from that previous projection
    val alreadySolvedProjections = solveds.get(in.id).tailOrSelf.horizon match {
      case solvedProjection: QueryProjection => solvedProjection.projections
      case _ => Map.empty[String, Expression]
    }
    projectionsToPlan -- alreadySolvedProjections.keys
  }
}
