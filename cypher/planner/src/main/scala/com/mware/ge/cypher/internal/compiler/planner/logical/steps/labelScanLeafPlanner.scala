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

import com.mware.ge.cypher.internal.compiler.planner.logical.{LeafPlanFromExpression, LeafPlanner, LeafPlansForVariable, LogicalPlanningContext}
import com.mware.ge.cypher.internal.ir.{QueryGraph, InterestingOrder}
import com.mware.ge.cypher.internal.logical.plans.LogicalPlan
import com.mware.ge.cypher.internal.ast.UsingScanHint
import com.mware.ge.cypher.internal.expressions.{Expression, HasLabels, Variable}

object labelScanLeafPlanner extends LeafPlanner with LeafPlanFromExpression {

  override def producePlanFor(e: Expression, qg: QueryGraph, interestingOrder: InterestingOrder, context: LogicalPlanningContext): Option[LeafPlansForVariable] = {
    e match {
      case labelPredicate@HasLabels(v@Variable(varName), labels) =>
        val id = varName
        if (qg.patternNodes(id) && !qg.argumentIds(id)) {
          val labelName = labels.head
          val hint = qg.hints.collectFirst {
            case hint@UsingScanHint(Variable(`varName`), `labelName`) => hint
          }
          val plan = context.logicalPlanProducer.planNodeByLabelScan(id, labelName, Seq(labelPredicate), hint, qg.argumentIds, context)
          Some(LeafPlansForVariable(varName, Set(plan)))
        } else
          None
      case _ =>
        None
    }
  }

  override def apply(qg: QueryGraph, interestingOrder: InterestingOrder, context: LogicalPlanningContext): Seq[LogicalPlan] =
    qg.selections.flatPredicates.flatMap(e => producePlanFor(e, qg, interestingOrder, context).toSeq.flatMap(_.plans))
}