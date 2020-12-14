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

import com.mware.ge.cypher.internal.planner.spi.IndexDescriptor
import com.mware.ge.cypher.internal.compiler.planner.logical._
import com.mware.ge.cypher.internal.ir.ProvidedOrder
import com.mware.ge.cypher.internal.logical.plans._
import com.mware.ge.cypher.internal.ast._
import com.mware.ge.cypher.internal.expressions.{Expression, LabelToken}

object indexSeekLeafPlanner extends AbstractIndexSeekLeafPlanner {
  override protected def constructPlan(idName: String,
                                       label: LabelToken,
                                       properties: Seq[IndexedProperty],
                                       isUnique: Boolean,
                                       valueExpr: QueryExpression[Expression],
                                       hint: Option[UsingIndexHint],
                                       argumentIds: Set[String],
                                       providedOrder: ProvidedOrder,
                                       context: LogicalPlanningContext)
                                      (solvedPredicates: Seq[Expression], predicatesForCardinalityEstimation: Seq[Expression]): LogicalPlan =
    if (isUnique) {
      context.logicalPlanProducer.planNodeUniqueIndexSeek(idName,
        label,
        properties,
        valueExpr,
        solvedPredicates,
        predicatesForCardinalityEstimation,
        hint,
        argumentIds,
        providedOrder,
        context)
    } else {
      context.logicalPlanProducer.planNodeIndexSeek(idName,
        label,
        properties,
        valueExpr,
        solvedPredicates,
        predicatesForCardinalityEstimation,
        hint,
        argumentIds,
        providedOrder,
        context)
    }

  override def findIndexesForLabel(labelId: String, context: LogicalPlanningContext): Iterator[IndexDescriptor] =
    context.planContext.indexesGetForLabel(labelId)
}
