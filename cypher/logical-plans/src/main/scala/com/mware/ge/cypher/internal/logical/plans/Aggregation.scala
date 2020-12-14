package com.mware.ge.cypher.internal.logical.plans

import com.mware.ge.cypher.internal.expressions.{Expression, Property}
import com.mware.ge.cypher.internal.logical.plans.{CachedNodeProperty, EagerLogicalPlan, LogicalPlan}
import com.mware.ge.cypher.internal.util.attribution.IdGen

/**
  * Aggregation is a more advanced version of Distinct, where source rows are grouped by the
  * values of the groupingsExpressions. When the source is fully consumed, one row is produced
  * for every group, containing the values of the groupingExpressions for that row, as well as
  * aggregates computed on all the rows in that group.
  *
  * If there are no groupingExpressions, aggregates are computed over all source rows.
  */
case class Aggregation(source: LogicalPlan,
                       groupingExpressions: Map[String, Expression],
                       aggregationExpression: Map[String, Expression])
                      (implicit idGen: IdGen)
  extends LogicalPlan(idGen) with EagerLogicalPlan {

  val lhs = Some(source)

  def rhs = None

  val groupingKeys: Set[String] = groupingExpressions.keySet

  val availableSymbols: Set[String] = groupingKeys ++ aggregationExpression.keySet

  /**
    * Aggregations delete columns which are not explicitly listed in groupingExpressions or aggregationExpression.
    * It will therefore simply remove any cached node properties.
    */
  override final def availableCachedNodeProperties: Map[Property, CachedNodeProperty] = Map.empty
}
