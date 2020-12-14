package com.mware.ge.cypher.internal.logical.plans

import com.mware.ge.cypher.internal.logical.plans.{LazyLogicalPlan, LogicalPlan}
import com.mware.ge.cypher.internal.util.attribution.IdGen

/**
  * Change the reads of all source plans to target the active tx-state instead of the stable. This is used for MERGE
  * to make sure that each merge row can observe the writes of previous rows.
  */
case class ActiveRead(source: LogicalPlan)(implicit idGen: IdGen) extends LogicalPlan(idGen) with LazyLogicalPlan {

  val lhs = Some(source)
  val rhs = None

  override val availableSymbols: Set[String] = source.availableSymbols
}
