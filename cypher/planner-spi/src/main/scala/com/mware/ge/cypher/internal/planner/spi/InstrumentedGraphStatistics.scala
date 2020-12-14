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
package com.mware.ge.cypher.internal.planner.spi

import java.lang.Math.{abs, max}

import com.mware.ge.cypher.internal.util.{Cardinality, LabelId, RelTypeId, Selectivity}

import scala.collection.mutable

sealed trait StatisticsKey
case class NodesWithLabelCardinality(labelId: Option[LabelId], planContext: PlanContext) extends StatisticsKey
case object NodesAllCardinality extends StatisticsKey
case class CardinalityByLabelsAndRelationshipType(lhs: Option[LabelId], relType: Option[RelTypeId], rhs: Option[LabelId], planContext: PlanContext) extends StatisticsKey
case class IndexSelectivity(index: IndexDescriptor, planContext: PlanContext) extends StatisticsKey
case class IndexPropertyExistsSelectivity(index: IndexDescriptor, planContext: PlanContext) extends StatisticsKey

class MutableGraphStatisticsSnapshot(val map: mutable.Map[StatisticsKey, Double] = mutable.Map.empty) {
  def freeze: GraphStatisticsSnapshot = GraphStatisticsSnapshot(map.toMap)
}

case class GraphStatisticsSnapshot(statsValues: Map[StatisticsKey, Double] = Map.empty) {
  def recompute(statistics: GraphStatistics): GraphStatisticsSnapshot = {
    val snapshot = new MutableGraphStatisticsSnapshot()
    val instrumented = InstrumentedGraphStatistics(statistics, snapshot)
    statsValues.keys.foreach {
      case NodesWithLabelCardinality(labelId, planContext) =>
        instrumented.nodesWithLabelCardinality(labelId, planContext)
      case NodesAllCardinality =>
        instrumented.nodesAllCardinality()
      case CardinalityByLabelsAndRelationshipType(lhs, relType, rhs, planContext) =>
        instrumented.cardinalityByLabelsAndRelationshipType(lhs, relType, rhs, planContext)
      case IndexSelectivity(index, planContext) =>
        instrumented.uniqueValueSelectivity(index, planContext)
      case IndexPropertyExistsSelectivity(index, planContext) =>
        instrumented.indexPropertyExistsSelectivity(index, planContext)
    }
    snapshot.freeze
  }

  //A plan has diverged if there is a relative change in any of the
  //statistics that is bigger than the threshold
  def diverges(snapshot: GraphStatisticsSnapshot, minThreshold: Double): Boolean = {
    assert(statsValues.keySet == snapshot.statsValues.keySet)
    //find the maximum relative difference (|e1 - e2| / max(e1, e2))
    val divergedStats = (statsValues map {
      case (k, e1) =>
        val e2 = snapshot.statsValues(k)
        val divergence = abs(e1 - e2) / max(e1, e2)
        if (divergence.isNaN) 0 else divergence
    }).max
    divergedStats > minThreshold
  }
}

case class InstrumentedGraphStatistics(inner: GraphStatistics, snapshot: MutableGraphStatisticsSnapshot) extends GraphStatistics {
  def nodesWithLabelCardinality(labelId: Option[LabelId], planContext: PlanContext = null): Cardinality =
    if(labelId.isEmpty){
      snapshot.map.getOrElseUpdate(NodesWithLabelCardinality(None, planContext), 1)
    } else {
      snapshot.map.getOrElseUpdate(NodesWithLabelCardinality(labelId, planContext), inner.nodesWithLabelCardinality(labelId, planContext).amount)
    }

  def cardinalityByLabelsAndRelationshipType(fromLabel: Option[LabelId], relTypeId: Option[RelTypeId], toLabel: Option[LabelId], planContext: PlanContext = null): Cardinality =
    snapshot.map.getOrElseUpdate(
      CardinalityByLabelsAndRelationshipType(fromLabel, relTypeId, toLabel, planContext),
      inner.cardinalityByLabelsAndRelationshipType(fromLabel, relTypeId, toLabel, planContext).amount
    )

  def uniqueValueSelectivity(index: IndexDescriptor, planContext: PlanContext = null): Option[Selectivity] = {
    val selectivity = inner.uniqueValueSelectivity(index, planContext)
    snapshot.map.getOrElseUpdate(IndexSelectivity(index, planContext), selectivity.fold(0.0)(_.factor))
    selectivity
  }

  def indexPropertyExistsSelectivity(index: IndexDescriptor, planContext: PlanContext = null): Option[Selectivity] = {
    val selectivity = inner.indexPropertyExistsSelectivity(index, planContext)
    snapshot.map.getOrElseUpdate(IndexPropertyExistsSelectivity(index, planContext), selectivity.fold(0.0)(_.factor))
    selectivity
  }

  override def nodesAllCardinality(): Cardinality = snapshot.map.getOrElseUpdate(NodesAllCardinality, inner.nodesAllCardinality().amount)
}
