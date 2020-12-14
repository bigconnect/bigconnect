/*
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
package com.mware.ge.cypher.internal.compiler.planner

import com.mware.ge.cypher.internal.planner.spi.{GraphStatistics, IndexDescriptor, PlanContext}
import com.mware.ge.cypher.internal.util.Cardinality
import com.mware.ge.cypher.internal.util.LabelId
import com.mware.ge.cypher.internal.util.RelTypeId
import com.mware.ge.cypher.internal.util.Selectivity

case object HardcodedGraphStatistics extends GraphStatistics {
  val NODES_CARDINALITY = Cardinality(10000)
  val NODES_WITH_LABEL_SELECTIVITY = Selectivity.of(0.2).get
  val NODES_WITH_LABEL_CARDINALITY = NODES_CARDINALITY * NODES_WITH_LABEL_SELECTIVITY
  val RELATIONSHIPS_CARDINALITY = Cardinality(50000)
  val INDEX_SELECTIVITY = Selectivity.of(.02).get
  val INDEX_PROPERTY_EXISTS_SELECTIVITY = Selectivity.of(.5).get

  def uniqueValueSelectivity(index: IndexDescriptor, planContext: PlanContext): Option[Selectivity] =
    Some(INDEX_SELECTIVITY * Selectivity.of(index.properties.length).get)

  def indexPropertyExistsSelectivity(index: IndexDescriptor, planContext: PlanContext): Option[Selectivity] =
    Some(INDEX_PROPERTY_EXISTS_SELECTIVITY * Selectivity.of(index.properties.length).get)

  def nodesWithLabelCardinality(labelId: Option[LabelId], planContext: PlanContext): Cardinality =
    labelId.map(_ => NODES_WITH_LABEL_CARDINALITY).getOrElse(Cardinality.SINGLE)

  def cardinalityByLabelsAndRelationshipType(fromLabel: Option[LabelId], relTypeId: Option[RelTypeId], toLabel: Option[LabelId], planContext: PlanContext): Cardinality =
    RELATIONSHIPS_CARDINALITY

  override def nodesAllCardinality(): Cardinality = NODES_CARDINALITY
}
