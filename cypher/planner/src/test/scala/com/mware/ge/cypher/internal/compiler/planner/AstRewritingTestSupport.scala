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

import com.mware.ge.cypher.internal.ir.PlannerQuery
import com.mware.ge.cypher.internal.ir.ProvidedOrder
import com.mware.ge.cypher.internal.planner.spi.PlanningAttributes.Cardinalities
import com.mware.ge.cypher.internal.planner.spi.PlanningAttributes.ProvidedOrders
import com.mware.ge.cypher.internal.planner.spi.PlanningAttributes.Solveds
import com.mware.ge.cypher.internal.logical.plans.CachedNodeProperty
import com.mware.ge.cypher.internal.logical.plans.GetValue
import com.mware.ge.cypher.internal.logical.plans.IndexOrderNone
import com.mware.ge.cypher.internal.logical.plans.IndexedProperty
import com.mware.ge.cypher.internal.logical.plans.NodeIndexScan
import com.mware.ge.cypher.internal.ast.AstConstructionTestSupport
import com.mware.ge.cypher.internal.expressions.LabelToken
import com.mware.ge.cypher.internal.expressions.PropertyKeyName
import com.mware.ge.cypher.internal.expressions.PropertyKeyToken
import com.mware.ge.cypher.internal.parser.ParserFixture
import com.mware.ge.cypher.internal.util.Cardinality
import com.mware.ge.cypher.internal.util.LabelId
import com.mware.ge.cypher.internal.util.PropertyKeyId
import com.mware.ge.cypher.internal.util.attribution.Id
import com.mware.ge.cypher.internal.util.attribution.SequentialIdGen
import com.mware.ge.cypher.internal.util.test_helpers.CypherTestSupport

import scala.language.implicitConversions

trait LogicalPlanConstructionTestSupport extends CypherTestSupport {
  self: AstConstructionTestSupport =>

  implicit val idGen = new SequentialIdGen()

  implicit protected def idSymbol(name: Symbol): String = name.name

  class StubSolveds extends Solveds {
    override def set(id: Id, t: PlannerQuery): Unit = {}

    override def isDefinedAt(id: Id): Boolean = true

    override def get(id: Id): PlannerQuery = PlannerQuery.empty

    override def copy(from: Id, to: Id): Unit = {}
  }

  class StubCardinalities extends Cardinalities {
    override def set(id: Id, t: Cardinality): Unit = {}

    override def isDefinedAt(id: Id): Boolean = true

    override def get(id: Id): Cardinality = 0.0

    override def copy(from: Id, to: Id): Unit = {}
  }

  class StubProvidedOrders extends ProvidedOrders {
    override def set(id: Id, t: ProvidedOrder): Unit = {}

    override def isDefinedAt(id: Id): Boolean = true

    override def get(id: Id): ProvidedOrder = ProvidedOrder.empty

    override def copy(from: Id, to: Id): Unit = {}
  }

  def nodeIndexScan(node: String, label: String, property: String) =
    NodeIndexScan(node, LabelToken(label, LabelId("1")), IndexedProperty(PropertyKeyToken(property, PropertyKeyId("1")), GetValue), Set.empty, IndexOrderNone)

  def cached(varAndProp: String): CachedNodeProperty = {
    val array = varAndProp.split("\\.", 2)
    val (v, prop) = (array(0), array(1))
    CachedNodeProperty(v, PropertyKeyName(prop)(pos))(pos)
  }

}

trait AstRewritingTestSupport extends CypherTestSupport with AstConstructionTestSupport {
  val parser = ParserFixture.parser
}
