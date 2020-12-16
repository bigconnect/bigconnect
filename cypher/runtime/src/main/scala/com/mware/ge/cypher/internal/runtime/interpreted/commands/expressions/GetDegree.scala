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
package com.mware.ge.cypher.internal.runtime.interpreted.commands.expressions

import com.mware.ge.values.AnyValue
import com.mware.ge.cypher.internal.runtime.QueryContext
import com.mware.ge.cypher.internal.util.CypherTypeException
import com.mware.ge.cypher.internal.runtime.interpreted.ExecutionContext
import com.mware.ge.cypher.internal.runtime.interpreted.commands.AstNode
import com.mware.ge.cypher.internal.runtime.interpreted.commands.values.KeyToken
import com.mware.ge.cypher.internal.runtime.interpreted.pipes.QueryState
import com.mware.ge.cypher.internal.expressions.SemanticDirection
import com.mware.ge.values.storable.Values
import com.mware.ge.values.virtual.NodeValue

case class GetDegree(node: Expression, typ: Option[KeyToken], direction: SemanticDirection) extends NullInNullOutExpression(node) {

  val getDegree: (QueryContext, String) => Long = typ match {
    case None    => (qtx, node) => qtx.nodeGetDegree(node, direction)
    case Some(t) => (qtx, node) => t.getOptId(qtx) match {
      case None            => 0
      case Some(relTypeId) => qtx.nodeGetDegree(node, direction, relTypeId)
    }
  }

  override def compute(value: AnyValue, m: ExecutionContext, state: QueryState): AnyValue = value match {
    case n: NodeValue => Values.longValue(getDegree(state.query, n.id()))
    case other   => throw new CypherTypeException(s"Type mismatch: expected a node but was $other of type ${other.getClass.getSimpleName}")
  }

  override def arguments: Seq[Expression] = Seq(node)

  override def children: Seq[AstNode[_]] = Seq(node) ++ typ

  override def rewrite(f: Expression => Expression): Expression = f(GetDegree(node.rewrite(f), typ, direction))

  override def symbolTableDependencies: Set[String] = node.symbolTableDependencies
}