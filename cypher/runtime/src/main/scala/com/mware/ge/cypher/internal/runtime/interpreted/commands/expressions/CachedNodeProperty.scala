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

import com.mware.ge.cypher.StatementConstants
import com.mware.ge.cypher.internal.planner.spi.TokenContext
import com.mware.ge.values.AnyValue
import com.mware.ge.cypher.internal.runtime.interpreted.ExecutionContext
import com.mware.ge.cypher.internal.runtime.interpreted.commands.AstNode
import com.mware.ge.cypher.internal.runtime.interpreted.commands.values.KeyToken
import com.mware.ge.cypher.internal.runtime.interpreted.pipes.QueryState
import com.mware.ge.cypher.internal.logical.plans
import com.mware.ge.values.storable.Values
import com.mware.ge.values.virtual.VirtualNodeValue
import com.mware.ge.cypher.internal.util.CypherTypeException

abstract class AbstractCachedNodeProperty extends Expression {

  // abstract stuff

  def getNodeId(ctx: ExecutionContext): String
  def getPropertyKey(tokenContext: TokenContext): String
  def getCachedProperty(ctx: ExecutionContext): AnyValue

  // encapsulated cached-node-property logic

  def apply(ctx: ExecutionContext, state: QueryState): AnyValue = {
    val nodeId = getNodeId(ctx)
    if (nodeId == StatementConstants.NO_SUCH_NODE)
      Values.NO_VALUE
    else {
      getPropertyKey(state.query) match {
        case null => Values.NO_VALUE
        case propId =>
          val cached = getCachedProperty(ctx)
          if (cached == null) // if the cached node property has been invalidated
            state.query.nodeProperty(nodeId, propId)
          else
            cached
      }
    }
  }

  override def rewrite(f: Expression => Expression): Expression = f(this)

  override def arguments: Seq[Expression] = Seq()
}

case class CachedNodeProperty(nodeName: String, propertyKey: KeyToken, key: plans.CachedNodeProperty)
  extends AbstractCachedNodeProperty
{
  override def symbolTableDependencies: Set[String] = Set(nodeName, key.cacheKey)

  override def toString: String = key.cacheKey

  override def getNodeId(ctx: ExecutionContext): String =
    ctx(nodeName) match {
      case Values.NO_VALUE => StatementConstants.NO_SUCH_NODE
      case n: VirtualNodeValue => n.id()
      case other => throw new CypherTypeException(s"Type mismatch: expected a node but was $other")
    }

  override def getCachedProperty(ctx: ExecutionContext): AnyValue = ctx.getCachedProperty(key)

  override def getPropertyKey(tokenContext: TokenContext): String = propertyKey.getOptId(tokenContext).getOrElse(StatementConstants.NO_SUCH_PROPERTY_KEY)

  override def children: Seq[AstNode[_]] = Seq(propertyKey)
}
