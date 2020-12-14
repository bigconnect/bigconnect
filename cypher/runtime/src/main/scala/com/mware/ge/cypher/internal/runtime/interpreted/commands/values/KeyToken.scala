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
package com.mware.ge.cypher.internal.runtime.interpreted.commands.values

import com.mware.ge.cypher.internal.planner.spi.TokenContext
import com.mware.ge.values.AnyValue
import com.mware.ge.cypher.internal.runtime.QueryContext
import com.mware.ge.cypher.internal.runtime.interpreted.commands.expressions.Expression
import com.mware.ge.cypher.internal.runtime.interpreted.pipes.QueryState
import com.mware.ge.cypher.internal.runtime.interpreted.ExecutionContext
import com.mware.ge.cypher.internal.runtime.interpreted.commands.AstNode

/*
KeyTokens are things with name and id. KeyTokens makes it possible to look up the id
at compile time and embed it into the execution plan if it's available.
 */
sealed abstract class KeyToken(typ: TokenType) extends Expression {

  def name: String

  def getOrCreateId(state: QueryContext): String
  def getIdOrFail(state: TokenContext): String
  def getOptId(state: TokenContext): Option[String]
  def getOptName(): Option[String] = Some(name)

  def resolve(tokenContext: TokenContext): KeyToken

  override def arguments: Seq[Expression] = Seq.empty

  override def rewrite(f: Expression => Expression): KeyToken = f(this).asInstanceOf[KeyToken]

  override def symbolTableDependencies: Set[String] = Set.empty

  override def apply(ctx: ExecutionContext, state: QueryState): AnyValue = throw new NotImplementedError()
}

object KeyToken {

  case class Unresolved(name: String, typ: TokenType) extends KeyToken(typ) {
    override def getOrCreateId(state: QueryContext): String = typ.getOrCreateIdForName(name, state)
    override def getIdOrFail(state: TokenContext): String = typ.getIdForNameOrFail(name, state)
    override def getOptId(state: TokenContext): Option[String] = typ.getOptIdForName(name, state)

    override def resolve(tokenContext: TokenContext): KeyToken  = getOptId(tokenContext).map(Resolved(name, _, typ)).getOrElse(this)

    override def children: Seq[AstNode[_]] = Seq.empty

    override def toString:String = name
  }

  case class Resolved(name: String, id: String, typ: TokenType) extends KeyToken(typ) {
    override def getOrCreateId(state: QueryContext): String = id
    override def getIdOrFail(state: TokenContext): String = id
    override def getOptId(state: TokenContext): Option[String] = Some(id)

    override def resolve(tokenContext: TokenContext): Resolved = this

    override def children: Seq[AstNode[_]] = Seq.empty

    override def toString:String = s"$name($id)"
  }

  object Ordering extends Ordering[KeyToken] {
    override def compare(x: KeyToken, y: KeyToken): Int = implicitly[Ordering[String]].compare(x.name, y.name)
  }
}

object UnresolvedLabel {
  def apply(name: String): KeyToken = KeyToken.Unresolved(name, TokenType.Label)
}

object UnresolvedProperty {
  def apply(name: String): KeyToken = KeyToken.Unresolved(name, TokenType.PropertyKey)
}

object UnresolvedRelType {
  def apply(name: String): KeyToken = KeyToken.Unresolved(name, TokenType.RelType)
}
