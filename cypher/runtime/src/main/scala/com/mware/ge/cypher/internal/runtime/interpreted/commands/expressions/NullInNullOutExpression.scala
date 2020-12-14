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
import com.mware.ge.cypher.internal.runtime.interpreted.ExecutionContext
import com.mware.ge.cypher.internal.runtime.interpreted.pipes.QueryState
import com.mware.ge.values.storable.Values

abstract class NullInNullOutExpression(argument: Expression) extends Expression {
  def compute(value: AnyValue, m: ExecutionContext, state:QueryState): AnyValue

  override def apply(ctx: ExecutionContext, state: QueryState): AnyValue = argument(ctx, state) match {
    case x if x == Values.NO_VALUE => Values.NO_VALUE
    case x    => compute(x, ctx, state)
  }
}
