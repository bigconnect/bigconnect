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
package com.mware.ge.cypher.internal.runtime.interpreted.pipes

import com.mware.ge.Element
import com.mware.ge.cypher.internal.runtime.interpreted.ExecutionContext
import com.mware.ge.cypher.internal.runtime.planDescription.Argument

trait EntityProducer[T <: Element] extends ((ExecutionContext, QueryState) => Iterator[T]) {
  def producerType: String
  def arguments: Seq[Argument]
}

object EntityProducer {
  def apply[T <: Element](nameStr: String, argument: Argument)(f:(ExecutionContext, QueryState) => Iterator[T]) =
    new EntityProducer[T] {
      def producerType = nameStr
      def arguments: Seq[Argument] = Seq(argument)
      def apply(m: ExecutionContext, q: QueryState) = f(m, q)
    }
}