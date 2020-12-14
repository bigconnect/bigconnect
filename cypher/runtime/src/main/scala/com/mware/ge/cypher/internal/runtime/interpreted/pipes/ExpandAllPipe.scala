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

import com.mware.ge.values.AnyValue
import com.mware.ge.cypher.internal.runtime.interpreted.ExecutionContext
import com.mware.ge.cypher.internal.expressions.SemanticDirection
import com.mware.ge.cypher.internal.util.InternalException
import com.mware.ge.cypher.internal.util.attribution.Id
import com.mware.ge.values.storable.Values
import com.mware.ge.values.virtual.{NodeValue, RelationshipValue}

case class ExpandAllPipe(source: Pipe,
                         fromName: String,
                         relName: String,
                         toName: String,
                         dir: SemanticDirection,
                         types: LazyTypes)
                        (val id: Id = Id.INVALID_ID) extends PipeWithSource(source) {

  protected def internalCreateResults(input: Iterator[ExecutionContext], state: QueryState): Iterator[ExecutionContext] = {
    input.flatMap {
      row =>
        getFromNode(row) match {
          case n: NodeValue =>
            val relationships: Iterator[RelationshipValue] = state.query.getRelationshipsForIds(n.id(), dir, types.types(state.query))
            relationships.map { r =>
                val other = r.otherNode(n)
                executionContextFactory.copyWith(row, relName, r, toName, other)
            }

          case Values.NO_VALUE => None

          case value => throw new InternalException(s"Expected to find a node at '$fromName' but found $value instead")
        }
    }
  }

  def typeNames = types.names

  def getFromNode(row: ExecutionContext): AnyValue =
    row.getOrElse(fromName, throw new InternalException(s"Expected to find a node at '$fromName' but found nothing"))
}
