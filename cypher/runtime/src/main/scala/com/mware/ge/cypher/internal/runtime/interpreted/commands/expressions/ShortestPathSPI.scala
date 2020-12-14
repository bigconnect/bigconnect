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

import com.mware.ge.cypher.Path
import com.mware.ge.{Element, Vertex}
import com.mware.ge.cypher.internal.runtime.{Expander, KernelPredicate}
import com.mware.ge.cypher.internal.expressions.SemanticDirection

abstract class BaseExpander() extends Expander {
  override def addRelationshipFilter(newFilter: KernelPredicate[Element]): Expander =
    newWith(newRelFilters = relFilters :+ newFilter)

  override def addNodeFilter(newFilter: KernelPredicate[Element]): Expander =
    newWith(newNodeFilters = nodeFilters :+ newFilter)

  protected def newWith(newNodeFilters: Seq[KernelPredicate[Element]] = nodeFilters,
                        newRelFilters: Seq[KernelPredicate[Element]] = relFilters): Expander
}

case class OnlyDirectionExpander(override val nodeFilters: Seq[KernelPredicate[Element]],
                                 override val relFilters: Seq[KernelPredicate[Element]],
                                 direction: SemanticDirection) extends BaseExpander {

  override protected def newWith(newNodeFilters: Seq[KernelPredicate[Element]],
                                 newRelFilters: Seq[KernelPredicate[Element]]): OnlyDirectionExpander =
    copy(nodeFilters = newNodeFilters, relFilters = newRelFilters)
}

case class TypeAndDirectionExpander(override val nodeFilters: Seq[KernelPredicate[Element]],
                                    override val relFilters: Seq[KernelPredicate[Element]],
                                    typDirs: Seq[(String, SemanticDirection)]) extends BaseExpander {

  override protected def newWith(newNodeFilters: Seq[KernelPredicate[Element]],
                                 newRelFilters: Seq[KernelPredicate[Element]]): TypeAndDirectionExpander =
    copy(nodeFilters = newNodeFilters, relFilters = newRelFilters)

  def add(typ: String, dir: SemanticDirection): TypeAndDirectionExpander =
    copy(typDirs = typDirs :+ typ -> dir)
}

object Expanders {
  def typeDir(): TypeAndDirectionExpander = TypeAndDirectionExpander(Seq.empty, Seq.empty, Seq.empty)
  def allTypes(dir: SemanticDirection): Expander = OnlyDirectionExpander(Seq.empty, Seq.empty, dir)
}

trait ShortestPathAlgo {
  def findSinglePath(var1: Vertex, var2: Vertex): Path
  def findAllPaths(var1: Vertex, var2: Vertex): Iterable[Path]
}
