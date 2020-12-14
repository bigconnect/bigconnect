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
package com.mware.ge.cypher.internal.compatibility.runtime.executionplan.procs

import java.util

import com.mware.ge.collection.Iterators
import com.mware.ge.io.ResourceIterator
import com.mware.ge.cypher.internal.runtime._
import com.mware.ge.cypher.result.QueryResult.QueryResultVisitor
import com.mware.ge.cypher.result.RuntimeResult.ConsumptionState
import com.mware.ge.cypher.result.{QueryProfile, RuntimeResult}

/**
  * Empty result, as produced by a schema write.
  */
case class SchemaWriteRuntimeResult(ctx: QueryContext) extends RuntimeResult {

  override def fieldNames(): Array[String] = Array.empty

  override def accept[EX <: Exception](visitor: QueryResultVisitor[EX]): Unit = {}

  override def queryStatistics(): QueryStatistics = ctx.getOptStatistics.getOrElse(QueryStatistics())

  override def isIterable: Boolean = true

  override def asIterator(): ResourceIterator[util.Map[String, AnyRef]] = Iterators.emptyResourceIterator()

  override def consumptionState: RuntimeResult.ConsumptionState = ConsumptionState.EXHAUSTED

  override def close(): Unit = {}

  override def queryProfile(): QueryProfile = QueryProfile.NONE
}


