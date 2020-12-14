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
package com.mware.ge.cypher.internal.compatibility.runtime

import java.io.PrintWriter
import java.util
import java.util.Collections

import com.mware.ge.collection.Iterators
import com.mware.ge.io.ResourceIterator
import com.mware.ge.cypher.internal.runtime._
import com.mware.ge.cypher.internal.runtime.planDescription.InternalPlanDescription
import com.mware.ge.cypher.result.QueryResult.QueryResultVisitor
import com.mware.ge.cypher.Result.ResultVisitor
import com.mware.ge.cypher.notification.Notification

case class ExplainExecutionResult(fieldNames: Array[String],
                                  planDescription: InternalPlanDescription,
                                  queryType: InternalQueryType,
                                  notifications: Set[Notification])
  extends InternalExecutionResult {

  override def initiate(): Unit = {}

  override def javaIterator: ResourceIterator[util.Map[String, AnyRef]] = Iterators.emptyResourceIterator()
  override def javaColumns: util.List[String] = Collections.emptyList()

  override def queryStatistics() = QueryStatistics()

  override def dumpToString(writer: PrintWriter): Unit = writer.print(dumpToString)

  override val dumpToString: String =
     """+--------------------------------------------+
       || No data returned, and nothing was changed. |
       |+--------------------------------------------+
       |""".stripMargin

  override def javaColumnAs[T](column: String): ResourceIterator[T] = Iterators.emptyResourceIterator()

  override def isClosed: Boolean = true

  override def close(reason: CloseReason): Unit = {}

  override def accept[EX <: Exception](visitor: ResultVisitor[EX]): Unit = {}
  override def accept[EX <: Exception](visitor: QueryResultVisitor[EX]): Unit = {}

  override def executionMode: ExecutionMode = ExplainMode

  override def executionPlanDescription(): InternalPlanDescription = planDescription
}
