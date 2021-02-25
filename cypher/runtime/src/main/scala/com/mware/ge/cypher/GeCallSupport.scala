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
package com.mware.ge.cypher

import java.time.Clock

import com.mware.ge.collection.RawIterator
import com.mware.ge.cypher.exception.ProcedureException
import com.mware.ge.cypher.ge.GeCypherQueryContext
import com.mware.ge.cypher.procedure.impl.{BasicContext, Context}
import com.mware.ge.values.AnyValue
import com.mware.ge.cypher.internal.logical.plans.QualifiedName
import scala.collection.JavaConverters._

/**
  * This class contains helpers for calling procedures, user-defined functions and user-defined aggregations.
  */
object GeCallSupport {
  type CypherProcedureCall = (Array[AnyRef]) => RawIterator[Array[AnyRef], ProcedureException]

  def callFunction(queryContext: GeQueryContext, id: Int, args: Seq[AnyValue], allowed: Array[String]): AnyValue = {
    queryContext.executionEngine.getProcedures.callFunction(newCallContext(queryContext.queryContext), id, args.toArray)
  }

  def callFunction(queryContext: GeQueryContext, name: QualifiedName, args: Seq[AnyValue], allowed: Array[String]): AnyValue = {
    val kn = new com.mware.ge.cypher.procedure.impl.QualifiedName(name.namespace.asJava, name.name)
    queryContext.executionEngine.getProcedures.callFunction(newCallContext(queryContext.queryContext), kn, args.toArray)
  }

  def callReadOnlyProcedure(queryContext: GeQueryContext, id: Int, args: Seq[Any], allowed: Array[String]): Iterator[Array[AnyRef]] = {
    var call: CypherProcedureCall = queryContext.executionEngine.callProcedure(newCallContext(queryContext.queryContext), id, _)
    callProcedure(args, call)
  }

  def callReadOnlyProcedure(queryContext: GeQueryContext, name: QualifiedName, args: Seq[Any], allowed: Array[String]): Iterator[Array[AnyRef]] = {
    val kn = new com.mware.ge.cypher.procedure.impl.QualifiedName(name.namespace.asJava, name.name)
    var call: CypherProcedureCall = queryContext.executionEngine.callProcedure(newCallContext(queryContext.queryContext), kn, _)
    callProcedure(args, call)
  }

  def callReadWriteProcedure(queryContext: GeQueryContext, id: Int, args: Seq[Any], allowed: Array[String]): Iterator[Array[AnyRef]] = {
    callReadOnlyProcedure(queryContext, id, args, allowed)
  }

  def callReadWriteProcedure(queryContext: GeQueryContext, name: QualifiedName, args: Seq[Any], allowed: Array[String]): Iterator[Array[AnyRef]] = {
    callReadOnlyProcedure(queryContext, name, args, allowed)
  }

  private def newCallContext(cypherQueryContext: GeCypherQueryContext): BasicContext = {
    val ctx = new BasicContext
    ctx.put(Context.THREAD, Thread.currentThread)
    ctx.put(Context.SYSTEM_CLOCK, Clock.systemUTC())
    ctx.put(Context.STATEMENT_CLOCK, Clock.systemUTC())
    ctx.put(Context.DEPENDENCY_RESOLVER, cypherQueryContext.getDependencyResolver)
    ctx.put(Context.CYPHER_QUERY_CONTEXT, cypherQueryContext)
    ctx.put(Context.SECURITY_CONTEXT, cypherQueryContext.securityContext())
    return ctx;
  }

  private def callProcedure(args: Seq[Any], call: CypherProcedureCall): Iterator[Array[AnyRef]] = {
    val toArray = args.map(_.asInstanceOf[AnyRef]).toArray
    val read = call(toArray)
    new scala.Iterator[Array[AnyRef]] {
      override def hasNext: Boolean = read.hasNext

      override def next(): Array[AnyRef] = read.next
    }
  }
}
