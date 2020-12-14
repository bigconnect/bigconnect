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
package com.mware.ge.cypher.internal.runtime.interpreted

import com.mware.ge.collection.RawIterator
import com.mware.ge.cypher.exception.ProcedureException
import com.mware.ge.cypher.ge.GeCypherQueryContext
import com.mware.ge.cypher.procedure.impl
import com.mware.ge.cypher.procedure.impl.UserAggregator
import com.mware.ge.values.AnyValue
import com.mware.ge.cypher.internal.runtime.UserDefinedAggregator
import com.mware.ge.cypher.internal.logical.plans.QualifiedName

import scala.collection.JavaConverters._

/**
  * This class contains helpers for calling procedures, user-defined functions and user-defined aggregations.
  */
object CallSupport {

  type KernelProcedureCall = (Array[AnyRef]) => RawIterator[Array[AnyRef], ProcedureException]

  def callFunction(cypherQueryContext: GeCypherQueryContext, id: Int, args: Seq[AnyValue],
                   allowed: Array[String]): AnyValue = {
    if (shouldElevate(cypherQueryContext, allowed))
      cypherQueryContext.procedures().functionCallOverride(id, args.toArray)
    else
      cypherQueryContext.procedures().functionCall(id, args.toArray)
  }

  def callFunction(cypherQueryContext: GeCypherQueryContext, name: QualifiedName, args: Seq[AnyValue],
                   allowed: Array[String]): AnyValue = {
    val kn = new impl.QualifiedName(name.namespace.asJava, name.name)
    if (shouldElevate(cypherQueryContext, allowed))
      cypherQueryContext.procedures().functionCallOverride(kn, args.toArray)
    else
      cypherQueryContext.procedures().functionCall(kn, args.toArray)
  }

  def callReadOnlyProcedure(cypherQueryContext: GeCypherQueryContext, id: Int, args: Seq[Any],
                            allowed: Array[String]): Iterator[Array[AnyRef]] = {
    val call: KernelProcedureCall =
      if (shouldElevate(cypherQueryContext, allowed))
        cypherQueryContext.procedures().procedureCallReadOverride(id, _)
      else
        cypherQueryContext.procedures().procedureCallRead(id, _)

    callProcedure(args, call)
  }

  def callReadWriteProcedure(cypherQueryContext: GeCypherQueryContext, id: Int, args: Seq[Any],
                             allowed: Array[String]): Iterator[Array[AnyRef]] = {
    val call: KernelProcedureCall =
      if (shouldElevate(cypherQueryContext, allowed))
        cypherQueryContext.procedures().procedureCallWriteOverride(id, _)
      else
        cypherQueryContext.procedures().procedureCallWrite(id, _)
    callProcedure(args, call)
  }

  def callSchemaWriteProcedure(cypherQueryContext: GeCypherQueryContext, id: Int, args: Seq[Any],
                               allowed: Array[String]): Iterator[Array[AnyRef]] = {
    val call: KernelProcedureCall =
      if (shouldElevate(cypherQueryContext, allowed))
        cypherQueryContext.procedures().procedureCallSchemaOverride(id, _)
      else
        cypherQueryContext.procedures().procedureCallSchema(id, _)
    callProcedure(args, call)
  }

  def callDbmsProcedure(cypherQueryContext: GeCypherQueryContext, id: Int, args: Seq[Any], allowed: Array[String]): Iterator[Array[AnyRef]] =
    callProcedure(args,
      cypherQueryContext.procedureCallDbms(
        id,
        _,
        cypherQueryContext.getDependencyResolver,
        cypherQueryContext.securityContext,
        cypherQueryContext.resourceTracker))

  def callReadOnlyProcedure(cypherQueryContext: GeCypherQueryContext, name: QualifiedName, args: Seq[Any],
                            allowed: Array[String]): Iterator[Array[AnyRef]] = {
    val kn = new impl.QualifiedName(name.namespace.asJava, name.name)
    val call: KernelProcedureCall =
      if (shouldElevate(cypherQueryContext, allowed))
        cypherQueryContext.procedures().procedureCallReadOverride(kn, _)
      else
        cypherQueryContext.procedures().procedureCallRead(kn, _)

    callProcedure(args, call)
  }

  def callReadWriteProcedure(cypherQueryContext: GeCypherQueryContext, name: QualifiedName, args: Seq[Any],
                             allowed: Array[String]): Iterator[Array[AnyRef]] = {
    val kn = new impl.QualifiedName(name.namespace.asJava, name.name)
    val call: KernelProcedureCall =
      if (shouldElevate(cypherQueryContext, allowed))
        cypherQueryContext.procedures().procedureCallWriteOverride(kn, _)
      else
        cypherQueryContext.procedures().procedureCallWrite(kn, _)
    callProcedure(args, call)
  }

  def callSchemaWriteProcedure(cypherQueryContext: GeCypherQueryContext, name: QualifiedName, args: Seq[Any],
                               allowed: Array[String]): Iterator[Array[AnyRef]] = {
    val kn = new impl.QualifiedName(name.namespace.asJava, name.name)
    val call: KernelProcedureCall =
      if (shouldElevate(cypherQueryContext: GeCypherQueryContext, allowed))
        cypherQueryContext.procedures().procedureCallSchemaOverride(kn, _)
      else
        cypherQueryContext.procedures().procedureCallSchema(kn, _)
    callProcedure(args, call)
  }

  def callDbmsProcedure(cypherQueryContext: GeCypherQueryContext, name: QualifiedName, args: Seq[Any],
                        allowed: Array[String]): Iterator[Array[AnyRef]] = {
    val kn = new impl.QualifiedName(name.namespace.asJava, name.name)
    callProcedure(args,
      cypherQueryContext.procedureCallDbms(
        kn,
        _,
        cypherQueryContext.getDependencyResolver,
        cypherQueryContext.securityContext,
        cypherQueryContext.resourceTracker))
  }

  def aggregateFunction(cypherQueryContext: GeCypherQueryContext, id: Int, allowed: Array[String]): UserDefinedAggregator = {
    val aggregator: UserAggregator =
      if (shouldElevate(cypherQueryContext, allowed))
        cypherQueryContext.procedures().aggregationFunctionOverride(id)
      else
        cypherQueryContext.procedures().aggregationFunction(id)

    userDefinedAggregator(aggregator)
  }

  def aggregateFunction(cypherQueryContext: GeCypherQueryContext, name: QualifiedName, allowed: Array[String]): UserDefinedAggregator = {
    val kn = new impl.QualifiedName(name.namespace.asJava, name.name)
    val aggregator: UserAggregator =
      if (shouldElevate(cypherQueryContext, allowed))
        cypherQueryContext.procedures().aggregationFunctionOverride(kn)
      else
        cypherQueryContext.procedures().aggregationFunction(kn)

    userDefinedAggregator(aggregator)
  }

  private def callProcedure(args: Seq[Any], call: KernelProcedureCall): Iterator[Array[AnyRef]] = {
    val toArray = args.map(_.asInstanceOf[AnyRef]).toArray
    val read = call(toArray)
    new scala.Iterator[Array[AnyRef]] {
      override def hasNext: Boolean = read.hasNext

      override def next(): Array[AnyRef] = read.next
    }
  }

  private def userDefinedAggregator(aggregator: UserAggregator): UserDefinedAggregator = {
    new UserDefinedAggregator {
      override def result: AnyRef = aggregator.result()

      override def update(args: IndexedSeq[Any]): Unit = {
        val toArray = args.map(_.asInstanceOf[AnyRef]).toArray
        aggregator.update(toArray)
      }
    }
  }

  private def shouldElevate(geCypherQueryContext: GeCypherQueryContext, allowed: Array[String]): Boolean = {
    // We have to be careful with elevation, since we cannot elevate permissions in a nested procedure call
    // above the original allowed procedure mode. We enforce this by checking if mode is already an overridden mode.
    val accessMode = geCypherQueryContext.securityContext().mode()
    allowed.nonEmpty && !accessMode.isOverridden && accessMode.allowsProcedureWith(allowed)
  }
}
