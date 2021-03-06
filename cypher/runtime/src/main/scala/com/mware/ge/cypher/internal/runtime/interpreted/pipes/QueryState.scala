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
import org.eclipse.collections.api.set.primitive.LongSet
import com.mware.ge.cypher.internal.runtime.interpreted.commands.expressions.PathValueBuilder
import com.mware.ge.cypher.internal.runtime.interpreted.commands.predicates.{InCheckContainer, SingleThreadedLRUCache}
import com.mware.ge.cypher.internal.runtime.interpreted.{ExecutionContext, MapExecutionContext, MutableMaps}
import com.mware.ge.cypher.internal.runtime.{QueryContext, QueryStatistics}
import com.mware.ge.values.virtual.MapValue
import com.mware.ge.cypher.internal.util.ParameterNotFoundException

import scala.collection.mutable

class QueryState(val query: QueryContext,
                 val resources: ExternalCSVResource,
                 val params: MapValue,
                 val decorator: PipeDecorator = NullPipeDecorator,
                 val initialContext: Option[ExecutionContext] = None,
                 val triadicState: mutable.Map[String, LongSet] = mutable.Map.empty,
                 val repeatableReads: mutable.Map[Pipe, Seq[ExecutionContext]] = mutable.Map.empty,
                 val cachedIn: SingleThreadedLRUCache[Any, InCheckContainer] = new SingleThreadedLRUCache(maxSize = 16),
                 val lenientCreateRelationship: Boolean = false) {

  private var _pathValueBuilder: PathValueBuilder = _
  private var _exFactory: ExecutionContextFactory = _

  def newExecutionContext(factory: ExecutionContextFactory): ExecutionContext = {
    initialContext match {
      case Some(init) => factory.copyWith(init)
      case None => factory.newExecutionContext()
    }
  }

  def clearPathValueBuilder: PathValueBuilder = {
    if (_pathValueBuilder == null) {
      _pathValueBuilder = new PathValueBuilder()
    }
    _pathValueBuilder.clear()
  }

  def getParam(key: String): AnyValue = {
    if (!params.containsKey(key)) throw new ParameterNotFoundException("Expected a parameter named " + key)
    params.get(key)
  }

  def getStatistics: QueryStatistics = query.getOptStatistics.getOrElse(QueryState.defaultStatistics)

  def withDecorator(decorator: PipeDecorator) =
    new QueryState(query, resources, params, decorator, initialContext, triadicState,
                   repeatableReads, cachedIn, lenientCreateRelationship)

  def withInitialContext(initialContext: ExecutionContext) =
    new QueryState(query, resources, params, decorator, Some(initialContext), triadicState,
                   repeatableReads, cachedIn, lenientCreateRelationship)

  /**
    * When running on the RHS of an Apply, this method will fill an execution context with argument data
    *
    * @param ctx ExecutionContext to fill with data
    */
  def copyArgumentStateTo(ctx: ExecutionContext, nLongs: Int, nRefs: Int): Unit = initialContext
    .foreach(initData => ctx.copyFrom(initData, nLongs, nRefs))

  def copyArgumentStateTo(ctx: ExecutionContext): Unit = initialContext.foreach(initData => initData.copyTo(ctx))

  def withQueryContext(query: QueryContext) =
    new QueryState(query, resources, params, decorator, initialContext, triadicState,
                   repeatableReads, cachedIn, lenientCreateRelationship)

  def setExecutionContextFactory(exFactory: ExecutionContextFactory) = {
    _exFactory = exFactory
  }

  def executionContextFactory = _exFactory
}

object QueryState {

  val defaultStatistics = QueryStatistics()
}

trait ExecutionContextFactory {

  def newExecutionContext(m: mutable.Map[String, AnyValue] = MutableMaps.empty): ExecutionContext

  def newExecutionContext(): ExecutionContext

  def copyWith(init: ExecutionContext): ExecutionContext

  def copyWith(row: ExecutionContext, newEntries: Seq[(String, AnyValue)]): ExecutionContext

  def copyWith(row: ExecutionContext, key: String, value: AnyValue): ExecutionContext

  def copyWith(row: ExecutionContext, key1: String, value1: AnyValue, key2: String, value2: AnyValue): ExecutionContext

  def copyWith(row: ExecutionContext,
               key1: String, value1: AnyValue,
               key2: String, value2: AnyValue,
               key3: String, value3: AnyValue): ExecutionContext
}

case class CommunityExecutionContextFactory() extends ExecutionContextFactory {

  override def newExecutionContext(m: mutable.Map[String, AnyValue] = MutableMaps.empty): ExecutionContext =
    ExecutionContext(m)

  override def newExecutionContext(): ExecutionContext = ExecutionContext.empty

  // Not using polymorphism here, instead cast since the cost of being megamorhpic is too high
  override def copyWith(init: ExecutionContext): ExecutionContext = init match {
    case context: MapExecutionContext =>
      context.createClone()
    case _ =>
      init.createClone()
  }

  // Not using polymorphism here, instead cast since the cost of being megamorhpic is too high
  override def copyWith(row: ExecutionContext, newEntries: Seq[(String, AnyValue)]): ExecutionContext = row match {
    case context: MapExecutionContext =>
      context.copyWith(newEntries)
    case _ =>
      row.copyWith(newEntries)
  }

  // Not using polymorphism here, instead cast since the cost of being megamorhpic is too high
  override def copyWith(row: ExecutionContext, key: String, value: AnyValue): ExecutionContext = row match {
    case context: MapExecutionContext =>
      context.copyWith(key, value)
    case _ =>
      row.copyWith(key, value)
  }

  // Not using polymorphism here, instead cast since the cost of being megamorhpic is too high
  override def copyWith(row : ExecutionContext,
                        key1: String, value1: AnyValue,
                        key2: String, value2: AnyValue): ExecutionContext = row match {
    case context: MapExecutionContext =>
      context.copyWith(key1, value1, key2, value2)
    case _ =>
      row.copyWith(key1, value1, key2, value2)
    }

  // Not using polymorphism here, instead cast since the cost of being megamorhpic is too high
  override def copyWith(row : ExecutionContext,
                        key1: String, value1: AnyValue,
                        key2: String, value2: AnyValue,
                        key3: String, value3: AnyValue): ExecutionContext = row match {
    case context: MapExecutionContext =>
      context.copyWith(key1, value1, key2, value2, key3, value3)
    case _ =>
      row.copyWith(key1, value1, key2, value2, key3, value3)
  }

}
