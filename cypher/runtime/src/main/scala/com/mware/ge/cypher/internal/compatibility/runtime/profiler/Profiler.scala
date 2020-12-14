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
package com.mware.ge.cypher.internal.compatibility.runtime.profiler

import com.mware.ge.cypher.util.{NodeValueIndexCursor, RelationshipIterator, RelationshipVisitor, StringIterator}
import com.mware.ge.cypher.internal.compatibility.runtime.helpers.PrimitiveLongHelper
import com.mware.ge.cypher.internal.runtime.interpreted.pipes.{Pipe, PipeDecorator, QueryState}
import com.mware.ge.cypher.internal.runtime.interpreted.{DelegatingOperations, DelegatingQueryContext, ExecutionContext}
import com.mware.ge.cypher.internal.runtime.{Operations, QueryContext}
import com.mware.ge.cypher.internal.util.attribution.Id
import com.mware.ge.values.storable.Value
import com.mware.ge.values.virtual.{NodeValue, RelationshipValue}

class Profiler(stats: InterpretedProfileInformation) extends PipeDecorator {
  outerProfiler =>

  private var parentPipe: Option[Pipe] = None

  def decorate(pipe: Pipe, iter: Iterator[ExecutionContext]): Iterator[ExecutionContext] = {
    val oldCount = stats.rowMap.get(pipe.id).map(_.count).getOrElse(0L)
    val resultIter =
      new ProfilingIterator(iter, oldCount, pipe.id)

    stats.rowMap(pipe.id) = resultIter
    resultIter
  }

  def decorate(pipe: Pipe, state: QueryState): QueryState = {
    val decoratedContext = stats.dbHitsMap.getOrElseUpdate(pipe.id, state.query match {
      case p: ProfilingPipeQueryContext => new ProfilingPipeQueryContext(p.inner, pipe)
      case _ => new ProfilingPipeQueryContext(state.query, pipe)
    })

    state.withQueryContext(decoratedContext)
  }

  def innerDecorator(owningPipe: Pipe): PipeDecorator = new PipeDecorator {
    innerProfiler =>

    def innerDecorator(pipe: Pipe): PipeDecorator = innerProfiler

    def decorate(pipe: Pipe, state: QueryState): QueryState =
      outerProfiler.decorate(owningPipe, state)

    def decorate(pipe: Pipe, iter: Iterator[ExecutionContext]): Iterator[ExecutionContext] = iter
  }

  def registerParentPipe(pipe: Pipe): Unit =
    parentPipe = Some(pipe)
}

trait Counter {
  protected var _count = 0L
  def count: Long = _count

  def increment() {
    _count += 1L
  }
}

final class ProfilingPipeQueryContext(inner: QueryContext, val p: Pipe)
  extends DelegatingQueryContext(inner) with Counter {
  self =>

  override def createNewQueryContext() = new ProfilingPipeQueryContext(inner.createNewQueryContext(), p)

  override protected def singleDbHit[A](value: A): A = {
    increment()
    value
  }

  override protected def manyDbHits[A](value: Iterator[A]): Iterator[A] = {
    increment()
    value.map {
      (v) =>
        increment()
        v
    }
  }

  override protected def manyDbHits[A](value: StringIterator): StringIterator = {
    increment()
    PrimitiveLongHelper.mapStringPrimitive(value, { x =>
      increment()
      x
    })
    null
  }

  override protected def manyDbHits[A](inner: RelationshipIterator): RelationshipIterator = new RelationshipIterator {
    increment()
    override def relationshipVisit[EXCEPTION <: Exception](relationshipId: String, visitor: RelationshipVisitor[EXCEPTION]): Boolean =
      inner.relationshipVisit(relationshipId, visitor)

    override def next(): String = {
      increment()
      inner.next()
    }

    override def hasNext: Boolean = inner.hasNext

    override def close(): Unit = {}
  }

  override protected def manyDbHits[A](inner: NodeValueIndexCursor): NodeValueIndexCursor = new NodeValueIndexCursor {

    override def hasValue: Boolean = inner.hasValue

    override def propertyValue(propertyName: String): Value = inner.propertyValue(propertyName)

    override def nodeReference(): String = inner.nodeReference()

    override def next(): Boolean = {
      increment()
      inner.next()
    }

    override def close(): Unit = inner.close()

    override def isClosed: Boolean = inner.isClosed
  }

  class ProfilerOperations[T](inner: Operations[T]) extends DelegatingOperations[T](inner) {
    override protected def singleDbHit[A](value: A): A = self.singleDbHit(value)
    override protected def manyDbHits[A](value: Iterator[A]): Iterator[A] = self.manyDbHits(value)

    override protected def manyDbHits[A](value: StringIterator): StringIterator = self.manyDbHits(value)
  }

  override def nodeOps: Operations[NodeValue] = new ProfilerOperations(inner.nodeOps)
  override def relationshipOps: Operations[RelationshipValue] = new ProfilerOperations(inner.relationshipOps)
}

class ProfilingIterator(inner: Iterator[ExecutionContext], startValue: Long, pipeId: Id) extends Iterator[ExecutionContext]
  with Counter {

  _count = startValue
  private var updatedStatistics = false

  def hasNext: Boolean = {
    val hasNext = inner.hasNext
    if (!hasNext && !updatedStatistics) {
      // TODO: update statistics
      updatedStatistics = true
    }
    hasNext
  }

  def next(): ExecutionContext = {
    increment()
    inner.next()
  }
}
