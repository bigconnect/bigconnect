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

import java.util.concurrent.atomic.AtomicInteger
import com.mware.ge.cypher.internal.expressions.SemanticDirection
import com.mware.ge.cypher.internal.runtime.{Operations, QueryContext, QueryStatistics}
import com.mware.ge.values.AnyValue
import com.mware.ge.values.storable.Value
import com.mware.ge.values.virtual.{NodeValue, RelationshipValue}

class UpdateCountingQueryContext(inner: QueryContext) extends DelegatingQueryContext(inner) {

  override def createNewQueryContext(): QueryContext = inner.createNewQueryContext()

  private val nodesCreated = new Counter
  private val relationshipsCreated = new Counter
  private val propertiesSet = new Counter
  private val nodesDeleted = new Counter
  private val relationshipsDeleted = new Counter
  private val labelsAdded = new Counter
  private val labelsRemoved = new Counter
  private val indexesAdded = new Counter
  private val indexesRemoved = new Counter
  private val uniqueConstraintsAdded = new Counter
  private val uniqueConstraintsRemoved = new Counter
  private val propertyExistenceConstraintsAdded = new Counter
  private val propertyExistenceConstraintsRemoved = new Counter
  private val nodekeyConstraintsAdded = new Counter
  private val nodekeyConstraintsRemoved = new Counter

  def getStatistics = QueryStatistics(
    nodesCreated = nodesCreated.count,
    relationshipsCreated = relationshipsCreated.count,
    propertiesSet = propertiesSet.count,
    nodesDeleted = nodesDeleted.count,
    labelsAdded = labelsAdded.count,
    labelsRemoved = labelsRemoved.count,
    relationshipsDeleted = relationshipsDeleted.count,
    indexesAdded = indexesAdded.count,
    indexesRemoved = indexesRemoved.count,
    uniqueConstraintsAdded = uniqueConstraintsAdded.count,
    uniqueConstraintsRemoved = uniqueConstraintsRemoved.count,
    existenceConstraintsAdded = propertyExistenceConstraintsAdded.count,
    existenceConstraintsRemoved = propertyExistenceConstraintsRemoved.count,
    nodekeyConstraintsAdded = nodekeyConstraintsAdded.count,
    nodekeyConstraintsRemoved = nodekeyConstraintsRemoved.count)

  override def getOptStatistics = Some(getStatistics)

  override def createNode(labels: Array[String], id: Option[AnyValue]) = {
    nodesCreated.increase()
    labelsAdded.increase(labels.length)
    inner.createNode(labels, id)
  }

  override def nodeOps: Operations[NodeValue] =
    new CountingOps[NodeValue](inner.nodeOps, nodesDeleted)

  override def relationshipOps: Operations[RelationshipValue] =
    new CountingOps[RelationshipValue](inner.relationshipOps, relationshipsDeleted)

  override def setLabelsOnNode(node: String, labelIds: Iterator[String]): Int = {
    val added = inner.setLabelsOnNode(node, labelIds)
    labelsAdded.increase(added)
    added
  }

  override def createRelationship(start: String, end: String, relType: String, id: Option[AnyValue]) = {
    relationshipsCreated.increase()
    inner.createRelationship(start, end, relType, id)
  }

  override def removeLabelsFromNode(node: String, labelIds: Iterator[String]): Int = {
    val removed = inner.removeLabelsFromNode(node, labelIds)
    labelsRemoved.increase(removed)
    removed
  }

  override def nodeGetDegree(node: String, dir: SemanticDirection): Int = super.nodeGetDegree(node, dir)

  override def detachDeleteNode(node: String): Int = {
    nodesDeleted.increase()
    val count = inner.detachDeleteNode(node)
    relationshipsDeleted.increase(count)
    count
  }

  class Counter {
    val counter: AtomicInteger = new AtomicInteger()

    def count: Int = counter.get()

    def increase(amount: Int = 1) {
      counter.addAndGet(amount)
    }
  }

  private class CountingOps[T](inner: Operations[T], deletes: Counter)
    extends DelegatingOperations[T](inner) {

    override def delete(id: String) {
      deletes.increase()
      inner.delete(id)
    }

    override def removeProperty(id: String, propertyKeyId: String) {
      propertiesSet.increase()
      inner.removeProperty(id, propertyKeyId)
    }

    override def setProperty(id: String, propertyKeyId: String, value: Value) {
      propertiesSet.increase()
      inner.setProperty(id, propertyKeyId, value)
    }
  }
}
