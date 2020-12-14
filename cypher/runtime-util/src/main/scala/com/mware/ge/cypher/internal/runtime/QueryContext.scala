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
package com.mware.ge.cypher.internal.runtime

import java.net.URL

import com.mware.ge.cypher.Path
import com.mware.ge.Element
import com.mware.ge.cypher.index.{IndexQuery, IndexReference}
import com.mware.ge.cypher.internal.planner.spi.{IdempotentResult, IndexDescriptor, TokenContext}
import com.mware.ge.cypher.util.NodeValueIndexCursor
import com.mware.ge.values.AnyValue
import com.mware.ge.cypher.internal.expressions.SemanticDirection
import com.mware.ge.cypher.internal.logical.plans.{IndexOrder, QualifiedName}
import com.mware.ge.values.storable.{TextValue, Value}
import com.mware.ge.values.virtual.{NodeValue, RelationshipValue}

import scala.collection.Iterator

trait QueryContext extends TokenContext with DbAccess {
  def withActiveRead: QueryContext

  def resources: ResourceManager

  def nodeOps: Operations[NodeValue]

  def relationshipOps: Operations[RelationshipValue]

  def createNode(labels: Array[String], id: Option[AnyValue]): NodeValue

  def createRelationship(start: String, end: String, relType: String, id: Option[AnyValue]): RelationshipValue

  def getOrCreateRelTypeId(relTypeName: String): String

  def getRelationshipsForIds(node: String, dir: SemanticDirection, types: Option[Array[String]]): Iterator[RelationshipValue]

  def getOrCreateLabelId(labelName: String): String

  def isLabelSetOnNode(label: String, node: String): Boolean

  def setLabelsOnNode(node: String, labelIds: Iterator[String]): Int

  def removeLabelsFromNode(node: String, labelIds: Iterator[String]): Int

  def getOrCreatePropertyKeyId(propertyKey: String): String = propertyKey

  def getOrCreatePropertyKeyIds(propertyKeys: Array[String]): Array[String]

  def addIndexRule(descriptor: IndexDescriptor): IdempotentResult[IndexReference]

  def dropIndexRule(descriptor: IndexDescriptor)

  def indexReference(label: String, properties: String*): IndexReference

  def indexSeek[RESULT <: AnyRef](index: IndexReference,
                                  needsValues: Boolean,
                                  indexOrder: IndexOrder,
                                  queries: Seq[IndexQuery]): NodeValueIndexCursor

  def indexSeekByContains[RESULT <: AnyRef](index: IndexReference,
                                            needsValues: Boolean,
                                            indexOrder: IndexOrder,
                                            value: TextValue): NodeValueIndexCursor

  def indexSeekByEndsWith[RESULT <: AnyRef](index: IndexReference,
                                            needsValues: Boolean,
                                            indexOrder: IndexOrder,
                                            value: TextValue): NodeValueIndexCursor

  def indexScan[RESULT <: AnyRef](index: IndexReference,
                                  needsValues: Boolean,
                                  indexOrder: IndexOrder): NodeValueIndexCursor

  def lockingUniqueIndexSeek[RESULT](index: IndexReference, queries: Seq[IndexQuery.ExactPredicate]): NodeValueIndexCursor

  def getNodesByLabel(label: String): Iterator[NodeValue]

  def getOptStatistics: Option[QueryStatistics] = None

  def getImportURL(url: URL): Either[String,URL]

  /**
   * This should not be used. We'll remove sooner (or later). Don't do it.
   */
  def withAnyOpenQueryContext[T](work: QueryContext => T): T

  /*
  This is an ugly hack to get multi threading to work
   */
  def createNewQueryContext(): QueryContext

  def nodeGetDegree(node: String, dir: SemanticDirection): Int = dir match {
    case SemanticDirection.OUTGOING => nodeGetOutgoingDegree(node)
    case SemanticDirection.INCOMING => nodeGetIncomingDegree(node)
    case SemanticDirection.BOTH => nodeGetTotalDegree(node)
  }

  def nodeGetDegree(node: String, dir: SemanticDirection, relTypeId: String): Int = dir match {
    case SemanticDirection.OUTGOING => nodeGetOutgoingDegree(node, relTypeId)
    case SemanticDirection.INCOMING => nodeGetIncomingDegree(node, relTypeId)
    case SemanticDirection.BOTH => nodeGetTotalDegree(node, relTypeId)
  }

  def nodeIsDense(node: String): Boolean

  def asObject(value: AnyValue): AnyRef

  // Legacy dependency between kernel and compiler
  def variableLengthPathExpand(realNode: String, minHops: Option[Int], maxHops: Option[Int], direction: SemanticDirection, relTypes: Seq[String]): Iterator[Path]

  def singleShortestPath(left: String, right: String, depth: Int, expander: Expander, pathPredicate: KernelPredicate[Path],
                         filters: Seq[KernelPredicate[Element]]): Option[Path]

  def allShortestPath(left: String, right: String, depth: Int, expander: Expander, pathPredicate: KernelPredicate[Path],
                      filters: Seq[KernelPredicate[Element]]): Iterator[Path]

  def nodeCountByCountStore(labelId: String): Long

  def relationshipCountByCountStore(startLabelId: String, typeId: String, endLabelId: String): Long

  def lockNodes(nodeIds: String*)

  def lockRelationships(relIds: String*)

  def callReadOnlyProcedure(id: Int, args: Seq[Any], allowed: Array[String]): Iterator[Array[AnyRef]]
  def callReadOnlyProcedure(name: QualifiedName, args: Seq[Any], allowed: Array[String]): Iterator[Array[AnyRef]]

  def callReadWriteProcedure(id: Int, args: Seq[Any], allowed: Array[String]): Iterator[Array[AnyRef]]
  def callReadWriteProcedure(name: QualifiedName, args: Seq[Any], allowed: Array[String]): Iterator[Array[AnyRef]]

  def callSchemaWriteProcedure(id: Int, args: Seq[Any], allowed: Array[String]): Iterator[Array[AnyRef]]
  def callSchemaWriteProcedure(name: QualifiedName, args: Seq[Any], allowed: Array[String]): Iterator[Array[AnyRef]]

  def callDbmsProcedure(id: Int, args: Seq[Any], allowed: Array[String]): Iterator[Array[AnyRef]]
  def callDbmsProcedure(name: QualifiedName, args: Seq[Any], allowed: Array[String]): Iterator[Array[AnyRef]]

  def callFunction(id: Int, args: Seq[AnyValue], allowed: Array[String]): AnyValue
  def callFunction(name: QualifiedName, args: Seq[AnyValue], allowed: Array[String]): AnyValue

  def aggregateFunction(id: Int, allowed: Array[String]): UserDefinedAggregator
  def aggregateFunction(name: QualifiedName, allowed: Array[String]): UserDefinedAggregator

  def detachDeleteNode(id: String): Int

  def assertSchemaWritesAllowed(): Unit

  override def nodeById(id: String): NodeValue = nodeOps.getById(id)

  override def relationshipById(id: String): RelationshipValue = relationshipOps.getById(id)

  override def propertyKey(name: String): String = name

  override def nodeLabel(name: String): String = name

  override def relationshipType(name: String): String = name

  override def nodeProperty(node: String, property: String): Value = nodeOps.getProperty(node, property)

  override def nodePropertyIds(node: String): Array[String] = nodeOps.propertyKeyIds(node)

  override def nodeHasProperty(node: String, property: String): Boolean = nodeOps.hasProperty(node, property)

  override def relationshipProperty(relationship: String, property: String): Value = relationshipOps.getProperty(relationship, property)

  override def relationshipPropertyIds(relationship: String): Array[String] = relationshipOps.propertyKeyIds(relationship)

  override def relationshipHasProperty(relationship: String, property: String): Boolean =
    relationshipOps.hasProperty(relationship, property)

  def commit: Unit
}

trait Operations[T] {
  def delete(id: String)

  def setProperty(obj: String, propertyKeyId: String, value: Value)

  def removeProperty(obj: String, propertyKeyId: String)

  def getProperty(obj: String, propertyKeyId: String): Value

  def hasProperty(obj: String, propertyKeyId: String): Boolean

  def propertyKeyIds(obj: String): Array[String]

  def getById(id: String): T

  def isDeletedInThisTx(id: String): Boolean

  def all: Iterator[T]

  def acquireExclusiveLock(obj: String): Unit

  def releaseExclusiveLock(obj: String): Unit

  def getByIdIfExists(id: String): Option[T]
}

trait KernelPredicate[T] {
  def test(obj: T): Boolean
}

trait Expander {
  def addRelationshipFilter(newFilter: KernelPredicate[Element]): Expander
  def addNodeFilter(newFilter: KernelPredicate[Element]): Expander
  def nodeFilters: Seq[KernelPredicate[Element]]
  def relFilters: Seq[KernelPredicate[Element]]
}

trait UserDefinedAggregator {
  def update(args: IndexedSeq[Any]): Unit
  def result: Any
}

trait CloseableResource {
  def close(success: Boolean)
}

object NodeValueHit {
  val EMPTY = new NodeValueHit(null, null)
}

class NodeValueHit(val nodeId: String, val values: Array[Value]) extends NodeValueIndexCursor {

  private var _next = nodeId != null

  override def hasValue: Boolean = true

  override def propertyValue(propertyName: String): Value = {
    val propIndex = values.indexOf(propertyName)
    values(propIndex)
  }

  override def nodeReference(): String = nodeId

  override def next(): Boolean = {
    val temp = _next
    _next = false
    temp
  }

  override def close(): Unit = _next = false

  override def isClosed: Boolean = _next
}
