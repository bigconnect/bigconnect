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

import java.net.URL

import com.mware.ge.Element
import com.mware.ge.cypher.Path
import com.mware.ge.cypher.index.{IndexQuery, IndexReference}
import com.mware.ge.cypher.internal.expressions.SemanticDirection
import com.mware.ge.cypher.internal.logical.plans.{IndexOrder, QualifiedName}
import com.mware.ge.cypher.internal.planner.spi.IndexDescriptor
import com.mware.ge.cypher.internal.runtime._
import com.mware.ge.cypher.util.{NodeValueIndexCursor, RelationshipIterator, StringIterator}
import com.mware.ge.values.AnyValue
import com.mware.ge.values.storable.{TextValue, Value}
import com.mware.ge.values.virtual.{ListValue, MapValue, NodeValue, RelationshipValue}

import scala.collection.Iterator

abstract class DelegatingQueryContext(val inner: QueryContext) extends QueryContext {

  protected def singleDbHit[A](value: A): A = value
  protected def manyDbHits[A](value: Iterator[A]): Iterator[A] = value

  protected def manyDbHits[A](value: StringIterator): StringIterator = value
  protected def manyDbHits[A](value: RelationshipIterator): RelationshipIterator = value
  protected def manyDbHits[A](value: NodeValueIndexCursor): NodeValueIndexCursor = value
  protected def manyDbHits(count: Int): Int = count

  override def resources: ResourceManager = inner.resources

  override def withActiveRead: QueryContext = inner.withActiveRead

  override def setLabelsOnNode(node: String, labelIds: Iterator[String]): Int =
    singleDbHit(inner.setLabelsOnNode(node, labelIds))

  override def createNode(labels: Array[String], id: Option[AnyValue]): NodeValue = singleDbHit(inner.createNode(labels, id))

  override def createRelationship(start: String, end: String, relType: String, id: Option[AnyValue]): RelationshipValue =
    singleDbHit(inner.createRelationship(start, end, relType, id))

  override def getOrCreateRelTypeId(relTypeName: String): String = singleDbHit(inner.getOrCreateRelTypeId(relTypeName))

  override def getLabelsForNode(node: String): ListValue = singleDbHit(inner.getLabelsForNode(node))

  override def getLabelName(id: String): String = singleDbHit(inner.getLabelName(id))

  override def getOptLabelId(labelName: String): Option[String] = singleDbHit(inner.getOptLabelId(labelName))

  override def getLabelId(labelName: String): String = singleDbHit(inner.getLabelId(labelName))

  override def getOrCreateLabelId(labelName: String): String = singleDbHit(inner.getOrCreateLabelId(labelName))

  override def getRelationshipsForIds(node: String, dir: SemanticDirection, types: Option[Array[String]]): Iterator[RelationshipValue] =
  manyDbHits(inner.getRelationshipsForIds(node, dir, types))

  override def nodeOps = inner.nodeOps

  override def relationshipOps = inner.relationshipOps

  override def removeLabelsFromNode(node: String, labelIds: Iterator[String]): Int =
    singleDbHit(inner.removeLabelsFromNode(node, labelIds))

  override def getPropertyKeyName(propertyKeyId: String): String = singleDbHit(inner.getPropertyKeyName(propertyKeyId))

  override def getOptPropertyKeyId(propertyKeyName: String): Option[String] =
    singleDbHit(inner.getOptPropertyKeyId(propertyKeyName))

  override def getPropertyKeyId(propertyKey: String) = singleDbHit(inner.getPropertyKeyId(propertyKey))

  override def getOrCreatePropertyKeyId(propertyKey: String) = singleDbHit(inner.getOrCreatePropertyKeyId(propertyKey))

  override def getOrCreatePropertyKeyIds(propertyKeys: Array[String]): Array[String] = {
    manyDbHits(propertyKeys.length)
    inner.getOrCreatePropertyKeyIds(propertyKeys)
  }

  override def addIndexRule(descriptor: IndexDescriptor) = singleDbHit(inner.addIndexRule(descriptor))

  override def dropIndexRule(descriptor: IndexDescriptor) = singleDbHit(inner.dropIndexRule(descriptor))

  override def indexReference(label: String, properties: String*): IndexReference = singleDbHit(inner.indexReference(label, properties:_*))

  override def indexSeek[RESULT <: AnyRef](index: IndexReference,
                                           needsValues: Boolean,
                                           indexOrder: IndexOrder,
                                           queries: Seq[IndexQuery]): NodeValueIndexCursor =
    manyDbHits(inner.indexSeek(index, needsValues, indexOrder, queries))

  override def indexScan[RESULT <: AnyRef](index: IndexReference,
                                           needsValues: Boolean,
                                           indexOrder: IndexOrder): NodeValueIndexCursor =
    manyDbHits(inner.indexScan(index, needsValues, indexOrder))

  override def indexSeekByContains[RESULT <: AnyRef](index: IndexReference,
                                                     needsValues: Boolean,
                                                     indexOrder: IndexOrder,
                                                     value: TextValue): NodeValueIndexCursor =
    manyDbHits(inner.indexSeekByContains(index, needsValues, indexOrder, value))

  override def indexSeekByEndsWith[RESULT <: AnyRef](index: IndexReference,
                                                     needsValues: Boolean,
                                                     indexOrder: IndexOrder,
                                                     value: TextValue): NodeValueIndexCursor =
    manyDbHits(inner.indexSeekByEndsWith(index, needsValues, indexOrder, value))

  override def getNodesByLabel(id: String): Iterator[NodeValue] = manyDbHits(inner.getNodesByLabel(id))

  override def nodeAsMap(id: String): MapValue = {
    val map = inner.nodeAsMap(id)
    //one hit finding the node, then finding the properies
    manyDbHits(1 + map.size())
    map
  }

  override def relationshipAsMap(id: String): MapValue = {
    val map = inner.relationshipAsMap(id)
    manyDbHits(1 + map.size())
    map
  }

  override def withAnyOpenQueryContext[T](work: (QueryContext) => T): T = inner.withAnyOpenQueryContext(work)

  override def lockingUniqueIndexSeek[RESULT](index: IndexReference,
                                              queries: Seq[IndexQuery.ExactPredicate]): NodeValueIndexCursor =
    singleDbHit(inner.lockingUniqueIndexSeek(index, queries))

  override def getRelTypeId(relType: String): String = singleDbHit(inner.getRelTypeId(relType))

  override def getOptRelTypeId(relType: String): Option[String] = singleDbHit(inner.getOptRelTypeId(relType))

  override def getRelTypeName(id: String): String = singleDbHit(inner.getRelTypeName(id))

  override def getImportURL(url: URL): Either[String,URL] = inner.getImportURL(url)

  override def relationshipGetStartNode(relationship: RelationshipValue) = inner.relationshipGetStartNode(relationship)

  override def relationshipGetEndNode(relationship: RelationshipValue) = inner.relationshipGetEndNode(relationship)


  override def nodeGetOutgoingDegree(node: String): Int = singleDbHit(inner.nodeGetOutgoingDegree(node))

  override def nodeGetOutgoingDegree(node: String, relationship: String): Int = singleDbHit(inner.nodeGetOutgoingDegree(node, relationship))

  override def nodeGetIncomingDegree(node: String): Int = singleDbHit(inner.nodeGetIncomingDegree(node))

  override def nodeGetIncomingDegree(node: String, relationship: String): Int = singleDbHit(inner.nodeGetIncomingDegree(node, relationship))

  override def nodeGetTotalDegree(node: String): Int = singleDbHit(inner.nodeGetTotalDegree(node))

  override def nodeGetTotalDegree(node: String, relationship: String): Int = singleDbHit(inner.nodeGetTotalDegree(node, relationship))

  override def nodeIsDense(node: String): Boolean = singleDbHit(inner.nodeIsDense(node))

  override def variableLengthPathExpand(realNode: String,
                                        minHops: Option[Int],
                                        maxHops: Option[Int],
                                        direction: SemanticDirection,
                                        relTypes: Seq[String]): Iterator[Path] =
    manyDbHits(inner.variableLengthPathExpand(realNode, minHops, maxHops, direction, relTypes))

  override def isLabelSetOnNode(label: String, node: String): Boolean = singleDbHit(inner.isLabelSetOnNode(label, node))

  override def nodeCountByCountStore(labelId: String): Long = singleDbHit(inner.nodeCountByCountStore(labelId))

  override def relationshipCountByCountStore(startLabelId: String, typeId: String, endLabelId: String): Long =
    singleDbHit(inner.relationshipCountByCountStore(startLabelId, typeId, endLabelId))

  override def lockNodes(nodeIds: String*): Unit = inner.lockNodes(nodeIds:_*)

  override def lockRelationships(relIds: String*): Unit = inner.lockRelationships(relIds:_*)

  override def singleShortestPath(left: String, right: String, depth: Int, expander: Expander,
                                  pathPredicate: KernelPredicate[Path],
                                  filters: Seq[KernelPredicate[Element]]): Option[Path] =
    singleDbHit(inner.singleShortestPath(left, right, depth, expander, pathPredicate, filters))

  override def allShortestPath(left: String, right: String, depth: Int, expander: Expander,
                               pathPredicate: KernelPredicate[Path],
                               filters: Seq[KernelPredicate[Element]]): Iterator[Path] =
    manyDbHits(inner.allShortestPath(left, right, depth, expander, pathPredicate, filters))

  override def callReadOnlyProcedure(id: Int, args: Seq[Any], allowed: Array[String]) =
    singleDbHit(inner.callReadOnlyProcedure(id, args, allowed))

  override def callReadWriteProcedure(id: Int, args: Seq[Any], allowed: Array[String]) =
    singleDbHit(inner.callReadWriteProcedure(id, args, allowed))

  override def callSchemaWriteProcedure(id: Int, args: Seq[Any], allowed: Array[String]) =
    singleDbHit(inner.callSchemaWriteProcedure(id, args, allowed))

  override def callDbmsProcedure(id: Int, args: Seq[Any], allowed: Array[String]) =
    inner.callDbmsProcedure(id, args, allowed)

  override def callReadOnlyProcedure(name: QualifiedName, args: Seq[Any], allowed: Array[String]) =
    singleDbHit(inner.callReadOnlyProcedure(name, args, allowed))

  override def callReadWriteProcedure(name: QualifiedName, args: Seq[Any], allowed: Array[String]) =
    singleDbHit(inner.callReadWriteProcedure(name, args, allowed))

  override def callSchemaWriteProcedure(name: QualifiedName, args: Seq[Any], allowed: Array[String]) =
    singleDbHit(inner.callSchemaWriteProcedure(name, args, allowed))

  override def callDbmsProcedure(name: QualifiedName, args: Seq[Any], allowed: Array[String]) =
    inner.callDbmsProcedure(name, args, allowed)

  override def callFunction(id: Int, args: Seq[AnyValue], allowed: Array[String]) =
    singleDbHit(inner.callFunction(id, args, allowed))

  override def callFunction(name: QualifiedName, args: Seq[AnyValue], allowed: Array[String]) =
    singleDbHit(inner.callFunction(name, args, allowed))

  override def aggregateFunction(id: Int,
                                 allowed: Array[String]): UserDefinedAggregator =
    singleDbHit(inner.aggregateFunction(id, allowed))

  override def aggregateFunction(name: QualifiedName,
                                 allowed: Array[String]): UserDefinedAggregator =
    singleDbHit(inner.aggregateFunction(name, allowed))

  override def detachDeleteNode(node: String): Int = manyDbHits(inner.detachDeleteNode(node))

  override def assertSchemaWritesAllowed(): Unit = inner.assertSchemaWritesAllowed()

  override def asObject(value: AnyValue): AnyRef = inner.asObject(value)

  override def commit(): Unit = inner.commit
}

class DelegatingOperations[T](protected val inner: Operations[T]) extends Operations[T] {

  protected def singleDbHit[A](value: A): A = value
  protected def manyDbHits[A](value: Iterator[A]): Iterator[A] = value

  protected def manyDbHits[A](value: StringIterator): StringIterator = value

  override def delete(id: String): Unit = singleDbHit(inner.delete(id))

  override def setProperty(obj: String, propertyKey: String, value: Value): Unit =
    singleDbHit(inner.setProperty(obj, propertyKey, value))

  override def getById(id: String): T = inner.getById(id)

  override def getProperty(obj: String, propertyKeyId: String): Value = singleDbHit(inner.getProperty(obj, propertyKeyId))

  override def hasProperty(obj: String, propertyKeyId: String): Boolean = singleDbHit(inner.hasProperty(obj, propertyKeyId))

  override def propertyKeyIds(obj: String): Array[String] = singleDbHit(inner.propertyKeyIds(obj))

  override def removeProperty(obj: String, propertyKeyId: String): Unit = singleDbHit(inner.removeProperty(obj, propertyKeyId))

  override def all: Iterator[T] = manyDbHits(inner.all)

  override def isDeletedInThisTx(id: String): Boolean = inner.isDeletedInThisTx(id)

  override def acquireExclusiveLock(obj: String): Unit = inner.acquireExclusiveLock(obj)

  override def releaseExclusiveLock(obj: String): Unit = inner.releaseExclusiveLock(obj)

  override def getByIdIfExists(id: String): Option[T] = singleDbHit(inner.getByIdIfExists(id))
}
