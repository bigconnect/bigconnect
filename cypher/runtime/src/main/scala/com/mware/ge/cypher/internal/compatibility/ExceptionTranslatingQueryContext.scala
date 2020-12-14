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
package com.mware.ge.cypher.internal.compatibility

import java.net.URL

import com.mware.ge.cypher.Path
import com.mware.ge.Element
import com.mware.ge.cypher.index.{IndexQuery, IndexReference}
import com.mware.ge.cypher.internal.planner.spi.IndexDescriptor
import com.mware.ge.cypher.util.NodeValueIndexCursor
import com.mware.ge.values.AnyValue
import com.mware.ge.cypher.internal.runtime._
import com.mware.ge.cypher.internal.expressions.SemanticDirection
import com.mware.ge.cypher.internal.logical.plans.{IndexOrder, QualifiedName}
import com.mware.ge.values.storable.{TextValue, Value}
import com.mware.ge.values.virtual.{ListValue, MapValue, NodeValue, RelationshipValue}
import com.mware.ge.cypher.internal.runtime.interpreted.DelegatingOperations

import scala.collection.Iterator

class ExceptionTranslatingQueryContext(val inner: QueryContext) extends QueryContext with ExceptionTranslationSupport {

  override def withActiveRead: QueryContext = inner.withActiveRead

  override def resources: ResourceManager = inner.resources

  override def setLabelsOnNode(node: String, labelIds: Iterator[String]): Int =
    translateException(inner.setLabelsOnNode(node, labelIds))

  override def createNode(labels: Array[String], id: Option[AnyValue]): NodeValue =
    translateException(inner.createNode(labels, id))

  override def getLabelsForNode(node: String): ListValue =
    translateException(inner.getLabelsForNode(node))

  override def getLabelName(id: String): String =
    translateException(inner.getLabelName(id))

  override def getOptLabelId(labelName: String): Option[String] =
    translateException(inner.getOptLabelId(labelName))

  override def getLabelId(labelName: String): String =
    translateException(inner.getLabelId(labelName))

  override def getOrCreateLabelId(labelName: String): String =
    translateException(inner.getOrCreateLabelId(labelName))

  override def nodeOps: Operations[NodeValue] =
    new ExceptionTranslatingOperations[NodeValue](inner.nodeOps)

  override def relationshipOps: Operations[RelationshipValue] =
    new ExceptionTranslatingOperations[RelationshipValue](inner.relationshipOps)

  override def removeLabelsFromNode(node: String, labelIds: Iterator[String]): Int =
    translateException(inner.removeLabelsFromNode(node, labelIds))

  override def getPropertyKeyName(propertyKeyId: String): String =
    translateException(inner.getPropertyKeyName(propertyKeyId))

  override def getOptPropertyKeyId(propertyKeyName: String): Option[String] =
    translateException(inner.getOptPropertyKeyId(propertyKeyName))

  override def getPropertyKeyId(propertyKey: String): String =
    translateException(inner.getPropertyKeyId(propertyKey))

  override def getOrCreatePropertyKeyId(propertyKey: String): String =
    translateException(inner.getOrCreatePropertyKeyId(propertyKey))

  override def getOrCreatePropertyKeyIds(propertyKeys: Array[String]): Array[String] =
    translateException(inner.getOrCreatePropertyKeyIds(propertyKeys))

  override def addIndexRule(descriptor: IndexDescriptor) =
    translateException(inner.addIndexRule(descriptor))

  override def dropIndexRule(descriptor: IndexDescriptor) =
    translateException(inner.dropIndexRule(descriptor))

  override def indexSeek[RESULT <: AnyRef](index: IndexReference,
                                           needsValues: Boolean,
                                           indexOrder: IndexOrder,
                                           values: Seq[IndexQuery]): NodeValueIndexCursor =
    translateException(inner.indexSeek(index, needsValues, indexOrder, values))

  override def getNodesByLabel(label: String): Iterator[NodeValue] =
    translateException(inner.getNodesByLabel(label))

  override def nodeAsMap(id: String): MapValue = translateException(inner.nodeAsMap(id))

  override def relationshipAsMap(id: String): MapValue = translateException(inner.relationshipAsMap(id))

  override def nodeGetOutgoingDegree(node: String): Int =
    translateException(inner.nodeGetOutgoingDegree(node))

  override def nodeGetOutgoingDegree(node: String, relationship: String): Int =
    translateException(inner.nodeGetOutgoingDegree(node, relationship))

  override def nodeGetIncomingDegree(node: String): Int =
    translateException(inner.nodeGetIncomingDegree(node))

  override def nodeGetIncomingDegree(node: String, relationship: String): Int =
    translateException(inner.nodeGetIncomingDegree(node, relationship))

  override def nodeGetTotalDegree(node: String): Int = translateException(inner.nodeGetTotalDegree(node))

  override def nodeGetTotalDegree(node: String, relationship: String): Int =
    translateException(inner.nodeGetTotalDegree(node, relationship))

  override def callReadOnlyProcedure(id: Int, args: Seq[Any], allowed: Array[String]): Iterator[Array[AnyRef]] =
    translateIterator(inner.callReadOnlyProcedure(id, args, allowed))

  override def callReadWriteProcedure(id: Int, args: Seq[Any], allowed: Array[String]): Iterator[Array[AnyRef]] =
    translateIterator(inner.callReadWriteProcedure(id, args, allowed))

  override def callSchemaWriteProcedure(id: Int, args: Seq[Any], allowed: Array[String]): Iterator[Array[AnyRef]] =
    translateIterator(inner.callSchemaWriteProcedure(id, args, allowed))

  override def callDbmsProcedure(id: Int, args: Seq[Any], allowed: Array[String]): Iterator[Array[AnyRef]] =
    translateIterator(inner.callDbmsProcedure(id, args, allowed))

  override def callReadOnlyProcedure(name: QualifiedName, args: Seq[Any], allowed: Array[String]): Iterator[Array[AnyRef]] =
    translateIterator(inner.callReadOnlyProcedure(name, args, allowed))

  override def callReadWriteProcedure(name: QualifiedName, args: Seq[Any], allowed: Array[String]): Iterator[Array[AnyRef]] =
    translateIterator(inner.callReadWriteProcedure(name, args, allowed))

  override def callSchemaWriteProcedure(name: QualifiedName, args: Seq[Any], allowed: Array[String]): Iterator[Array[AnyRef]] =
    translateIterator(inner.callSchemaWriteProcedure(name, args, allowed))

  override def callDbmsProcedure(name: QualifiedName, args: Seq[Any], allowed: Array[String]): Iterator[Array[AnyRef]] =
    translateIterator(inner.callDbmsProcedure(name, args, allowed))

  override def callFunction(id: Int, args: Seq[AnyValue], allowed: Array[String]) =
    translateException(inner.callFunction(id, args, allowed))

  override def callFunction(name: QualifiedName, args: Seq[AnyValue], allowed: Array[String]) =
    translateException(inner.callFunction(name, args, allowed))

  override def aggregateFunction(id: Int,
                                 allowed: Array[String]): UserDefinedAggregator =
    translateException(inner.aggregateFunction(id, allowed))

  override def aggregateFunction(name: QualifiedName,
                                 allowed: Array[String]): UserDefinedAggregator =
    translateException(inner.aggregateFunction(name, allowed))

  override def withAnyOpenQueryContext[T](work: (QueryContext) => T): T =
    inner.withAnyOpenQueryContext(qc =>
      translateException(
        work(new ExceptionTranslatingQueryContext(qc))
      ))

  override def isLabelSetOnNode(label: String, node: String): Boolean =
    translateException(inner.isLabelSetOnNode(label, node))

  override def getRelTypeId(relType: String) =
    translateException(inner.getRelTypeId(relType))

  override def getRelTypeName(id: String) =
    translateException(inner.getRelTypeName(id))

  override def lockingUniqueIndexSeek[RESULT](index: IndexReference,
                                              values: Seq[IndexQuery.ExactPredicate]): NodeValueIndexCursor =
    translateException(inner.lockingUniqueIndexSeek(index, values))

  override def getImportURL(url: URL) =
    translateException(inner.getImportURL(url))

  override def relationshipGetStartNode(relationships: RelationshipValue) =
    translateException(inner.relationshipGetStartNode(relationships))

  override def relationshipGetEndNode(relationships: RelationshipValue) =
    translateException(inner.relationshipGetEndNode(relationships))

  override def createRelationship(start: String, end: String, relType: String, id: Option[AnyValue]) =
    translateException(inner.createRelationship(start, end, relType, id))

  override def getOrCreateRelTypeId(relTypeName: String) =
    translateException(inner.getOrCreateRelTypeId(relTypeName))

  override def getRelationshipsForIds(node: String, dir: SemanticDirection, types: Option[Array[String]]) =
    translateException(inner.getRelationshipsForIds(node, dir, types))

   override def indexSeekByContains[RESULT <: AnyRef](index: IndexReference,
                                                     needsValues: Boolean,
                                                     indexOrder: IndexOrder,
                                                     value: TextValue): NodeValueIndexCursor =
    translateException(inner.indexSeekByContains(index, needsValues, indexOrder, value))

  override def indexSeekByEndsWith[RESULT <: AnyRef](index: IndexReference,
                                                     needsValues: Boolean,
                                                     indexOrder: IndexOrder,
                                                     value: TextValue): NodeValueIndexCursor =
    translateException(inner.indexSeekByEndsWith(index, needsValues, indexOrder, value))

  override def indexScan[RESULT <: AnyRef](index: IndexReference,
                                           needsValues: Boolean,
                                           indexOrder: IndexOrder): NodeValueIndexCursor =
    translateException(inner.indexScan(index, needsValues, indexOrder))

  override def nodeIsDense(node: String) =
    translateException(inner.nodeIsDense(node))

  override def asObject(value: AnyValue): AnyRef =
    translateException(inner.asObject(value))

  override def variableLengthPathExpand(realNode: String, minHops: Option[Int], maxHops: Option[Int], direction: SemanticDirection, relTypes: Seq[String]) =
    translateException(inner.variableLengthPathExpand(realNode, minHops, maxHops, direction, relTypes))

  override def singleShortestPath(left: String, right: String, depth: Int, expander: Expander, pathPredicate: KernelPredicate[Path], filters: Seq[KernelPredicate[Element]]) =
    translateException(inner.singleShortestPath(left, right, depth, expander, pathPredicate, filters))

  override def allShortestPath(left: String, right: String, depth: Int, expander: Expander, pathPredicate: KernelPredicate[Path], filters: Seq[KernelPredicate[Element]]) =
    translateException(inner.allShortestPath(left, right, depth, expander, pathPredicate, filters))

  override def nodeCountByCountStore(labelId: String) =
    translateException(inner.nodeCountByCountStore(labelId))

  override def relationshipCountByCountStore(startLabelId: String, typeId: String, endLabelId: String) =
  translateException(inner.relationshipCountByCountStore(startLabelId, typeId, endLabelId))

  override def lockNodes(nodeIds: String*) =
    translateException(inner.lockNodes(nodeIds:_*))

  override def lockRelationships(relIds: String*) =
    translateException(inner.lockRelationships(relIds:_*))

  override def getOptRelTypeId(relType: String) =
    translateException(inner.getOptRelTypeId(relType))

  override def detachDeleteNode(node: String): Int =
    translateException(inner.detachDeleteNode(node))

  override def commit: Unit = translateException(inner.commit)

  override def assertSchemaWritesAllowed(): Unit = translateException(inner.assertSchemaWritesAllowed())

  class ExceptionTranslatingOperations[T](inner: Operations[T])
    extends DelegatingOperations[T](inner) {
    override def delete(id: String) =
      translateException(inner.delete(id))

    override def setProperty(id: String, propertyKey: String, value: Value) =
      translateException(inner.setProperty(id, propertyKey, value))

    override def getById(id: String): T =
      translateException(inner.getById(id))

    override def getProperty(id: String, propertyKeyId: String): Value =
      translateException(inner.getProperty(id, propertyKeyId))

    override def hasProperty(id: String, propertyKeyId: String): Boolean =
      translateException(inner.hasProperty(id, propertyKeyId))

    override def propertyKeyIds(id: String): Array[String] =
      translateException(inner.propertyKeyIds(id))

    override def removeProperty(id: String, propertyKeyId: String) =
      translateException(inner.removeProperty(id, propertyKeyId))

    override def all: Iterator[T] =
      translateException(inner.all)

    override def isDeletedInThisTx(id: String): Boolean =
      translateException(inner.isDeletedInThisTx(id))

    override def getByIdIfExists(id: String): Option[T] =
      translateException(inner.getByIdIfExists(id))
  }

  override def createNewQueryContext() = new ExceptionTranslatingQueryContext(inner.createNewQueryContext())

  override def indexReference(label: String, properties: String*): IndexReference =
    translateException(inner.indexReference(label, properties:_*))
}

