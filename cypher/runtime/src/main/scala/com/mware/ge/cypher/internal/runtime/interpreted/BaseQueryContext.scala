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

import com.mware.ge.cypher.Path
import com.mware.ge.Element
import com.mware.ge.cypher.index.{IndexQuery, IndexReference}
import com.mware.ge.cypher.internal.planner.spi.{IdempotentResult, IndexDescriptor}
import com.mware.ge.cypher.util.NodeValueIndexCursor
import com.mware.ge.values.AnyValue
import com.mware.ge.cypher.internal.runtime._
import com.mware.ge.cypher.internal.expressions.SemanticDirection
import com.mware.ge.cypher.internal.logical.plans.{IndexOrder, QualifiedName}
import com.mware.ge.values.storable.TextValue
import com.mware.ge.values.virtual.{ListValue, MapValue, NodeValue, RelationshipValue}

import scala.collection.Iterator

abstract class BaseQueryContext extends QueryContext {

  def notSupported(): Nothing

  override def withActiveRead: QueryContext = notSupported()

  override def resources: ResourceManager = notSupported()

  override def nodeOps: Operations[NodeValue] = notSupported()

  override def relationshipOps: Operations[RelationshipValue] = notSupported()

  override def createNode(labels: Array[String], id: Option[AnyValue]): NodeValue = notSupported()

  override def createRelationship(start: String, end: String,
                                  relType: String, id: Option[AnyValue]): RelationshipValue = notSupported()

  override def nodeIsDense(node: String): Boolean = false

  override def getOrCreateRelTypeId(relTypeName: String): String = notSupported()

  override def getRelationshipsForIds(node: String, dir: SemanticDirection,
                                      types: Option[Array[String]]): Iterator[RelationshipValue] = notSupported()

  override def getOrCreateLabelId(labelName: String): String = notSupported()

  override def isLabelSetOnNode(label: String, node: String): Boolean = notSupported()

  override def setLabelsOnNode(node: String, labelIds: Iterator[String]): Int = notSupported()

  override def removeLabelsFromNode(node: String, labelIds: Iterator[String]): Int = notSupported()

  override def getOrCreatePropertyKeyId(propertyKey: String): String = notSupported()

  override def getOrCreatePropertyKeyIds(propertyKeys: Array[String]): Array[String] = notSupported()

  override def addIndexRule(descriptor: IndexDescriptor): IdempotentResult[IndexReference] = notSupported()
  override def dropIndexRule(descriptor: IndexDescriptor): Unit = notSupported()

  override def indexReference(label: String, properties: String*): IndexReference = notSupported()

  override def getNodesByLabel(id: String): Iterator[NodeValue] = notSupported()

  override def getImportURL(url: URL): Either[String, URL] = notSupported()

  /**
    * This should not be used. We'll remove sooner (or later). Don't do it.
    */
  override def withAnyOpenQueryContext[T](work: QueryContext => T): T = notSupported()

  override def createNewQueryContext(): QueryContext = notSupported()

  override def asObject(value: AnyValue): AnyRef = notSupported()

  override def variableLengthPathExpand(realNode: String, minHops: Option[Int],
                                        maxHops: Option[Int],
                                        direction: SemanticDirection,
                                        relTypes: Seq[String]): Iterator[Path] = notSupported()

  override def singleShortestPath(left: String, right: String, depth: Int,
                                  expander: Expander,
                                  pathPredicate: KernelPredicate[Path],
                                  filters: Seq[KernelPredicate[Element]]): Option[Path] = notSupported()

  override def allShortestPath(left: String, right: String, depth: Int,
                               expander: Expander,
                               pathPredicate: KernelPredicate[Path],
                               filters: Seq[KernelPredicate[Element]]): Iterator[Path] = notSupported()

  override def nodeCountByCountStore(labelId: String): Long = notSupported()

  override def relationshipCountByCountStore(startLabelId: String, typeId: String, endLabelId: String): Long = notSupported()

  override def lockNodes(nodeIds: String*): Unit = notSupported()

  override def lockRelationships(relIds: String*): Unit = notSupported()

  override def callReadOnlyProcedure(id: Int, args: Seq[Any],
                                     allowed: Array[String]): Iterator[Array[AnyRef]] = notSupported()

  override def callReadOnlyProcedure(name: QualifiedName,
                                     args: Seq[Any],
                                     allowed: Array[String]): Iterator[Array[AnyRef]] = notSupported()

  override def callReadWriteProcedure(id: Int, args: Seq[Any],
                                      allowed: Array[String]): Iterator[Array[AnyRef]] = notSupported()

  override def callReadWriteProcedure(name: QualifiedName,
                                      args: Seq[Any],
                                      allowed: Array[String]): Iterator[Array[AnyRef]] = notSupported()

  override def callSchemaWriteProcedure(id: Int, args: Seq[Any],
                                        allowed: Array[String]): Iterator[Array[AnyRef]] = notSupported()

  override def callSchemaWriteProcedure(name: QualifiedName,
                                        args: Seq[Any],
                                        allowed: Array[String]): Iterator[Array[AnyRef]] = notSupported()

  override def callDbmsProcedure(id: Int, args: Seq[Any],
                                 allowed: Array[String]): Iterator[Array[AnyRef]] = notSupported()

  override def callDbmsProcedure(name: QualifiedName,
                                 args: Seq[Any],
                                 allowed: Array[String]): Iterator[Array[AnyRef]] = notSupported()

  override def callFunction(id: Int, args: Seq[AnyValue],
                            allowed: Array[String]): AnyValue = notSupported()

  override def callFunction(name: QualifiedName,
                            args: Seq[AnyValue],
                            allowed: Array[String]): AnyValue = notSupported()

  override def aggregateFunction(id: Int,
                                 allowed: Array[String]): UserDefinedAggregator = notSupported()

  override def aggregateFunction(name: QualifiedName,
                                 allowed: Array[String]): UserDefinedAggregator = notSupported()

  override def detachDeleteNode(id: String): Int = notSupported()

  override def assertSchemaWritesAllowed(): Unit = notSupported()

  override def getLabelName(id: String): String = notSupported()

  override def getOptLabelId(labelName: String): Option[String] = notSupported()

  override def getLabelId(labelName: String): String = notSupported()

  override def getOptPropertyKeyId(propertyKeyName: String): Option[String] = notSupported()

  override def getPropertyKeyId(propertyKeyName: String): String = notSupported()

  override def getRelTypeName(id: String): String = notSupported()

  override def getOptRelTypeId(relType: String): Option[String] = notSupported()

  override def getRelTypeId(relType: String): String = notSupported()

  override def nodeGetOutgoingDegree(node: String): Int = notSupported()

  override def nodeGetOutgoingDegree(node: String, relationship: String): Int = notSupported()

  override def nodeGetIncomingDegree(node: String): Int = notSupported()

  override def nodeGetIncomingDegree(node: String, relationship: String): Int = notSupported()

  override def nodeGetTotalDegree(node: String): Int = notSupported()

  override def nodeGetTotalDegree(node: String, relationship: String): Int = notSupported()

  override def relationshipGetStartNode(relationship: RelationshipValue): NodeValue = notSupported()

  override def relationshipGetEndNode(relationship: RelationshipValue): NodeValue = notSupported()

  override def getLabelsForNode(id: String): ListValue = notSupported()

  override def getPropertyKeyName(token: String): String = notSupported()

  override def nodeAsMap(id: String): MapValue = notSupported()

  override def relationshipAsMap(id: String): MapValue = notSupported()

  override def indexSeek[RESULT <: AnyRef](index: IndexReference,
                                           needsValues: Boolean,
                                           indexOrder: IndexOrder,
                                           queries: Seq[IndexQuery]): NodeValueIndexCursor = notSupported()

  override def indexSeekByContains[RESULT <: AnyRef](index: IndexReference,
                                                     needsValues: Boolean,
                                                     indexOrder: IndexOrder,
                                                     value: TextValue): NodeValueIndexCursor = notSupported()

  override def indexSeekByEndsWith[RESULT <: AnyRef](index: IndexReference,
                                                     needsValues: Boolean,
                                                     indexOrder: IndexOrder,
                                                     value: TextValue): NodeValueIndexCursor = notSupported()

  override def indexScan[RESULT <: AnyRef](index: IndexReference,
                                           needsValues: Boolean,
                                           indexOrder: IndexOrder): NodeValueIndexCursor = notSupported()

  override def lockingUniqueIndexSeek[RESULT](index: IndexReference,
                                              queries: Seq[IndexQuery.ExactPredicate]): NodeValueIndexCursor = notSupported()


  override def commit: Unit = notSupported()
}
