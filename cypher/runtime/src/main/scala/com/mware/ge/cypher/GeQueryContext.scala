/*
 * This file is part of the BigConnect project.
 *
 * Copyright (c) 2013-2020 MWARE SOLUTIONS SRL
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License version 3
 * as published by the Free Software Foundation with the addition of the
 * following permission added to Section 15 as permitted in Section 7(a):
 * FOR ANY PART OF THE COVERED WORK IN WHICH THE COPYRIGHT IS OWNED BY
 * MWARE SOLUTIONS SRL, MWARE SOLUTIONS SRL DISCLAIMS THE WARRANTY OF
 * NON INFRINGEMENT OF THIRD PARTY RIGHTS

 * This program is distributed in the hope that it will be useful, but
 * WITHOUT ANY WARRANTY; without even the implied warranty of MERCHANTABILITY
 * or FITNESS FOR A PARTICULAR PURPOSE.
 * See the GNU Affero General Public License for more details.
 * You should have received a copy of the GNU Affero General Public License
 * along with this program; if not, see http://www.gnu.org/licenses or write to
 * the Free Software Foundation, Inc., 51 Franklin Street, Fifth Floor,
 * Boston, MA, 02110-1301 USA, or download the license from the following URL:
 * https://www.gnu.org/licenses/agpl-3.0.txt
 *
 * The interactive user interfaces in modified source and object code versions
 * of this program must display Appropriate Legal Notices, as required under
 * Section 5 of the GNU Affero General Public License.
 *
 * You can be released from the requirements of the license by purchasing
 * a commercial license. Buying such a license is mandatory as soon as you
 * develop commercial activities involving the BigConnect software without
 * disclosing the source code of your own applications.
 *
 * These activities include: offering paid services to customers as an ASP,
 * embedding the product in a web application, shipping BigConnect with a
 * closed source product.
 */
package com.mware.ge.cypher

import java.net.URL

import com.mware.core.model.schema.{SchemaConstants, SchemaRepository}
import com.mware.ge._
import com.mware.ge.cypher.ge.GeCypherQueryContext
import com.mware.ge.cypher.index.{IndexQuery, IndexReference, SearchIndexReader}
import com.mware.ge.cypher.util.{DefaultValueMapper, NodeValueIndexCursor}
import com.mware.ge.mutation.VertexMutation
import com.mware.ge.query.QueryBase
import com.mware.ge.query.aggregations.{TermsAggregation, TermsResult}
import com.mware.ge.search.SearchIndex
import org.apache.commons.lang3.StringUtils
import com.mware.ge.cypher.internal.runtime._
import com.mware.ge.cypher.internal.runtime.interpreted.IndexDescriptorCompatibility
import com.mware.ge.cypher.internal.expressions.SemanticDirection
import com.mware.ge.cypher.internal.logical.plans._
import com.mware.ge.cypher.index.IndexQuery.ExactPredicate
import com.mware.ge.cypher.internal.planner.spi.{IdempotentResult, IndexDescriptor}
import com.mware.ge.cypher.values.virtual.{GeEdgeBuilderWrappingValue, GeEdgeWrappingValue, GeVertexMutationWrappingNodeValue, GeVertexWrappingNodeValue, GeWrappingPath}
import com.mware.ge.values.{AnyValue, ValueMapper}
import com.mware.ge.values.storable.{TextValue, ValueGroup, Values}
import com.mware.ge.values.virtual.PathValue.DirectPathValue
import com.mware.ge.values.virtual._

import scala.collection.Iterator
import scala.collection.JavaConverters._

class GeQueryContext(val executionEngine: GeCypherExecutionEngine,
                     val resources: ResourceManager,
                     val queryContext: GeCypherQueryContext) extends BaseGeContext(executionEngine, queryContext) with QueryContext with IndexDescriptorCompatibility {

  private lazy val valueMapper: ValueMapper[java.lang.Object] = new DefaultValueMapper()
  private val indexReader = new SearchIndexReader(queryContext)

  override def withActiveRead: QueryContext = this

  override def createNode(labels: Array[String], id: Option[AnyValue]): NodeValue = {
    if (labels.length > 1)
      throw new UnsupportedOperationException("Cannot create a vertex with multiple concept types");

    var conceptType = SchemaRepository.THING_CONCEPT_NAME;
    if (!labels.isEmpty)
      conceptType = labels(0)

    if (conceptType == null) {
      throw new IllegalStateException("Concept type with id=" + labels(0) + " does not exist");
    }

    val v = queryContext.createVertex(conceptType, java.util.Optional.ofNullable(id.orNull))

    new GeVertexMutationWrappingNodeValue(v.asInstanceOf[VertexMutation], queryContext);
  }

  override def createRelationship(start: String, end: String, relType: String, id: Option[AnyValue]): RelationshipValue = {
    if (relType == null) {
      throw new IllegalStateException("Edge label with id=" + relType + " does not exist");
    }

    val e = queryContext.createEdge(start, end, relType, java.util.Optional.ofNullable(id.orNull))
    new GeEdgeBuilderWrappingValue(e.asInstanceOf[EdgeBuilderBase], queryContext);
  }

  override def getOrCreateRelTypeId(relTypeName: String): String = {
    val rel = queryContext.getSchemaRepository.getRelationshipByName(relTypeName, queryContext.getWorkspaceId)
    if (rel == null) {
      queryContext.createNewRelationship(relTypeName, queryContext.getWorkspaceId);
    }
    relTypeName
  }

  override def getRelationshipsForIds(node: String, dir: SemanticDirection, types: Option[Array[String]]): Iterator[RelationshipValue] = {
    val geDir = dir match {
      case SemanticDirection.INCOMING => Direction.IN
      case SemanticDirection.OUTGOING => Direction.OUT
      case SemanticDirection.BOTH => Direction.BOTH
    }

    queryContext.getEdgesForVertex(node, geDir, java.util.Optional.ofNullable(types.orNull))
      .asScala
  }

  override def getOrCreateLabelId(labelName: String): String = {
    val concept = queryContext.getSchemaRepository.getConceptByName(labelName, queryContext.getWorkspaceId);

    if (concept == null)
      queryContext.createNewConcept(labelName, queryContext.getWorkspaceId)

    labelName
  }

  override def setLabelsOnNode(node: String, labelIds: Iterator[String]): Int = {
    val list = labelIds.toList

    if (list.size > 1)
      throw new UnsupportedOperationException("Cannot create a vertex with multiple concept types");

    if (list.isEmpty)
      return 0

    val concept = queryContext.getSchemaRepository.getConceptByName(list.head, queryContext.getWorkspaceId)
    if (concept != null) {
      queryContext.setConceptType(node, list.head)
      1
    } else
      0
  }

  override def removeLabelsFromNode(node: String, labelIds: Iterator[String]): Int = {
    val list = labelIds.toList

    if (list.size > 1)
      throw new UnsupportedOperationException("Cannot create a vertex with multiple concept types");

    if (list.isEmpty)
      return 0

    queryContext.setConceptType(node, SchemaConstants.CONCEPT_TYPE_THING)

    1
  }

  override def getOrCreatePropertyKeyIds(propertyKeys: Array[String]): Array[String] = propertyKeys

  override def addIndexRule(descriptor: IndexDescriptor): IdempotentResult[IndexReference] = {
    throw new UnsupportedOperationException("not supported");
    //
    //    val kernelDescriptor = cypherToKernelSchema(descriptor)
    //    try {
    //      IdempotentResult(queryContext.indexCreate(kernelDescriptor))
    //    } catch {
    //      case _: AlreadyIndexedException =>
    //        val indexReference = queryContext.index(kernelDescriptor.getLabelId, kernelDescriptor.getPropertyIds: _*)
    //        if (queryContext.indexGetState(indexReference) == InternalIndexState.FAILED) {
    //          val message = queryContext.indexGetFailure(indexReference)
    //          throw new com.mware.ge.cypher.internal.util.FailedIndexException(indexReference.userDescription, message)
    //        }
    //        IdempotentResult(indexReference, wasCreated = false)
    //    }
  }

  override def dropIndexRule(descriptor: IndexDescriptor): Unit = {
    throw new UnsupportedOperationException("not supported");

    //    val kernelDescriptor = cypherToKernelSchema(descriptor)
    //    queryContext.dropIndex(kernelDescriptor)
  }

  override def indexReference(label: String, properties: String*): IndexReference =
    queryContext.index(label, properties: _*)

  val RANGE_SEEKABLE_VALUE_GROUPS = Array(ValueGroup.NUMBER,
    ValueGroup.TEXT,
    ValueGroup.GEOMETRY,
    ValueGroup.DATE,
    ValueGroup.LOCAL_DATE_TIME,
    ValueGroup.ZONED_DATE_TIME,
    ValueGroup.LOCAL_TIME,
    ValueGroup.ZONED_TIME,
    ValueGroup.DURATION)

  private def asKernelIndexOrder(indexOrder: IndexOrder): com.mware.ge.cypher.index.IndexOrder = indexOrder match {
    case IndexOrderAscending => com.mware.ge.cypher.index.IndexOrder.ASCENDING
    case IndexOrderDescending => com.mware.ge.cypher.index.IndexOrder.DESCENDING
    case IndexOrderNone => com.mware.ge.cypher.index.IndexOrder.NONE
  }

  override def indexSeek[RESULT <: AnyRef](index: IndexReference,
                                           needsValues: Boolean,
                                           indexOrder: IndexOrder,
                                           predicates: Seq[IndexQuery]): NodeValueIndexCursor = {

    val impossiblePredicate =
      predicates.exists {
        case p: IndexQuery.ExactPredicate => p.value() == Values.NO_VALUE
        case p: IndexQuery =>
          !RANGE_SEEKABLE_VALUE_GROUPS.contains(p.valueGroup())
      }

    if (impossiblePredicate) NodeValueIndexCursor.EMPTY
    else seek(index, needsValues, indexOrder, predicates: _*)
  }

  override def indexSeekByContains[RESULT <: AnyRef](index: IndexReference,
                                                     needsValues: Boolean,
                                                     indexOrder: IndexOrder,
                                                     value: TextValue): NodeValueIndexCursor =
    seek(index, needsValues, indexOrder, IndexQuery.stringContains(index.properties()(0), value))

  override def indexSeekByEndsWith[RESULT <: AnyRef](index: IndexReference,
                                                     needsValues: Boolean,
                                                     indexOrder: IndexOrder,
                                                     value: TextValue): NodeValueIndexCursor =
    seek(index, needsValues, indexOrder, IndexQuery.stringSuffix(index.properties()(0), value))


  private def seek[RESULT <: AnyRef](index: IndexReference,
                                     needsValues: Boolean,
                                     indexOrder: IndexOrder,
                                     queries: IndexQuery*): NodeValueIndexCursor = {

    val actualValues =
      if (needsValues && queries.forall(_.isInstanceOf[ExactPredicate]))
      // We don't need property values from the index for an exact seek
      queries.map(_.asInstanceOf[ExactPredicate].value()).toArray
        else
        null

    val needsValuesFromIndexSeek = actualValues == null && needsValues
    indexReader.seek(index, asKernelIndexOrder(indexOrder), needsValuesFromIndexSeek, queries: _*)
  }

  override def indexScan[RESULT <: AnyRef](index: IndexReference, needsValues: Boolean, indexOrder: IndexOrder): NodeValueIndexCursor = {
    indexReader.scan(index, asKernelIndexOrder(indexOrder), needsValues)
  }

  override def lockingUniqueIndexSeek[RESULT](index: IndexReference, queries: Seq[IndexQuery.ExactPredicate]): NodeValueIndexCursor = {
    if (queries.exists(q => q.value() == Values.NO_VALUE))
      NodeValueHit.EMPTY
    else {
      NodeValueHit.EMPTY
    }
  }

  override def getNodesByLabel(label: String): Iterator[NodeValue] = {
    queryContext.getNodesWithConceptType(label)
      .iterator()
      .asScala
  }

  override def getImportURL(url: URL): Either[String, URL] = Right(url)

  override def withAnyOpenQueryContext[T](work: QueryContext => T): T = work(this)

  override def createNewQueryContext(): QueryContext = this

  override def nodeIsDense(nodeId: String): Boolean = {
    // a node is considered dense if it has > 50 edges
    nodeGetTotalDegree(nodeId) > 50
  }

  override def asObject(value: AnyValue): AnyRef = {
    value match {
      case v: GeVertexWrappingNodeValue => v.getVertex;
      case _: GeVertexMutationWrappingNodeValue => throw new IllegalStateException("Should not be here")
      case e: GeEdgeWrappingValue => e.getEdge
      case _: GeEdgeBuilderWrappingValue => throw new IllegalStateException("Should not be here")
      case p: DirectPathValue => new GeWrappingPath(queryContext.getGraph, p, queryContext.getAuthorizations)
      case _ => value.map(valueMapper)
    }
  }

  override def variableLengthPathExpand(realNode: String, minHops: Option[Int], maxHops: Option[Int], direction: SemanticDirection, relTypes: Seq[String]): Iterator[Path] = ???

  override def singleShortestPath(left: String, right: String, depth: Int, expander: Expander, pathPredicate: KernelPredicate[Path], filters: Seq[KernelPredicate[Element]]): Option[Path] = ???

  override def allShortestPath(left: String, right: String, depth: Int, expander: Expander, pathPredicate: KernelPredicate[Path], filters: Seq[KernelPredicate[Element]]): Iterator[Path] = ???

  override def nodeCountByCountStore(labelId: String): Long = {
    if (StringUtils.isEmpty(labelId)) {
      val q = queryContext.getGraph.query(queryContext.getAuthorizations)
        .limit(0L)

      withResources(q.vertices())(
        iterable =>
          iterable.getTotalHits
      )
    } else {
      val q = queryContext.getGraph.query(queryContext.getAuthorizations)
        .limit(0L)
        .addAggregation(new TermsAggregation("count", SearchIndex.CONCEPT_TYPE_FIELD_NAME))

      val found = withResources(q.vertexIds(IdFetchHint.NONE))(
        iterable =>
          iterable.getAggregationResult("count", classOf[TermsResult])
            .getBuckets.asScala
            .find(b => b.key == labelId)
      )

      if (found.isEmpty) 0 else found.get.count
    }
  }

  override def relationshipCountByCountStore(startLabelId: String, typeId: String, endLabelId: String): Long = {
    val q = queryContext.getGraph.query(queryContext.getAuthorizations).asInstanceOf[QueryBase]

    if (!StringUtils.isEmpty(startLabelId))
      q.hasOutVertexTypes(startLabelId)

    if (!StringUtils.isEmpty(typeId)) {
      q.hasEdgeLabel(typeId)
    }

    if (!StringUtils.isEmpty(endLabelId)) {
      q.hasInVertexTypes(endLabelId)
    }

    q.getParameters.setLimit(0L);

    withResources(q.edgeIds(IdFetchHint.NONE))(iterable =>
      iterable.getTotalHits
    )
  }

  override def lockNodes(nodeIds: String*): Unit = {}

  override def lockRelationships(relIds: String*): Unit = {}

  override def callReadOnlyProcedure(id: Int, args: Seq[Any], allowed: Array[String]): Iterator[Array[AnyRef]] = {
    GeCallSupport.callReadOnlyProcedure(this, id, args, allowed)
  }

  override def callReadOnlyProcedure(name: QualifiedName, args: Seq[Any], allowed: Array[String]): Iterator[Array[AnyRef]] = {
    GeCallSupport.callReadOnlyProcedure(this, name, args, allowed)
  }

  override def callReadWriteProcedure(id: Int, args: Seq[Any], allowed: Array[String]): Iterator[Array[AnyRef]] = {
    GeCallSupport.callReadWriteProcedure(this, id, args, allowed)
  }

  override def callReadWriteProcedure(name: QualifiedName, args: Seq[Any], allowed: Array[String]): Iterator[Array[AnyRef]] = {
    GeCallSupport.callReadWriteProcedure(this, name, args, allowed)
  }

  override def callSchemaWriteProcedure(id: Int, args: Seq[Any], allowed: Array[String]): Iterator[Array[AnyRef]] = ???

  override def callSchemaWriteProcedure(name: QualifiedName, args: Seq[Any], allowed: Array[String]): Iterator[Array[AnyRef]] = ???

  override def callDbmsProcedure(id: Int, args: Seq[Any], allowed: Array[String]): Iterator[Array[AnyRef]] = {
    GeCallSupport.callReadOnlyProcedure(this, id, args, allowed)
  }

  override def callDbmsProcedure(name: QualifiedName, args: Seq[Any], allowed: Array[String]): Iterator[Array[AnyRef]] = {
    GeCallSupport.callReadOnlyProcedure(this, name, args, allowed)
  }

  override def callFunction(id: Int, args: Seq[AnyValue], allowed: Array[String]): AnyValue = {
    GeCallSupport.callFunction(this, id, args, allowed)
  }

  override def callFunction(name: QualifiedName, args: Seq[AnyValue], allowed: Array[String]): AnyValue = {
    GeCallSupport.callFunction(this, name, args, allowed)
  }

  override def aggregateFunction(id: Int, allowed: Array[String]): UserDefinedAggregator = ???

  override def aggregateFunction(name: QualifiedName, allowed: Array[String]): UserDefinedAggregator = ???

  override def detachDeleteNode(id: String): Int = {
    val edgeCount = queryContext.vertexGetDegree(id, Direction.BOTH, null);
    queryContext.deleteElement(id, ElementType.VERTEX)
    edgeCount
  }

  override def assertSchemaWritesAllowed(): Unit = {}

  override def commit: Unit = {
    queryContext.commit()
  }
}
