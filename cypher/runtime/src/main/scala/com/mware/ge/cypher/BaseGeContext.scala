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

import com.mware.core.model.schema.SchemaRepository
import com.mware.ge._
import com.mware.ge.cypher.ge.GeCypherQueryContext
import com.mware.ge.cypher.internal.planner.spi.TokenContext
import com.mware.ge.cypher.internal.runtime.{DbAccess, Operations}
import com.mware.ge.values.storable.{Value, Values}
import com.mware.ge.values.virtual._
import org.eclipse.collections.api.iterator.LongIterator

import scala.collection.Iterator
import scala.collection.JavaConverters._

abstract class BaseGeContext(executionEngine: GeCypherExecutionEngine, queryContext: GeCypherQueryContext) extends TokenContext with DbAccess with ResourceCloser {
  val nodeOps: NodeOperations = new NodeOperations
  val relationshipOps: RelationshipOperations = new RelationshipOperations

  override def getLabelName(labelId: String): String = {
    labelId
  }

  override def getOptPropertyKeyId(propertyKeyName: String): Option[String] = {
    val ontologyProperty = queryContext.getPropertyByName(propertyKeyName);
    if (ontologyProperty != null)
      Some(propertyKeyName)
    else None
  }

  override def getPropertyKeyId(propertyKeyName: String): String = {
    propertyKeyName
  }

  override def propertyKey(name: String): String = {
    name
  }

  override def getPropertyKeyName(propertyKeyId: String): String = {
    propertyKeyId
  }

  override def getLabelId(labelName: String): String = {
    labelName
  }

  override def getOptLabelId(labelName: String): Option[String] = {
    val concept = queryContext.getSchemaRepository.getConceptByName(labelName, queryContext.getWorkspaceId);
    if (concept != null)
      Some(labelName)
    else None
  }

  override def getOptRelTypeId(relType: String): Option[String] = {
    val rel = queryContext.getSchemaRepository.getRelationshipByName(relType, queryContext.getWorkspaceId);
    if (rel != null) Some(relType) else None
  }

  override def getRelTypeId(relType: String): String = {
    relType
  }

  override def getRelTypeName(id: String): String = {
    id
  }

  override def relationshipType(name: String): String = name

  override def nodeLabel(name: String): String = name

  override def getLabelsForNode(id: String): ListValue = {
    val conceptType = queryContext.getConceptType(id)

    if (conceptType == null || Values.stringValue(SchemaRepository.THING_CONCEPT_NAME) == conceptType) {
      VirtualValues.EMPTY_LIST
    } else {
      VirtualValues.list(conceptType)
    }
  }

  override def isLabelSetOnNode(label: String, node: String): Boolean = {
    if (label == null) false
    else {
      val conceptType = queryContext.getConceptType(node)
      Values.stringValue(label) == conceptType
    }
  }

  override def nodeAsMap(id: String): MapValue = {
    val builder = new MapValueBuilder()
    val propMap = queryContext.getElementProperties(id, ElementType.VERTEX)
    for (p <- propMap.asScala) {
      builder.add(p._1, p._2)
    }
    builder.build()
  }

  override def nodeGetOutgoingDegree(node: String): Int = {
    queryContext.vertexGetDegree(node, Direction.OUT, null);
  }

  override def nodeGetOutgoingDegree(node: String, relTypeName: String): Int = {
    queryContext.vertexGetDegree(node, Direction.OUT, relTypeName);
  }

  override def nodeGetIncomingDegree(node: String): Int = {
    queryContext.vertexGetDegree(node, Direction.IN, null);
  }

  override def nodeGetIncomingDegree(node: String, relationship: String): Int = {
    queryContext.vertexGetDegree(node, Direction.IN, relationship);
  }

  override def nodeGetTotalDegree(node: String): Int = {
    queryContext.vertexGetDegree(node, Direction.BOTH, null);
  }

  override def nodeGetTotalDegree(node: String, relationship: String): Int = {
    queryContext.vertexGetDegree(node, Direction.BOTH, relationship);
  }

  override def relationshipAsMap(id: String): MapValue = {
    val builder = new MapValueBuilder()
    val propMap = queryContext.getElementProperties(id, ElementType.EDGE)
    for (p <- propMap.asScala) {
      builder.add(p._1, p._2)
    }
    builder.build()
  }

  override def relationshipGetStartNode(relationship: RelationshipValue): NodeValue = relationship.startNode()

  override def relationshipGetEndNode(relationship: RelationshipValue): NodeValue = relationship.endNode()

  override def nodeById(id: String): NodeValue = nodeOps.getById(id)

  override def nodeHasProperty(node: String, property: String): Boolean = nodeOps.hasProperty(node, property)

  override def nodeProperty(node: String, property: String): Value = nodeOps.getProperty(node, property)

  override def nodePropertyIds(node: String): Array[String] = nodeOps.propertyKeyIds(node)

  override def relationshipById(id: String): RelationshipValue = relationshipOps.getById(id)

  override def relationshipProperty(relationship: String, property: String): Value = relationshipOps.getProperty(relationship, property)

  override def relationshipPropertyIds(relationship: String): Array[String] = relationshipOps.propertyKeyIds(relationship)

  override def relationshipHasProperty(relationship: String, property: String): Boolean =
    relationshipOps.hasProperty(relationship, property)

  class NodeOperations extends BaseOperations[NodeValue] {
    override def delete(id: String): Unit = {
      queryContext.deleteElement(id, ElementType.VERTEX)
    }

    override def setProperty(nodeId: String, propertyName: String, value: Value): Unit = {
      queryContext.setProperty(nodeId, ElementType.VERTEX, propertyName, value)
    }

    override def removeProperty(id: String, propertyName: String): Unit = {
      queryContext.removeProperty(id, ElementType.VERTEX, propertyName)
    }

    override def getProperty(id: String, propertyName: String): Value = {
      try {
        queryContext.getPropertyValue(id, ElementType.VERTEX, propertyName)
      } catch {
        case _: exception.EntityNotFoundException =>
          throw new EntityNotFoundException(s"Node with id $id was not found")
      }

    }

    override def hasProperty(id: String, propertyKeyId: String): Boolean = {
      queryContext.hasProperty(id, ElementType.VERTEX, propertyKeyId)
    }

    override def propertyKeyIds(id: String): Array[String] = {
      val propSet = queryContext.getElementProperties(id, ElementType.VERTEX).keySet().asScala;
      propSet.toArray
    }

    override def getById(id: String): NodeValue = {
      queryContext.getVertexById(id, true)
    }

    override def isDeletedInThisTx(id: String): Boolean = {
      queryContext.getVertexById(id, false) == null
    }

    override def all: Iterator[NodeValue] = {
      val vertices = queryContext.getVertices;
      asScalaIteratorConverter(vertices).asScala;
    }

    override def acquireExclusiveLock(obj: String): Unit = {}

    override def releaseExclusiveLock(obj: String): Unit = {}

    override def getByIdIfExists(id: String): Option[NodeValue] = {
      val v = queryContext.getVertexById(id, false)
      Option(v)
    }
  }

  class RelationshipOperations extends BaseOperations[RelationshipValue] {
    override def delete(id: String): Unit = {
      queryContext.deleteElement(id, ElementType.EDGE)
    }

    override def setProperty(id: String, propertyName: String, value: Value): Unit = {
      queryContext.setProperty(id, ElementType.EDGE, propertyName, value)
    }

    override def removeProperty(id: String, propertyKeyId: String): Unit = {
      queryContext.removeProperty(id, ElementType.EDGE, propertyKeyId)
    }

    override def getProperty(id: String, propertyKeyId: String): Value = {
      try {
        queryContext.getPropertyValue(id, ElementType.EDGE, propertyKeyId)
      } catch {
        case _: exception.EntityNotFoundException =>
          throw new EntityNotFoundException(s"Relationship with id $id was not found")
      }
    }

    override def hasProperty(id: String, propertyKeyId: String): Boolean = {
      queryContext.hasProperty(id, ElementType.EDGE, propertyKeyId)
    }

    override def propertyKeyIds(id: String): Array[String] = {
      val propSet = queryContext.getElementProperties(id, ElementType.EDGE).keySet().asScala;
      propSet.toArray
    }

    override def getById(id: String): RelationshipValue = {
      queryContext.getEdgeById(id, true)
    }

    override def isDeletedInThisTx(id: String): Boolean = {
      queryContext.getEdgeById(id, false) == null
    }

    override def all: Iterator[RelationshipValue] = {
      val edges = queryContext.getEdges
      asScalaIteratorConverter(edges).asScala;
    }

    override def acquireExclusiveLock(obj: String): Unit = {}

    override def releaseExclusiveLock(obj: String): Unit = {}

    override def getByIdIfExists(id: String): Option[RelationshipValue] = {
      val v = queryContext.getEdgeById(id, false)
      Option(v)
    }
  }

  abstract class BaseOperations[T] extends Operations[T] {
    def primitiveLongIteratorToScalaIterator(primitiveIterator: LongIterator): Iterator[Long] =
      new Iterator[Long] {
        override def hasNext: Boolean = primitiveIterator.hasNext

        override def next(): Long = primitiveIterator.next
      }
  }
}
