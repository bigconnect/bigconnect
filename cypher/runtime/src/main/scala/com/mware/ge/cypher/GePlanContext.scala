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

import java.util.Optional

import com.mware.ge.cypher.ge.GeCypherQueryContext
import com.mware.ge.cypher.internal.planner.spi
import com.mware.ge.cypher.internal.planner.spi.{IndexDescriptor, IndexLimitation, IndexOrderCapability, InstrumentedGraphStatistics, MutableGraphStatisticsSnapshot, PlanContext}
import com.mware.ge.cypher.procedure.impl.{DefaultParameterValue, Neo4jTypes}
import com.mware.ge.cypher.procedure.Mode
import com.mware.ge.cypher.procedure.impl.Neo4jTypes.AnyType
import com.mware.ge.cypher.internal.planner.spi._
import com.mware.ge.cypher.internal.frontend.phases.{InternalNotificationLogger, RecordingNotificationLogger}
import com.mware.ge.cypher.internal.logical.plans._
import com.mware.ge.cypher.internal.util.symbols.{CTAny, CTBoolean, CTDate, CTDateTime, CTDuration, CTFloat, CTGeometry, CTInteger, CTList, CTLocalDateTime, CTLocalTime, CTMap, CTNode, CTNumber, CTPath, CTPoint, CTRelationship, CTString, CTTime, CypherType}
import com.mware.ge.cypher.internal.util.{LabelId, PropertyKeyId}

import scala.collection.JavaConverters._

object GePlanContext {
  def apply(executionEngine: GeCypherExecutionEngine, queryContext: GeCypherQueryContext): GePlanContext =
    new GePlanContext(executionEngine, queryContext, InstrumentedGraphStatistics(GeGraphStatistics(executionEngine.getGraph), new MutableGraphStatisticsSnapshot()))
}

class GePlanContext(val executionEngine: GeCypherExecutionEngine, val queryContext: GeCypherQueryContext, val statistics: InstrumentedGraphStatistics)
  extends BaseGeContext(executionEngine, queryContext) with PlanContext
{
  override def indexesGetForLabel(labelId: String): Iterator[IndexDescriptor] = {
    if (labelId == null)
      Iterator.empty

    val properties = queryContext.getIndexablePropertyKeys(labelId, queryContext.getWorkspaceId).asScala.map(pk => PropertyKeyId(pk))
    properties.map(p => IndexDescriptor(LabelId(labelId), p)).iterator
  }

  override def uniqueIndexesGetForLabel(labelId: String): Iterator[IndexDescriptor] = {
    Iterator.empty
  }

  override def indexExistsForLabel(labelId: String): Boolean = {
    indexesGetForLabel(labelId).nonEmpty
  }

  override def indexGetForLabelAndProperties(labelName: String, propertyKeys: Seq[String]): Option[IndexDescriptor] = {
    val indexableProps = queryContext.getIndexablePropertyKeys(labelName, queryContext.getWorkspaceId);
    val descriptorProps = indexableProps.asScala.filter(p => propertyKeys.contains(p));
    val labelId = LabelId(labelName);
    val propIds = descriptorProps.map(p => PropertyKeyId(p)).to[Seq]

    if (propIds.nonEmpty) {
      Some(
        spi.IndexDescriptor(
          labelId,
          propIds,
          Set.empty[IndexLimitation],
          _ => IndexOrderCapability.BOTH,
          s => s.map(_ => CanGetValue)
        )
      )
    }

    None
  }

  override def indexExistsForLabelAndProperties(labelName: String, propertyKeys: Seq[String]): Boolean = {
    indexGetForLabelAndProperties(labelName, propertyKeys).isDefined
  }

  override def notificationLogger(): InternalNotificationLogger = {
    new RecordingNotificationLogger();
  }

  override def procedureSignature(name: QualifiedName): ProcedureSignature = {
    val kn = new com.mware.ge.cypher.procedure.impl.QualifiedName(name.namespace.asJava, name.name)
    val handle = executionEngine.getProcedures.procedure(kn)
    val signature = handle.signature()
    val input = signature.inputSignature().asScala
      .map(s => FieldSignature(s.name(), asCypherType(s.neo4jType()), asOption(s.defaultValue()).map(asCypherValue)))
      .toIndexedSeq
    val output = if (signature.isVoid) None else Some(
      signature.outputSignature().asScala.map(s => FieldSignature(s.name(), asCypherType(s.neo4jType()), deprecated = s.isDeprecated)).toIndexedSeq)
    val deprecationInfo = asOption(signature.deprecated())
    val mode = asCypherProcMode(signature.mode(), signature.allowed())
    val description = asOption(signature.description())
    val warning = asOption(signature.warning())

    ProcedureSignature(name, input, output, deprecationInfo, mode, description, warning, signature.eager(), Some(handle.id()))
  }

  override def functionSignature(name: QualifiedName): Option[UserFunctionSignature] = {
    val kn = new com.mware.ge.cypher.procedure.impl.QualifiedName(name.namespace.asJava, name.name)
    val func = executionEngine.getProcedures.function(kn)
    val (fcn, aggregation) = if (func != null) (func, false)
    else (executionEngine.getProcedures.aggregationFunction(kn), true)
    if (fcn == null) None
    else {
      val signature = fcn.signature()
      val input = signature.inputSignature().asScala
        .map(s => FieldSignature(s.name(), asCypherType(s.neo4jType()), asOption(s.defaultValue()).map(asCypherValue)))
        .toIndexedSeq

      val output = asCypherType(signature.outputType())
      val deprecationInfo = asOption(signature.deprecated())
      val description = asOption(signature.description())

      Some(UserFunctionSignature(name, input, output, deprecationInfo,
        signature.allowed(), description, isAggregate = aggregation, id = Some(fcn.id())))
    }
  }

  private def asCypherType(neoType: AnyType): CypherType = neoType match {
    case Neo4jTypes.NTString => CTString
    case Neo4jTypes.NTInteger => CTInteger
    case Neo4jTypes.NTFloat => CTFloat
    case Neo4jTypes.NTNumber => CTNumber
    case Neo4jTypes.NTBoolean => CTBoolean
    case l: Neo4jTypes.ListType => CTList(asCypherType(l.innerType()))
    case Neo4jTypes.NTByteArray => CTList(CTAny)
    case Neo4jTypes.NTDateTime => CTDateTime
    case Neo4jTypes.NTLocalDateTime => CTLocalDateTime
    case Neo4jTypes.NTDate => CTDate
    case Neo4jTypes.NTTime => CTTime
    case Neo4jTypes.NTLocalTime => CTLocalTime
    case Neo4jTypes.NTDuration => CTDuration
    case Neo4jTypes.NTPoint => CTPoint
    case Neo4jTypes.NTNode => CTNode
    case Neo4jTypes.NTRelationship => CTRelationship
    case Neo4jTypes.NTPath => CTPath
    case Neo4jTypes.NTGeometry => CTGeometry
    case Neo4jTypes.NTMap => CTMap
    case Neo4jTypes.NTAny => CTAny
  }

  private def asOption[T](optional: Optional[T]): Option[T] = if (optional.isPresent) Some(optional.get()) else None

  private def asCypherValue(neo4jValue: DefaultParameterValue) = CypherValue(neo4jValue.value,
    asCypherType(neo4jValue.neo4jType()))

  private def asCypherProcMode(mode: Mode, allowed: Array[String]): ProcedureAccessMode = mode match {
    case Mode.READ => ProcedureReadOnlyAccess(allowed)
    case Mode.DEFAULT => ProcedureReadOnlyAccess(allowed)
    case Mode.WRITE => ProcedureReadWriteAccess(allowed)
    case Mode.SCHEMA => ProcedureSchemaWriteAccess(allowed)
    case Mode.DBMS => ProcedureDbmsAccess(allowed)

    case _ => throw new CypherExecutionException(
      "Unable to execute procedure, because it requires an unrecognized execution mode: " + mode.name(), null)
  }
}
