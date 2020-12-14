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

import com.mware.ge.cypher.internal.planner.spi.{IndexDescriptor, InstrumentedGraphStatistics, PlanContext}
import com.mware.ge.cypher.internal.logical.plans.{ProcedureSignature, QualifiedName, UserFunctionSignature}
import com.mware.ge.cypher.internal.frontend.phases.InternalNotificationLogger

class ExceptionTranslatingPlanContext(val inner: PlanContext) extends PlanContext with ExceptionTranslationSupport {

  override def indexesGetForLabel(labelId: String): Iterator[IndexDescriptor] =
    translateException(inner.indexesGetForLabel(labelId))

  override def indexGetForLabelAndProperties(labelName: String, propertyKeys: Seq[String]): Option[IndexDescriptor] =
    translateException(inner.indexGetForLabelAndProperties(labelName, propertyKeys))

  override def indexExistsForLabelAndProperties(labelName: String, propertyKey: Seq[String]): Boolean =
    translateException(inner.indexExistsForLabelAndProperties(labelName, propertyKey))

  override def uniqueIndexesGetForLabel(labelId: String): Iterator[IndexDescriptor] =
    translateException(inner.uniqueIndexesGetForLabel(labelId))

  override def statistics: InstrumentedGraphStatistics =
    translateException(inner.statistics)

  override def procedureSignature(name: QualifiedName): ProcedureSignature =
    translateException(inner.procedureSignature(name))

  override def functionSignature(name: QualifiedName): Option[UserFunctionSignature] =
    translateException(inner.functionSignature(name))

  override def indexExistsForLabel(labelId: String): Boolean =
    translateException(inner.indexExistsForLabel(labelId))

  override def getOptRelTypeId(relType: String): Option[String] =
    translateException(inner.getOptRelTypeId(relType))

  override def getRelTypeName(id: String): String =
    translateException(inner.getRelTypeName(id))

  override def getRelTypeId(relType: String): String =
    translateException(inner.getRelTypeId(relType))

  override def getOptPropertyKeyId(propertyKeyName: String): Option[String] =
    translateException(inner.getOptPropertyKeyId(propertyKeyName))

  override def getLabelName(id: String): String =
    translateException(inner.getLabelName(id))

  override def getOptLabelId(labelName: String): Option[String] =
    translateException(inner.getOptLabelId(labelName))

  override def getPropertyKeyId(propertyKeyName: String): String =
    translateException(inner.getPropertyKeyId(propertyKeyName))

  override def getPropertyKeyName(id: String): String =
    translateException(inner.getPropertyKeyName(id))

  override def getLabelId(labelName: String): String =
    translateException(inner.getLabelId(labelName))

  override def notificationLogger(): InternalNotificationLogger =
    translateException(inner.notificationLogger())
}
