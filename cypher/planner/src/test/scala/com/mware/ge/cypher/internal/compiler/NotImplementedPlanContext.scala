/*
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
package com.mware.ge.cypher.internal.compiler

import com.mware.ge.cypher.internal.planner.spi.{IndexDescriptor, InstrumentedGraphStatistics, PlanContext}
import com.mware.ge.cypher.internal.frontend.phases.InternalNotificationLogger
import com.mware.ge.cypher.internal.logical.plans.{ProcedureSignature, QualifiedName, UserFunctionSignature}

class NotImplementedPlanContext extends PlanContext {
  override def indexesGetForLabel(labelId: String): Iterator[IndexDescriptor] = ???

  override def indexExistsForLabel(labelId: String): Boolean = ???

  override def indexGetForLabelAndProperties(labelName: String, propertyKeys: Seq[String]): Option[IndexDescriptor] = ???

  override def indexExistsForLabelAndProperties(labelName: String, propertyKey: Seq[String]): Boolean = ???

  override def uniqueIndexesGetForLabel(labelId: String): Iterator[IndexDescriptor] = ???

  override def statistics: InstrumentedGraphStatistics = ???

  override def notificationLogger(): InternalNotificationLogger = ???

  override def procedureSignature(name: QualifiedName): ProcedureSignature = ???

  override def functionSignature(name: QualifiedName): Option[UserFunctionSignature] = ???

  override def getLabelName(id: String): String = ???

  override def getOptLabelId(labelName: String): Option[String] = ???

  override def getLabelId(labelName: String): String = ???

  override def getPropertyKeyName(id: String): String = ???

  override def getOptPropertyKeyId(propertyKeyName: String): Option[String] = ???

  override def getPropertyKeyId(propertyKeyName: String): String = ???

  override def getRelTypeName(id: String): String = ???

  override def getOptRelTypeId(relType: String): Option[String] = ???

  override def getRelTypeId(relType: String): String = ???
}
