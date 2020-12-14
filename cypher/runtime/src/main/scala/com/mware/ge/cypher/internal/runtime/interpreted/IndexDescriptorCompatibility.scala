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

import com.mware.ge.cypher.index
import com.mware.ge.cypher.internal.planner.spi.{IndexDescriptor, IndexLimitation}
import com.mware.ge.cypher.schema.{LabelSchemaDescriptor, SchemaDescriptorFactory}
import com.mware.ge.cypher.internal.planner.spi.{IndexLimitation, SlowContains}

trait IndexDescriptorCompatibility {
  def kernelToCypher(limitation: index.IndexLimitation): IndexLimitation = {
    limitation match {
      case _ => throw new IllegalStateException("Missing kernel to cypher mapping for limitation: " + limitation)
    }
  }

  def cypherToKernelSchema(index: IndexDescriptor): LabelSchemaDescriptor =
    SchemaDescriptorFactory.forLabel(index.label.id, index.properties.map(_.id):_*)

  def toLabelSchemaDescriptor(labelId: String, propertyKeyIds: Seq[String]): LabelSchemaDescriptor =
      SchemaDescriptorFactory.forLabel(labelId, propertyKeyIds.toArray:_*)

  def toLabelSchemaDescriptor(tc: BaseQueryContext, labelName: String, propertyKeys: Seq[String]): LabelSchemaDescriptor = {
    val labelId: String = tc.getLabelId(labelName)
    val propertyKeyIds: Seq[String] = propertyKeys.map(tc.getPropertyKeyId)
    toLabelSchemaDescriptor(labelId, propertyKeyIds)
  }
}
