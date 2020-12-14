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
package com.mware.ge.cypher.internal.compatibility.runtime.executionplan

import com.mware.ge.cypher.internal.compatibility.runtime.RuntimeName
import com.mware.ge.cypher.internal.runtime.QueryContext
import com.mware.ge.cypher.internal.runtime.planDescription.Argument
import com.mware.ge.cypher.result.RuntimeResult
import com.mware.ge.values.virtual.MapValue
import com.mware.ge.cypher.internal.util.InternalNotification

abstract class ExecutionPlan {

  def run(queryContext: QueryContext, doProfile: Boolean, params: MapValue): RuntimeResult

  def runtimeName: RuntimeName

  def metadata: Seq[Argument]

  def notifications: Set[InternalNotification]
}

abstract class DelegatingExecutionPlan(inner: ExecutionPlan) extends ExecutionPlan {
  override def run(queryContext: QueryContext, doProfile: Boolean,
                   params: MapValue): RuntimeResult = inner.run(queryContext, doProfile, params)

  override def runtimeName: RuntimeName = inner.runtimeName

  override def metadata: Seq[Argument] = inner.metadata

  override def notifications: Set[InternalNotification] = inner.notifications
}
