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

import java.time.Clock

import com.mware.ge.cypher.internal.planner.spi.TokenContext
import com.mware.ge.cypher.internal.compiler.CypherPlannerConfiguration
import com.mware.ge.cypher.internal.frontend.phases.InternalNotificationLogger

/**
  * The regular community runtime context.
  */
case class CommunityRuntimeContext(tokenContext: TokenContext,
                                   readOnly: Boolean,
                                   config: CypherPlannerConfiguration) extends RuntimeContext {

  override def compileExpressions: Boolean = false
}

case class CommunityRuntimeContextCreator(config: CypherPlannerConfiguration) extends RuntimeContextCreator[RuntimeContext] {
  override def create(tokenContext: TokenContext,
                      clock: Clock,
                      debugOptions: Set[String],
                      readOnly: Boolean,
                      ignore: Boolean
                     ): RuntimeContext =
    CommunityRuntimeContext(tokenContext, readOnly, config)
}
