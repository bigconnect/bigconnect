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

import com.mware.core.util.BcLoggerFactory
import com.mware.ge.collection.Pair
import com.mware.ge.cypher
import com.mware.ge.cypher.internal.{CacheTracer, CachingPlanner, FineToReuse, MaybeReusable, ReusabilityState}
import com.mware.ge.cypher.internal.QueryCache.ParameterTypeMap
import com.mware.ge.cypher.internal._
import com.mware.ge.cypher.internal.planner.spi.PlanContext
import com.mware.ge.cypher.internal.compiler._
import com.mware.ge.cypher.internal.compiler.phases.LogicalPlanState
import com.mware.ge.cypher.internal.frontend.phases._

/**
  * Base planner.
  *
  * @tparam STATEMENT type of AST statement used by this planner.
  * @tparam PARSED_STATE type of the state which represents a parsed query.
  */
abstract class BasePlanner[STATEMENT <: AnyRef, PARSED_STATE <: AnyRef](
                                                config: CypherPlannerConfiguration,
                                                clock: Clock,
                                                kernelMonitors: cypher.Monitors) extends CachingPlanner[PARSED_STATE] {

  protected val logger = BcLoggerFactory.getLogger(getClass)
  protected val monitors: Monitors = WrappedMonitors(kernelMonitors)

  protected val cacheTracer: CacheTracer[Pair[STATEMENT, ParameterTypeMap]] = monitors.newMonitor[CacheTracer[Pair[STATEMENT, ParameterTypeMap]]]("cypher3.5")

  override def parserCacheSize: Int = config.queryCacheSize

  protected val planCache: AstLogicalPlanCache[STATEMENT] =
    new AstLogicalPlanCache(config.queryCacheSize,
                            cacheTracer,
                            clock,
                            config.statsDivergenceCalculator)

  override def clearCaches(): Long = {
    Math.max(super.clearCaches(), planCache.clear())
  }

  protected def logStalePlanRemovalMonitor(): CacheTracer[STATEMENT] =
    new CacheTracer[STATEMENT] {
      override def queryCacheStale(key: STATEMENT, secondsSinceReplan: Int, metaData: String) {
        logger.debug(s"Discarded stale query from the query cache after $secondsSinceReplan seconds: $metaData")
      }

      override def queryCacheHit(queryKey: STATEMENT, metaData: String): Unit = {}
      override def queryCacheMiss(queryKey: STATEMENT, metaData: String): Unit = {}
      override def queryCacheFlush(sizeOfCacheBeforeFlush: Long): Unit = {}
      override def queryCacheRecompile(queryKey: STATEMENT, metaData: String): Unit = {}
    }

  protected def createReusabilityState(logicalPlanState: LogicalPlanState,
                                       planContext: PlanContext): ReusabilityState = {

    if (ProcedureCallOrSchemaCommandRuntime
      .logicalToExecutable
      .isDefinedAt(logicalPlanState.maybeLogicalPlan.get))
      FineToReuse
    else {
      val fingerprint = PlanFingerprint.take(clock, planContext.statistics)
      val fingerprintReference = new PlanFingerprintReference(fingerprint)
      MaybeReusable(fingerprintReference)
    }
  }
}

trait CypherCacheFlushingMonitor {
  def cacheFlushDetected(sizeBeforeFlush: Long) {}
}

trait CypherCacheHitMonitor[T] {
  def cacheHit(key: T) {}
  def cacheMiss(key: T) {}
  def cacheDiscard(key: T, userKey: String, secondsSinceReplan: Int) {}
  def cacheRecompile(key: T) {}
}

trait CypherCacheMonitor[T] extends CypherCacheHitMonitor[T] with CypherCacheFlushingMonitor
trait AstCacheMonitor[STATEMENT <: AnyRef] extends CypherCacheMonitor[STATEMENT]
