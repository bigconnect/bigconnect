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
package com.mware.ge.cypher

import java.time.Clock

import com.mware.core.util.BcLoggerFactory
import com.mware.ge.collection.Pair
import com.mware.ge.cypher.ge.GeCypherQueryContext
import com.mware.ge.cypher.notification.Notification
import com.mware.ge.cypher.internal.QueryCache.ParameterTypeMap
import com.mware.ge.cypher.internal.compatibility.runtime.helpers.InternalWrapping.asKernelNotification
import com.mware.ge.cypher.internal.compatibility.{CommunityRuntimeContextCreator, FallbackRuntime, RuntimeContext}
import com.mware.ge.cypher.internal.planner.spi.GraphStatistics
import com.mware.ge.cypher.internal.compiler.CypherPlannerConfiguration
import com.mware.ge.cypher.internal.tracing.CompilationTracer
import com.mware.ge.cypher.internal.tracing.CompilationTracer.QueryCompilationEvent
import com.mware.ge.cypher.internal.frontend.phases.RecordingNotificationLogger
import com.mware.ge.cypher.internal.{ExecutableQuery, _}
import com.mware.ge.cypher.{CypherPlannerOption, CypherRuntimeOption, CypherUpdateStrategy, exceptionHandler}
import com.mware.ge.values.virtual.MapValue

class InternalCypherExecutionEngine(val executionEngine: GeCypherExecutionEngine,
                                    val tracer: CompilationTracer,
                                    val cacheTracer: CacheTracer[Pair[String, ParameterTypeMap]],
                                    val config: CypherConfiguration,
                                    val clock: Clock = Clock.systemUTC(),
                                    var plannerConfig: CypherPlannerConfiguration
                                   ) {
  private val preParser = new PreParser(config.version, config.planner, config.runtime, config.expressionEngineOption, config.queryCacheSize)
  private val logger = BcLoggerFactory.getLogger(getClass)

  val compiler =
    GeCypherCurrentCompiler(executionEngine,
      GeCypherPlanner(plannerConfig, clock, executionEngine.getMonitors, CypherPlannerOption.default, CypherUpdateStrategy.default, executionEngine),
      new FallbackRuntime[RuntimeContext](List(GeInterpretedRuntime), CypherRuntimeOption.default),
      CommunityRuntimeContextCreator(plannerConfig))

  private def planReusabilitiy(executableQuery: ExecutableQuery,
                               cypherQueryContext: GeCypherQueryContext): ReusabilityState =
    executableQuery.reusabilityState(cypherQueryContext)

  private val planStalenessCaller =
    new PlanStalenessCaller[ExecutableQuery](clock, config.statsDivergenceCalculator, planReusabilitiy)

  private val queryCache: QueryCache[String,Pair[String, ParameterTypeMap], ExecutableQuery] =
    new QueryCache[String, Pair[String, ParameterTypeMap], ExecutableQuery](config.queryCacheSize, planStalenessCaller, cacheTracer)

  def profile(query: String, params: MapValue, context: GeCypherQueryContext): Result =
    execute(query, params, context, profile = true)

  def execute(query: String, params: MapValue, context: GeCypherQueryContext, profile: Boolean = false): Result = {
    val queryTracer = tracer.compileQuery(query)

    try {
      val start = System.currentTimeMillis();
      val preParsedQuery = preParser.preParseQuery(query, profile)
      val executableQuery = getOrCompile(context, preParsedQuery, queryTracer, params)
      if (preParsedQuery.executionMode.name != "explain") {
        checkParameters(executableQuery.paramNames, params, executableQuery.extractedParams)
      }
      val combinedParams = params.updatedWith(executableQuery.extractedParams)
      if (logger.isDebugEnabled)
        logger.debug("Query compilation ("+ Thread.currentThread.getName +"): "+(System.currentTimeMillis() - start))
      val result = executableQuery.execute(context, preParsedQuery, combinedParams, context.getAuthorizations)
      result
    } catch {
      case t: Throwable =>
        throw t
    } finally {
      queryTracer.close()
      context.commit()
    }
  }

  @throws(classOf[ParameterNotFoundException])
  private def checkParameters(queryParams: Seq[String], givenParams: MapValue, extractedParams: MapValue) {
    exceptionHandler.runSafely {
      val missingKeys = queryParams.filter(key => !(givenParams.containsKey(key) || extractedParams.containsKey(key))).distinct
      if (missingKeys.nonEmpty) {
        throw new ParameterNotFoundException("Expected parameter(s): " + missingKeys.mkString(", "))
      }
    }
  }

  private def getOrCompile(context: GeCypherQueryContext,
                           preParsedQuery: PreParsedQuery,
                           tracer: QueryCompilationEvent,
                           params: MapValue
                          ): ExecutableQuery = {
    val logger = new RecordingNotificationLogger(Some(preParsedQuery.offset))
    def notificationsSoFar(): Set[Notification] = logger.notifications.map(asKernelNotification(None))

    val cacheKey = Pair.of(preParsedQuery.statementWithVersionAndPlanner, QueryCache.extractParameterTypeMap(params))
    var n = 0
    while (n < ExecutionEngine.PLAN_BUILDING_TRIES) {
      val cacheLookup = queryCache.computeIfAbsentOrStale(cacheKey,
        context,
        () => compiler.compile(preParsedQuery, tracer, notificationsSoFar(), params, context),
        (Int) => None,
        preParsedQuery.rawStatement)

      cacheLookup match {
        case _: CacheHit[_] |
             _: CacheDisabled[_] =>
          val executableQuery = cacheLookup.executableQuery
          return executableQuery
        case CacheMiss(executableQuery) =>
        // Do nothing. In the next attempt we will find the plan in the cache and
        // used it unless the schema has changed during planning.
      }

      n += 1
    }

    throw new IllegalStateException("Could not compile query after "+ExecutionEngine.PLAN_BUILDING_TRIES+" retries")
  }
}

object ExecutionEngine {
  val PLAN_BUILDING_TRIES: Int = 20
}
