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

import java.time.Clock
import java.util.function.BiFunction

import com.mware.ge.collection.Pair
import com.mware.ge.cypher.ge.GeCypherQueryContext
import com.mware.ge.values.AnyValue
import com.mware.ge.cypher._
import com.mware.ge.cypher.exceptionHandler.runSafely
import com.mware.ge.cypher.internal._
import com.mware.ge.cypher.internal.compatibility._
import com.mware.ge.cypher.internal.compatibility.runtime.helpers.simpleExpressionEvaluator
import com.mware.ge.cypher.internal.compatibility.{ExceptionTranslatingPlanContext, notification}
import com.mware.ge.cypher.internal.compiler.CypherPlanner
import com.mware.ge.cypher.internal.compiler._
import com.mware.ge.cypher.internal.compiler.phases.{PlannerContext, PlannerContextCreator}
import com.mware.ge.cypher.internal.compiler.planner.logical.idp._
import com.mware.ge.cypher.internal.compiler.planner.logical.{CachedMetricsFactory, SimpleMetricsFactory}
import com.mware.ge.cypher.internal.planner.spi.{DPPlannerName, IDPPlannerName}
import com.mware.ge.cypher.internal.runtime.interpreted._
import com.mware.ge.cypher.internal.expressions.Parameter
import com.mware.ge.cypher.internal.frontend.phases._
import com.mware.ge.cypher.internal.rewriting.RewriterStepSequencer
import com.mware.ge.cypher.internal.util.attribution.SequentialIdGen
import com.mware.ge.values.virtual.MapValue
import com.mware.ge.cypher.internal.compatibility.notification.LogicalPlanNotifications
import com.mware.ge.cypher.internal.planner.spi.{CostBasedPlannerName, DPPlannerName, IDPPlannerName}

case class GeCypherPlanner(config: CypherPlannerConfiguration,
                           clock: Clock,
                           kernelMonitors: Monitors,
                           plannerOption: CypherPlannerOption,
                           updateStrategy: CypherUpdateStrategy,
                           geExecutionEngine: GeCypherExecutionEngine)
  extends BasePlanner[com.mware.ge.cypher.internal.ast.Statement, BaseState](config, clock, kernelMonitors) {

  monitors.addMonitorListener(logStalePlanRemovalMonitor(), "cypher3.5")

  val plannerName: CostBasedPlannerName =
    plannerOption match {
      case CypherPlannerOption.default => CostBasedPlannerName.default
      case CypherPlannerOption.cost | CypherPlannerOption.idp => IDPPlannerName
      case CypherPlannerOption.dp => DPPlannerName
      case _ => throw new IllegalArgumentException(s"unknown cost based planner: ${plannerOption.name}")
    }

  private val maybeUpdateStrategy: Option[UpdateStrategy] = updateStrategy match {
    case CypherUpdateStrategy.eager => Some(eagerUpdateStrategy)
    case _ => None
  }

  protected val rewriterSequencer: String => RewriterStepSequencer = {
    import RewriterStepSequencer._

    newValidating
  }

  private val contextCreator = PlannerContextCreator

  protected val planner: CypherPlanner[PlannerContext] =
    new CypherPlannerFactory().costBasedCompiler(config, clock, monitors, rewriterSequencer,
      maybeUpdateStrategy, contextCreator)

  private def createQueryGraphSolver(): IDPQueryGraphSolver =
    plannerName match {
      case IDPPlannerName =>
        val monitor = monitors.newMonitor[IDPQueryGraphSolverMonitor]()
        val solverConfig = new ConfigurableIDPSolverConfig(
          maxTableSize = config.idpMaxTableSize,
          iterationDurationLimit = config.idpIterationDuration
        )
        val singleComponentPlanner = SingleComponentPlanner(monitor, solverConfig)
        IDPQueryGraphSolver(singleComponentPlanner, cartesianProductsOrValueJoins, monitor)

      case DPPlannerName =>
        val monitor = monitors.newMonitor[IDPQueryGraphSolverMonitor]()
        val singleComponentPlanner = SingleComponentPlanner(monitor, DPSolverConfig)
        IDPQueryGraphSolver(singleComponentPlanner, cartesianProductsOrValueJoins, monitor)
    }

  def parseAndPlan(preParsedQuery: PreParsedQuery,
                            tracer: CompilationPhaseTracer,
                            params: MapValue,
                            queryContext: GeCypherQueryContext
                           ): LogicalPlanResult = {

    runSafely {
      val notificationLogger = new RecordingNotificationLogger(Some(preParsedQuery.offset))
      val syntacticQuery =
        getOrParse(preParsedQuery, new GeParser(planner, notificationLogger, preParsedQuery.offset, tracer))

      val planContext = new ExceptionTranslatingPlanContext(GePlanContext(geExecutionEngine, queryContext))

      // Context used to create logical plans
      val logicalPlanIdGen = new SequentialIdGen()
      val context = contextCreator.create(tracer,
        notificationLogger,
        planContext,
        syntacticQuery.queryText,
        preParsedQuery.debugOptions,
        Some(preParsedQuery.offset),
        monitors,
        CachedMetricsFactory(SimpleMetricsFactory),
        createQueryGraphSolver(),
        config,
        maybeUpdateStrategy.getOrElse(defaultUpdateStrategy),
        clock,
        logicalPlanIdGen,
        simpleExpressionEvaluator(PlanningQueryContext(queryContext)))

      // Prepare query for caching
      val preparedQuery = planner.normalizeQuery(syntacticQuery, context)
      val queryParamNames: Seq[String] = preparedQuery.statement().findByAllClass[Parameter].map(x => x.name)

      // If the query is not cached we want to do the full planning
      def createPlan(): CacheableLogicalPlan = {
        val logicalPlanState = planner.planPreparedQuery(preparedQuery, context)
        LogicalPlanNotifications
          .checkForNotifications(logicalPlanState.maybeLogicalPlan.get, planContext, config)
          .foreach(notificationLogger.log)

        val reusabilityState = createReusabilityState(logicalPlanState, planContext)
        CacheableLogicalPlan(logicalPlanState, reusabilityState, notificationLogger.notifications)
      }

      // Filter the parameters to retain only those that are actually used in the query
      val filteredParams = params.filter(new BiFunction[String, AnyValue, java.lang.Boolean] {
        override def apply(name: String, value: AnyValue): java.lang.Boolean = queryParamNames.contains(name)
      })

      val cacheableLogicalPlan =
        if (preParsedQuery.debugOptions.isEmpty)
          planCache.computeIfAbsentOrStale(Pair.of(syntacticQuery.statement(), QueryCache.extractParameterTypeMap(filteredParams)),
            queryContext,
            createPlan,
            _ => None,
            syntacticQuery.queryText).executableQuery
        else
          createPlan()

      LogicalPlanResult(
        cacheableLogicalPlan.logicalPlanState,
        queryParamNames,
        ValueConversion.asValues(preparedQuery.extractedParams()),
        cacheableLogicalPlan.reusability,
        context,
        cacheableLogicalPlan.notifications)
    }
  }
}

private class GeParser(planner: CypherPlanner[PlannerContext],
                       notificationLogger: InternalNotificationLogger,
                       offset: com.mware.ge.cypher.internal.util.InputPosition,
                       tracer: CompilationPhaseTracer
                             ) extends Parser[BaseState] {

  override def parse(preParsedQuery: PreParsedQuery): BaseState = {
    planner.parseQuery(preParsedQuery.statement,
      preParsedQuery.rawStatement,
      notificationLogger,
      preParsedQuery.planner.name,
      preParsedQuery.debugOptions,
      Some(offset),
      tracer)
  }
}

