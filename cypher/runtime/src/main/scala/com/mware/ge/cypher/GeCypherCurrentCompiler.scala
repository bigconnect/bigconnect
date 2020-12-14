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

import com.mware.ge.cypher.notification.Notification
import com.mware.ge.cypher.query.{CompilerInfo, ExplicitIndexUsage, QueryExecutionMonitor, SchemaIndexUsage}
import com.mware.ge.Authorizations
import com.mware.ge.cypher.ge.GeCypherQueryContext
import com.mware.ge.cypher.CypherExecutionMode
import com.mware.ge.cypher.exceptionHandler.runSafely
import com.mware.ge.cypher.internal.compatibility._
import com.mware.ge.cypher.internal.compatibility.runtime.executionplan.{ExecutionPlan, StandardInternalExecutionResult}
import com.mware.ge.cypher.internal.compatibility.runtime.helpers.InternalWrapping.asKernelNotification
import com.mware.ge.cypher.internal.compatibility.runtime.profiler.PlanDescriptionBuilder
import com.mware.ge.cypher.internal.compatibility.runtime.{ExplainExecutionResult, RuntimeName}
import com.mware.ge.cypher.internal.compiler.phases.LogicalPlanState
import com.mware.ge.cypher.internal.javacompat.ExecutionResult
import com.mware.ge.cypher.internal.planner.spi.PlanningAttributes.{Cardinalities, ProvidedOrders}
import com.mware.ge.cypher.internal.runtime._
import com.mware.ge.cypher.internal.runtime.planDescription.InternalPlanDescription
import com.mware.ge.cypher.internal.frontend.PlannerName
import com.mware.ge.cypher.internal.frontend.phases.CompilationPhaseTracer
import com.mware.ge.cypher.internal.logical.plans._
import com.mware.ge.cypher.internal.util.{InternalNotification, TaskCloser}
import com.mware.ge.cypher.internal.{Compiler, ExecutableQuery, PreParsedQuery, ReusabilityState}
import com.mware.ge.values.virtual.MapValue

import scala.collection.JavaConverters._

/**
  * Composite [[Compiler]], which uses a [[CypherPlanner]] and [[CypherRuntime]] to compile
  * a preparsed query into a [[ExecutableQuery]].
  *
  * @param planner        the planner
  * @param runtime        the runtime
  * @param contextCreator the runtime context creator
  * @tparam CONTEXT type of runtime context used
  */
case class GeCypherCurrentCompiler[CONTEXT <: RuntimeContext](executionEngine: GeCypherExecutionEngine,
                                                              planner: GeCypherPlanner,
                                                              runtime: CypherRuntime[CONTEXT],
                                                              contextCreator: RuntimeContextCreator[CONTEXT]
                                                             ) extends com.mware.ge.cypher.internal.Compiler {

  /**
    * Compile [[PreParsedQuery]] into [[ExecutableQuery]].
    *
    * @param preParsedQuery          pre-parsed query to convert
    * @param tracer                  compilation tracer to which events of the compilation process are reported
    * @param preParsingNotifications notifications from pre-parsing
    * @throws CypherException public cypher exceptions on compilation problems
    * @return a compiled and executable query
    */
  override def compile(preParsedQuery: PreParsedQuery,
                       tracer: CompilationPhaseTracer,
                       preParsingNotifications: Set[Notification],
                       params: MapValue,
                       context: GeCypherQueryContext
                      ): ExecutableQuery = {

    val logicalPlanResult =
      planner.parseAndPlan(preParsedQuery, tracer, params, context)

    val planState = logicalPlanResult.logicalPlanState
    val logicalPlan = planState.logicalPlan
    val queryType = getQueryType(planState)

    val runtimeContext = contextCreator.create(logicalPlanResult.plannerContext.planContext,
      logicalPlanResult.plannerContext.clock,
      logicalPlanResult.plannerContext.debugOptions,
      queryType == READ_ONLY,
      preParsedQuery.useCompiledExpressions)

    val executionPlan: ExecutionPlan = runtime.compileToExecutable(planState, runtimeContext)

    new CypherExecutableQuery(
      logicalPlan,
      runtimeContext.readOnly,
      logicalPlanResult.logicalPlanState.planningAttributes.cardinalities,
      logicalPlanResult.logicalPlanState.planningAttributes.providedOrders,
      executionPlan,
      preParsingNotifications,
      logicalPlanResult.notifications,
      logicalPlanResult.reusability,
      logicalPlanResult.paramNames,
      logicalPlanResult.extractedParams,
      buildCompilerInfo(logicalPlan, planState.plannerName, executionPlan.runtimeName),
      planState.plannerName,
      queryType)
  }

  private def buildCompilerInfo(logicalPlan: LogicalPlan,
                                plannerName: PlannerName,
                                runtimeName: RuntimeName): CompilerInfo =

    new CompilerInfo(plannerName.name, runtimeName.name, logicalPlan.indexUsage.map {
      case SchemaIndexSeekUsage(identifier, labelId, label, propertyKeys) => new SchemaIndexUsage(identifier, labelId, label, propertyKeys: _*)
      case SchemaIndexScanUsage(identifier, labelId, label, propertyKey) => new SchemaIndexUsage(identifier, labelId, label, propertyKey)
      case ExplicitNodeIndexUsage(identifier, index) => new ExplicitIndexUsage(identifier, "NODE", index)
      case ExplicitRelationshipIndexUsage(identifier, index) => new ExplicitIndexUsage(identifier, "RELATIONSHIP", index)
    }.asJava)

  private def getQueryType(planState: LogicalPlanState): InternalQueryType = {
    val procedureOrSchema = ProcedureCallOrSchemaCommandRuntime.queryType(planState.logicalPlan)
    if (procedureOrSchema.isDefined) // check this first, because if this is true solveds will be empty
      procedureOrSchema.get
    else if (planState.planningAttributes.solveds(planState.logicalPlan.id).readOnly)
      READ_ONLY
    else if (columnNames(planState.logicalPlan).isEmpty)
      WRITE
    else
      READ_WRITE
  }

  private def columnNames(logicalPlan: LogicalPlan): Array[String] =
    logicalPlan match {
      case produceResult: ProduceResult => produceResult.columns.toArray

      case procedureCall: StandAloneProcedureCall =>
        procedureCall.signature.outputSignature.map(_.seq.map(_.name).toArray).getOrElse(Array.empty)

      case _ => Array()
    }

  protected class CypherExecutableQuery(logicalPlan: LogicalPlan,
                                        readOnly: Boolean,
                                        cardinalities: Cardinalities,
                                        providedOrders: ProvidedOrders,
                                        executionPlan: ExecutionPlan,
                                        preParsingNotifications: Set[Notification],
                                        planningNotifications: Set[InternalNotification],
                                        reusabilityState: ReusabilityState,
                                        override val paramNames: Seq[String],
                                        override val extractedParams: MapValue,
                                        override val compilerInfo: CompilerInfo,
                                        plannerName: PlannerName,
                                        queryType: InternalQueryType) extends ExecutableQuery {

    private val resourceMonitor = executionEngine.getMonitors.newMonitor(classOf[ResourceMonitor])
    private val planDescriptionBuilder =
      new PlanDescriptionBuilder(logicalPlan,
        plannerName,
        readOnly,
        cardinalities,
        providedOrders,
        executionPlan.runtimeName,
        executionPlan.metadata)


    override def execute(context: GeCypherQueryContext, preParsedQuery: PreParsedQuery, params: MapValue, authorizations: Authorizations): Result = {
      val innerExecutionMode = preParsedQuery.executionMode match {
        case CypherExecutionMode.explain => ExplainMode
        case CypherExecutionMode.profile => ProfileMode
        case CypherExecutionMode.normal => NormalMode
      }
      val taskCloser = new TaskCloser
      val qc = new GeExceptionTranslatingQueryContext(new GeQueryContext(executionEngine, new ResourceManager(resourceMonitor), context))
      taskCloser.addTask(qc.resources.close)

      runSafely {

        val internalExecutionResult =
          if (innerExecutionMode == ExplainMode) {
            taskCloser.close(success = true)
            val columns = columnNames(logicalPlan)

            val allNotifications =
              preParsingNotifications ++ (planningNotifications ++ executionPlan.notifications).map(asKernelNotification(Some(preParsedQuery.offset)))
            ExplainExecutionResult(columns,
              planDescriptionBuilder.explain(),
              queryType, allNotifications)
          } else {

            val doProfile = innerExecutionMode == ProfileMode
            val runtimeResult = executionPlan.run(qc, doProfile, params)

            new StandardInternalExecutionResult(qc,
              executionPlan.runtimeName,
              runtimeResult,
              taskCloser,
              queryType,
              innerExecutionMode,
              planDescriptionBuilder)
          }

        new ExecutionResult(
          ClosingExecutionResult.wrapAndInitiate(
            context.executingQuery(),
            internalExecutionResult,
            runSafely,
            executionEngine.getMonitors.newMonitor(classOf[QueryExecutionMonitor])
          )
        )
      }(e => taskCloser.close(false))
    }

    override def reusabilityState(ctx: GeCypherQueryContext): ReusabilityState = reusabilityState

    override def planDescription(): InternalPlanDescription = planDescriptionBuilder.explain()
  }

}
