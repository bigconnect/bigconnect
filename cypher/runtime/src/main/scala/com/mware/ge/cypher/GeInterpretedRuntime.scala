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

import com.mware.ge.cypher.internal.compatibility.runtime._
import com.mware.ge.cypher.internal.compatibility.runtime.executionplan._
import com.mware.ge.cypher.internal.compatibility.runtime.profiler.{InterpretedProfileInformation, Profiler}
import com.mware.ge.cypher.internal.compatibility.{CypherRuntime, RuntimeContext}
import com.mware.ge.cypher.internal.compiler.phases.LogicalPlanState
import com.mware.ge.cypher.internal.runtime.QueryContext
import com.mware.ge.cypher.internal.runtime.interpreted.commands.convert.{CommunityExpressionConverter, ExpressionConverters}
import com.mware.ge.cypher.internal.runtime.interpreted.pipes.PipeExecutionBuilderContext
import com.mware.ge.cypher.internal.runtime.planDescription.Argument
import com.mware.ge.cypher.internal.util.{InternalNotification, PeriodicCommitInOpenTransactionException}
import com.mware.ge.cypher.result.RuntimeResult
import com.mware.ge.values.virtual.MapValue

object GeInterpretedRuntime extends CypherRuntime[RuntimeContext] {
  override def compileToExecutable(state: LogicalPlanState, context: RuntimeContext): ExecutionPlan = {
    val logicalPlan = state.logicalPlan
    val converters = new ExpressionConverters(CommunityExpressionConverter(context.tokenContext))
    val executionPlanBuilder = new PipeExecutionPlanBuilder(
      expressionConverters = converters,
      pipeBuilderFactory = InterpretedPipeBuilderFactory)
    val pipeBuildContext = PipeExecutionBuilderContext(state.semanticTable(), context.readOnly)
    val pipe = executionPlanBuilder.build(logicalPlan)(pipeBuildContext, context.tokenContext)
    val periodicCommitInfo = state.periodicCommit.map(x => PeriodicCommitInfo(x.batchSize))
    val columns = state.statement().returnColumns
    val resultBuilderFactory = InterpretedExecutionResultBuilderFactory(pipe,
      context.readOnly,
      columns,
      logicalPlan,
      context.config.lenientCreateRelationship)

    new GeInterpretedRuntime(periodicCommitInfo,
      resultBuilderFactory,
      InterpretedRuntimeName,
      context.readOnly)
  }

  /**
    * Executable plan for a single cypher query. Warning, this class will get cached! Do not leak transaction objects
    * or other resources in here.
    */
  class GeInterpretedRuntime(periodicCommit: Option[PeriodicCommitInfo],
                                 resultBuilderFactory: ExecutionResultBuilderFactory,
                                 override val runtimeName: RuntimeName,
                                 readOnly: Boolean) extends ExecutionPlan {

    override def run(queryContext: QueryContext, doProfile: Boolean, params: MapValue): RuntimeResult = {
      val builderContext = if (!readOnly || doProfile) new GeUpdateCountingQueryContext(queryContext) else queryContext
      val builder = resultBuilderFactory.create(builderContext)

      val profileInformation = new InterpretedProfileInformation

      if (periodicCommit.isDefined) {
        builder.setLoadCsvPeriodicCommitObserver(periodicCommit.get.batchRowCount)
      }

      if (doProfile)
        builder.setPipeDecorator(new Profiler(profileInformation))

      builder.build(params,
        readOnly,
        profileInformation)
    }

    override def metadata: Seq[Argument] = Nil

    override def notifications: Set[InternalNotification] = Set.empty
  }
}
