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

import com.mware.ge.cypher.internal.compatibility.runtime.executionplan.ExecutionPlan
import com.mware.ge.cypher.internal.compatibility.runtime.executionplan.procs.{ProcedureCallExecutionPlan, SchemaWriteExecutionPlan}
import com.mware.ge.cypher.internal.planner.spi.IndexDescriptor
import com.mware.ge.cypher.internal.compiler.phases.LogicalPlanState
import com.mware.ge.cypher.internal.compiler.planner.CantCompileQueryException
import com.mware.ge.cypher.internal.runtime.interpreted.commands.convert.{CommunityExpressionConverter, ExpressionConverters}
import com.mware.ge.cypher.internal.runtime.{InternalQueryType, ProcedureCallMode, QueryContext, SCHEMA_WRITE}
import com.mware.ge.cypher.internal.logical.plans._
import com.mware.ge.cypher.internal.expressions.{LabelName, PropertyKeyName, RelTypeName}
import com.mware.ge.cypher.internal.util.{LabelId, PropertyKeyId}

/**
  * This runtime takes on queries that require no planning, such as procedures and schema commands
  */
object ProcedureCallOrSchemaCommandRuntime extends CypherRuntime[RuntimeContext] {
  override def compileToExecutable(state: LogicalPlanState, context: RuntimeContext): ExecutionPlan = {

    def throwCantCompile(unknownPlan: LogicalPlan): Nothing = {
      throw new CantCompileQueryException(
        s"Plan is not a procedure call or schema command: ${unknownPlan.getClass.getSimpleName}")
    }

    logicalToExecutable.applyOrElse(state.maybeLogicalPlan.get, throwCantCompile).apply(context)
  }

  def queryType(logicalPlan: LogicalPlan): Option[InternalQueryType] =
    if (logicalToExecutable.isDefinedAt(logicalPlan)) {
      logicalPlan match {
        case StandAloneProcedureCall(signature, args, types, indices) =>
          Some(ProcedureCallMode.fromAccessMode(signature.accessMode).queryType)
        case _ => Some(SCHEMA_WRITE)
      }
    } else None

  val logicalToExecutable: PartialFunction[LogicalPlan, RuntimeContext => ExecutionPlan] = {
    // Global call: CALL foo.bar.baz("arg1", 2)
    case plan@StandAloneProcedureCall(signature, args, types, indices) => runtimeContext =>
      ProcedureCallExecutionPlan(signature, args, types, indices, new ExpressionConverters(CommunityExpressionConverter(runtimeContext.tokenContext)), plan.id)

    // CREATE INDEX ON :LABEL(prop)
    case CreateIndex(label, props) => runtimeContext =>
      SchemaWriteExecutionPlan("CreateIndex", ctx => {
        ctx.addIndexRule(IndexDescriptor(labelToId(ctx)(label), propertiesToIds(ctx)(props)))
      })

    // DROP INDEX ON :LABEL(prop)
    case DropIndex(label, props) => runtimeContext =>
      SchemaWriteExecutionPlan("DropIndex", ctx => {
        ctx.dropIndexRule(IndexDescriptor(labelToId(ctx)(label), propertiesToIds(ctx)(props)))
      })
  }

  implicit private def labelToId(ctx: QueryContext)(label: LabelName): LabelId =
    LabelId(ctx.getOrCreateLabelId(label.name))

  implicit private def propertyToId(ctx: QueryContext)(property: PropertyKeyName): PropertyKeyId =
    PropertyKeyId(ctx.getOrCreatePropertyKeyId(property.name))

  implicit private def propertiesToIds(ctx: QueryContext)(properties: List[PropertyKeyName]): List[PropertyKeyId] =
    properties.map(property => PropertyKeyId(ctx.getOrCreatePropertyKeyId(property.name)))
}
