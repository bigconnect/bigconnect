/*
 * Copyright (c) 2013-2020 "BigConnect,"
 * MWARE SOLUTIONS SRL
 *
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.mware.ge.cypher.internal.frontend.phases

import com.mware.ge.cypher.internal.rewriting.rewriters._
import com.mware.ge.cypher.internal.util.inSequence
import com.mware.ge.cypher.internal.frontend.phases.CompilationPhaseTracer.CompilationPhase.AST_REWRITE
import com.mware.ge.cypher.internal.rewriting.Deprecations

case class PreparatoryRewriting(deprecations: Deprecations) extends Phase[BaseContext, BaseState, BaseState] {

  override def process(from: BaseState, context: BaseContext): BaseState = {

    val rewrittenStatement = from.statement().endoRewrite(inSequence(
      normalizeWithAndReturnClauses(context.exceptionCreator),
      expandCallWhere,
      replaceAliasedFunctionInvocations(deprecations),
      mergeInPredicates))

    from.withStatement(rewrittenStatement)
  }

  override val phase = AST_REWRITE

  override val description = "rewrite the AST into a shape that semantic analysis can be performed on"

  override def postConditions: Set[Condition] = Set.empty
}
