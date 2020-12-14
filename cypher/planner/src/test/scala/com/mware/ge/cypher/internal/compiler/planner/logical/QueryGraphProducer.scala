/*
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
package com.mware.ge.cypher.internal.compiler.planner.logical

import com.mware.ge.cypher.internal.compiler._
import com.mware.ge.cypher.internal.compiler.phases.LogicalPlanState
import com.mware.ge.cypher.internal.compiler.planner._
import com.mware.ge.cypher.internal.compiler.test_helpers.ContextHelper
import com.mware.ge.cypher.internal.ir.PlannerQuery
import com.mware.ge.cypher.internal.planner.spi.IDPPlannerName
import com.mware.ge.cypher.internal.planner.spi.PlanningAttributes
import com.mware.ge.cypher.internal.ast.semantics.SemanticCheckResult
import com.mware.ge.cypher.internal.ast.semantics.SemanticChecker
import com.mware.ge.cypher.internal.ast.semantics.SemanticTable
import com.mware.ge.cypher.internal.ast.Query
import com.mware.ge.cypher.internal.ast.Statement
import com.mware.ge.cypher.internal.frontend.phases.CNFNormalizer
import com.mware.ge.cypher.internal.frontend.phases.LateAstRewriting
import com.mware.ge.cypher.internal.frontend.phases.Namespacer
import com.mware.ge.cypher.internal.frontend.phases.rewriteEqualityToInPredicate
import com.mware.ge.cypher.internal.rewriting.rewriters._
import com.mware.ge.cypher.internal.util.inSequence
import org.scalatest.mock.MockitoSugar

trait QueryGraphProducer extends MockitoSugar {

  self: LogicalPlanningTestSupport =>

  import com.mware.ge.cypher.internal.compiler.ast.convert.plannerQuery.StatementConverters._

  def producePlannerQueryForPattern(query: String): (PlannerQuery, SemanticTable) = {
    val q = query + " RETURN 1 AS Result"
    val ast = parser.parse(q)
    val mkException = new SyntaxExceptionCreator(query, Some(pos))
    val cleanedStatement: Statement = ast.endoRewrite(inSequence(normalizeWithAndReturnClauses(mkException)))
    val onError = SyntaxExceptionCreator.throwOnError(mkException)
    val SemanticCheckResult(semanticState, errors) = SemanticChecker.check(cleanedStatement)
    onError(errors)

    val (firstRewriteStep, _, _) = astRewriter.rewrite(query, cleanedStatement, semanticState)
    val state = LogicalPlanState(query, None, IDPPlannerName, PlanningAttributes(new StubSolveds, new StubCardinalities, new StubProvidedOrders), Some(firstRewriteStep), Some(semanticState))
    val context = ContextHelper.create(logicalPlanIdGen = idGen)
    val output = (Namespacer andThen rewriteEqualityToInPredicate andThen CNFNormalizer andThen LateAstRewriting).transform(state, context)

    (toUnionQuery(output.statement().asInstanceOf[Query], output.semanticTable()).queries.head, output.semanticTable())
  }
}
