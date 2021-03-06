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
package com.mware.ge.cypher.internal.ast

import com.mware.ge.cypher.internal.ast.semantics._
import com.mware.ge.cypher.internal.expressions._
import com.mware.ge.cypher.internal.util.ASTNode
import com.mware.ge.cypher.internal.util.symbols.CTInteger
import com.mware.ge.cypher.internal.ast.semantics.{SemanticAnalysisTooling, SemanticCheckResult, SemanticCheckable, SemanticExpressionCheck}

// Skip/Limit
trait ASTSlicingPhrase extends SemanticCheckable with SemanticAnalysisTooling {
  self: ASTNode =>
  def name: String
  def dependencies: Set[LogicalVariable] = expression.dependencies
  def expression: Expression

  def semanticCheck: SemanticCheck =
    containsNoVariables chain
      literalShouldBeUnsignedInteger chain
      SemanticExpressionCheck.simple(expression) chain
      expectType(CTInteger.covariant, expression)

  private def containsNoVariables: SemanticCheck = {
    val deps = dependencies
    if (deps.nonEmpty) {
      val id = deps.toSeq.minBy(_.position)
      error(s"It is not allowed to refer to variables in $name", id.position)
    }
    else SemanticCheckResult.success
  }

  private def literalShouldBeUnsignedInteger: SemanticCheck = {
    try {
      expression match {
        case _: UnsignedDecimalIntegerLiteral => SemanticCheckResult.success
        case i: SignedDecimalIntegerLiteral if i.value >= 0 => SemanticCheckResult.success
        case lit: Literal => error(s"Invalid input '${lit.asCanonicalStringVal}' is not a valid value, " +
          "must be a positive integer", lit.position)
        case _ => SemanticCheckResult.success
      }
    } catch {
      case nfe: NumberFormatException => SemanticError("Invalid input for " + name +
        ". Either the string does not have the appropriate format or the provided number is bigger then 2^63-1", expression.position)
    }
  }
}
