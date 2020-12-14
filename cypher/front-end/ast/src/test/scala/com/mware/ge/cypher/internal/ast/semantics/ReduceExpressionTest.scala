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
package com.mware.ge.cypher.internal.ast.semantics

import com.mware.ge.cypher.internal.expressions
import com.mware.ge.cypher.internal.expressions.DummyExpression
import com.mware.ge.cypher.internal.expressions.ReduceExpression
import com.mware.ge.cypher.internal.util.DummyPosition
import com.mware.ge.cypher.internal.util.symbols._
import com.mware.ge.cypher.internal.expressions.Variable

class ReduceExpressionTest extends SemanticFunSuite {

  test("shouldEvaluateReduceExpressionWithTypedVariables") {
    val error = SemanticError("dummy error", DummyPosition(10))

    val reduceExpression = CustomExpression(
      (ctx, self) =>
        s => {
          s.symbolTypes("x") should equal(CTString.invariant)
          s.symbolTypes("y") should equal(CTInteger.invariant)
          (specifyType(CTString, self) chain error)(s)
        }
      )

    val filter = ReduceExpression(
      accumulator = Variable("x")(DummyPosition(2)),
      init = DummyExpression(CTString),
      variable = Variable("y")(DummyPosition(6)),
      list = expressions.DummyExpression(CTList(CTInteger)),
      expression = reduceExpression
    )(DummyPosition(0))

    val result = SemanticExpressionCheck.simple(filter)(SemanticState.clean)
    result.errors should equal(Seq(error))
    result.state.symbol("x") shouldBe empty
    result.state.symbol("y") shouldBe empty
  }

  test("shouldReturnMinimalTypeOfAccumulatorAndReduceFunction") {
    val initType = CTString.covariant | CTFloat.covariant
    val listType = CTList(CTInteger)

    val reduceExpression = CustomExpression(
      (ctx, self) =>
        s => {
          s.symbolTypes("x") should equal(CTString | CTFloat)
          s.symbolTypes("y") should equal(listType.innerType.invariant)
          (specifyType(CTFloat, self) chain SemanticCheckResult.success)(s)
        }
      )

    val filter = ReduceExpression(
      accumulator = Variable("x")(DummyPosition(2)),
      init = expressions.DummyExpression(initType),
      variable = Variable("y")(DummyPosition(6)),
      list = expressions.DummyExpression(listType),
      expression = reduceExpression
    )(DummyPosition(0))

    val result = SemanticExpressionCheck.simple(filter)(SemanticState.clean)
    result.errors shouldBe empty
    types(filter)(result.state) should equal(CTAny | CTFloat)
  }

  test("shouldFailSemanticCheckIfReduceFunctionTypeDiffersFromAccumulator") {
    val accumulatorType = CTString | CTNumber
    val listType = CTList(CTInteger)

    val reduceExpression = CustomExpression(
      (ctx, self) =>
        s => {
          s.symbolTypes("x") should equal(accumulatorType)
          s.symbolTypes("y") should equal(listType.innerType.invariant)
          (specifyType(CTNode, self) chain SemanticCheckResult.success)(s)
        }
      )

    val filter = ReduceExpression(
      accumulator = Variable("x")(DummyPosition(2)),
      init = expressions.DummyExpression(accumulatorType),
      variable = Variable("y")(DummyPosition(6)),
      list = expressions.DummyExpression(listType),
      expression = reduceExpression
    )(DummyPosition(0))

    val result = SemanticExpressionCheck.simple(filter)(SemanticState.clean)
    result.errors should have size 1
    result.errors.head.msg should equal("Type mismatch: accumulator is Number or String but expression has type Node")
    result.errors.head.position should equal(reduceExpression.position)
  }
}
