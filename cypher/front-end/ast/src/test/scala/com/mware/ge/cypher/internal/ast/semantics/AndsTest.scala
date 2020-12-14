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
import com.mware.ge.cypher.internal.expressions.Ands
import com.mware.ge.cypher.internal.util.DummyPosition
import com.mware.ge.cypher.internal.util.symbols.CTBoolean

class AndsTest extends SemanticFunSuite {
  test("should semantic check all expressions in ands") {
    val dummyExpr1 = DummyExpression(CTBoolean, DummyPosition(1))
    val dummyExpr2 = expressions.DummyExpression(CTBoolean, DummyPosition(2))
    val dummyExpr3 = expressions.DummyExpression(CTBoolean, DummyPosition(3))
    val ands = Ands(Set(dummyExpr1, dummyExpr2, dummyExpr3))(pos)
    val result = SemanticExpressionCheck.simple(ands)(SemanticState.clean)

    result.errors shouldBe empty
    assert(result.state.typeTable.size === 3)
  }
}
