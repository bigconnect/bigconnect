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
import com.mware.ge.cypher.internal.util.DummyPosition
import com.mware.ge.cypher.internal.util.symbols._

class NotEqualsTest extends InfixExpressionTestBase(expressions.NotEquals(_, _)(DummyPosition(0))) {
  private val types = List(CTList(CTAny), CTInteger, CTFloat, CTNumber, CTNode, CTPath, CTRelationship, CTMap, CTPoint,
                   CTDate, CTDuration, CTBoolean, CTString, CTDateTime, CTGeometry, CTLocalDateTime, CTLocalTime, CTTime)

  test("should support equality checks among all types with Cypher 9 comparison semantics") {
    types.foreach { t1 =>
      types.foreach { t2 =>
        testValidTypes(t1, t2, useCypher9ComparisonSemantics = true)(CTBoolean)
      }
    }
  }
}
