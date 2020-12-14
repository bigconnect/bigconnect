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
package com.mware.ge.cypher.internal.rewriting.conditions

import com.mware.ge.cypher.internal.ast._
import com.mware.ge.cypher.internal.expressions.EveryPath
import com.mware.ge.cypher.internal.expressions.NodePattern
import com.mware.ge.cypher.internal.expressions.Pattern
import com.mware.ge.cypher.internal.util.ASTNode
import com.mware.ge.cypher.internal.util.test_helpers.CypherFunSuite

class NoReferenceEqualityAmongVariablesTest extends CypherFunSuite with AstConstructionTestSupport {

  private val collector: Any => Seq[String] = noReferenceEqualityAmongVariables

  test("unhappy when same Variable instance is used multiple times") {
    val id = varFor("a")
    val ast: ASTNode = Match(optional = false, Pattern(Seq(EveryPath(NodePattern(Some(id), Seq(), Some(id))_)))_, Seq(), None)_

    collector(ast) should equal(Seq(s"The instance $id is used 2 times"))
  }

  test("happy when all variable are no reference equal") {
    val ast: ASTNode = Match(optional = false, Pattern(Seq(EveryPath(NodePattern(Some(varFor("a")), Seq(), Some(varFor("a")))_)))_, Seq(), None)_

    collector(ast) shouldBe empty
  }
}
