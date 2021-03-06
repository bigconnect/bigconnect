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

import com.mware.ge.cypher.internal.ast.AstConstructionTestSupport
import com.mware.ge.cypher.internal.ast.Match
import com.mware.ge.cypher.internal.expressions.EveryPath
import com.mware.ge.cypher.internal.expressions.NodePattern
import com.mware.ge.cypher.internal.expressions.Pattern
import com.mware.ge.cypher.internal.expressions.Variable
import com.mware.ge.cypher.internal.util.ASTNode
import com.mware.ge.cypher.internal.util.test_helpers.CypherFunSuite

class CollectNodesOfTypeTest extends CypherFunSuite with AstConstructionTestSupport {

    private val collector: Any => Seq[Variable] = collectNodesOfType[Variable]()

    test("collect all variables") {
      val idA = varFor("a")
      val idB = varFor("b")
      val ast: ASTNode = Match(optional = false, Pattern(Seq(EveryPath(NodePattern(Some(idA), Seq(), Some(idB))_)))_, Seq(), None)_

      collector(ast) should equal(Seq(idA, idB))
    }

    test("collect no variable") {
      val ast: ASTNode = Match(optional = false, Pattern(Seq(EveryPath(NodePattern(None, Seq(), None)_)))_, Seq(), None)_

      collector(ast) shouldBe empty
    }
}
