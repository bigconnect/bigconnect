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

import com.mware.ge.cypher.internal.compiler.planner.LogicalPlanningTestSupport2
import com.mware.ge.cypher.internal.compiler.planner.logical.plans.rewriter.unnestOptional
import com.mware.ge.cypher.internal.ir.SimplePatternLength
import com.mware.ge.cypher.internal.planner.spi.{DelegatingGraphStatistics, PlanContext}
import com.mware.ge.cypher.internal.logical.plans.Limit
import com.mware.ge.cypher.internal.logical.plans._
import com.mware.ge.cypher.internal.expressions._
import com.mware.ge.cypher.internal.util.Foldable._
import com.mware.ge.cypher.internal.util.test_helpers.CypherFunSuite
import com.mware.ge.cypher.internal.util.Cardinality
import com.mware.ge.cypher.internal.util.LabelId
import com.mware.ge.cypher.internal.util.RelTypeId
import org.scalatest.Inside

class OptionalMatchPlanningIntegrationTest extends CypherFunSuite with LogicalPlanningTestSupport2 with Inside {

  test("should build plans containing left outer joins") {
    (new given {
      cost = {
        case (_: AllNodesScan, _, _) => 2000000.0
        case (_: NodeByLabelScan, _, _) => 20.0
        case (p: Expand, _, _) if p.findByAllClass[CartesianProduct].nonEmpty => Double.MaxValue
        case (_: Expand, _, _) => 10.0
        case (_: LeftOuterHashJoin, _, _) => 20.0
        case (_: Argument, _, _) => 1.0
        case _ => Double.MaxValue
      }
    } getLogicalPlanFor "MATCH (a:X)-[r1]->(b) OPTIONAL MATCH (b)-[r2]->(c:Y) RETURN b")._2 should equal(
      LeftOuterHashJoin(Set("b"),
        Expand(NodeByLabelScan("a", lblName("X"), Set.empty), "a", SemanticDirection.OUTGOING, Seq(), "b", "r1"),
        Expand(NodeByLabelScan("c", lblName("Y"), Set.empty), "c", SemanticDirection.INCOMING, Seq(), "b", "r2")
      )
    )
  }

  test("should build plans containing right outer joins") {
    (new given {
      cost = {
        case (_: AllNodesScan, _, _) => 2000000.0
        case (_: NodeByLabelScan, _, _) => 20.0
        case (p: Expand, _, _) if p.findByAllClass[CartesianProduct].nonEmpty => Double.MaxValue
        case (_: Expand, _, _) => 10.0
        case (_: RightOuterHashJoin, _, _) => 20.0
        case (_: Argument, _, _) => 1.0
        case _ => Double.MaxValue
      }
    } getLogicalPlanFor "MATCH (a:X)-[r1]->(b) OPTIONAL MATCH (b)-[r2]->(c:Y) RETURN b")._2 should equal(
      RightOuterHashJoin(Set("b"),
        Expand(NodeByLabelScan("c", lblName("Y"), Set.empty), "c", SemanticDirection.INCOMING, Seq(), "b", "r2"),
        Expand(NodeByLabelScan("a", lblName("X"), Set.empty), "a", SemanticDirection.OUTGOING, Seq(), "b", "r1")
      )
    )
  }

  test("should choose left outer join if lhs has small cardinality") {
    (new given {
      labelCardinality = Map("X" -> 1.0, "Y" -> 10.0)
      statistics = new DelegatingGraphStatistics(parent.graphStatistics) {
        override def cardinalityByLabelsAndRelationshipType(fromLabel: Option[LabelId], relTypeId: Option[RelTypeId], toLabel: Option[LabelId], planContext: PlanContext): Cardinality = {
          // TODO proper lookup from semantic table somehow
          // X = 0, Y = 1
          if (fromLabel.exists(_.id == "0") && relTypeId.isEmpty && toLabel.isEmpty) {
            // low from a to b
            100.0
          } else if (fromLabel.isEmpty && relTypeId.isEmpty && toLabel.exists(_.id == "1")) {
            // high from b to c
            1000000000.0
          } else {
            super.cardinalityByLabelsAndRelationshipType(fromLabel, relTypeId, toLabel, planContext)
          }
        }
      }
      cost = {
        case (_: Apply, _, _) => Double.MaxValue
        case x => parent.costModel()(x)
      }
    } getLogicalPlanFor "MATCH (a:X)-[r1]->(b) OPTIONAL MATCH (b)-[r2]->(c:Y) RETURN b")._2 should equal(
      LeftOuterHashJoin(Set("b"),
        Expand(NodeByLabelScan("a", lblName("X"), Set.empty), "a", SemanticDirection.OUTGOING, Seq(), "b", "r1"),
        Expand(NodeByLabelScan("c", lblName("Y"), Set.empty), "c", SemanticDirection.INCOMING, Seq(), "b", "r2")
      )
    )
  }

  test("should choose right outer join if rhs has small cardinality") {
    (new given {
      labelCardinality = Map("X" -> 10.0, "Y" -> 1.0)
      statistics = new DelegatingGraphStatistics(parent.graphStatistics) {
        override def cardinalityByLabelsAndRelationshipType(fromLabel: Option[LabelId], relTypeId: Option[RelTypeId], toLabel: Option[LabelId], planContext: PlanContext): Cardinality = {
          // TODO proper lookup from semantic table somehow
          // X = 0, Y = 1
          if (fromLabel.exists(_.id == 0) && relTypeId.isEmpty && toLabel.isEmpty) {
            // high from a to b
            1000000000.0
          } else if ( fromLabel.isEmpty && relTypeId.isEmpty && toLabel.exists(_.id == "1")) {
            // low from b to c
            100.0
          } else {
            super.cardinalityByLabelsAndRelationshipType(fromLabel, relTypeId, toLabel, planContext)
          }
        }
      }
      cost = {
        case (_: Apply, _, _) => Double.MaxValue
        case x => parent.costModel()(x)
      }
    } getLogicalPlanFor "MATCH (a:X)-[r1]->(b) OPTIONAL MATCH (b)-[r2]->(c:Y) RETURN b")._2 should equal(
      RightOuterHashJoin(Set("b"),
        Expand(NodeByLabelScan("c", lblName("Y"), Set.empty), "c", SemanticDirection.INCOMING, Seq(), "b", "r2"),
        Expand(NodeByLabelScan("a", lblName("X"), Set.empty), "a", SemanticDirection.OUTGOING, Seq(), "b", "r1")
      )
    )
  }

  test("should build simple optional match plans") { // This should be built using plan rewriting
    planFor("OPTIONAL MATCH (a) RETURN a")._2 should equal(
      Optional(AllNodesScan("a", Set.empty)))
  }

  test("should build simple optional expand") {
    planFor("MATCH (n) OPTIONAL MATCH (n)-[:NOT_EXIST]->(x) RETURN n")._2.endoRewrite(unnestOptional) match {
      case OptionalExpand(
      AllNodesScan("n", _),
      "n",
      SemanticDirection.OUTGOING,
      _,
      "x",
      _,
      _,
      _
      ) => ()
    }
  }

  test("should build optional ProjectEndpoints") {
    planFor("MATCH (a1)-[r]->(b1) WITH r, a1 LIMIT 1 OPTIONAL MATCH (a1)<-[r]-(b2) RETURN a1, r, b2")._2 match {
      case
        Apply(
        Limit(
        Expand(
        AllNodesScan("b1", _), _, _, _, _, _, _), _, _),
        Optional(
        ProjectEndpoints(
        Argument(args), "r", "b2", false, "a1", true, None, true, SimplePatternLength
        ), _
        )
        ) =>
        args should equal(Set("r", "a1"))
    }
  }

  test("should build optional ProjectEndpoints with extra predicates") {
    planFor("MATCH (a1)-[r]->(b1) WITH r, a1 LIMIT 1 OPTIONAL MATCH (a2)<-[r]-(b2) WHERE a1 = a2 RETURN a1, r, b2")._2 match {
      case Apply(
      Limit(Expand(AllNodesScan("b1", _), _, _, _, _, _, _), _, _),
      Optional(
      Selection(
      predicates,
      ProjectEndpoints(
      Argument(args),
      "r", "b2", false, "a2", false, None, true, SimplePatternLength
      )
      ), _
      )
      ) =>
        args should equal(Set("r", "a1"))
        val predicate: Expression = Equals(Variable("a1") _, Variable("a2") _) _
        predicates.exprs should equal(Set(predicate))
    }
  }

  test("should build optional ProjectEndpoints with extra predicates 2") {
    planFor("MATCH (a1)-[r]->(b1) WITH r LIMIT 1 OPTIONAL MATCH (a2)-[r]->(b2) RETURN a2, r, b2")._2 match {
      case Apply(
      Limit(Expand(AllNodesScan("b1", _), _, _, _, _, _, _), _, _),
      Optional(
      ProjectEndpoints(
      Argument(args),
      "r", "a2", false, "b2", false, None, true, SimplePatternLength
      ), _
      )
      ) =>
        args should equal(Set("r"))
    }
  }

  test("should solve multiple optional matches") {
    val plan = planFor("MATCH (a) OPTIONAL MATCH (a)-[:R1]->(x1) OPTIONAL MATCH (a)-[:R2]->(x2) RETURN a, x1, x2")._2.endoRewrite(unnestOptional)
    plan should equal(
      OptionalExpand(
        OptionalExpand(
          AllNodesScan("a", Set.empty),
          "a", SemanticDirection.OUTGOING, List(RelTypeName("R1") _), "x1", "  UNNAMED29", ExpandAll, Seq.empty),
        "a", SemanticDirection.OUTGOING, List(RelTypeName("R2") _), "x2", "  UNNAMED60", ExpandAll, Seq.empty)
    )
  }

  test("should solve optional matches with arguments and predicates") {
    val plan = new given {
      cost = {
        case (_: Expand, _, _) => 1000.0
      }
    }.getLogicalPlanFor(
      """MATCH (n:X)
        |OPTIONAL MATCH (n)-[r]-(m:Y)
        |WHERE m.prop = 42
        |RETURN m""".stripMargin)._2.endoRewrite(unnestOptional)
    val allNodesN: LogicalPlan = NodeByLabelScan("n", LabelName("X") _, Set.empty)
    val propEquality: Expression =
      In(Property(varFor("m"), PropertyKeyName("prop") _) _, ListLiteral(List(SignedDecimalIntegerLiteral("42") _)) _) _

    val labelCheck: Expression =
      HasLabels(varFor("m"), List(LabelName("Y") _)) _

    plan should equal(
      OptionalExpand(allNodesN, "n", SemanticDirection.BOTH, Seq.empty, "m", "r", ExpandAll,
        Seq(propEquality, labelCheck))
    )
  }

  test("should not plan outer hash joins when rhs has arguments other than join nodes") {
    val query = """
        |WITH 1 AS x
        |MATCH (a)
        |OPTIONAL MATCH (a)-[r]->(c)
        |WHERE c.id = x
        |RETURN c
        |""".stripMargin

    val cfg = new given {
      cost = {
        case (_: RightOuterHashJoin, _, _) => 1.0
        case (_: LeftOuterHashJoin, _, _) => 1.0
        case _ => Double.MaxValue
      }
    }

    val plan = cfg.getLogicalPlanFor(query)._2
    inside(plan) {
      case Apply(_:Projection, Apply(_:AllNodesScan, Optional(Expand(Selection(_, AllNodesScan("c", arguments)), _, _, _, _, _, _), _))) =>
        arguments should equal(Set("a", "x"))
    }
  }
}
