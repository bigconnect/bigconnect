package com.mware.ge.cypher

import org.junit.Assert.fail
import org.junit.jupiter.api.Test
import org.opencypher.tools.tck.api.{CypherTCK, Scenario}

abstract class BaseTCKTests extends BaseFeatureTest {

  // these two should be empty on commit!
  val featureToRun = ""
  val scenarioToRun = ""

  val scenarios: Seq[BcScenario] = filterScenarios(CypherTCK.allTckScenarios, featureToRun, scenarioToRun)

  @Test
  def debugTokensNeedToBeEmpty(): Unit = {
    // besides the obvious reason this test is also here (and not using assert)
    // to ensure that any import optimizer doesn't remove the correct import for fail (used by the commented out methods further down)
    if (!scenarioToRun.equals(""))
      fail("scenarioToRun is only for debugging and should not be committed")

    if (!featureToRun.equals(""))
      fail("featureToRun is only for debugging and should not be committed")
  }
}

