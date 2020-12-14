package com.mware.ge.it.cypher

import com.mware.ge.cypher.ScenarioTestHelper.{createTests, printComputedBlacklist}
import com.mware.ge.cypher.{BaseTCKTests, DefaultTestConfig, TestCypherQueryContextFactory}
import com.mware.ge.it.CypherQueryContextFactoryIT
import org.junit.jupiter.api.Assertions.fail
import org.junit.jupiter.api.{Disabled, DynamicTest, TestFactory}

class AcceptanceTCKTests extends BaseTCKTests {
  // If you want to only run a specific feature or scenario, go to the BaseTCKTests

  @TestFactory
  def runCostInterpreted(): java.util.Collection[DynamicTest] = {
    createTests(scenarios, DefaultTestConfig, new CypherQueryContextFactoryIT())
  }

  @Disabled
  def generateBlacklistCostInterpreted(): Unit = {
    printComputedBlacklist(scenarios, DefaultTestConfig, new CypherQueryContextFactoryIT())
    fail("Do not forget to add @Disabled to this method")
  }
}
