package com.mware.ge.cypher

import com.mware.ge.cypher.ScenarioTestHelper.{createTests, printComputedBlacklist}
import org.junit.jupiter.api.Assertions.fail
import org.junit.jupiter.api.{Disabled, DynamicTest, TestFactory}

class InMemoryAcceptanceTCKTests extends BaseTCKTests {
  // If you want to only run a specific feature or scenario, go to the BaseTCKTests

  @TestFactory
  def runCostInterpreted(): java.util.Collection[DynamicTest] = {
    createTests(scenarios, DefaultTestConfig, new TestCypherQueryContextFactory())
  }

  @Disabled
  def generateBlacklistCostInterpreted(): Unit = {
    printComputedBlacklist(scenarios, DefaultTestConfig, new TestCypherQueryContextFactory())
    fail("Do not forget to add @Disabled to this method")
  }
}

