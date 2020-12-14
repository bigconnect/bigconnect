package com.mware.ge.cypher

import org.opencypher.tools.tck.api.Scenario

import scala.util.matching.Regex

case class BlacklistEntry(featureName: Option[String], scenarioName: String) {
  def isBlacklisted(scenario: BcScenario): Boolean = {
    scenarioName == scenario.name && (featureName.isEmpty || featureName.get == scenario.featureName)
  }

  override def toString: String = {
    if (featureName.isDefined) {
      s"""Feature "${featureName.get}": Scenario "$scenarioName""""
    } else {
      s"""$scenarioName"""  // legacy version
    }
  }
}

object BlacklistEntry {
  val entryPattern: Regex = """Feature "(.*)": Scenario "(.*)"""".r

  def apply(line: String): BlacklistEntry = {
    if (line.startsWith("Feature")) {
      line match {
        case entryPattern(featureName, scenarioName) => new BlacklistEntry(Some(featureName), scenarioName)
        case other => throw new UnsupportedOperationException(s"Could not parse blacklist entry $other")
      }

    } else new BlacklistEntry(None, line)
  }
}

