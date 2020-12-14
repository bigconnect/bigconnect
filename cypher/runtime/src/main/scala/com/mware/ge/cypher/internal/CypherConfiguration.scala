/*
 * Copyright (c) 2013-2020 "BigConnect,"
 * MWARE SOLUTIONS SRL
 *
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
package com.mware.ge.cypher.internal

import java.util.concurrent.TimeUnit

import com.mware.ge.cypher.internal.compatibility.CypherRuntimeConfiguration
import com.mware.ge.cypher.internal.compiler.{CypherPlannerConfiguration, StatsDivergenceCalculator}
import com.mware.ge.cypher._

import scala.concurrent.duration.Duration

/**
  * Holds all configuration options for the Neo4j Cypher execution engine, compilers and runtimes.
  */
object CypherConfiguration {
  def fromConfig(): CypherConfiguration = {
    CypherConfiguration(
      CypherVersion(CypherOption.DEFAULT),
      CypherPlannerOption(CypherOption.DEFAULT),
      CypherRuntimeOption(CypherOption.DEFAULT),
      1000,
      statsDivergenceFromConfig(),
      false,
      128,
      1000,
      false,
      true,
      true,
      2 * 1024 * 1024,
      true,
      CypherExpressionEngineOption(CypherOption.DEFAULT),
      false,
      0,
      10000,
      false,
      30000,
      1
    )
  }

  def statsDivergenceFromConfig(): StatsDivergenceCalculator = {
    val divergenceThreshold = 0.75
    val targetThreshold = 0.10
    val minReplanTime = java.time.Duration.ofSeconds(10).toMillis.longValue()
    val targetReplanTime = java.time.Duration.ofHours(7).toMillis.longValue()
    val divergenceAlgorithm = CypherOption.DEFAULT
    StatsDivergenceCalculator.divergenceCalculatorFor(divergenceAlgorithm,
                                                      divergenceThreshold,
                                                      targetThreshold,
                                                      minReplanTime,
                                                      targetReplanTime)
  }
}

case class CypherConfiguration(version: CypherVersion,
                               planner: CypherPlannerOption,
                               runtime: CypherRuntimeOption,
                               queryCacheSize: Int,
                               statsDivergenceCalculator: StatsDivergenceCalculator,
                               useErrorsOverWarnings: Boolean,
                               idpMaxTableSize: Int,
                               idpIterationDuration: Long,
                               errorIfShortestPathFallbackUsedAtRuntime: Boolean,
                               errorIfShortestPathHasCommonNodesAtRuntime: Boolean,
                               legacyCsvQuoteEscaping: Boolean,
                               csvBufferSize: Int,
                               planWithMinimumCardinalityEstimates: Boolean,
                               expressionEngineOption: CypherExpressionEngineOption,
                               lenientCreateRelationship: Boolean,
                               workers: Int,
                               morselSize: Int,
                               doSchedulerTracing: Boolean,
                               waitTimeout: Int,
                               recompilationLimit: Int) {

  def toCypherRuntimeConfiguration: CypherRuntimeConfiguration =
    CypherRuntimeConfiguration(
      workers = workers,
      morselSize = morselSize,
      doSchedulerTracing = doSchedulerTracing,
      waitTimeout = Duration(waitTimeout, TimeUnit.MILLISECONDS)
    )

  def toCypherPlannerConfiguration(): CypherPlannerConfiguration =
    CypherPlannerConfiguration(
      queryCacheSize = queryCacheSize,
      statsDivergenceCalculator = CypherConfiguration.statsDivergenceFromConfig(),
      useErrorsOverWarnings = useErrorsOverWarnings,
      idpMaxTableSize = idpMaxTableSize,
      idpIterationDuration = idpIterationDuration,
      errorIfShortestPathFallbackUsedAtRuntime = errorIfShortestPathFallbackUsedAtRuntime,
      errorIfShortestPathHasCommonNodesAtRuntime = errorIfShortestPathHasCommonNodesAtRuntime,
      legacyCsvQuoteEscaping = legacyCsvQuoteEscaping,
      csvBufferSize = csvBufferSize,
      nonIndexedLabelWarningThreshold = 10000,
      planWithMinimumCardinalityEstimates = planWithMinimumCardinalityEstimates,
      lenientCreateRelationship = lenientCreateRelationship
    )
}
