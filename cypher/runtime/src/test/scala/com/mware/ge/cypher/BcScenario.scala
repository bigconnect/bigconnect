package com.mware.ge.cypher

import gherkin.pickles.Pickle
import org.junit.jupiter.api.function.Executable
import org.opencypher.tools.tck.api.events.TCKEvents
import org.opencypher.tools.tck.api.events.TCKEvents.{StepFinished, StepStarted, setStepFinished, setStepStarted}
import org.opencypher.tools.tck.api.{Dummy, Execute, ExpectError, ExpectResult, Graph, Measure, Parameters, ProcedureSupport, RegisterProcedure, Scenario, SideEffects, Step}

import scala.compat.Platform.EOL

object BcScenario {
  def apply(s: Scenario): BcScenario = {
    new BcScenario(s.featureName, s.name, s.tags, s.steps, s.source)
  }
}

class BcScenario(featureName: String, name: String,  tags: Set[String],  steps: List[Step],  source: Pickle)
  extends Scenario(featureName, name, tags, steps, source) {

  self =>

  override def apply(graph: => Graph): Executable = new Executable {
    override def execute(): Unit = {
      val g = graph // ensure that lazy parameter is only evaluated once
      try {
        TCKEvents.setScenario(self)
        executeOnGraph(g)
      } finally g.close()
    }
  }

  override def executeOnGraph(empty: Graph): Unit = {
    steps.foldLeft(ScenarioExecutionContext(empty)) { (context, step) =>
    {
      val eventId = setStepStarted(StepStarted(step))
      val stepResult: Either[ScenarioFailedException, ScenarioExecutionContext] = (context, step) match {

        case (ctx, Execute(query, qt, _)) =>
          Right(ctx.execute(query, qt))

        case (ctx, Measure(_)) =>
          Right(ctx.measure)

        case (ctx, RegisterProcedure(signature, table, _)) =>
          ctx.graph match {
            case support: ProcedureSupport =>
              support.registerProcedure(signature, table)
            case _ =>
          }
          Right(ctx)

        case (ctx, ExpectResult(expected, _, sorted)) =>
          ctx.lastResult match {
            case Right(records) =>
              val correctResult =
                if (sorted)
                  expected == records
                else
                  expected.equalsUnordered(records)

              if (!correctResult) {
                val detail = if (sorted) "ordered rows" else "in any order of rows"
                Left(ScenarioFailedException(s"${EOL}Expected ($detail):$EOL$expected${EOL}Actual:$EOL$records"))
              } else {
                Right(ctx)
              }
            case Left(error) =>
              Left(ScenarioFailedException(s"Expected: $expected, got error $error", error.exception.orNull))
          }

        case (ctx, e @ ExpectError(errorType, phase, detail, _)) =>
          ctx.lastResult match {
            case Left(error) =>
              if (error.errorType != errorType)
                Left(
                  ScenarioFailedException(
                    s"Wrong error type: expected $errorType, got ${error.errorType}",
                    error.exception.orNull))
              if (error.phase != phase)
                Left(
                  ScenarioFailedException(
                    s"Wrong error phase: expected $phase, got ${error.phase}",
                    error.exception.orNull))
              if (error.detail != detail)
                Left(
                  ScenarioFailedException(
                    s"Wrong error detail: expected $detail, got ${error.detail}",
                    error.exception.orNull))
              else {
                Right(ctx)
              }

            case Right(records) =>
              Left(ScenarioFailedException(s"Expected: $e, got records $records"))
          }

        case (ctx, SideEffects(expected, _)) =>
          val before = ctx.state
          val after = ctx.measure.state
          val diff = before diff after
          if (diff != expected) {
            System.out.println(s"${EOL}Expected side effects:$EOL$expected${EOL}Actual side effects:$EOL$diff")
          }

          Right(ctx)

        case (ctx, Parameters(ps, _)) =>
          Right(ctx.copy(parameters = ps))

        case (ctx, _: Dummy) => Right(ctx)
        case (_, s) =>
          throw new UnsupportedOperationException(s"Unsupported step: $s")
      }
      stepResult match {
        case Right(ctx) =>
          setStepFinished(StepFinished(step, Right(ctx.lastResult), eventId))
          ctx
        case Left(throwable) =>
          setStepFinished(StepFinished(step, Left(throwable), eventId))
          throw throwable
      }
    }
    }
  }
}
