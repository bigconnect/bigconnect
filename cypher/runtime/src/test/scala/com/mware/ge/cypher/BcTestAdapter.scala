package com.mware.ge.cypher

import com.mware.core.model.schema.SchemaRepository
import com.mware.ge.{Authorizations}
import com.mware.ge.cypher.BcExceptionToExecutionFailed.convert
import Result.ResultVisitor
import org.opencypher.tools.tck.api._
import org.opencypher.tools.tck.values.CypherValue

import scala.collection.JavaConverters._
import scala.collection.mutable.ArrayBuffer
import scala.util.{Failure, Success, Try}

object BcTestAdapter {
  def apply(executionPrefix: String, bcEngine: CypherQueryContextFactory): BcTestAdapter = {
    new BcTestAdapter(executionPrefix, bcEngine.emptyGraph())
  }
}

class BcTestAdapter(executionPrefix: String, ctx: GeCypherExecutionEngine) extends Graph with BcProcedureAdapter {
  private val explainPrefix = "EXPLAIN\n"
  protected val executionEngine = ctx;

  override def cypher(query: String, params: Map[String, CypherValue], meta: QueryType): Result = {
    val bcParams = params.mapValues(v => TCKValueToBcValue(v)).asJava

    val queryToExecute = if (meta == ExecQuery) {
      s"$executionPrefix $query"
    } else query

    val result: Result =
      Try(ctx.executeQuery(queryToExecute, bcParams, new Authorizations(), SchemaRepository.PUBLIC))
        .flatMap(r => Try(convertResult(r))) match {
        case Success(converted) =>
          converted
        case Failure(exception) =>
          val explainedResult = Try(ctx.executeQuery(explainPrefix + queryToExecute, new Authorizations(), SchemaRepository.PUBLIC))
          val phase = explainedResult match {
            case Failure(_) => Phase.compile
            case Success(_) => Phase.runtime
          }
          convert(phase, exception)
      }

    result
  }

  def convertResult(result: com.mware.ge.cypher.Result): Result = {
    val header = result.columns().asScala.toList
    val rows = ArrayBuffer[Map[String, String]]()
    result.accept(new ResultVisitor[RuntimeException] {
      override def visit(row: Result.ResultRow): Boolean = {
        rows.append(header.map(k => k -> BcValueToString(row.get(k))).toMap)
        true
      }
    })
    StringRecords(header, rows.toList)
  }

  override def close(): Unit = {
    // do nothing
  }
}
