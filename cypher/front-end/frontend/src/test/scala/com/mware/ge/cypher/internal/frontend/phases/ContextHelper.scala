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
package com.mware.ge.cypher.internal.frontend.phases

import com.mware.ge.cypher.internal.ast.semantics.SemanticErrorDef
import com.mware.ge.cypher.internal.frontend.phases.CompilationPhaseTracer.NO_TRACING
import com.mware.ge.cypher.internal.util.CypherException
import com.mware.ge.cypher.internal.util.InputPosition
import com.mware.ge.cypher.internal.util.InternalException
import org.scalatest.mock.MockitoSugar

object ContextHelper extends MockitoSugar {
  def create(exceptionCreatorArg: (String, InputPosition) => CypherException = (_, _) => new InternalException("apa"),
             tracerArg: CompilationPhaseTracer = NO_TRACING,
             notificationLoggerArg: InternalNotificationLogger = devNullLogger,
             monitorsArg: Monitors = mock[Monitors]): BaseContext = {
    new BaseContext {
      override def tracer: CompilationPhaseTracer = tracerArg

      override def notificationLogger: InternalNotificationLogger = notificationLoggerArg

      override def exceptionCreator: (String, InputPosition) => CypherException = exceptionCreatorArg

      override def monitors: Monitors = monitorsArg

      override def errorHandler: Seq[SemanticErrorDef] => Unit =
        (errors: Seq[SemanticErrorDef]) => errors.foreach(e => throw exceptionCreator(e.msg, e.position))
    }
  }
}
