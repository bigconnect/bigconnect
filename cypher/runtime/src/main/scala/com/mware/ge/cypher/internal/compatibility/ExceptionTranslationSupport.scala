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
package com.mware.ge.cypher.internal.compatibility

import com.mware.ge.cypher.TokenNameLookup
import com.mware.ge.cypher.exception.KernelException
import com.mware.ge.io.ResourceCloseFailureException
import com.mware.ge.cypher.CypherExecutionException
import com.mware.ge.cypher.internal.planner.spi.TokenContext

trait ExceptionTranslationSupport {
  inner: TokenContext =>

  protected def translateException[A](f: => A) = try {
    f
  } catch {
    case e: KernelException => throw new CypherExecutionException(e.getUserMessage(new TokenNameLookup {
      def propertyKeyGetName(propertyKeyId: String): String = inner.getPropertyKeyName(propertyKeyId)

      def labelGetName(labelId: String): String = inner.getLabelName(labelId)

      def relationshipTypeGetName(relTypeId: String): String = inner.getRelTypeName(relTypeId)
    }), e)
    case e : ResourceCloseFailureException => throw new CypherExecutionException(e.getMessage, e)
  }

  protected def translateIterator[A](iteratorFactory: => Iterator[A]): Iterator[A] = {
    val iterator = translateException(iteratorFactory)
    new Iterator[A] {
      override def hasNext: Boolean = translateException(iterator.hasNext)
      override def next(): A = translateException(iterator.next())
    }
  }
}
