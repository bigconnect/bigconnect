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
package com.mware.ge.cypher.internal

import com.mware.ge.cypher.internal.parser.matchers.{IdentifierPartMatcher, IdentifierStartMatcher, WhitespaceCharMatcher}
import org.parboiled.scala.Rule0

package object parser {
  lazy val IdentifierStart: Rule0 = new IdentifierStartMatcher()
  lazy val IdentifierPart: Rule0 = new IdentifierPartMatcher()
  lazy val WSChar: Rule0 = new WhitespaceCharMatcher()
}
