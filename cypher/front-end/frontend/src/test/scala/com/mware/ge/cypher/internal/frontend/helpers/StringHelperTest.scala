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
package com.mware.ge.cypher.internal.frontend.helpers

import org.apache.commons.lang3.SystemUtils
import com.mware.ge.cypher.internal.util.test_helpers.CypherFunSuite

class StringHelperTest extends CypherFunSuite {

  import com.mware.ge.cypher.internal.util.helpers.StringHelper._

  test("should not fix position when the text contains no line break") {
    val text = "(line 1, column 8 (offset: 7))"

    text.fixPosition should equal("(line 1, column 8 (offset: 7))")
  }

  test("should fix positions on Windows after line breaks") {
    val text = "(line 3, column 8 (offset: 7))"

    if (SystemUtils.IS_OS_WINDOWS) {
      text.fixPosition should equal("(line 3, column 8 (offset: 9))")
    } else {
      text.fixPosition should equal("(line 3, column 8 (offset: 7))")
    }
  }
}
