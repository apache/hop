/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hop.neo4j;

import static org.junit.jupiter.api.Assertions.assertFalse;

import java.util.regex.Pattern;

/** Assertions shared by the tests which verify generated Cypher statements. */
public class CypherAssertions {

  /**
   * Matches size() applied to a pattern expression, for example {@code
   * size((err)-[:EXECUTES]->())}. Neo4j 5 removed that in favour of {@code COUNT {}} and pattern
   * predicates. size() applied to a list, for example {@code size(RELATIONSHIPS(p))}, is still
   * valid and is deliberately not matched here.
   */
  private static final Pattern SIZE_OF_PATTERN_EXPRESSION = Pattern.compile("size\\s*\\(\\s*\\(");

  private CypherAssertions() {
    // Static utility class
  }

  /**
   * Asserts that a Cypher statement does not use size() on a pattern expression, which fails on
   * Neo4j 5 and later.
   *
   * @param cypher the Cypher statement to verify
   */
  public static void assertNoSizeOfPatternExpression(String cypher) {
    assertFalse(
        SIZE_OF_PATTERN_EXPRESSION.matcher(cypher).find(),
        "Neo4j 5 removed size() on a pattern expression, use a pattern predicate instead: "
            + cypher);
  }
}
