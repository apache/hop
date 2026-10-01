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

package org.apache.hop.neo4j.perspective;

import static org.apache.hop.neo4j.CypherAssertions.assertNoSizeOfPatternExpression;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.apache.hop.i18n.BaseMessages;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

class HopNeo4jPerspectiveTest {

  /** BaseMessages returns !key! when a key is missing from the bundle. */
  @ParameterizedTest
  @ValueSource(
      strings = {
        "Neo4jPerspectiveDialog.NoFileFound.Header",
        "Neo4jPerspectiveDialog.NoFileFound.Message",
        "Neo4jPerspectiveDialog.NoLoggingFound.Message"
      })
  void messageKeysAreTranslated(String key) {
    String message = BaseMessages.getString(HopNeo4jPerspective.PKG, key, "a-name", "PIPELINE");

    assertFalse(message.startsWith("!"), "Missing key in the message bundle: " + key);
  }

  @Test
  void errorPathCypherDoesNotUseSizeOnAPatternExpression() {
    assertNoSizeOfPatternExpression(HopNeo4jPerspective.getErrorPathCypher());
  }

  @Test
  void errorPathCypherOnlyKeepsLeafErrors() {
    assertTrue(
        HopNeo4jPerspective.getErrorPathCypher().contains("AND NOT (err)-[:EXECUTES]->()"),
        "The error node has to be a leaf, expressed as a pattern predicate");
  }

  @Test
  void errorPathCypherStillSortsOnTheLengthOfThePath() {
    // size() on a list is not affected by the Neo4j 5 removal and has to stay as it is.
    //
    assertTrue(
        HopNeo4jPerspective.getErrorPathCypher().contains("ORDER BY size(RELATIONSHIPS(p)) DESC"));
  }

  @Test
  void errorPathCypherPassesTheSubjectAsParameters() {
    String cypher = HopNeo4jPerspective.getErrorPathCypher();

    assertTrue(cypher.contains("$" + HopNeo4jPerspective.CONST_SUBJECT_NAME));
    assertTrue(cypher.contains("$" + HopNeo4jPerspective.CONST_SUBJECT_TYPE));
    assertTrue(cypher.contains("$" + HopNeo4jPerspective.CONST_SUBJECT_ID));
  }

  @Test
  void errorPathCypherIsComplete() {
    assertEquals(
        "MATCH(top:Execution { name : $subjectName, type : $subjectType, id : $subjectId })-[rel:EXECUTES*]-(err:Execution) "
            + "   , p=shortestpath((top)-[:EXECUTES*]-(err)) "
            + "WHERE top.registrationDate IS NOT NULL "
            + "  AND err.errors > 0 "
            + "  AND NOT (err)-[:EXECUTES]->() "
            + "RETURN p "
            + "ORDER BY size(RELATIONSHIPS(p)) DESC "
            + "LIMIT 10",
        HopNeo4jPerspective.getErrorPathCypher());
  }
}
