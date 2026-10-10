/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hop.core.graph;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;
import org.apache.hop.core.exception.HopException;
import org.junit.jupiter.api.Test;

class CypherGraphDialectTest {

  @Test
  void testQuote() {
    assertEquals("`Person`", CypherGraphDialect.quote("Person"));
    assertEquals("`My Label`", CypherGraphDialect.quote("My Label"));
    assertEquals("`has-part`", CypherGraphDialect.quote("has-part"));
    assertEquals("`we``ird`", CypherGraphDialect.quote("we`ird"));
    // Quoted already: left alone
    assertEquals("`My Label`", CypherGraphDialect.quote("`My Label`"));
    assertEquals("`we``ird`", CypherGraphDialect.quote("`we``ird`"));
    // Not quoted as a whole: a backtick inside isn't doubled
    assertEquals("```a``b```", CypherGraphDialect.quote("`a`b`"));
    assertEquals("````", CypherGraphDialect.quote("`"));
    assertNull(CypherGraphDialect.quote(null));
  }

  /** Graph databases which don't tell their dialect get Cypher with the syntax of Neo4j 5. */
  @Test
  void testDefaults() throws HopException {
    IGraphDatabase database =
        new BaseGraphDatabase() {
          @Override
          public IGraphConnection connect(
              org.apache.hop.core.logging.ILogChannel log,
              org.apache.hop.core.variables.IVariables variables,
              String connectionName) {
            return null;
          }

          @Override
          public String test(
              org.apache.hop.core.variables.IVariables variables, String connectionName) {
            return null;
          }
        };
    assertEquals(CypherGraphDialect.DEFAULT, database.getGraphDialect());
    assertEquals(CypherGraphDialect.DEFAULT, database.getGraphDialect(null));
    assertEquals(
        "CREATE INDEX `i` IF NOT EXISTS FOR (n:`Person`) ON (n.`id`)",
        CypherGraphDialect.DEFAULT.getCreateIndexStatement(
            new GraphIndexDefinition("i", GraphObjectType.NODE, "Person", List.of("id"))));
  }

  /** A dialect which only has an ID supports nothing. */
  @Test
  void testNeutralDialect() {
    IGraphDialect dialect = () -> "NEUTRAL";
    assertTrue(dialect.isCypher());
    assertFalse(dialect.isSupportingIndexes());
    assertFalse(dialect.isSupportingConstraints());
    assertFalse(dialect.isSupportingVectorIndexes());
    assertFalse(dialect.isSupportingShortestPath());
    assertEquals("$v", dialect.vectorValue("$v"));
    assertNull(dialect.getCreateNodeKeyIndexStatement("Person", List.of("id")));
    HopException e =
        assertThrows(
            HopException.class,
            () ->
                dialect.getCreateIndexStatement(
                    new GraphIndexDefinition("i", GraphObjectType.NODE, "Person", List.of("id"))));
    assertTrue(e.getMessage().contains("not supported by NEUTRAL"));
  }
}
