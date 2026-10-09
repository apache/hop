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

package org.apache.hop.falkordb;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.graph.GraphIndexDefinition;
import org.apache.hop.core.graph.GraphObjectType;
import org.apache.hop.core.graph.GraphVectorIndexDefinition;
import org.apache.hop.core.graph.GraphVectorSimilarity;
import org.junit.jupiter.api.Test;

/** The statements were checked against FalkorDB 6, see FalkorDbIT. */
class FalkorDbGraphDialectTest {

  private final FalkorDbGraphDialect falkordb = FalkorDbGraphDialect.INSTANCE;

  @Test
  void testDatabaseAndConnectionUseTheDialect() {
    assertTrue(new FalkorDbGraphDatabase().getGraphDialect() instanceof FalkorDbGraphDialect);
  }

  @Test
  void testQuoting() throws Exception {
    assertEquals(
        "CREATE INDEX FOR (n:`My Label`) ON (n.`first name`, n.`a-b`)",
        falkordb.getCreateIndexStatement(
            new GraphIndexDefinition(
                null, GraphObjectType.NODE, "My Label", List.of("first name", "a-b"))));
    // FalkorDB's parser has no way to write a backtick in a name
    assertThrows(
        IllegalArgumentException.class,
        () ->
            falkordb.getCreateIndexStatement(
                new GraphIndexDefinition(null, GraphObjectType.NODE, "We`ird", List.of("id"))));
    assertEquals(
        "DROP VECTOR INDEX FOR ()-[n:`HAS-PART`]-() ON (n.`embed-ding`)",
        falkordb.getDropVectorIndexStatement(
            new GraphVectorIndexDefinition(
                null,
                GraphObjectType.RELATIONSHIP,
                "HAS-PART",
                List.of("embed-ding"),
                null,
                null,
                null)));
    assertEquals(
        "CREATE INDEX FOR (n:`Per son`) ON (n.`id`)",
        falkordb.getCreateNodeKeyIndexStatement("Per son", List.of("id")));
  }

  @Test
  void testVectorIndexOnSingleProperty() {
    assertThrows(
        HopException.class,
        () ->
            falkordb.getCreateVectorIndexStatement(
                new GraphVectorIndexDefinition(
                    "i",
                    GraphObjectType.NODE,
                    "Doc",
                    List.of("a", "b"),
                    3,
                    GraphVectorSimilarity.COSINE,
                    null)));
  }

  /** FalkorDB has no IF [NOT] EXISTS: its errors about existing or missing indexes are ignored. */
  @Test
  void testExistingOrMissingIndex() {
    // The texts FalkorDB 6 gives, see FalkorDbIT
    assertTrue(
        falkordb.isExistingOrMissingIndexError(
            new HopException("Attribute 'name' is already indexed")));
    assertTrue(
        falkordb.isExistingOrMissingIndexError(
            new HopException("wrapper", new RuntimeException("ERR no such index"))));
    assertFalse(falkordb.isExistingOrMissingIndexError(new HopException("Index already exists.")));
    assertFalse(falkordb.isExistingOrMissingIndexError(new HopException("Index doesn't exist.")));
    assertFalse(falkordb.isExistingOrMissingIndexError(new HopException("boom")));
    // Only the root cause counts, not the messages wrapping it, which contain the statement
    assertFalse(
        falkordb.isExistingOrMissingIndexError(
            new HopException(
                "Error executing statement: DROP INDEX ON :`no such index`(name)",
                new RuntimeException("Connection refused"))));
  }

  @Test
  void testVectorValue() {
    assertEquals("vecf32($p)", falkordb.vectorValue("$p"));
    assertEquals("vecf32(pr.p)", falkordb.vectorValue("pr.p"));
  }
}
