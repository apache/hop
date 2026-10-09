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

package org.apache.hop.neo4j.bolt;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.graph.GraphConstraintType;
import org.apache.hop.neo4j.actions.constraint.ConstraintUpdate;
import org.apache.hop.neo4j.actions.constraint.Neo4jConstraint;
import org.apache.hop.neo4j.actions.index.IndexUpdate;
import org.apache.hop.neo4j.actions.index.Neo4jIndex;
import org.apache.hop.neo4j.actions.index.ObjectType;
import org.apache.hop.neo4j.actions.index.UpdateType;
import org.junit.jupiter.api.Test;

/** The statements were checked against Memgraph 3.6, see MemgraphBoltIT. */
class BoltGraphDialectTest {

  private static IndexUpdate index(ObjectType objectType, String name, String properties) {
    return new IndexUpdate(UpdateType.CREATE, objectType, "idx", name, properties);
  }

  private static ConstraintUpdate constraint(
      GraphConstraintType type, String name, String label, String properties) {
    return new ConstraintUpdate(
        org.apache.hop.neo4j.actions.constraint.UpdateType.CREATE,
        org.apache.hop.neo4j.actions.constraint.ObjectType.NODE,
        type,
        name,
        label,
        properties);
  }

  /** Labels, types, properties and names with spaces, backticks and dashes are quoted. */
  @Test
  void testNeo4jQuoting() throws Exception {
    Neo4jGraphDialect neo4j = Neo4jGraphDialect.INSTANCE;
    assertEquals(
        "CREATE INDEX `my-index` IF NOT EXISTS FOR (n:`My Label`) ON (n.`first name`,"
            + " n.`we``ird`)",
        Neo4jIndex.generateCreateIndexCypher(
            new IndexUpdate(
                UpdateType.CREATE, ObjectType.NODE, "my-index", "My Label", "first name, we`ird"),
            neo4j));
    assertEquals(
        "CREATE INDEX `idx` IF NOT EXISTS FOR ()-[n:`HAS-PART`]-() ON (n.`since`)",
        Neo4jIndex.generateCreateIndexCypher(
            index(ObjectType.RELATIONSHIP, "HAS-PART", "since"), neo4j));
    assertEquals(
        "DROP INDEX `my index` IF EXISTS",
        Neo4jIndex.generateDropIndexCypher(
            new IndexUpdate(UpdateType.DROP, ObjectType.NODE, "my index", "L", "p"), neo4j));
    // Names quoted already are left alone
    assertEquals(
        "CREATE CONSTRAINT `c` IF NOT EXISTS FOR (n:`My Label`) REQUIRE  n.`id` IS UNIQUE ",
        Neo4jConstraint.generateCreateConstraintCypher(
            constraint(GraphConstraintType.UNIQUE, "`c`", "`My Label`", "`id`"), neo4j));
    assertEquals(
        "CREATE CONSTRAINT IF NOT EXISTS FOR (n:`Per-son`) REQUIRE n.`the id` IS UNIQUE;",
        neo4j.getCreateNodeKeyIndexStatement("Per-son", List.of("the id")));
  }

  @Test
  void testMemgraphQuoting() throws Exception {
    MemgraphGraphDialect memgraph = MemgraphGraphDialect.INSTANCE;
    assertEquals(
        "CREATE INDEX ON :`My Label`(`first name`, `a-b`)",
        Neo4jIndex.generateCreateIndexCypher(
            index(ObjectType.NODE, "My Label", "first name, a-b"), memgraph));
    assertEquals(
        "CREATE CONSTRAINT ON (n:`We``ird`) ASSERT EXISTS (n.`my id`)",
        Neo4jConstraint.generateCreateConstraintCypher(
            constraint(GraphConstraintType.NOT_NULL, null, "We`ird", "my id"), memgraph));
    assertEquals(
        "CREATE INDEX ON :`Per son`(`a`, `b`)",
        memgraph.getCreateNodeKeyIndexStatement("Per son", List.of("a", "b")));
  }

  @Test
  void testNeo4jCompositeConstraints() throws Exception {
    assertEquals(
        "CREATE CONSTRAINT `c` IF NOT EXISTS FOR (n:`Person`) REQUIRE  (n.`a`, n.`b`) IS UNIQUE ",
        Neo4jConstraint.generateCreateConstraintCypher(
            constraint(GraphConstraintType.UNIQUE, "c", "Person", "a, b"),
            Neo4jGraphDialect.INSTANCE));
    // Neo4j has no composite existence constraints
    assertThrows(
        HopException.class,
        () ->
            Neo4jConstraint.generateCreateConstraintCypher(
                constraint(GraphConstraintType.NOT_NULL, "c", "Person", "a, b"),
                Neo4jGraphDialect.INSTANCE));
  }

  @Test
  void testNeptuneHasNoIndexesOrConstraints() {
    assertFalse(NeptuneGraphDialect.INSTANCE.isSupportingIndexes());
    assertFalse(NeptuneGraphDialect.INSTANCE.isSupportingConstraints());
    assertThrows(
        HopException.class,
        () ->
            Neo4jIndex.generateCreateIndexCypher(
                index(ObjectType.NODE, "Person", "name"), NeptuneGraphDialect.INSTANCE));
  }

  @Test
  void testMemgraphAutoCommitStatements() {
    MemgraphGraphDialect memgraph = MemgraphGraphDialect.INSTANCE;
    assertTrue(memgraph.isRequiringAutoCommit("SHOW INDEX INFO"));
    assertTrue(memgraph.isRequiringAutoCommit("CREATE VECTOR INDEX v ON :Doc(e) WITH CONFIG {}"));
    assertTrue(
        memgraph.isRequiringAutoCommit("CREATE VECTOR EDGE INDEX v ON :LINKS(e) WITH CONFIG {}"));
    assertTrue(memgraph.isRequiringAutoCommit("DROP VECTOR EDGE INDEX v"));
    assertFalse(memgraph.isRequiringAutoCommit("MATCH (n:Showcase) RETURN n"));
    assertFalse(Neo4jGraphDialect.INSTANCE.isRequiringAutoCommit("SHOW INDEXES"));
  }

  /** Memgraph has no IF [NOT] EXISTS: its errors about existing or missing schema are tolerated. */
  @Test
  void testMemgraphExistingOrMissing() {
    MemgraphGraphDialect memgraph = MemgraphGraphDialect.INSTANCE;
    assertTrue(memgraph.isExistingOrMissingIndexError(new HopException("Index already exists.")));
    assertTrue(
        memgraph.isExistingOrMissingIndexError(
            new HopException("wrapper", new RuntimeException("Vector index doesn't exist."))));
    assertTrue(
        memgraph.isExistingOrMissingIndexError(new HopException("Constraint does not exist")));
    assertFalse(memgraph.isExistingOrMissingIndexError(new HopException("Syntax error")));
    // The text Memgraph 3 gives, see MemgraphBoltIT
    assertTrue(
        memgraph.isExistingOrMissingIndexError(
            new HopException(
                "Schema change failed",
                new RuntimeException("Given vector index already exists."))));
    // Only the root cause counts, not the messages wrapping it
    assertFalse(
        memgraph.isExistingOrMissingIndexError(
            new HopException("Index already exists?", new RuntimeException("Connection refused"))));
    assertFalse(
        Neo4jGraphDialect.INSTANCE.isExistingOrMissingIndexError(
            new HopException("Index already exists.")));
  }
}
