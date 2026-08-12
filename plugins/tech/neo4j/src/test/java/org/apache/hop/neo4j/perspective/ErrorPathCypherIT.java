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

package org.apache.hop.neo4j.perspective;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assumptions.abort;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.neo4j.driver.AuthTokens;
import org.neo4j.driver.Driver;
import org.neo4j.driver.GraphDatabase;
import org.neo4j.driver.Record;
import org.neo4j.driver.Session;
import org.neo4j.driver.exceptions.ClientException;
import org.neo4j.driver.types.Node;
import org.testcontainers.DockerClientFactory;
import org.testcontainers.containers.Neo4jContainer;
import org.testcontainers.utility.DockerImageName;

/**
 * Runs the error path lookup of the Neo4j perspective against a real Neo4j 5 server.
 *
 * <p>Neo4j 5 removed size() on a pattern expression, which made the Error path tab of the Neo4j
 * perspective fail. This verifies both that the statement is accepted by Neo4j 5 and that it still
 * selects the deepest execution which reported errors.
 *
 * <p>Skipped when Docker is unavailable or the Neo4j container cannot start in time (common on
 * constrained CI agents). Unit tests in {@link HopNeo4jPerspectiveTest} still guard the Cypher
 * shape without Docker.
 */
class ErrorPathCypherIT {

  private static Neo4jContainer<?> neo4j;
  private static Driver driver;

  @BeforeAll
  static void setUp() {
    assumeTrue(
        DockerClientFactory.instance().isDockerAvailable(),
        "Docker is required for ErrorPathCypherIT");

    // Cap heap/pagecache so Neo4j can start on small CI agents; default memory can hang/timeout.
    neo4j =
        new Neo4jContainer<>(DockerImageName.parse("neo4j:5.26"))
            .withoutAuthentication()
            .withEnv("NEO4J_server_memory_heap_initial__size", "256m")
            .withEnv("NEO4J_server_memory_heap_max__size", "512m")
            .withEnv("NEO4J_server_memory_pagecache_size", "64m")
            .withStartupTimeout(Duration.ofMinutes(5));

    try {
      neo4j.start();
    } catch (Exception e) {
      abort("Neo4j container did not become ready (issue #7898): " + e.getMessage());
    }

    driver = GraphDatabase.driver(neo4j.getBoltUrl(), AuthTokens.none());

    // A workflow which executes a pipeline, which in turn executes a transform that failed.
    // The pipeline reports the errors of the transform, so both have errors > 0 and only the
    // transform is a leaf. A second transform completed without errors.
    //
    try (Session session = driver.session()) {
      session.executeWrite(
          tx -> {
            tx.run(
                """
                CREATE (workflow:Execution { id : 'workflow-id', name : 'main', type : 'WORKFLOW',\
                 registrationDate : '2026-07-29T10:00:00', errors : 1 })
                CREATE (pipeline:Execution { id : 'pipeline-id', name : 'load-customers',\
                 type : 'PIPELINE', registrationDate : '2026-07-29T10:00:01', errors : 1 })
                CREATE (failed:Execution { id : 'failed-id', name : 'Table output',\
                 type : 'TRANSFORM', registrationDate : '2026-07-29T10:00:02', errors : 1 })
                CREATE (ok:Execution { id : 'ok-id', name : 'Dummy', type : 'TRANSFORM',\
                 registrationDate : '2026-07-29T10:00:02', errors : 0 })
                CREATE (workflow)-[:EXECUTES]->(pipeline)
                CREATE (pipeline)-[:EXECUTES]->(failed)
                CREATE (pipeline)-[:EXECUTES]->(ok)
                """);
            return null;
          });
    }
  }

  @AfterAll
  static void tearDown() {
    if (driver != null) {
      driver.close();
    }
    if (neo4j != null) {
      neo4j.stop();
    }
  }

  private List<Record> runErrorPathCypher(String cypher) {
    try (Session session = driver.session()) {
      return session.executeRead(
          tx ->
              tx.run(
                      cypher,
                      Map.of(
                          HopNeo4jPerspective.CONST_SUBJECT_NAME, "main",
                          HopNeo4jPerspective.CONST_SUBJECT_TYPE, "WORKFLOW",
                          HopNeo4jPerspective.CONST_SUBJECT_ID, "workflow-id"))
                  .list());
    }
  }

  private List<String> executionIds(Record record) {
    List<String> ids = new ArrayList<>();
    for (Node node : record.get(0).asPath().nodes()) {
      ids.add(node.get("id").asString());
    }
    return ids;
  }

  @Test
  void errorPathCypherReturnsThePathToTheDeepestError() {
    List<Record> records = runErrorPathCypher(HopNeo4jPerspective.getErrorPathCypher());

    // Only the failed transform is a leaf with errors, so there is exactly one path.
    //
    assertEquals(1, records.size());
    assertEquals(List.of("workflow-id", "pipeline-id", "failed-id"), executionIds(records.get(0)));
  }

  @Test
  void errorPathCypherSkipsExecutionsWhichAreNotLeaves() {
    List<String> ids =
        executionIds(runErrorPathCypher(HopNeo4jPerspective.getErrorPathCypher()).get(0));

    // The pipeline reports the errors of the transform it executes, but it is not the deepest
    // error, so the path must not stop there.
    //
    assertEquals("failed-id", ids.get(ids.size() - 1));
    assertFalse(
        ids.contains("ok-id"), "An execution without errors must not show up in the error path");
  }

  @Test
  void sizeOnAPatternExpressionIsRejectedByNeo4j5() {
    // This is what the statement used to look like, kept here to document why it was changed.
    //
    String removedSyntax =
        HopNeo4jPerspective.getErrorPathCypher()
            .replace("NOT (err)-[:EXECUTES]->()", "size((err)-[:EXECUTES]->())=0");

    assertThrows(ClientException.class, () -> runErrorPathCypher(removedSyntax));
  }
}
