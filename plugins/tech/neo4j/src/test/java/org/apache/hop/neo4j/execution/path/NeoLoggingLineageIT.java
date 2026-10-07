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

package org.apache.hop.neo4j.execution.path;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assumptions.abort;
import static org.junit.jupiter.api.Assumptions.assumeTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;

import java.time.Duration;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;
import org.apache.hop.core.logging.ILogChannel;
import org.apache.hop.core.logging.LoggingHierarchy;
import org.apache.hop.core.logging.LoggingObject;
import org.apache.hop.core.logging.LoggingObjectType;
import org.apache.hop.neo4j.execution.path.base.NeoExecutionViewerTabBase;
import org.apache.hop.neo4j.logging.util.LoggingCore;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.neo4j.driver.AuthTokens;
import org.neo4j.driver.Driver;
import org.neo4j.driver.GraphDatabase;
import org.neo4j.driver.Record;
import org.neo4j.driver.Session;
import org.neo4j.driver.types.Node;
import org.testcontainers.DockerClientFactory;
import org.testcontainers.containers.Neo4jContainer;
import org.testcontainers.utility.DockerImageName;

/**
 * Reads the error lineage and the lineage of the Execution information perspective from a graph
 * written by both the Neo4j execution information location and Neo4j logging, see issue #8704.
 *
 * <p>Both write Execution nodes keyed on the log channel ID. The execution information location
 * merges on the ID alone and stores registrationDate as a local date time. Neo4j logging merges on
 * name, type and ID, so it creates a second node next to the one of the execution information
 * location, and stores registrationDate as a string. Later state updates of the execution
 * information location match both nodes. The perspective used to walk through the nodes of Neo4j
 * logging as well, which failed with "Cannot coerce STRING to LocalDateTime" and showed every path
 * more than once.
 *
 * <p>Skipped when Docker is unavailable or the Neo4j container cannot start in time.
 */
class NeoLoggingLineageIT {

  private static final String WORKFLOW_ID = "it-8704-workflow";
  private static final String PIPELINE_ID = "it-8704-pipeline";
  private static final String FAILED_ID = "it-8704-failed";
  private static final String OK_ID = "it-8704-ok";

  private static Neo4jContainer<?> neo4j;
  private static Driver driver;
  private static ILogChannel log;

  @BeforeAll
  static void setUp() {
    assumeTrue(
        DockerClientFactory.instance().isDockerAvailable(),
        "Docker is required for NeoLoggingLineageIT");

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
      abort("Neo4j container did not become ready: " + e.getMessage());
    }

    driver = GraphDatabase.driver(neo4j.getBoltUrl(), AuthTokens.none());

    try (Session session = driver.session()) {
      // What the Neo4j execution information location registers: a workflow which executes a
      // pipeline with one failed and one successful transform.
      //
      session.executeWrite(
          tx -> {
            tx.run(
                """
                CREATE (workflow:Execution { id : $workflowId, name : 'main',\
                 executionType : 'Workflow', registrationDate : $date })
                CREATE (pipeline:Execution { id : $pipelineId, name : 'load',\
                 executionType : 'Pipeline', parentId : $workflowId, registrationDate : $date })
                CREATE (failed:Execution { id : $failedId, name : 'Fail',\
                 executionType : 'Transform', parentId : $pipelineId, registrationDate : $date })
                CREATE (ok:Execution { id : $okId, name : 'Rows',\
                 executionType : 'Transform', parentId : $pipelineId, registrationDate : $date })
                CREATE (workflow)-[:EXECUTES]->(pipeline)
                CREATE (pipeline)-[:EXECUTES]->(failed)
                CREATE (pipeline)-[:EXECUTES]->(ok)
                """,
                Map.of(
                    "workflowId", WORKFLOW_ID,
                    "pipelineId", PIPELINE_ID,
                    "failedId", FAILED_ID,
                    "okId", OK_ID,
                    "date", LocalDateTime.of(2026, 9, 30, 14, 15, 16)));
            return null;
          });

      // What Neo4j logging writes at the end of the top level workflow, through the real code.
      //
      LoggingObject workflow = loggingObject(LoggingObjectType.WORKFLOW, WORKFLOW_ID, "main", null);
      LoggingObject action =
          loggingObject(LoggingObjectType.ACTION, "it-8704-action", "load.hpl", workflow);
      LoggingObject pipeline =
          loggingObject(LoggingObjectType.PIPELINE, PIPELINE_ID, "load", action);
      LoggingObject failed =
          loggingObject(LoggingObjectType.TRANSFORM, FAILED_ID, "Fail", pipeline);
      LoggingObject ok = loggingObject(LoggingObjectType.TRANSFORM, OK_ID, "Rows", pipeline);
      List<LoggingHierarchy> hierarchies = new ArrayList<>();
      for (LoggingObject loggingObject : List.of(workflow, action, pipeline, failed, ok)) {
        hierarchies.add(new LoggingHierarchy(WORKFLOW_ID, loggingObject));
      }

      log = mock(ILogChannel.class);
      session.executeWrite(
          tx -> {
            LoggingCore.writeHierarchies(log, null, tx, hierarchies, WORKFLOW_ID);
            return null;
          });

      // The final state updates of the execution information location merge on the ID only, so
      // they reach the nodes of Neo4j logging as well.
      //
      session.executeWrite(
          tx -> {
            tx.run(
                """
                MATCH (e:Execution) WHERE e.id IN [$workflowId, $pipelineId, $failedId]
                SET e.failed = true
                """,
                Map.of(
                    "workflowId", WORKFLOW_ID, "pipelineId", PIPELINE_ID, "failedId", FAILED_ID));
            tx.run(
                "MATCH (e:Execution { id : $okId }) SET e.failed = false", Map.of("okId", OK_ID));
            return null;
          });
    }
  }

  private static LoggingObject loggingObject(
      LoggingObjectType type, String id, String name, LoggingObject parent) {
    LoggingObject loggingObject = new LoggingObject(name);
    loggingObject.setObjectType(type);
    loggingObject.setLogChannelId(id);
    loggingObject.setObjectName(name);
    loggingObject.setParent(parent);
    loggingObject.setRegistrationDate(new Date());
    return loggingObject;
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

  @Test
  void neo4jLoggingWroteStringDatesNextToTheExecutionInformation() {
    verify(log, never()).logError(anyString(), any(Throwable.class));

    try (Session session = driver.session()) {
      long stringDates =
          session.executeRead(
              tx ->
                  tx.run(
                          """
                          MATCH (e:Execution { id : $failedId })
                          WHERE valueType(e.registrationDate) STARTS WITH 'STRING'
                          RETURN count(e)
                          """,
                          Map.of("failedId", FAILED_ID))
                      .single()
                      .get(0)
                      .asLong());
      assertEquals(1, stringDates);
    }
  }

  @Test
  void errorLineageIsOnePathThroughTheExecutionInformation() {
    List<List<Node>> paths =
        readPaths(NeoExecutionViewerTabBase.buildPathToFailedCypher(), WORKFLOW_ID);

    assertEquals(1, paths.size());
    assertExecutionInformation(paths.get(0));
    assertEquals(
        List.of(WORKFLOW_ID, PIPELINE_ID, FAILED_ID),
        paths.get(0).stream().map(node -> node.get("id").asString()).toList());
  }

  @Test
  void lineageIsOnePathFromTheRootThroughTheExecutionInformation() {
    // Neo4j logging writes a second node for the failed transform without a parentId. Picking
    // roots by a missing parentId made that node its own root, which Neo4j 5 rejects for a
    // shortestPath.
    //
    List<List<Node>> paths =
        readPaths(NeoExecutionViewerTabBase.buildPathToRootCypher(true), FAILED_ID);

    assertEquals(1, paths.size());
    assertExecutionInformation(paths.get(0));
    assertEquals(
        List.of(WORKFLOW_ID, PIPELINE_ID, FAILED_ID),
        paths.get(0).stream().map(node -> node.get("id").asString()).toList());
  }

  private static List<List<Node>> readPaths(String cypher, String executionId) {
    try (Session session = driver.session()) {
      List<Record> records =
          session.executeRead(tx -> tx.run(cypher, Map.of("executionId", executionId)).list());
      List<List<Node>> paths = new ArrayList<>();
      for (Record record : records) {
        List<Node> path = new ArrayList<>();
        record.get(0).asPath().nodes().forEach(path::add);
        paths.add(path);
      }
      return paths;
    }
  }

  /** Checks the nodes and reads every date the way the Error lineage and Lineage tabs do. */
  private static void assertExecutionInformation(List<Node> path) {
    for (Node node : path) {
      String id = node.get("id").asString();
      assertTrue(node.get("type").isNull(), "Execution " + id + " was written by Neo4j logging");
      assertNotNull(
          LoggingCore.getDateValue(node, "registrationDate"),
          "No registration date for execution " + id);
    }
  }
}
