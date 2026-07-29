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

package org.apache.hop.neo4j.execution.path;

import static org.apache.hop.neo4j.CypherAssertions.assertNoSizeOfPatternExpression;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;
import org.junit.jupiter.api.Test;

/**
 * The Cypher generated here is not executed by Hop, it is presented in the execution information
 * viewer for the user to run in the Neo4j browser. It still has to be valid Cypher.
 */
class PathResultTest {

  private PathResult createWorkflowWithErrorPath() {
    PathResult pipeline = new PathResult();
    pipeline.setId("pipeline-id");
    pipeline.setName("load-customers");
    pipeline.setType("PIPELINE");

    PathResult transform = new PathResult();
    transform.setId("transform-id");
    transform.setName("Table output");
    transform.setType("TRANSFORM");

    PathResult workflow = new PathResult();
    workflow.setId("workflow-id");
    workflow.setName("main");
    workflow.setType("WORKFLOW");
    workflow.setShortestPaths(List.of(List.of(pipeline, transform)));

    return workflow;
  }

  @Test
  void errorPathWithMetadataCommandDoesNotUseSizeOnAPatternExpression() {
    assertNoSizeOfPatternExpression(
        createWorkflowWithErrorPath().getErrorPathWithMetadataCommand(0));
  }

  @Test
  void errorPathWithMetadataCommandOnlyKeepsLeafErrors() {
    assertTrue(
        createWorkflowWithErrorPath()
            .getErrorPathWithMetadataCommand(0)
            .contains("AND NOT (err)-[:EXECUTES]->()"),
        "The error node has to be a leaf, expressed as a pattern predicate");
  }

  @Test
  void errorPathWithMetadataCommandLinksTheMetadataOfEveryEntryInThePath() {
    String cypher = createWorkflowWithErrorPath().getErrorPathWithMetadataCommand(0);

    assertTrue(
        cypher.contains(
            "MATCH (:Execution { type : \"PIPELINE\", id : \"pipeline-id\"})-[metaRel1]->(meta1:Pipeline)"));
    assertTrue(
        cypher.contains(
            "MATCH (:Execution { type : \"TRANSFORM\", id : \"transform-id\"})-[metaRel2]->(meta2:Transform)"));
    assertTrue(cypher.contains("RETURN p, metaRel1, meta1, metaRel2, meta2"));
  }

  @Test
  void errorPathCommandDoesNotUseSizeOnAPatternExpression() {
    assertNoSizeOfPatternExpression(createWorkflowWithErrorPath().getErrorPathCommand());
  }
}
