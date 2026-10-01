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

import static org.apache.hop.neo4j.CypherAssertions.assertNoSizeOfPatternExpression;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.apache.hop.neo4j.execution.path.base.NeoExecutionViewerTabBase;
import org.junit.jupiter.api.Test;

class NeoExecutionViewerTabBaseTest {

  @Test
  void lineageCypherIsDirectedAndBoundByChildId() {
    String cypher = NeoExecutionViewerTabBase.buildPathToRootCypher(true);

    assertTrue(cypher.contains("MATCH (child:Execution {id: $executionId })"));
    assertTrue(cypher.contains("[:EXECUTES*]->(child)"));
    assertFalse(cypher.contains("MATCH(top:Execution), (child:Execution"));
    assertNoSizeOfPatternExpression(cypher);
  }

  @Test
  void errorPathCypherUsesALeafPredicateAndBooleanFailed() {
    String cypher = NeoExecutionViewerTabBase.buildPathToFailedCypher();

    assertTrue(cypher.contains("child.failed = true"));
    assertTrue(cypher.contains("AND   NOT (child)-[:EXECUTES]->()"));
    assertTrue(cypher.contains("[:EXECUTES*]->(child:Execution)"));
    assertNoSizeOfPatternExpression(cypher);
  }

  @Test
  void rootExecutionLineageDoesNotWalkAPath() {
    String cypher = NeoExecutionViewerTabBase.buildPathToRootCypher(false);

    assertTrue(cypher.contains("RETURN e"));
    assertFalse(cypher.contains("shortestPath"));
  }
}
