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

package org.apache.hop.neo4j.execution;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Date;
import org.junit.jupiter.api.Test;

class NeoExecutionInfoLocationDeleteTest {

  @Test
  void parentIdQueryFiltersByDateAndLimitsThePage() {
    Date cutoff = new Date(1_700_000_000_000L);
    String cypher = NeoExecutionInfoLocation.parentIdsToDelete(cutoff, 200).cypher();

    assertTrue(cypher.contains("parentId IS NULL"));
    assertTrue(cypher.contains("executionStartDate"));
    assertTrue(cypher.contains("$olderThan"));
    assertTrue(cypher.contains("IS NULL"));
    assertTrue(cypher.contains("LIMIT 200"));
  }

  @Test
  void parentIdQueryWithoutACutoffHasNoDateParameter() {
    String cypher = NeoExecutionInfoLocation.parentIdsToDelete(null, 200).cypher();

    assertTrue(cypher.contains("parentId IS NULL"));
    assertFalse(cypher.contains("$olderThan"));
    assertTrue(cypher.contains("LIMIT 200"));
  }

  @Test
  void batchDeleteLimitsEachTransaction() {
    String cypher =
        NeoExecutionInfoLocation.batchDelete("ExecutionDataSetRow", "parentId", "abc", 500)
            .cypher();

    assertTrue(cypher.contains("ExecutionDataSetRow"));
    assertTrue(cypher.contains("LIMIT 500"));
    assertTrue(cypher.contains("DETACH DELETE"));
    assertTrue(cypher.contains("AS deleted"));
  }
}
