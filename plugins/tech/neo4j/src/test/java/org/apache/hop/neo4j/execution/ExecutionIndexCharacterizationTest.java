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

package org.apache.hop.neo4j.execution;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.ArrayList;
import java.util.List;
import org.apache.hop.neo4j.bolt.BoltGraphDialect;
import org.apache.hop.neo4j.bolt.MemgraphGraphDialect;
import org.apache.hop.neo4j.bolt.Neo4jGraphDialect;
import org.apache.hop.neo4j.bolt.NeptuneGraphDialect;
import org.apache.hop.neo4j.shared.NamedGraphConnection;
import org.apache.hop.neo4j.shared.NeoConnection;
import org.junit.jupiter.api.DynamicTest;
import org.junit.jupiter.api.TestFactory;

/** Characterization of the execution information indexes, recorded before the dialect SPI. */
class ExecutionIndexCharacterizationTest {

  @TestFactory
  List<DynamicTest> executionIndexes() {
    List<DynamicTest> tests = new ArrayList<>();
    String one = "CREATE INDEX `idx_execution_id` IF NOT EXISTS FOR (n:`Execution`) ON (n.`id`)";
    String two =
        "CREATE INDEX `idx_execution_id` IF NOT EXISTS FOR (n:`Execution`)"
            + " ON (n.`name`, n.`type`)";
    // FalkorDB, AGE and Gremlin: see the characterization tests in their plugins
    Object[][] expected = {
      {Neo4jGraphDialect.INSTANCE, one, two},
      {
        MemgraphGraphDialect.INSTANCE,
        "CREATE INDEX ON :`Execution`(`id`)",
        "CREATE INDEX ON :`Execution`(`name`, `type`)"
      },
      {NeptuneGraphDialect.INSTANCE, one, two},
    };
    for (Object[] row : expected) {
      BoltGraphDialect dialect = (BoltGraphDialect) row[0];
      NeoConnection neo = new NeoConnection();
      neo.setDialect(dialect);
      NamedGraphConnection connection = new NamedGraphConnection("c", neo, null);
      tests.add(
          DynamicTest.dynamicTest(
              dialect.getId() + "|one",
              () ->
                  assertEquals(
                      row[1],
                      NeoExecutionInfoLocation.getCreateIndexStatement(
                          connection, "idx_execution_id", "Execution", List.of("id")))));
      tests.add(
          DynamicTest.dynamicTest(
              dialect.getId() + "|two",
              () ->
                  assertEquals(
                      row[2],
                      NeoExecutionInfoLocation.getCreateIndexStatement(
                          connection, "idx_execution_id", "Execution", List.of("name", "type")))));
    }
    tests.add(
        DynamicTest.dynamicTest(
            "null|one",
            () ->
                assertEquals(
                    one,
                    NeoExecutionInfoLocation.getCreateIndexStatement(
                        null, "idx_execution_id", "Execution", List.of("id")))));
    return tests;
  }
}
