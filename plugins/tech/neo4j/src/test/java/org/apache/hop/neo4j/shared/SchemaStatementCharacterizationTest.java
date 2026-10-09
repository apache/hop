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

package org.apache.hop.neo4j.shared;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.ArrayList;
import java.util.List;
import org.apache.hop.core.HopClientEnvironment;
import org.apache.hop.core.graph.GraphDatabaseMeta;
import org.apache.hop.core.graph.IGraphDialect;
import org.apache.hop.core.logging.LogChannel;
import org.apache.hop.core.variables.Variables;
import org.apache.hop.neo4j.bolt.MemgraphGraphDialect;
import org.apache.hop.neo4j.bolt.Neo4jGraphDialect;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DynamicTest;
import org.junit.jupiter.api.TestFactory;

/**
 * Characterization of which errors of index and constraint statements are ignored, recorded before
 * the dialect SPI.
 */
class SchemaStatementCharacterizationTest {

  static final List<String> MESSAGES =
      List.of(
          "Index already indexed",
          "no such index",
          "Index already exists.",
          "Index doesn't exist.",
          "boom");

  @BeforeAll
  static void setUp() throws Exception {
    HopClientEnvironment.init();
  }

  @TestFactory
  List<DynamicTest> schemaStatements() {
    List<DynamicTest> tests = new ArrayList<>();
    // FalkorDB: see FalkorDbDialectCharacterizationTest in its plugin. Memgraph tolerates its
    // "already exists" and "doesn't exist" errors since the dialect SPI.
    String[][] expected = {
      // MEMGRAPH, NEO4J
      {"error", "error"},
      {"error", "error"},
      {"ok", "error"},
      {"ok", "error"},
      {"error", "error"},
    };
    IGraphDialect[] dialects = {MemgraphGraphDialect.INSTANCE, Neo4jGraphDialect.INSTANCE};
    for (int m = 0; m < MESSAGES.size(); m++) {
      for (int d = 0; d < dialects.length; d++) {
        String message = MESSAGES.get(m);
        IGraphDialect dialect = dialects[d];
        String result = expected[m][d];
        tests.add(
            DynamicTest.dynamicTest(
                "runSchemaStatement|" + dialect.getId() + "|" + message,
                () -> {
                  FakeGraphDatabase database = new FakeGraphDatabase(dialect, message);
                  NamedGraphConnection connection =
                      new NamedGraphConnection(
                          "fake", null, new GraphDatabaseMeta("fake", database));
                  String actual;
                  try {
                    NeoConnectionUtils.runSchemaStatement(
                        connection,
                        new LogChannel("test"),
                        Variables.getADefaultVariableSpace(),
                        "CREATE INDEX",
                        "Creating index");
                    actual = "ok";
                  } catch (Exception e) {
                    actual = "error";
                  }
                  assertEquals(result, actual);
                }));
        tests.add(
            DynamicTest.dynamicTest(
                "createNodeIndex|" + dialect.getId() + "|" + message,
                () -> {
                  FakeGraphDatabase database = new FakeGraphDatabase(dialect, message);
                  String actual;
                  try {
                    NeoConnectionUtils.createNodeIndex(
                        new LogChannel("test"),
                        database.newConnection(),
                        List.of("Person"),
                        List.of("id"));
                    actual = "ok";
                  } catch (Exception e) {
                    actual = "error";
                  }
                  assertEquals(result, actual);
                }));
      }
    }
    return tests;
  }
}
