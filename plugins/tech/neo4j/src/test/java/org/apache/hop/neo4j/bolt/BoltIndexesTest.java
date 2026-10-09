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
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.hop.core.graph.GraphIndex;
import org.junit.jupiter.api.Test;

class BoltIndexesTest {

  private static Map<String, Object> row(Object... keyValues) {
    Map<String, Object> row = new HashMap<>();
    for (int i = 0; i < keyValues.length; i += 2) {
      row.put((String) keyValues[i], keyValues[i + 1]);
    }
    return row;
  }

  @Test
  void testNeo4j5() {
    List<GraphIndex> indexes =
        BoltIndexes.fromNeo4j(
            List.of(
                row(
                    "name",
                    "v_i",
                    "type",
                    "RANGE",
                    "entityType",
                    "NODE",
                    "labelsOrTypes",
                    List.of("VTest"),
                    "properties",
                    List.of("a", "b"),
                    "owningConstraint",
                    null),
                row(
                    "name",
                    "v_r",
                    "type",
                    "RANGE",
                    "entityType",
                    "RELATIONSHIP",
                    "labelsOrTypes",
                    List.of("VREL"),
                    "properties",
                    List.of("w")),
                row(
                    "name",
                    "v_u",
                    "type",
                    "RANGE",
                    "entityType",
                    "NODE",
                    "labelsOrTypes",
                    List.of("VTest"),
                    "properties",
                    List.of("k"),
                    "owningConstraint",
                    "v_u"),
                row(
                    "name",
                    "lookup",
                    "type",
                    "LOOKUP",
                    "entityType",
                    "NODE",
                    "labelsOrTypes",
                    null,
                    "properties",
                    null)));
    assertEquals(3, indexes.size());
    assertEquals(
        new GraphIndex("v_i", false, List.of("VTest"), List.of("a", "b"), false), indexes.get(0));
    assertTrue(indexes.get(1).relationship());
    assertTrue(indexes.get(2).unique());
    assertTrue(indexes.get(2).covers("VTest", "k"));
  }

  @Test
  void testNeo4j4() {
    List<GraphIndex> indexes =
        BoltIndexes.fromNeo4j(
            List.of(
                row(
                    "name",
                    "c",
                    "uniqueness",
                    "UNIQUE",
                    "type",
                    "BTREE",
                    "entityType",
                    "NODE",
                    "labelsOrTypes",
                    List.of("Customer"),
                    "properties",
                    List.of("id"))));
    assertTrue(indexes.get(0).unique());
  }

  @Test
  void testMemgraph() {
    List<GraphIndex> indexes =
        BoltIndexes.fromMemgraph(
            List.of(
                row("index type", "edge-type+property", "label", "VREL", "property", "w"),
                row(
                    "index type",
                    "label+property",
                    "label",
                    "VTest",
                    "property",
                    List.of("a", "b")),
                row("index type", "label", "label", "VTest", "property", null)),
            List.of(
                row("constraint type", "unique", "label", "VTest", "properties", List.of("k")),
                row("constraint type", "exists", "label", "VTest", "properties", List.of("x"))));
    assertEquals(3, indexes.size());
    assertTrue(indexes.get(0).relationship());
    assertEquals(Arrays.asList("a", "b"), indexes.get(1).properties());
    assertFalse(indexes.get(1).unique());
    assertTrue(indexes.get(2).unique());
    assertTrue(indexes.get(2).covers("VTest", "k"));
  }
}
