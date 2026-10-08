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
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.math.BigDecimal;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.apache.hop.core.graph.GraphNodeValue;
import org.apache.hop.core.graph.GraphRelationshipValue;
import org.junit.jupiter.api.Test;

class FalkorDbCypherTest {

  @Test
  void testLiterals() {
    assertEquals("null", FalkorDbCypher.toLiteral(null));
    assertEquals("'Bob\\'s \\\\ \"x\"'", FalkorDbCypher.toLiteral("Bob's \\ \"x\""));
    assertEquals("42", FalkorDbCypher.toLiteral(42L));
    assertEquals("1.5", FalkorDbCypher.toLiteral(1.5d));
    assertEquals("null", FalkorDbCypher.toLiteral(Double.NaN));
    assertEquals("123.4500", FalkorDbCypher.toLiteral(new BigDecimal("123.4500")));
    assertEquals("true", FalkorDbCypher.toLiteral(true));
    assertEquals("'2024-01-02'", FalkorDbCypher.toLiteral(LocalDate.of(2024, 1, 2)));
    assertEquals("[1, 'a', null]", FalkorDbCypher.toLiteral(Arrays.asList(1L, "a", null)));
    assertEquals("[1, 2]", FalkorDbCypher.toLiteral(new long[] {1, 2}));
    Map<String, Object> map = new LinkedHashMap<>();
    map.put("id", 1L);
    map.put("my `name`", "x");
    assertEquals("{`id`: 1, `my ``name```: 'x'}", FalkorDbCypher.toLiteral(map));
  }

  @Test
  void testWithParameters() {
    assertEquals("RETURN 1", FalkorDbCypher.withParameters("RETURN 1", Map.of()));
    assertEquals(
        "CYPHER props=[{`id`: 1}] UNWIND $props AS pr RETURN pr",
        FalkorDbCypher.withParameters(
            "UNWIND $props AS pr RETURN pr", Map.of("props", List.of(Map.of("id", 1L)))));
  }

  private static final FalkorDbCypher.NameResolver NAMES =
      new FalkorDbCypher.NameResolver() {
        @Override
        public String label(int id) {
          return List.of("Person", "Company").get(id);
        }

        @Override
        public String relationshipType(int id) {
          return List.of("KNOWS").get(id);
        }

        @Override
        public String propertyKey(int id) {
          return List.of("id", "name", "since").get(id);
        }
      };

  /** A compact reply: header, rows of typed values and statistics. */
  @Test
  void testRows() {
    List<Object> node =
        List.of(8L, List.of(3L, List.of(0L), List.of(List.of(1L, 2L, "Ann"), List.of(0L, 3L, 1L))));
    List<Object> edge = List.of(7L, List.of(5L, 0L, 3L, 4L, List.of(List.of(2L, 3L, 2020L))));
    List<Object> list = List.of(6L, List.of(List.of(2L, "Person"), List.of(3L, 2L)));
    List<Object> map = List.of(10L, List.of("k", List.of(5L, "1.5")));
    List<Object> date = List.of(14L, 1704153600L);
    List<Object> reply =
        List.<Object>of(
            List.of(
                List.of(1L, "n"),
                List.of(1L, "r"),
                List.of(1L, "l"),
                List.of(1L, "m"),
                List.of(1L, "d")),
            List.of(List.of(node, edge, list, map, date)),
            List.of("Cached execution: 0"));
    List<Map<String, Object>> rows = FalkorDbCypher.toRows(new ArrayList<>(reply), NAMES);
    assertEquals(1, rows.size());
    GraphNodeValue n = (GraphNodeValue) rows.get(0).get("n");
    assertEquals("3", n.id());
    assertEquals(List.of("Person"), n.labels());
    assertEquals(Map.of("name", "Ann", "id", 1L), n.properties());
    GraphRelationshipValue r = (GraphRelationshipValue) rows.get(0).get("r");
    assertEquals("KNOWS", r.type());
    assertEquals(Map.of("since", 2020L), r.properties());
    assertEquals(List.of("Person", 2L), rows.get(0).get("l"));
    assertEquals(Map.of("k", 1.5d), rows.get(0).get("m"));
    assertEquals(LocalDate.of(2024, 1, 2), rows.get(0).get("d"));

    // A statement without results only has statistics
    assertTrue(
        FalkorDbCypher.toRows(new ArrayList<>(List.of(List.of("Nodes created: 1"))), NAMES)
            .isEmpty());
  }
}
