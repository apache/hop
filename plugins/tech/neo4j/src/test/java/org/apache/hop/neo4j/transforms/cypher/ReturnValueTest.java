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

package org.apache.hop.neo4j.transforms.cypher;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.List;
import java.util.Map;
import org.apache.hop.core.HopClientEnvironment;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.graph.GraphNodeValue;
import org.apache.hop.core.graph.GraphPathValue;
import org.apache.hop.core.graph.GraphRelationshipValue;
import org.apache.hop.neo4j.core.data.GraphPropertyDataType;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.neo4j.driver.Values;

/** The return values derived from the values of a result row. */
class ReturnValueTest {

  @BeforeAll
  static void beforeAll() throws HopException {
    HopClientEnvironment.init();
  }

  @Test
  void plainValues() throws Exception {
    assertReturnValue("Integer", "Integer", ReturnValue.fromValue("n", 1L));
    assertReturnValue("Integer", "Integer", ReturnValue.fromValue("n", 1));
    assertReturnValue("Number", "Float", ReturnValue.fromValue("n", 1.5d));
    assertReturnValue("String", "String", ReturnValue.fromValue("n", "a"));
    assertReturnValue("Boolean", "Boolean", ReturnValue.fromValue("n", true));
    assertReturnValue("Date", "Date", ReturnValue.fromValue("n", LocalDate.of(2026, 1, 2)));
    assertReturnValue(
        "Date", "LocalDateTime", ReturnValue.fromValue("n", LocalDateTime.of(2026, 1, 2, 3, 4)));
    assertReturnValue("String", "List", ReturnValue.fromValue("n", List.of(1L, 2L)));
    assertReturnValue("String", "Map", ReturnValue.fromValue("n", Map.of("a", 1L)));
    assertReturnValue("String", "String", ReturnValue.fromValue("n", null));
    assertEquals("n", ReturnValue.fromValue("n", 1L).getName());
  }

  @Test
  void graphValues() {
    GraphNodeValue node = new GraphNodeValue("1", List.of("Person"), Map.of());
    GraphRelationshipValue relationship =
        new GraphRelationshipValue("2", "KNOWS", "1", "1", Map.of());
    assertEquals(GraphPropertyDataType.Node, ReturnValue.getSourceType(node));
    assertEquals(GraphPropertyDataType.Relationship, ReturnValue.getSourceType(relationship));
    assertEquals(
        GraphPropertyDataType.Path,
        ReturnValue.getSourceType(new GraphPathValue(List.of(node), List.of(relationship))));
  }

  /** The plain values give the same types as the Bolt values of Neo4j. */
  @Test
  void boltValuesGiveTheSameTypes() throws Exception {
    Object[] values = {
      1L, 1.5d, "a", true, LocalDate.of(2026, 1, 2), LocalDateTime.of(2026, 1, 2, 3, 4), List.of(1L)
    };
    for (Object value : values) {
      ReturnValue bolt = ReturnValue.fromBoltValue("n", Values.value(value));
      ReturnValue plain = ReturnValue.fromValue("n", value);
      assertEquals(bolt.getType(), plain.getType(), "Hop type of " + value);
      assertEquals(bolt.getSourceType(), plain.getSourceType(), "Source type of " + value);
    }
  }

  private static void assertReturnValue(String type, String sourceType, ReturnValue returnValue) {
    assertEquals(type, returnValue.getType());
    assertEquals(sourceType, returnValue.getSourceType());
  }
}
