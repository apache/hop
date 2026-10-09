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

package org.apache.hop.gremlin;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.time.LocalDate;
import java.util.Date;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.apache.tinkerpop.gremlin.structure.T;
import org.junit.jupiter.api.Test;

class GremlinValuesTest {

  @Test
  void testRows() {
    Map<Object, Object> elementMap = new LinkedHashMap<>();
    elementMap.put(T.id, 3);
    elementMap.put(T.label, "Person");
    elementMap.put("name", "Ann");
    assertEquals(
        Map.of("id", 3L, "label", "Person", "name", "Ann"), GremlinValues.toRow(elementMap));
    // A property called id wins over the element id
    Map<Object, Object> withId = new LinkedHashMap<>();
    withId.put(T.id, 3);
    withId.put("id", 1L);
    assertEquals(Map.of("id", 1L), GremlinValues.toRow(withId));
    assertEquals(Map.of("result", 42L), GremlinValues.toRow(42));
    assertEquals(Map.of("result", List.of("a", 1L)), GremlinValues.toRow(List.of("a", 1)));
  }

  @Test
  void testPropertyValues() {
    assertEquals(new Date(0), GremlinValues.toPropertyValue(LocalDate.of(1970, 1, 1)));
    assertEquals("x", GremlinValues.toPropertyValue("x"));
  }
}
