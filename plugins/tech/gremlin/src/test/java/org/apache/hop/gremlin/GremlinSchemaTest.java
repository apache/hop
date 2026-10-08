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
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Date;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.apache.hop.core.graph.GraphSchema;
import org.apache.hop.core.graph.GraphSchemaEntry;
import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.T;
import org.junit.jupiter.api.Test;

/** The schema from element maps, as g.V().elementMap() and g.E().elementMap() return them. */
class GremlinSchemaTest {

  private static Map<Object, Object> element(Object... keyValues) {
    Map<Object, Object> map = new LinkedHashMap<>();
    for (int i = 0; i < keyValues.length; i += 2) {
      map.put(keyValues[i], keyValues[i + 1]);
    }
    return map;
  }

  @Test
  void testFromElementMaps() {
    List<Map<Object, Object>> vertices =
        List.of(
            element(T.id, 1L, T.label, "Person", "name", "a", "born", new Date()),
            element(T.id, 2L, T.label, "Person", "name", "b"),
            element(T.id, 3L, T.label, "Company"));
    List<Map<Object, Object>> edges =
        List.of(
            element(
                T.id,
                4L,
                T.label,
                "WORKS_AT",
                Direction.OUT,
                element(T.id, 1L, T.label, "Person"),
                Direction.IN,
                element(T.id, 3L, T.label, "Company"),
                "since",
                2020L));

    GraphSchema schema = GremlinGraphConnection.fromElementMaps(vertices, edges);

    assertTrue(schema.sampled());
    List<GraphSchemaEntry> entries = schema.entries();
    assertEquals(4, entries.size(), entries.toString());
    assertEquals("Company", entries.get(0).name());
    assertEquals(null, entries.get(0).property());
    GraphSchemaEntry name = entries.get(1);
    assertEquals("Person", name.name());
    assertEquals("name", name.property());
    assertEquals(true, name.mandatory());
    GraphSchemaEntry born = entries.get(2);
    assertEquals(List.of("DateTime"), born.propertyTypes());
    assertFalse(born.mandatory());
    GraphSchemaEntry since = entries.get(3);
    assertTrue(since.isRelationship());
    assertEquals(List.of("Person"), since.startLabels());
    assertEquals(List.of("Company"), since.endLabels());
    assertEquals(List.of("Integer"), since.propertyTypes());
    // No indexes, not unknown
    assertEquals(false, schema.isIndexed(name));

    assertTrue(GremlinGraphDialect.INSTANCE.isSupportingSchemaIntrospection());
    assertFalse(GremlinGraphDialect.INSTANCE.isSupportingVectorSearch());
    assertFalse(GremlinGraphDialect.INSTANCE.isSupportingRelationshipVectorSearch());
  }
}
