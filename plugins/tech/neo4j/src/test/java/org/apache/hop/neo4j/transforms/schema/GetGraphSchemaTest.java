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

package org.apache.hop.neo4j.transforms.schema;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.List;
import java.util.Map;
import org.apache.hop.core.HopClientEnvironment;
import org.apache.hop.core.graph.GraphIndex;
import org.apache.hop.core.graph.GraphSchema;
import org.apache.hop.core.graph.GraphSchemaBuilder;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.core.row.RowMeta;
import org.apache.hop.core.row.value.ValueMetaString;
import org.apache.hop.core.variables.Variables;
import org.apache.hop.pipeline.transform.TransformSerializationTestUtil;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

class GetGraphSchemaTest {

  @BeforeAll
  static void init() throws Exception {
    HopClientEnvironment.init();
  }

  @Test
  void testSerialization() throws Exception {
    GetGraphSchemaMeta meta =
        TransformSerializationTestUtil.testSerialization(
            "/get-graph-schema-transform.xml", GetGraphSchemaMeta.class);
    assertEquals("graph", meta.getConnection());
    assertEquals("250", meta.getSampleSize());
    assertEquals("kind", meta.getElementTypeField());
    assertEquals("label", meta.getNameField());
    assertEquals("types", meta.getPropertyTypesField());
    assertEquals("", nvl(meta.getIndexedField()));
    assertEquals("from_labels", meta.getStartLabelsField());
  }

  private static String nvl(String s) {
    return s == null ? "" : s;
  }

  @Test
  void testDefaults() {
    GetGraphSchemaMeta meta = new GetGraphSchemaMeta();
    assertEquals("1000", meta.getSampleSize());
    assertEquals("element_type", meta.getElementTypeField());
  }

  /** The output fields replace any input, fields without a name are left out. */
  @Test
  void testFieldsAndRows() throws Exception {
    GetGraphSchemaMeta meta = new GetGraphSchemaMeta();
    meta.setIndexedField("");
    IRowMeta row = new RowMeta();
    row.addValueMeta(new ValueMetaString("input"));
    meta.getFields(row, "schema", null, null, new Variables(), null);
    assertArrayEquals(
        new String[] {
          "element_type",
          "name",
          "property",
          "property_types",
          "mandatory",
          "unique",
          "start_labels",
          "end_labels"
        },
        row.getFieldNames());
    assertEquals(IValueMeta.TYPE_BOOLEAN, row.getValueMeta(4).getType());

    GraphSchema schema =
        new GraphSchemaBuilder()
            .addSampledNode("Person", Map.of("id", 1L))
            .addSampledRelationship("KNOWS", List.of("Person"), List.of("Person"), Map.of())
            .build(
                List.of(new GraphIndex("", false, List.of("Person"), List.of("id"), true)), true);
    List<Object[]> rows = GetGraphSchema.getRows(meta, schema);
    assertEquals(2, rows.size());
    assertArrayEquals(
        new Object[] {"NODE", "Person", "id", "Integer", true, true, null, null}, rows.get(0));
    assertArrayEquals(
        new Object[] {"RELATIONSHIP", "KNOWS", null, null, null, false, "Person", "Person"},
        rows.get(1));
  }
}
