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

package org.apache.hop.avro.transforms.avrodecode;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.hop.avro.transforms.avrodecode.AvroDecodeFieldFinder.FieldRow;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.row.value.ValueMetaAvroRecord;
import org.apache.hop.core.util.TestUtil;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

class AvroDecodeFieldFinderTest {

  @BeforeAll
  static void setUp() throws HopException {
    TestUtil.registerTestPluginTypes();
  }

  @Test
  void hopValueAndBareSchemaUseTheRecordSchema() throws Exception {
    Schema schema =
        SchemaBuilder.record("person")
            .fields()
            .name("name")
            .type()
            .nullable()
            .stringType()
            .noDefault()
            .name("age")
            .type()
            .longType()
            .noDefault()
            .endRecord();
    GenericRecord record = new GenericData.Record(schema);
    record.put("name", "alice");
    record.put("age", 10L);

    List<FieldRow> fromHopValue =
        AvroDecodeFieldFinder.rowsForSchema(
            AvroDecodeFieldFinder.schemaFromJson(
                ValueMetaAvroRecord.convertGenericRecordToString(record)));
    List<FieldRow> fromSchema =
        AvroDecodeFieldFinder.rowsForSchema(
            AvroDecodeFieldFinder.schemaFromJson(schema.toString()));

    assertEquals(fromSchema, fromHopValue);
    assertEquals(List.of("age", "name"), names(fromSchema));
    assertField(fromSchema, "name", "Union", "String");
    assertField(fromSchema, "age", "Long", "Integer");
  }

  @Test
  void sampleRecordInfersTopLevelTypes() throws Exception {
    String json =
        """
        {
          "label": "alice",
          "count": 10,
          "ratio": 1.5,
          "ok": true,
          "tags": ["a"],
          "child": {"left": 1, "right": 2},
          "missing": null,
          "asString": {"string": "alice"},
          "asLong": {"long": 10},
          "asInt": {"int": 3},
          "asFloat": {"float": 1.25},
          "asDouble": {"double": 2.5},
          "asBytes": {"bytes": "YQ=="},
          "asBoolean": {"boolean": true},
          "status": {"com.example.Status": "OPEN"}
        }
        """;

    List<FieldRow> rows =
        AvroDecodeFieldFinder.rowsForSchema(AvroDecodeFieldFinder.schemaFromJson(json));

    assertEquals(
        List.of(
            "asBoolean",
            "asBytes",
            "asDouble",
            "asFloat",
            "asInt",
            "asLong",
            "asString",
            "child",
            "count",
            "label",
            "missing",
            "ok",
            "ratio",
            "status",
            "tags"),
        names(rows));
    assertField(rows, "label", "String", "String");
    assertField(rows, "count", "Long", "Integer");
    assertField(rows, "ratio", "Double", "Number");
    assertField(rows, "ok", "Boolean", "Boolean");
    assertField(rows, "tags", "Array", "String");
    assertField(rows, "child", "Record", "String");
    assertField(rows, "missing", "String", "String");
    assertField(rows, "asString", "String", "String");
    assertField(rows, "asLong", "Long", "Integer");
    assertField(rows, "asInt", "Int", "Integer");
    assertField(rows, "asFloat", "Float", "Number");
    assertField(rows, "asDouble", "Double", "Number");
    assertField(rows, "asBytes", "Bytes", "Binary");
    assertField(rows, "asBoolean", "Boolean", "Boolean");
    assertField(rows, "status", "Record", "String");
    assertTrue(names(rows).stream().noneMatch(name -> name.equals("left") || name.equals("right")));
  }

  @Test
  void fieldNamesAreSortedCaseInsensitively() throws Exception {
    List<FieldRow> rows =
        AvroDecodeFieldFinder.rowsForSchema(
            AvroDecodeFieldFinder.schemaFromJson("{\"b\": 1, \"A\": 2}"));

    assertEquals(List.of("A", "b"), names(rows));
  }

  @Test
  void blankJsonHasNoSchema() throws Exception {
    assertNull(AvroDecodeFieldFinder.schemaFromJson(null));
    assertNull(AvroDecodeFieldFinder.schemaFromJson(""));
    assertNull(AvroDecodeFieldFinder.schemaFromJson("   "));
  }

  @Test
  void malformedJsonIsRejected() {
    assertThrows(HopException.class, () -> AvroDecodeFieldFinder.schemaFromJson("{"));
    assertThrows(HopException.class, () -> AvroDecodeFieldFinder.schemaFromJson("[1, 2]"));
    assertThrows(HopException.class, () -> AvroDecodeFieldFinder.schemaFromJson("\"hello\""));
  }

  @Test
  void illegalSampleFieldNameIsRejected() {
    assertThrows(HopException.class, () -> AvroDecodeFieldFinder.schemaFromJson("{\"a-b\": 1}"));
  }

  private static List<String> names(List<FieldRow> rows) {
    return rows.stream().map(FieldRow::sourceField).toList();
  }

  private static void assertField(
      List<FieldRow> rows, String name, String avroType, String hopType) {
    FieldRow row =
        rows.stream()
            .filter(candidate -> candidate.sourceField().equals(name))
            .findFirst()
            .orElseThrow();
    assertEquals(name, row.targetFieldName());
    assertEquals(avroType, row.sourceAvroType());
    assertEquals(hopType, row.targetType());
  }
}
