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

package org.apache.hop.beam.core.fn;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;

import com.google.api.services.bigquery.model.TableFieldSchema;
import com.google.api.services.bigquery.model.TableSchema;
import java.util.Arrays;
import java.util.List;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.beam.sdk.io.gcp.bigquery.SchemaAndRecord;
import org.apache.hop.beam.core.HopRow;
import org.apache.hop.core.HopEnvironment;
import org.apache.hop.core.exception.HopPluginException;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.core.row.JsonRowMeta;
import org.apache.hop.core.row.RowMeta;
import org.apache.hop.core.row.value.ValueMetaFactory;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

/**
 * Issue #5064: the BigQuery input transform must not blow up on nested {@code RECORD} fields.
 *
 * <p>Before the fix, {@code AvroType.valueOf("RECORD")} threw and failed the entire read. Nested
 * and repeated fields now come back as JSON strings.
 *
 * <p>The Avro schemas are built with {@link SchemaBuilder} rather than hand-written JSON: Avro's
 * own parser rejects some hand-written nested-schema forms, and a test that fails for a reason
 * unrelated to the behaviour under test is worse than no test.
 */
class BQSchemaAndRecordToHopFnTest {

  private static final Schema ADDRESS =
      SchemaBuilder.record("Address")
          .fields()
          .requiredString("city")
          .requiredString("zip")
          .endRecord();

  private static final Schema ROW =
      SchemaBuilder.record("Row")
          .fields()
          .requiredLong("id")
          .name("address")
          .type(ADDRESS)
          .noDefault()
          .name("tags")
          .type(SchemaBuilder.array().items().stringType())
          .noDefault()
          .endRecord();

  @BeforeAll
  static void initHopEnvironment() throws Exception {
    HopEnvironment.init();
  }

  /** The row layout the transform asks for: all three fields as Hop Strings. */
  private static String rowMetaJson() throws HopPluginException {
    IRowMeta rowMeta = new RowMeta();
    for (String name : List.of("id", "address", "tags")) {
      rowMeta.addValueMeta(ValueMetaFactory.createValueMeta(name, IValueMeta.TYPE_STRING, -1, -1));
    }
    return JsonRowMeta.toJson(rowMeta);
  }

  /** id INTEGER, address RECORD{city STRING, zip STRING}, tags STRING repeated. */
  private static TableSchema nestedTableSchema() {
    TableFieldSchema id = new TableFieldSchema().setName("id").setType("INTEGER");

    TableFieldSchema address =
        new TableFieldSchema()
            .setName("address")
            .setType("RECORD")
            .setMode("NULLABLE")
            .setFields(
                Arrays.asList(
                    new TableFieldSchema().setName("city").setType("STRING"),
                    new TableFieldSchema().setName("zip").setType("STRING")));

    TableFieldSchema tags =
        new TableFieldSchema().setName("tags").setType("STRING").setMode("REPEATED");

    return new TableSchema().setFields(Arrays.asList(id, address, tags));
  }

  private static GenericRecord nestedRow() {
    GenericRecord address = new GenericData.Record(ADDRESS);
    address.put("city", "Ghent");
    address.put("zip", "9000");

    GenericRecord row = new GenericData.Record(ROW);
    row.put("id", 1L);
    row.put("address", address);
    row.put("tags", List.of("a", "b"));
    return row;
  }

  @Test
  void avroTypeEnumKnowsRecordAndStruct() {
    // BigQuery reports RECORD; some schemas use the STRUCT spelling.
    assertEquals(IValueMeta.TYPE_STRING, BQSchemaAndRecordToHopFn.AvroType.RECORD.getHopType());
    assertEquals(IValueMeta.TYPE_STRING, BQSchemaAndRecordToHopFn.AvroType.STRUCT.getHopType());
  }

  @Test
  void nestedRecordBecomesAJsonString() throws Exception {
    HopRow hopRow =
        assertDoesNotThrow(
            () ->
                new BQSchemaAndRecordToHopFn("nested", rowMetaJson())
                    .apply(new SchemaAndRecord(nestedRow(), nestedTableSchema())));

    // The nested record is rendered as JSON of its field values, not as Avro's toString()
    // container wrapper.
    //
    assertEquals("{\"city\": \"Ghent\", \"zip\": \"9000\"}", hopRow.getRow()[1]);
  }

  @Test
  void aRepeatedFieldBecomesAJsonArray() throws Exception {
    HopRow hopRow =
        new BQSchemaAndRecordToHopFn("nested", rowMetaJson())
            .apply(new SchemaAndRecord(nestedRow(), nestedTableSchema()));

    assertEquals("[\"a\", \"b\"]", hopRow.getRow()[2]);
  }

  @Test
  void aNestedValueIsNotSerialisedAsItsInternalBeanStructure() throws Exception {
    HopRow hopRow =
        new BQSchemaAndRecordToHopFn("nested", rowMetaJson())
            .apply(new SchemaAndRecord(nestedRow(), nestedTableSchema()));

    // Guards against a Jackson-based implementation, which would emit the GenericRecord's
    // internal fields ({"schema": ..., "elementType": ...}) rather than its contents.
    //
    String address = (String) hopRow.getRow()[1];
    assertFalse(address.contains("elementType"), "leaked Avro internals: " + address);
    assertFalse(address.contains("\"schema\""), "leaked Avro internals: " + address);
  }

  @Test
  void aPlainStringFieldIsUnaffected() throws Exception {
    Schema schema =
        SchemaBuilder.record("PlainRow")
            .fields()
            .requiredString("id")
            .name("address")
            .type(Schema.create(Schema.Type.STRING))
            .noDefault()
            .name("tags")
            .type(Schema.create(Schema.Type.STRING))
            .noDefault()
            .endRecord();

    GenericRecord row = new GenericData.Record(schema);
    row.put("id", "hello");
    row.put("address", "x");
    row.put("tags", "y");

    TableSchema tableSchema =
        new TableSchema()
            .setFields(
                List.of(
                    new TableFieldSchema().setName("id").setType("STRING"),
                    new TableFieldSchema().setName("address").setType("STRING"),
                    new TableFieldSchema().setName("tags").setType("STRING")));

    HopRow hopRow =
        new BQSchemaAndRecordToHopFn("plain", rowMetaJson())
            .apply(new SchemaAndRecord(row, tableSchema));

    assertEquals("hello", hopRow.getRow()[0]);
    assertEquals("x", hopRow.getRow()[1]);
    assertEquals("y", hopRow.getRow()[2]);
  }

  @Test
  void aNullNestedValueStaysNull() throws Exception {
    GenericRecord row = new GenericData.Record(ROW);
    row.put("id", 1L);
    row.put("address", null);
    row.put("tags", null);

    HopRow hopRow =
        new BQSchemaAndRecordToHopFn("nulls", rowMetaJson())
            .apply(new SchemaAndRecord(row, nestedTableSchema()));

    assertNull(hopRow.getRow()[1], "a null RECORD stays null");
    assertNull(hopRow.getRow()[2], "a null REPEATED stays null");
  }

  @Test
  void aFieldMissingFromTheTableSchemaDoesNotFailTheRead() throws Exception {
    // Common for a fromQuery result: the table schema does not describe every selected field.
    // That used to throw "Unable to find field" and kill the whole read.
    //
    TableSchema partial =
        new TableSchema()
            .setFields(List.of(new TableFieldSchema().setName("id").setType("INTEGER")));

    GenericRecord row = new GenericData.Record(ROW);
    row.put("id", 1L);
    row.put("address", new GenericData.Record(ADDRESS));
    row.put("tags", List.of("a"));

    HopRow hopRow =
        assertDoesNotThrow(
            () ->
                new BQSchemaAndRecordToHopFn("partial", rowMetaJson())
                    .apply(new SchemaAndRecord(row, partial)));

    assertEquals("1", hopRow.getRow()[0]);
  }
}
