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

package org.apache.hop.beam.transforms.bq;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.google.cloud.bigquery.Field;
import com.google.cloud.bigquery.InsertAllRequest;
import com.google.cloud.bigquery.Schema;
import com.google.cloud.bigquery.StandardSQLTypeName;
import java.math.BigDecimal;
import java.util.Base64;
import java.util.Date;
import java.util.Map;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.row.RowMeta;
import org.apache.hop.core.row.value.ValueMetaBigNumber;
import org.apache.hop.core.row.value.ValueMetaBinary;
import org.apache.hop.core.row.value.ValueMetaBoolean;
import org.apache.hop.core.row.value.ValueMetaDate;
import org.apache.hop.core.row.value.ValueMetaInteger;
import org.apache.hop.core.row.value.ValueMetaInternetAddress;
import org.apache.hop.core.row.value.ValueMetaNumber;
import org.apache.hop.core.row.value.ValueMetaString;
import org.apache.hop.core.row.value.ValueMetaTimestamp;
import org.junit.jupiter.api.Test;

/**
 * Unit tests for the pure-function helpers on {@link BeamBQOutput}: type mapping, schema building,
 * and row → {@code InsertAllRequest.RowToInsert} conversion. The BigQuery-client-bound paths
 * (prepareTable, truncateTable, flushBatch, init/processRow/dispose) are out of scope here — they
 * require an actual {@code BigQuery} service and are exercised by the GCP integration tests in
 * {@code hop-infra-tests/tests/GCP/main-0008-bigquery-output.hwf}.
 */
class BeamBQOutputTest {

  // ─── mapType: Hop value-meta type → BigQuery StandardSQLTypeName ─────────────

  @Test
  void mapType_stringToString() {
    assertEquals(StandardSQLTypeName.STRING, BeamBQOutput.mapType(new ValueMetaString("s")));
  }

  @Test
  void mapType_integerToInt64() {
    assertEquals(StandardSQLTypeName.INT64, BeamBQOutput.mapType(new ValueMetaInteger("n")));
  }

  @Test
  void mapType_numberToFloat64() {
    assertEquals(StandardSQLTypeName.FLOAT64, BeamBQOutput.mapType(new ValueMetaNumber("d")));
  }

  @Test
  void mapType_booleanToBool() {
    assertEquals(StandardSQLTypeName.BOOL, BeamBQOutput.mapType(new ValueMetaBoolean("b")));
  }

  @Test
  void mapType_dateToTimestamp() {
    assertEquals(StandardSQLTypeName.TIMESTAMP, BeamBQOutput.mapType(new ValueMetaDate("d")));
  }

  @Test
  void mapType_timestampToTimestamp() {
    assertEquals(StandardSQLTypeName.TIMESTAMP, BeamBQOutput.mapType(new ValueMetaTimestamp("t")));
  }

  @Test
  void mapType_bigNumberToNumeric() {
    assertEquals(StandardSQLTypeName.NUMERIC, BeamBQOutput.mapType(new ValueMetaBigNumber("bn")));
  }

  @Test
  void mapType_binaryToBytes() {
    assertEquals(StandardSQLTypeName.BYTES, BeamBQOutput.mapType(new ValueMetaBinary("bin")));
  }

  @Test
  void mapType_unknownFallsBackToString() {
    // InternetAddress isn't in the explicit switch arms — must default to STRING.
    assertEquals(
        StandardSQLTypeName.STRING, BeamBQOutput.mapType(new ValueMetaInternetAddress("ip")));
  }

  // ─── buildSchema: IRowMeta → BigQuery Schema ─────────────────────────────────

  @Test
  void buildSchema_mapsEveryColumnNullable() {
    RowMeta rowMeta = new RowMeta();
    rowMeta.addValueMeta(new ValueMetaInteger("id"));
    rowMeta.addValueMeta(new ValueMetaString("name"));
    rowMeta.addValueMeta(new ValueMetaNumber("amount"));

    Schema schema = BeamBQOutput.buildSchema(rowMeta);

    assertEquals(3, schema.getFields().size(), "one BigQuery Field per input value meta");
    assertEquals("id", schema.getFields().get(0).getName());
    assertEquals(StandardSQLTypeName.INT64, schema.getFields().get(0).getType().getStandardType());
    assertEquals(Field.Mode.NULLABLE, schema.getFields().get(0).getMode());
    assertEquals("name", schema.getFields().get(1).getName());
    assertEquals(StandardSQLTypeName.STRING, schema.getFields().get(1).getType().getStandardType());
    assertEquals(
        StandardSQLTypeName.FLOAT64, schema.getFields().get(2).getType().getStandardType());
  }

  @Test
  void buildSchema_emptyRowMetaProducesEmptySchema() {
    Schema schema = BeamBQOutput.buildSchema(new RowMeta());
    assertEquals(0, schema.getFields().size());
  }

  // ─── toInsertRow: Hop Object[] → InsertAllRequest.RowToInsert ────────────────

  @Test
  void toInsertRow_skipsNullValues() throws HopException {
    RowMeta rowMeta = new RowMeta();
    rowMeta.addValueMeta(new ValueMetaString("a"));
    rowMeta.addValueMeta(new ValueMetaInteger("b"));

    InsertAllRequest.RowToInsert insert =
        BeamBQOutput.toInsertRow(rowMeta, new Object[] {"hello", null});

    Map<String, Object> content = insert.getContent();
    assertEquals("hello", content.get("a"));
    assertFalse(content.containsKey("b"), "null values are omitted, not written as null");
    assertEquals(1, content.size());
  }

  @Test
  void toInsertRow_convertsIntegerAsLong() throws HopException {
    RowMeta rowMeta = new RowMeta();
    rowMeta.addValueMeta(new ValueMetaInteger("n"));

    Map<String, Object> content =
        BeamBQOutput.toInsertRow(rowMeta, new Object[] {42L}).getContent();

    assertEquals(Long.valueOf(42L), content.get("n"));
  }

  @Test
  void toInsertRow_convertsNumberAsDouble() throws HopException {
    RowMeta rowMeta = new RowMeta();
    rowMeta.addValueMeta(new ValueMetaNumber("d"));

    Map<String, Object> content =
        BeamBQOutput.toInsertRow(rowMeta, new Object[] {3.14d}).getContent();

    assertEquals(Double.valueOf(3.14d), content.get("d"));
  }

  @Test
  void toInsertRow_convertsBooleanAsBoolean() throws HopException {
    RowMeta rowMeta = new RowMeta();
    rowMeta.addValueMeta(new ValueMetaBoolean("flag"));

    Map<String, Object> content =
        BeamBQOutput.toInsertRow(rowMeta, new Object[] {Boolean.TRUE}).getContent();

    assertEquals(Boolean.TRUE, content.get("flag"));
  }

  @Test
  void toInsertRow_convertsDateAsIso8601String() throws HopException {
    RowMeta rowMeta = new RowMeta();
    rowMeta.addValueMeta(new ValueMetaDate("when"));

    // 2024-01-15T10:30:00Z = 1705314600000 epoch millis
    Date d = new Date(1705314600000L);
    Map<String, Object> content = BeamBQOutput.toInsertRow(rowMeta, new Object[] {d}).getContent();

    assertEquals("2024-01-15T10:30:00Z", content.get("when"));
  }

  @Test
  void toInsertRow_convertsBigNumberAsPlainString() throws HopException {
    RowMeta rowMeta = new RowMeta();
    rowMeta.addValueMeta(new ValueMetaBigNumber("bn"));

    // toPlainString — avoids scientific notation that loses precision in BigQuery NUMERIC
    Map<String, Object> content =
        BeamBQOutput.toInsertRow(rowMeta, new Object[] {new BigDecimal("123456789012345.6789")})
            .getContent();

    assertEquals("123456789012345.6789", content.get("bn"));
  }

  @Test
  void toInsertRow_convertsBinaryAsBase64String() throws HopException {
    RowMeta rowMeta = new RowMeta();
    rowMeta.addValueMeta(new ValueMetaBinary("blob"));

    byte[] bytes = new byte[] {0x01, 0x02, 0x03, 0x04};
    String expected = Base64.getEncoder().encodeToString(bytes);
    Map<String, Object> content =
        BeamBQOutput.toInsertRow(rowMeta, new Object[] {bytes}).getContent();

    assertEquals(expected, content.get("blob"));
  }

  @Test
  void toInsertRow_unknownTypeFallsBackToString() throws HopException {
    RowMeta rowMeta = new RowMeta();
    rowMeta.addValueMeta(new ValueMetaInternetAddress("ip"));

    Map<String, Object> content =
        BeamBQOutput.toInsertRow(rowMeta, new Object[] {java.net.InetAddress.getLoopbackAddress()})
            .getContent();

    // The IValueMeta-based getString() formats this as the loopback host representation;
    // we only assert it lands as a non-empty String (the unknown-type fallback path).
    assertTrue(content.get("ip") instanceof String);
    assertFalse(((String) content.get("ip")).isEmpty());
  }

  @Test
  void toInsertRow_emptyRow() throws HopException {
    InsertAllRequest.RowToInsert insert = BeamBQOutput.toInsertRow(new RowMeta(), new Object[0]);
    assertTrue(insert.getContent().isEmpty());
    // RowToInsert.getId is null when not explicitly set — confirming behavior we rely on
    assertNull(insert.getId());
  }

  @Test
  void toInsertRow_preservesFieldNamesExactly() throws HopException {
    RowMeta rowMeta = new RowMeta();
    rowMeta.addValueMeta(new ValueMetaString("MixedCase_FieldName"));

    Map<String, Object> content =
        BeamBQOutput.toInsertRow(rowMeta, new Object[] {"v"}).getContent();

    assertTrue(content.containsKey("MixedCase_FieldName"));
  }
}
