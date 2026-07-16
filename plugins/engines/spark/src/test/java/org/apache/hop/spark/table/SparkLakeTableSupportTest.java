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

package org.apache.hop.spark.table;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.variables.Variables;
import org.apache.hop.spark.transforms.table.SparkLakeTableInputMeta;
import org.apache.hop.spark.transforms.table.SparkLakeTableOutputMeta;
import org.junit.jupiter.api.Test;

/** Unit tests that do not require Delta/Iceberg connectors on the classpath. */
class SparkLakeTableSupportTest {

  @Test
  void normalizeFormatDefaultsAndValidates() throws Exception {
    assertEquals(SparkLakeFormats.FORMAT_DELTA, SparkLakeTableSupport.normalizeFormat(null));
    assertEquals(SparkLakeFormats.FORMAT_DELTA, SparkLakeTableSupport.normalizeFormat(" DELTA "));
    assertEquals(SparkLakeFormats.FORMAT_ICEBERG, SparkLakeTableSupport.normalizeFormat("iceberg"));
    HopException ex =
        assertThrows(HopException.class, () -> SparkLakeTableSupport.normalizeFormat("hudi"));
    assertTrue(ex.getMessage().contains("Unsupported"));
  }

  @Test
  void normalizeIdentifierMode() throws Exception {
    assertEquals(
        SparkLakeTableInputMeta.MODE_PATH, SparkLakeTableSupport.normalizeIdentifierMode(null));
    assertEquals(
        SparkLakeTableInputMeta.MODE_TABLE, SparkLakeTableSupport.normalizeIdentifierMode("table"));
    assertThrows(HopException.class, () -> SparkLakeTableSupport.normalizeIdentifierMode("uri"));
  }

  @Test
  void collectFormatAddsNormalized() throws Exception {
    Set<String> set = new LinkedHashSet<>();
    SparkLakeTableSupport.collectFormat("Delta", set);
    assertTrue(set.contains(SparkLakeFormats.FORMAT_DELTA));
  }

  @Test
  void icebergPathSqlIdentifierQuotesUri() {
    String id = SparkLakeTableSupport.icebergPathSqlIdentifier("/tmp/orders");
    assertTrue(id.startsWith(SparkLakeFormats.ICEBERG_PATH_CATALOG_NAME + ".`"));
    assertTrue(id.endsWith("`"));
    assertTrue(id.contains("file:"));
    assertTrue(id.contains("orders"));
  }

  @Test
  void toTableLocationUriPreservesSchemes() {
    assertEquals("s3a://bucket/t", SparkLakeTableSupport.toTableLocationUri("s3a://bucket/t"));
    assertTrue(SparkLakeTableSupport.toTableLocationUri("/tmp/x").startsWith("file:"));
  }

  @Test
  void resolveTableIdentifierPrefixesCatalog() throws Exception {
    assertEquals(
        "lake.db.orders",
        SparkLakeTableSupport.resolveTableIdentifier("db.orders", "lake", new Variables()));
    assertEquals(
        "lake.db.orders",
        SparkLakeTableSupport.resolveTableIdentifier("lake.db.orders", "lake", new Variables()));
  }

  @Test
  void resolveTableIdentifierRequiresValue() {
    assertThrows(
        HopException.class,
        () -> SparkLakeTableSupport.resolveTableIdentifier("", "lake", new Variables()));
  }

  @Test
  void resolveWriteRejectsMissingPath() {
    SparkLakeTableOutputMeta meta = new SparkLakeTableOutputMeta();
    meta.setFormat(SparkLakeFormats.FORMAT_DELTA);
    meta.setIdentifierMode(SparkLakeTableInputMeta.MODE_PATH);
    meta.setTablePath("");
    HopException ex =
        assertThrows(
            HopException.class,
            () -> SparkLakeTableSupport.resolveWrite(null, null, new Variables(), "out", meta));
    assertTrue(ex.getMessage().contains("path"));
  }

  @Test
  void defaultSaveModeIsErrorIfExists() {
    SparkLakeTableOutputMeta meta = new SparkLakeTableOutputMeta();
    assertEquals(
        org.apache.hop.spark.transforms.io.SparkFileOutputMeta.MODE_ERROR, meta.getSaveMode());
  }

  @Test
  void timeTravelOptionMapDelta() throws Exception {
    Map<String, String> v =
        SparkLakeTableSupport.timeTravelOptionMap(
            SparkLakeFormats.FORMAT_DELTA, SparkLakeTableInputMeta.TIME_TRAVEL_VERSION, "12", null);
    assertEquals("12", v.get("versionAsOf"));
    assertEquals(1, v.size());

    Map<String, String> t =
        SparkLakeTableSupport.timeTravelOptionMap(
            SparkLakeFormats.FORMAT_DELTA,
            SparkLakeTableInputMeta.TIME_TRAVEL_TIMESTAMP,
            null,
            "2024-01-15 10:00:00");
    assertEquals("2024-01-15 10:00:00", t.get("timestampAsOf"));
  }

  @Test
  void timeTravelOptionMapIceberg() throws Exception {
    Map<String, String> v =
        SparkLakeTableSupport.timeTravelOptionMap(
            SparkLakeFormats.FORMAT_ICEBERG,
            SparkLakeTableInputMeta.TIME_TRAVEL_VERSION,
            "999",
            null);
    assertEquals("999", v.get("snapshot-id"));

    Map<String, String> t =
        SparkLakeTableSupport.timeTravelOptionMap(
            SparkLakeFormats.FORMAT_ICEBERG,
            SparkLakeTableInputMeta.TIME_TRAVEL_TIMESTAMP,
            null,
            "2024-06-01 12:00:00");
    assertEquals("2024-06-01 12:00:00", t.get("as-of-timestamp"));
  }

  @Test
  void timeTravelNoneYieldsEmptyMap() throws Exception {
    assertTrue(
        SparkLakeTableSupport.timeTravelOptionMap(
                SparkLakeFormats.FORMAT_DELTA, SparkLakeTableInputMeta.TIME_TRAVEL_NONE, "1", "t")
            .isEmpty());
  }

  @Test
  void timeTravelVersionRequiresValue() {
    assertThrows(
        HopException.class,
        () ->
            SparkLakeTableSupport.timeTravelOptionMap(
                SparkLakeFormats.FORMAT_DELTA,
                SparkLakeTableInputMeta.TIME_TRAVEL_VERSION,
                "",
                null));
  }

  @Test
  void icebergTimeTravelSql() throws Exception {
    String id = "hop_iceberg.`file:///tmp/t`";
    assertEquals(
        "SELECT * FROM " + id,
        SparkLakeTableSupport.buildIcebergTimeTravelSql(
            id, SparkLakeTableInputMeta.TIME_TRAVEL_NONE, null, null));
    assertEquals(
        "SELECT * FROM " + id + " VERSION AS OF 42",
        SparkLakeTableSupport.buildIcebergTimeTravelSql(
            id, SparkLakeTableInputMeta.TIME_TRAVEL_VERSION, "42", null));
    assertEquals(
        "SELECT * FROM " + id + " TIMESTAMP AS OF TIMESTAMP '2024-01-01 00:00:00'",
        SparkLakeTableSupport.buildIcebergTimeTravelSql(
            id, SparkLakeTableInputMeta.TIME_TRAVEL_TIMESTAMP, null, "2024-01-01 00:00:00"));
  }
}
