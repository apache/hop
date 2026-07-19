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
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.apache.hop.core.exception.HopException;
import org.junit.jupiter.api.Test;

class SparkMaintenanceSqlBuilderTest {

  @Test
  void deltaOptimize() throws Exception {
    String sql =
        SparkMaintenanceSqlBuilder.build(
            SparkLakeFormats.FORMAT_DELTA,
            "delta.`/tmp/t`",
            null,
            null,
            SparkMaintenanceSqlBuilder.OP_OPTIMIZE,
            null,
            null,
            "col1,col2",
            null);
    assertEquals("OPTIMIZE delta.`/tmp/t` ZORDER BY (col1,col2)", sql);
  }

  @Test
  void deltaVacuumRequiresRetention() {
    assertThrows(
        HopException.class,
        () ->
            SparkMaintenanceSqlBuilder.build(
                SparkLakeFormats.FORMAT_DELTA,
                "delta.`/tmp/t`",
                null,
                null,
                SparkMaintenanceSqlBuilder.OP_VACUUM,
                null,
                null,
                null,
                null));
  }

  @Test
  void deltaVacuum() throws Exception {
    String sql =
        SparkMaintenanceSqlBuilder.build(
            SparkLakeFormats.FORMAT_DELTA,
            "delta.`/tmp/t`",
            null,
            null,
            SparkMaintenanceSqlBuilder.OP_VACUUM,
            "168",
            null,
            null,
            null);
    assertEquals("VACUUM delta.`/tmp/t` RETAIN 168 HOURS", sql);
  }

  @Test
  void icebergRewriteDataFiles() throws Exception {
    String sql =
        SparkMaintenanceSqlBuilder.build(
            SparkLakeFormats.FORMAT_ICEBERG,
            "hop_iceberg.`file:///t`",
            "hop_iceberg",
            "file:///t",
            SparkMaintenanceSqlBuilder.OP_OPTIMIZE,
            null,
            null,
            null,
            null);
    assertTrue(sql.contains("rewrite_data_files"));
    assertTrue(sql.contains("hop_iceberg.system"));
    assertTrue(sql.contains("file:///t"));
  }

  @Test
  void icebergExpireSnapshots() throws Exception {
    String sql =
        SparkMaintenanceSqlBuilder.build(
            SparkLakeFormats.FORMAT_ICEBERG,
            "lake.db.t",
            "lake",
            "db.t",
            SparkMaintenanceSqlBuilder.OP_EXPIRE_SNAPSHOTS,
            "24",
            null,
            null,
            "2");
    assertTrue(sql.contains("expire_snapshots"));
    assertTrue(sql.contains("retain_last => 2"));
  }

  @Test
  void deleteWhereRequiresClause() {
    assertThrows(
        HopException.class,
        () ->
            SparkMaintenanceSqlBuilder.build(
                SparkLakeFormats.FORMAT_DELTA,
                "delta.`/t`",
                null,
                null,
                SparkMaintenanceSqlBuilder.OP_DELETE_WHERE,
                null,
                "",
                null,
                null));
  }

  @Test
  void deleteWhere() throws Exception {
    String sql =
        SparkMaintenanceSqlBuilder.build(
            SparkLakeFormats.FORMAT_DELTA,
            "delta.`/t`",
            null,
            null,
            SparkMaintenanceSqlBuilder.OP_DELETE_WHERE,
            null,
            "id < 10",
            null,
            null);
    assertEquals("DELETE FROM delta.`/t` WHERE id < 10", sql);
  }

  @Test
  void destructiveAckFlags() throws Exception {
    assertTrue(SparkMaintenanceSqlBuilder.requiresDestructiveAck("VACUUM"));
    assertTrue(SparkMaintenanceSqlBuilder.requiresDestructiveAck("EXPIRE_SNAPSHOTS"));
    assertTrue(SparkMaintenanceSqlBuilder.requiresDestructiveAck("DELETE_WHERE"));
    assertFalse(SparkMaintenanceSqlBuilder.requiresDestructiveAck("OPTIMIZE"));
  }

  @Test
  void vacuumNotForIceberg() {
    assertThrows(
        HopException.class,
        () ->
            SparkMaintenanceSqlBuilder.build(
                SparkLakeFormats.FORMAT_ICEBERG,
                "x",
                "hop_iceberg",
                "file:///t",
                SparkMaintenanceSqlBuilder.OP_VACUUM,
                "1",
                null,
                null,
                null));
  }
}
