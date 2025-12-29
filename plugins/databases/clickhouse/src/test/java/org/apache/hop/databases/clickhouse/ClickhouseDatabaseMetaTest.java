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
 *
 */

package org.apache.hop.databases.clickhouse;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import org.apache.hop.core.HopClientEnvironment;
import org.apache.hop.core.database.DatabaseMeta;
import org.apache.hop.junit.rules.RestoreHopEngineEnvironmentExtension;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

class ClickhouseDatabaseMetaTest {
  @RegisterExtension
  static RestoreHopEngineEnvironmentExtension env = new RestoreHopEngineEnvironmentExtension();

  ClickhouseDatabaseMeta nativeMeta;

  @BeforeEach
  void setupOnce() throws Exception {
    nativeMeta = new ClickhouseDatabaseMeta();
    nativeMeta.setAccessType(DatabaseMeta.TYPE_ACCESS_NATIVE);
    nativeMeta.addDefaultOptions();

    HopClientEnvironment.init();
  }

  @Test
  void testAccessType() {
    int[] aTypes = new int[] {DatabaseMeta.TYPE_ACCESS_NATIVE};
    assertArrayEquals(aTypes, nativeMeta.getAccessTypeList());
  }

  @Test
  void testUrl() {
    assertEquals("com.clickhouse.jdbc.ClickHouseDriver", nativeMeta.getDriverClass());

    assertEquals(
        "jdbc:clickhouse://localhost:8123/sampledb",
        nativeMeta.getURL("localhost", "8123", "sampledb"));
    try {
      assertEquals(
          "jdbc:clickhouse://localhost:8123/sampledb", nativeMeta.getURL("", "8123", "sampledb"));
      fail("Should have thrown IllegalArgumentException");
    } catch (IllegalArgumentException dummy) {
      // expected if host is null or empty
    }
  }

  @Test
  void testSupport() {
    assertFalse(nativeMeta.isSupportsSchemas());
    assertTrue(nativeMeta.isSupportsViews());
    assertFalse(nativeMeta.isSupportsSequences());
    assertTrue(nativeMeta.IsSupportsErrorHandlingOnBatchUpdates());
    assertFalse(nativeMeta.isSupportsBooleanDataType());
    assertTrue(nativeMeta.isSupportsBitmapIndex());
    assertFalse(nativeMeta.isSupportsTransactions());
    assertFalse(nativeMeta.isSupportsTimeStampToDateConversion());
    assertTrue(nativeMeta.isSupportsSynonyms());
  }
}
