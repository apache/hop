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

package org.apache.hop.pipeline.transforms.mssqlbulkloader;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.apache.hop.pipeline.transform.TransformSerializationTestUtil;
import org.junit.jupiter.api.Test;

class MsSqlServerBulkLoaderMetaTest {

  private static final String TRANSFORM_XML = "/mssql-bulkloader-transform.xml";

  @Test
  void serializationRoundTripKeepsEveryOption() throws Exception {
    MsSqlServerBulkLoaderMeta meta =
        TransformSerializationTestUtil.testSerialization(
            TRANSFORM_XML, MsSqlServerBulkLoaderMeta.class);

    assertEquals("mssql", meta.getConnection());
    assertEquals("staging", meta.getSchemaName());
    assertEquals("orders", meta.getTableName());
    assertEquals("25000", meta.getBatchSize());
    assertEquals("120", meta.getBulkCopyTimeout());

    assertTrue(meta.isTruncateTable());
    assertTrue(meta.isOnlyWhenHaveRows());
    assertTrue(meta.isSpecifyFields());
    assertTrue(meta.isTableLock());
    assertTrue(meta.isKeepIdentity());
    assertTrue(meta.isKeepNulls());
    assertTrue(meta.isCheckConstraints());
    assertFalse(meta.isFireTriggers());
    assertFalse(meta.isAllowEncryptedValueModifications());
  }

  @Test
  void serializationRoundTripKeepsTheFieldMapping() throws Exception {
    MsSqlServerBulkLoaderMeta meta =
        TransformSerializationTestUtil.testSerialization(
            TRANSFORM_XML, MsSqlServerBulkLoaderMeta.class);

    assertEquals(2, meta.getFields().size());

    MsSqlServerBulkLoaderMeta.Field first = meta.getFields().get(0);
    assertEquals("order_id", first.getFieldTable());
    assertEquals("id", first.getFieldStream());
    assertEquals(MsSqlServerBulkLoaderMeta.OrderHint.ASCENDING, first.getOrderHint());

    MsSqlServerBulkLoaderMeta.Field second = meta.getFields().get(1);
    assertEquals("customer_name", second.getFieldTable());
    assertEquals("customer", second.getFieldStream());
    assertEquals(MsSqlServerBulkLoaderMeta.OrderHint.NONE, second.getOrderHint());
  }

  @Test
  void cloneCopiesEveryOption() throws Exception {
    MsSqlServerBulkLoaderMeta meta =
        TransformSerializationTestUtil.testSerialization(
            TRANSFORM_XML, MsSqlServerBulkLoaderMeta.class);

    MsSqlServerBulkLoaderMeta clone = (MsSqlServerBulkLoaderMeta) meta.clone();

    assertEquals(meta.getConnection(), clone.getConnection());
    assertEquals(meta.getSchemaName(), clone.getSchemaName());
    assertEquals(meta.getTableName(), clone.getTableName());
    assertEquals(meta.getBatchSize(), clone.getBatchSize());
    assertEquals(meta.getBulkCopyTimeout(), clone.getBulkCopyTimeout());
    assertEquals(meta.isTruncateTable(), clone.isTruncateTable());
    assertEquals(meta.isTableLock(), clone.isTableLock());
    assertEquals(meta.getFields().size(), clone.getFields().size());
  }

  @Test
  void tableLockIsOnByDefaultAndTheRestIsOff() {
    MsSqlServerBulkLoaderMeta meta = new MsSqlServerBulkLoaderMeta();
    meta.setDefault();

    // The step this transform replaces hardcoded a table lock, so keeping it on by default is what
    // an existing bulk load would expect. Everything else follows the driver's own defaults.
    assertTrue(meta.isTableLock());
    assertFalse(meta.isKeepIdentity());
    assertFalse(meta.isKeepNulls());
    assertFalse(meta.isCheckConstraints());
    assertFalse(meta.isFireTriggers());
    assertFalse(meta.isAllowEncryptedValueModifications());
    assertEquals(MsSqlServerBulkLoaderMeta.DEFAULT_BATCH_SIZE, meta.getBatchSize());
  }

  @Test
  void aMissingOrderHintReadsAsNoHint() {
    // Injected, hand-edited or legacy fields carry no <order_hint>. Returning null there would NPE
    // the dialog on open and the bulk copy setup at runtime.
    MsSqlServerBulkLoaderMeta.Field field = new MsSqlServerBulkLoaderMeta.Field();
    assertEquals(MsSqlServerBulkLoaderMeta.OrderHint.NONE, field.getOrderHint());
  }

  @Test
  void orderHintIsLookedUpByCodeAndByDescription() {
    assertEquals(
        MsSqlServerBulkLoaderMeta.OrderHint.DESCENDING,
        MsSqlServerBulkLoaderMeta.lookupOrderHint("DESC"));
    assertEquals(
        MsSqlServerBulkLoaderMeta.OrderHint.ASCENDING,
        MsSqlServerBulkLoaderMeta.lookupOrderHint(
            MsSqlServerBulkLoaderMeta.OrderHint.ASCENDING.getDescription()));
    assertEquals(
        MsSqlServerBulkLoaderMeta.OrderHint.NONE, MsSqlServerBulkLoaderMeta.lookupOrderHint(""));
  }
}
