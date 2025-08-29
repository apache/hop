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

package org.apache.hop.pipeline.transforms.dimensionlookup;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.apache.hop.core.HopClientEnvironment;
import org.apache.hop.core.database.DatabaseMeta;
import org.apache.hop.metadata.serializer.memory.MemoryMetadataProvider;
import org.apache.hop.pipeline.transform.TransformSerializationTestUtil;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class DimensionLookupMetaTest {

  @BeforeEach
  public void before() throws Exception {
    HopClientEnvironment.init();
  }

  @Test
  public void testSerialization() throws Exception {
    MemoryMetadataProvider metadataProvider = new MemoryMetadataProvider();
    DatabaseMeta unitTestDb = new DatabaseMeta();
    unitTestDb.setName("unit-test-db");
    metadataProvider.getSerializer(DatabaseMeta.class).save(unitTestDb);

    DimensionLookupMeta meta =
        TransformSerializationTestUtil.testSerialization(
            "/dimension-update-transform.xml", DimensionLookupMeta.class, metadataProvider);

    assertNotNull(meta.getConnection());
    assertNotNull(meta.getTableName());
    assertNotNull(meta.getSchemaName());
    assertTrue(meta.isUpdate());
    assertEquals(100, meta.getCommitSize());
    assertEquals(5000, meta.getCacheSize());
    assertEquals(1, meta.getFields().getKeys().size());
    assertEquals(2, meta.getFields().getFields().size());

    assertEquals("value", meta.getFields().getFields().get(0).getName());
    assertEquals("valueLookup", meta.getFields().getFields().get(0).getLookup());
    assertEquals("Insert", meta.getFields().getFields().get(0).getUpdate());
    assertEquals(
        DimensionLookupMeta.DimensionUpdateType.INSERT,
        meta.getFields().getFields().get(0).getUpdateType());

    assertEquals("lastVersion", meta.getFields().getFields().get(1).getName());
    assertEquals("lastVersionLookup", meta.getFields().getFields().get(1).getLookup());
    assertEquals("LastVersion", meta.getFields().getFields().get(1).getUpdate());
    assertEquals(
        DimensionLookupMeta.DimensionUpdateType.LAST_VERSION,
        meta.getFields().getFields().get(1).getUpdateType());

    meta =
        TransformSerializationTestUtil.testSerialization(
            "/dimension-lookup-transform.xml", DimensionLookupMeta.class, metadataProvider);

    assertFalse(meta.isUpdate());
    assertEquals("Number", meta.getFields().getFields().get(0).getReturnType());
    assertEquals("String", meta.getFields().getFields().get(1).getReturnType());
  }
}
