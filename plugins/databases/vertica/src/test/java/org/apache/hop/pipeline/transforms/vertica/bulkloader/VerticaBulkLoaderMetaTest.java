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

package org.apache.hop.pipeline.transforms.vertica.bulkloader;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;
import org.apache.hop.core.injection.bean.BeanInjectionInfo;
import org.apache.hop.pipeline.transform.TransformSerializationTestUtil;
import org.junit.jupiter.api.Test;

class VerticaBulkLoaderMetaTest {

  @Test
  void readsAllOptionsFromXml() throws Exception {
    VerticaBulkLoaderMeta meta =
        TransformSerializationTestUtil.testSerialization(
            "/vertica-bulkloader-transform.xml", VerticaBulkLoaderMeta.class);

    assertEquals("vertica", meta.getConnection());
    assertEquals("staging", meta.getSchemaName());
    assertEquals("INVOICE", meta.getTableName());
    assertTrue(meta.isTruncateTable());
    assertTrue(meta.isOnlyWhenHaveRows());
    assertTrue(meta.isDirect());
    assertFalse(meta.isAbortOnError());
    assertEquals("/tmp/exceptions.log", meta.getExceptionsFileName());
    assertEquals("/tmp/rejected.log", meta.getRejectedDataFileName());
    assertEquals("nightly load", meta.getStreamName());
    assertTrue(meta.isSpecifyFields());
    assertEquals(
        List.of(
            new VerticaBulkLoaderField("INVOICE_ID", "id"),
            new VerticaBulkLoaderField("COST_CURRENCY", "currency")),
        meta.getFields());
  }

  @Test
  void defaultsToLoadingTheWholeInputRow() {
    VerticaBulkLoaderMeta meta = new VerticaBulkLoaderMeta();
    meta.setDefault();

    assertFalse(meta.specifyFields());
    assertTrue(meta.getFields().isEmpty());
  }

  @Test
  void exposesTheStreamAndColumnNamesToMetadataInjection() {
    // The column names used to be declared twice: once on the field list and once on a String[]
    // that was never filled in, which left the target column unreachable for injection.
    BeanInjectionInfo<VerticaBulkLoaderMeta> info =
        new BeanInjectionInfo<>(VerticaBulkLoaderMeta.class);

    assertTrue(info.getProperties().containsKey("STREAM_FIELDNAME"));
    assertTrue(info.getProperties().containsKey("DATABASE_FIELDNAME"));
    assertEquals("FIELDS", info.getProperties().get("DATABASE_FIELDNAME").getGroupKey());
  }
}
