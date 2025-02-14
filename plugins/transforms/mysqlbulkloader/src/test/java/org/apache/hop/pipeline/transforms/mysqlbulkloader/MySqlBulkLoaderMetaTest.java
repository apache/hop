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

package org.apache.hop.pipeline.transforms.mysqlbulkloader;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.apache.hop.pipeline.transform.TransformSerializationTestUtil;
import org.junit.jupiter.api.Test;

class MySqlBulkLoaderMetaTest {

  @Test
  void testNewSerialization() throws Exception {
    MySqlBulkLoaderMeta meta =
        TransformSerializationTestUtil.testSerialization(
            "/mysql-bulkloader-transform.xml", MySqlBulkLoaderMeta.class);

    assertEquals("testTable", meta.getTableName());
    assertEquals("mysql", meta.getConnection());
    assertEquals("\\", meta.getEscapeChar());
    assertEquals("\"", meta.getEnclosure());
    assertEquals("\t", meta.getDelimiter());
    assertEquals("/tmp/fifo", meta.getFifoFileName());
    assertEquals(2, meta.getFields().size());
    assertFalse(meta.isIgnoringErrors());
    assertFalse(meta.isReplacingData());
    assertTrue(meta.isLocalFile());
  }

  @Test
  void testClone() throws Exception {
    MySqlBulkLoaderMeta meta =
        TransformSerializationTestUtil.testSerialization(
            "/mysql-bulkloader-transform.xml", MySqlBulkLoaderMeta.class);

    MySqlBulkLoaderMeta clone = (MySqlBulkLoaderMeta) meta.clone();

    assertEquals(clone.getTableName(), meta.getTableName());
    assertEquals(clone.getConnection(), meta.getConnection());
    assertEquals(clone.getEscapeChar(), meta.getEscapeChar());
    assertEquals(clone.getEnclosure(), meta.getEnclosure());
    assertEquals(clone.getDelimiter(), meta.getDelimiter());
    assertEquals(clone.getFifoFileName(), meta.getFifoFileName());
    assertEquals(clone.getFields().size(), meta.getFields().size());
    assertEquals(clone.isIgnoringErrors(), meta.isIgnoringErrors());
    assertEquals(clone.isReplacingData(), meta.isReplacingData());
    assertEquals(clone.isLocalFile(), meta.isLocalFile());
  }
}
