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
package org.apache.hop.pipeline.transforms.sqlfileoutput;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.apache.hop.pipeline.transform.TransformSerializationTestUtil;
import org.junit.jupiter.api.Test;

class SQLFileOutputMetaTest {
  @Test
  void testSerialization() throws Exception {
    SQLFileOutputMeta meta =
        TransformSerializationTestUtil.testSerialization(
            "/sql-file-output-transform.xml", SQLFileOutputMeta.class);

    assertEquals("testtable", meta.getTableName());
    assertEquals("${DATABASE_NAME}", meta.getConnection());
    assertEquals("public", meta.getSchemaName());
    assertFalse(meta.isTruncateTable());
    assertTrue(meta.isStartNewLine());

    assertEquals("${PROJECT_HOME}/output/filename", meta.getFile().getFileName());
    assertFalse(meta.getFile().isFileAppended());
    assertFalse(meta.getFile().isTransformNrInFilename());
    assertEquals(0, meta.getFile().getSplitEvery());
  }

  @Test
  void testClone() throws Exception {
    SQLFileOutputMeta meta =
        TransformSerializationTestUtil.testSerialization(
            "/sql-file-output-transform.xml", SQLFileOutputMeta.class);

    SQLFileOutputMeta clone = (SQLFileOutputMeta) meta.clone();

    assertEquals(clone.getTableName(), meta.getTableName());
    assertEquals(clone.getConnection(), meta.getConnection());
    assertEquals(clone.getSchemaName(), meta.getSchemaName());
    assertEquals(clone.isTruncateTable(), meta.isTruncateTable());
    assertEquals(clone.isStartNewLine(), meta.isStartNewLine());

    assertEquals(clone.getFile().getFileName(), meta.getFile().getFileName());
    assertEquals(clone.getFile().isFileAppended(), meta.getFile().isFileAppended());
    assertEquals(
        clone.getFile().isTransformNrInFilename(), meta.getFile().isTransformNrInFilename());
    assertEquals(clone.getFile().getSplitEvery(), meta.getFile().getSplitEvery());
  }
}
