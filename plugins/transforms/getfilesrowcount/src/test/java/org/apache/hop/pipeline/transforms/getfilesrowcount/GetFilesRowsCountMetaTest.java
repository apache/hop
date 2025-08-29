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

package org.apache.hop.pipeline.transforms.getfilesrowcount;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.apache.hop.pipeline.transform.TransformSerializationTestUtil;
import org.junit.jupiter.api.Test;

class GetFilesRowsCountMetaTest {
  @Test
  void testSerialization() throws Exception {
    GetFilesRowsCountMeta meta =
        TransformSerializationTestUtil.testSerialization(
            "/get-files-rows-count-transform.xml", GetFilesRowsCountMeta.class);

    assertEquals("filesCount", meta.getFilesCountFieldName());
    assertEquals("rowsCount", meta.getRowsCountFieldName());
    assertEquals(GetFilesRowsCountMeta.SeparatorFormat.LF, meta.getRowSeparatorFormat());
    assertNull(meta.getRowSeparator());
    assertTrue(meta.isIncludeFilesCount());
    assertTrue(meta.isAddResultFilename());
    assertFalse(meta.isFileFromField());
    assertEquals(1, meta.getFiles().size());
    assertEquals("${PROJECT_HOME}/files/", meta.getFiles().get(0).getName());
    assertEquals(".*\\.txt$", meta.getFiles().get(0).getMask());
    assertNull(meta.getFiles().get(0).getExcludeMask());
    assertTrue(meta.getFiles().get(0).isRequired());
    assertTrue(meta.getFiles().get(0).isIncludeSubFolder());
  }
}
