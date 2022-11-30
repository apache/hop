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

import org.apache.hop.pipeline.transform.TransformSerializationTestUtil;
import org.junit.Assert;
import org.junit.Test;

public class GetFilesRowsCountMetaTest {
  @Test
  public void testSerialization() throws Exception {
    GetFilesRowsCountMeta meta =
        TransformSerializationTestUtil.testSerialization(
            "/get-files-rows-count-transform.xml", GetFilesRowsCountMeta.class);

    Assert.assertEquals("filesCount", meta.getFilesCountFieldName());
    Assert.assertEquals("rowsCount", meta.getRowsCountFieldName());
    Assert.assertEquals(GetFilesRowsCountMeta.SeparatorFormat.LF, meta.getRowSeparatorFormat());
    Assert.assertNull(meta.getRowSeparator());
    Assert.assertTrue(meta.isIncludeFilesCount());
    Assert.assertTrue(meta.isAddResultFilename());
    Assert.assertFalse(meta.isFileFromField());
    Assert.assertEquals(1, meta.getFiles().size());
    Assert.assertEquals("${PROJECT_HOME}/files/", meta.getFiles().get(0).getName());
    Assert.assertEquals(".*\\.txt$", meta.getFiles().get(0).getMask());
    Assert.assertNull(meta.getFiles().get(0).getExcludeMask());
    Assert.assertTrue(meta.getFiles().get(0).isRequired());
    Assert.assertTrue(meta.getFiles().get(0).isIncludeSubFolder());
  }
}
