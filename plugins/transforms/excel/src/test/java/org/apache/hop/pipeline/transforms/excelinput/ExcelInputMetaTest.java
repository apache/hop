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

package org.apache.hop.pipeline.transforms.excelinput;

import static org.junit.Assert.assertEquals;

import org.apache.hop.pipeline.transform.TransformSerializationTestUtil;
import org.junit.Test;

public class ExcelInputMetaTest {

  @Test
  public void testSerialization() throws Exception {
    ExcelInputMeta meta =
        TransformSerializationTestUtil.testSerialization(
            "/excel-input-transform.xml", ExcelInputMeta.class);
    assertEquals(2, meta.getFiles().size());
    assertEquals("file1.xls", meta.getFiles().get(0).getName());
    assertEquals("file2.xls", meta.getFiles().get(1).getName());
    assertEquals(1, meta.getSheets().size());
    assertEquals(4, meta.getFields().size());
  }

  @Test
  public void testClone() throws Exception {
    ExcelInputMeta meta =
        TransformSerializationTestUtil.testSerialization(
            "/excel-input-transform.xml", ExcelInputMeta.class);

    ExcelInputMeta clone = meta.clone();

    assertEquals(meta.getFiles().size(), clone.getFiles().size());
  }
}
