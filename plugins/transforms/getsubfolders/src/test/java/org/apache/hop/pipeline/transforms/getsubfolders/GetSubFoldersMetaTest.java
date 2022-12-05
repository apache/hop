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

package org.apache.hop.pipeline.transforms.getsubfolders;

import org.apache.hop.pipeline.transform.TransformSerializationTestUtil;
import org.junit.Assert;
import org.junit.Test;

public class GetSubFoldersMetaTest {
  @Test
  public void testSerialization() throws Exception {
    GetSubFoldersMeta meta =
        TransformSerializationTestUtil.testSerialization(
            "/get-subfolder-names-transform.xml", GetSubFoldersMeta.class);

    Assert.assertTrue(meta.isIncludeRowNumber());
    Assert.assertEquals(1, meta.getFiles().size());
    Assert.assertEquals("${PROJECT_HOME}", meta.getFiles().get(0).getName());
    Assert.assertTrue(meta.getFiles().get(0).isRequired());
    Assert.assertEquals("rowNumber", meta.getRowNumberField());
    Assert.assertEquals(123L, meta.getRowLimit());
    Assert.assertEquals("inputField", meta.getDynamicFolderNameField());
    Assert.assertTrue(meta.isFolderNameDynamic());
  }
}
