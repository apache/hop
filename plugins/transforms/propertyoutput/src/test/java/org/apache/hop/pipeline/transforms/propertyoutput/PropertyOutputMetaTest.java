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

package org.apache.hop.pipeline.transforms.propertyoutput;

import org.apache.commons.lang.StringUtils;
import org.apache.hop.pipeline.transform.TransformSerializationTestUtil;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

class PropertyOutputMetaTest {
  @Test
  void testXmlRoundTrip() throws Exception {
    PropertyOutputMeta meta =
        TransformSerializationTestUtil.testSerialization(
            "/properties-output.xml", PropertyOutputMeta.class);

    Assertions.assertEquals("key-field", meta.getKeyField());
    Assertions.assertEquals("value-field", meta.getValueField());
    Assertions.assertEquals("This is a comment", meta.getComment());
    Assertions.assertEquals("filename-field", meta.getFileNameField());
    Assertions.assertFalse(meta.isFileNameInField());
    Assertions.assertEquals("filename", meta.getFileDetails().getFileName());
    Assertions.assertEquals("properties", meta.getFileDetails().getExtension());
    Assertions.assertTrue(meta.getFileDetails().isTransformNrInFilename());
    Assertions.assertFalse(meta.getFileDetails().isPartitionIdInFilename());
    Assertions.assertTrue(meta.getFileDetails().isDateInFilename());
    Assertions.assertTrue(meta.getFileDetails().isTimeInFilename());
    Assertions.assertTrue(meta.getFileDetails().isCreateParentFolder());
    Assertions.assertTrue(meta.getFileDetails().isAddToResult());
    Assertions.assertTrue(meta.getFileDetails().isAppending());
  }

  @Test
  void testLegacyXmlRoundTrip() throws Exception {
    PropertyOutputMeta meta =
        TransformSerializationTestUtil.testSerialization(
            "/properties-output-legacy.xml", PropertyOutputMeta.class);

    Assertions.assertEquals("propertyName", meta.getKeyField());
    Assertions.assertEquals("propertyValue", meta.getValueField());
    Assertions.assertTrue(StringUtils.isEmpty(meta.getComment()));
    Assertions.assertTrue(StringUtils.isEmpty(meta.getFileNameField()));
    Assertions.assertFalse(meta.isFileNameInField());
    Assertions.assertEquals(
        "${Internal.Entry.Current.Directory}\\propertyOutput", meta.getFileDetails().getFileName());
    Assertions.assertEquals("properties", meta.getFileDetails().getExtension());
    Assertions.assertFalse(meta.getFileDetails().isTransformNrInFilename());
    Assertions.assertFalse(meta.getFileDetails().isPartitionIdInFilename());
    Assertions.assertFalse(meta.getFileDetails().isDateInFilename());
    Assertions.assertFalse(meta.getFileDetails().isTimeInFilename());
    Assertions.assertFalse(meta.getFileDetails().isCreateParentFolder());
    Assertions.assertFalse(meta.getFileDetails().isAddToResult());
    Assertions.assertTrue(meta.getFileDetails().isAppending());
  }
}
