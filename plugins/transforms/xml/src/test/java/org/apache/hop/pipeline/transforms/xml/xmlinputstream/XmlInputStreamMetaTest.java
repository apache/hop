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
 *
 */

package org.apache.hop.pipeline.transforms.xml.xmlinputstream;

import org.apache.hop.pipeline.transform.TransformSerializationTestUtil;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

class XmlInputStreamMetaTest {
  @Test
  void testSerializationRoundTrip() throws Exception {
    XmlInputStreamMeta meta =
        TransformSerializationTestUtil.testSerialization(
            "/xml-input-stream.xml", XmlInputStreamMeta.class);

    validateBasics(meta);
    validateFields(meta);
  }

  private static void validateFields(XmlInputStreamMeta meta) {
    Assertions.assertTrue(meta.isIncludeFilenameField());
    Assertions.assertEquals("xml_filename", meta.getFilenameField());

    Assertions.assertTrue(meta.isIncludeRowNumberField());
    Assertions.assertEquals("xml_row_number", meta.getRowNumberField());

    Assertions.assertTrue(meta.isIncludeXmlDataTypeNumericField());
    Assertions.assertEquals("xml_data_type_numeric", meta.getXmlDataTypeNumericField());

    Assertions.assertTrue(meta.isIncludeXmlDataTypeDescriptionField());
    Assertions.assertEquals("xml_data_type_description", meta.getXmlDataTypeDescriptionField());

    Assertions.assertTrue(meta.isIncludeXmlLocationLineField());
    Assertions.assertEquals("xml_location_line", meta.getXmlLocationLineField());

    Assertions.assertTrue(meta.isIncludeXmlLocationColumnField());
    Assertions.assertEquals("xml_location_column", meta.getXmlLocationColumnField());

    Assertions.assertTrue(meta.isIncludeXmlElementIDField());
    Assertions.assertEquals("xml_element_id", meta.getXmlElementIDField());

    Assertions.assertTrue(meta.isIncludeXmlParentElementIDField());
    Assertions.assertEquals("xml_parent_element_id", meta.getXmlParentElementIDField());

    Assertions.assertTrue(meta.isIncludeXmlElementLevelField());
    Assertions.assertEquals("xml_element_level", meta.getXmlElementLevelField());

    Assertions.assertTrue(meta.isIncludeXmlPathField());
    Assertions.assertEquals("xml_path", meta.getXmlPathField());

    Assertions.assertTrue(meta.isIncludeXmlParentPathField());
    Assertions.assertEquals("xml_parent_path", meta.getXmlParentPathField());

    Assertions.assertTrue(meta.isIncludeXmlDataNameField());
    Assertions.assertEquals("xml_data_name", meta.getXmlDataNameField());

    Assertions.assertTrue(meta.isIncludeXmlDataValueField());
    Assertions.assertEquals("xml_data_value", meta.getXmlDataValueField());
  }

  private static void validateBasics(XmlInputStreamMeta meta) {
    Assertions.assertTrue(meta.isSourceFromInput());
    Assertions.assertEquals("sourceFieldName", meta.getSourceFieldName());
    Assertions.assertEquals("filename.xml", meta.getFilename());
    Assertions.assertTrue(meta.isAddResultFile());
    Assertions.assertEquals("123", meta.getNrRowsToSkip());
    Assertions.assertEquals("456", meta.getRowLimit());
    Assertions.assertEquals("2048", meta.getDefaultStringLen());
    Assertions.assertEquals("UTF-8", meta.getEncoding());
    Assertions.assertTrue(meta.isEnableNamespaces());
    Assertions.assertTrue(meta.isEnableTrim());
  }
}
