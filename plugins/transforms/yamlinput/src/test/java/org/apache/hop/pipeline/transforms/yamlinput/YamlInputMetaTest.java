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
package org.apache.hop.pipeline.transforms.yamlinput;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.apache.hop.core.plugins.PluginRegistry;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.core.row.value.ValueMetaDate;
import org.apache.hop.core.row.value.ValueMetaInteger;
import org.apache.hop.core.row.value.ValueMetaJson;
import org.apache.hop.core.row.value.ValueMetaNumber;
import org.apache.hop.core.row.value.ValueMetaPlugin;
import org.apache.hop.core.row.value.ValueMetaPluginType;
import org.apache.hop.core.row.value.ValueMetaString;
import org.apache.hop.pipeline.transform.TransformSerializationTestUtil;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class YamlInputMetaTest {
  @BeforeEach
  void beforeEach() throws Exception {
    PluginRegistry registry = PluginRegistry.getInstance();
    String[] classNames = {
      ValueMetaString.class.getName(),
      ValueMetaInteger.class.getName(),
      ValueMetaDate.class.getName(),
      ValueMetaNumber.class.getName(),
      ValueMetaJson.class.getName()
    };
    for (String className : classNames) {
      registry.registerPluginClass(className, ValueMetaPluginType.class, ValueMetaPlugin.class);
    }
  }

  @Test
  void testSerializationRoundTrip() throws Exception {
    YamlInputMeta meta =
        TransformSerializationTestUtil.testSerialization("/yaml-input.xml", YamlInputMeta.class);

    assertTrue(meta.isIncludeFilename());
    assertEquals("includeField", meta.getFilenameField());
    assertTrue(meta.isIncludeRowNumber());
    assertEquals("rowNumField", meta.getRowNumberField());
    assertTrue(meta.isValidating());
    assertTrue(meta.isAddingResultFile());
    assertTrue(meta.isIgnoringEmptyFile());
    assertTrue(meta.isDoNotFailIfNoFile());
    assertEquals("rowNumField", meta.getRowNumberField());
    assertEquals("UTF-8", meta.getEncoding());
    assertEquals(999L, meta.getRowLimit());
    assertTrue(meta.isSourceFile());
    assertTrue(meta.isInFields());
    assertEquals("filenameField", meta.getYamlField());

    assertEquals(2, meta.getYamlFiles().size());
    YamlInputMeta.YamlFile file = meta.getYamlFiles().getFirst();
    assertEquals("folder1/", file.getFilename());
    assertEquals(".*\\.yaml", file.getFileMask());
    assertTrue(file.isFileRequired());
    assertTrue(file.isIncludingSubFolders());
    file = meta.getYamlFiles().get(1);
    assertEquals("folder1/", file.getFilename());
    assertEquals(".*\\.yml", file.getFileMask());
    assertTrue(file.isFileRequired());
    assertTrue(file.isIncludingSubFolders());

    assertEquals(3, meta.getInputFields().size());
    YamlInputField field = meta.getInputFields().getFirst();
    assertEquals("field1", field.getName());
    assertEquals("key1", field.getPath());
    assertEquals(IValueMeta.TYPE_STRING, field.getType());
    assertEquals(100, field.getLength());
    assertEquals(-1, field.getPrecision());
    assertEquals(IValueMeta.TRIM_TYPE_LEFT, field.getTrimType());
    field = meta.getInputFields().get(1);
    assertEquals("field2", field.getName());
    assertEquals("key2", field.getPath());
    assertEquals(IValueMeta.TYPE_INTEGER, field.getType());
    assertEquals(7, field.getLength());
    assertEquals(0, field.getPrecision());
    assertEquals(IValueMeta.TRIM_TYPE_RIGHT, field.getTrimType());
    field = meta.getInputFields().getLast();
    assertEquals("field3", field.getName());
    assertEquals("key3", field.getPath());
    assertEquals(IValueMeta.TYPE_NUMBER, field.getType());
    assertEquals(9, field.getLength());
    assertEquals(2, field.getPrecision());
    assertEquals(IValueMeta.TRIM_TYPE_BOTH, field.getTrimType());
  }
}
