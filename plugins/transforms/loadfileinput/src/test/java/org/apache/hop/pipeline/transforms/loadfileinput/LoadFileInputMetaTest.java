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

package org.apache.hop.pipeline.transforms.loadfileinput;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.apache.hop.core.fileinput.InputFile;
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

class LoadFileInputMetaTest {
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
  void testLoadSave() throws Exception {
    LoadFileInputMeta meta =
        TransformSerializationTestUtil.testSerialization("/transform.xml", LoadFileInputMeta.class);

    validateTransformProperties(meta);
    validateFilesProperties(meta);
    validateFieldsProperties(meta);
  }

  private static void validateTransformProperties(LoadFileInputMeta meta) {
    assertTrue(meta.isIncludeFilename());
    assertEquals("filename", meta.getFilenameField());
    assertTrue(meta.isIncludeRowNumber());
    assertEquals("rowNum", meta.getRowNumberField());
    assertTrue(meta.isIgnoreEmptyFile());
    assertTrue(meta.isIgnoreMissingPath());
    assertEquals("UTF-8", meta.getEncoding());
    assertEquals(999L, meta.getRowLimit());
    assertTrue(meta.isFileInField());
    assertEquals("dynamicField", meta.getDynamicFilenameField());
    assertEquals("shortFile", meta.getAdditionalFields().getShortFilenameField());
    assertEquals("path", meta.getAdditionalFields().getPathField());
    assertEquals("hidden", meta.getAdditionalFields().getHiddenField());
    assertEquals("lastUpd", meta.getAdditionalFields().getLastModificationField());
    assertEquals("uri", meta.getAdditionalFields().getUriField());
    assertEquals("rootUri", meta.getAdditionalFields().getRootUriField());
    assertEquals("extension", meta.getAdditionalFields().getExtensionField());
  }

  // Files
  private static void validateFilesProperties(LoadFileInputMeta meta) {
    assertEquals(2, meta.getInputFiles().size());
    InputFile f = meta.getInputFiles().getFirst();
    assertEquals("sample-file1.txt", f.getFileName());
    assertEquals("mask1", f.getFileMask());
    assertEquals("excludeMask1", f.getExcludeFileMask());
    assertTrue(f.isFileRequired());
    assertTrue(f.isIncludeSubFolders());
    f = meta.getInputFiles().getLast();
    assertEquals("sample-file2.txt", f.getFileName());
    assertEquals("mask2", f.getFileMask());
    assertEquals("excludeMask2", f.getExcludeFileMask());
    assertTrue(f.isFileRequired());
    assertTrue(f.isIncludeSubFolders());
  }

  // Fields (We don't really care about the other fields)
  private static void validateFieldsProperties(LoadFileInputMeta meta) {
    assertEquals(2, meta.getInputFields().size());
    LoadFileInputField field = meta.getInputFields().getFirst();
    assertEquals("File content", field.getName());
    assertEquals(LoadFileInputField.ElementType.FILE_CONTENT, field.getElementType());
    assertEquals(IValueMeta.TYPE_STRING, field.getType());
    assertEquals(65535, field.getLength());

    field = meta.getInputFields().getLast();
    assertEquals("File size", field.getName());
    assertEquals(LoadFileInputField.ElementType.FILE_SIZE, field.getElementType());
    assertEquals(IValueMeta.TYPE_INTEGER, field.getType());
    assertEquals(6, field.getLength());
  }
}
