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

package org.apache.hop.pipeline.transforms.getfilenames;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayList;
import java.util.List;
import org.apache.hop.core.HopEnvironment;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.plugins.PluginRegistry;
import org.apache.hop.junit.rules.RestoreHopEngineEnvironmentExtension;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

/** Additional test class for GetFileNamesMeta */
class GetFileNamesMetaAdditionalTest {

  @RegisterExtension
  static RestoreHopEngineEnvironmentExtension env = new RestoreHopEngineEnvironmentExtension();

  private GetFileNamesMeta meta;

  @BeforeEach
  void setUp() throws HopException {
    HopEnvironment.init();
    PluginRegistry.init();
    meta = new GetFileNamesMeta();
  }

  @Test
  void testDefaultConstructor() {
    assertNotNull(meta);
    assertNotNull(meta.getFilesList());
    assertNotNull(meta.getFilterItemList());
    assertTrue(meta.getFilesList().isEmpty());
    assertTrue(meta.getFilterItemList().isEmpty());
  }

  @Test
  void testSetDefault() {
    meta.setDefault();

    assertFalse(meta.isDoNotFailIfNoFile());
    assertTrue(meta.isAddResultFile());
    assertFalse(meta.isFileField());
    assertFalse(meta.isIncludeRowNumber());
    assertEquals("", meta.getRowNumberField());
    assertEquals("", meta.getDynamicFilenameField());
    assertEquals("", meta.getDynamicWildcardField());
    assertFalse(meta.isDynamicIncludeSubFolders());
    assertEquals("", meta.getDynamicExcludeWildcardField());
    assertFalse(meta.isRaiseAnExceptionIfNoFile());
    assertEquals(0, meta.getRowLimit());

    // Check that default filter item is added
    assertEquals(1, meta.getFilterItemList().size());
    assertEquals("all_files", meta.getFilterItemList().get(0).getFileTypeFilterSelection());
  }

  @Test
  void testClone() {
    meta.setIncludeRowNumber(true);
    meta.setRowNumberField("rowNum");
    meta.setFileField(true);
    meta.setDynamicFilenameField("filename");
    meta.setDynamicWildcardField("wildcard");
    meta.setDynamicExcludeWildcardField("exclude");
    meta.setDynamicIncludeSubFolders(true);
    meta.setAddResultFile(false);
    meta.setDoNotFailIfNoFile(true);
    meta.setRaiseAnExceptionIfNoFile(true);
    meta.setRowLimit(100);

    GetFileNamesMeta cloned = (GetFileNamesMeta) meta.clone();

    assertNotSame(meta, cloned);
    assertEquals(meta.isIncludeRowNumber(), cloned.isIncludeRowNumber());
    assertEquals(meta.getRowNumberField(), cloned.getRowNumberField());
    assertEquals(meta.isFileField(), cloned.isFileField());
    assertEquals(meta.getDynamicFilenameField(), cloned.getDynamicFilenameField());
    assertEquals(meta.getDynamicWildcardField(), cloned.getDynamicWildcardField());
    assertEquals(meta.getDynamicExcludeWildcardField(), cloned.getDynamicExcludeWildcardField());
    assertEquals(meta.isDynamicIncludeSubFolders(), cloned.isDynamicIncludeSubFolders());
    assertEquals(meta.isAddResultFile(), cloned.isAddResultFile());
    assertEquals(meta.isDoNotFailIfNoFile(), cloned.isDoNotFailIfNoFile());
    assertEquals(meta.isRaiseAnExceptionIfNoFile(), cloned.isRaiseAnExceptionIfNoFile());
    assertEquals(meta.getRowLimit(), cloned.getRowLimit());
  }

  @Test
  void testGetRequiredFilesDesc() {
    assertEquals("N", meta.getRequiredFilesDesc(null));
    assertEquals("N", meta.getRequiredFilesDesc("N"));
    assertEquals("Y", meta.getRequiredFilesDesc("Y"));
    assertEquals("N", meta.getRequiredFilesDesc("invalid"));
  }

  @Test
  void testFilesListOperations() {
    List<FileItem> filesList = new ArrayList<>();
    filesList.add(new FileItem("file1.txt", "*.txt", "*.tmp", "Y", "N"));
    filesList.add(new FileItem("file2.txt", "*.txt", "*.tmp", "N", "Y"));

    meta.setFilesList(filesList);

    assertEquals(2, meta.getFilesList().size());
    assertEquals("file1.txt", meta.getFilesList().get(0).getFileName());
    assertEquals("file2.txt", meta.getFilesList().get(1).getFileName());
  }

  @Test
  void testFilterItemListOperations() {
    List<FilterItem> filterList = new ArrayList<>();
    filterList.add(new FilterItem("FILES_ONLY"));
    filterList.add(new FilterItem("FILES_AND_FOLDERS"));

    meta.setFilterItemList(filterList);

    assertEquals(2, meta.getFilterItemList().size());
    assertEquals("FILES_ONLY", meta.getFilterItemList().get(0).getFileTypeFilterSelection());
    assertEquals("FILES_AND_FOLDERS", meta.getFilterItemList().get(1).getFileTypeFilterSelection());
  }

  @Test
  void testBooleanSettersAndGetters() {
    // Test includeRowNumber
    meta.setIncludeRowNumber(true);
    assertTrue(meta.isIncludeRowNumber());
    meta.setIncludeRowNumber(false);
    assertFalse(meta.isIncludeRowNumber());

    // Test fileField
    meta.setFileField(true);
    assertTrue(meta.isFileField());
    meta.setFileField(false);
    assertFalse(meta.isFileField());

    // Test dynamicIncludeSubFolders
    meta.setDynamicIncludeSubFolders(true);
    assertTrue(meta.isDynamicIncludeSubFolders());
    meta.setDynamicIncludeSubFolders(false);
    assertFalse(meta.isDynamicIncludeSubFolders());

    // Test addResultFile
    meta.setAddResultFile(true);
    assertTrue(meta.isAddResultFile());
    meta.setAddResultFile(false);
    assertFalse(meta.isAddResultFile());

    // Test doNotFailIfNoFile
    meta.setDoNotFailIfNoFile(true);
    assertTrue(meta.isDoNotFailIfNoFile());
    meta.setDoNotFailIfNoFile(false);
    assertFalse(meta.isDoNotFailIfNoFile());

    // Test raiseAnExceptionIfNoFile
    meta.setRaiseAnExceptionIfNoFile(true);
    assertTrue(meta.isRaiseAnExceptionIfNoFile());
    meta.setRaiseAnExceptionIfNoFile(false);
    assertFalse(meta.isRaiseAnExceptionIfNoFile());
  }

  @Test
  void testStringSettersAndGetters() {
    // Test rowNumberField
    meta.setRowNumberField("rowNum");
    assertEquals("rowNum", meta.getRowNumberField());

    // Test dynamicFilenameField
    meta.setDynamicFilenameField("filename");
    assertEquals("filename", meta.getDynamicFilenameField());

    // Test dynamicWildcardField
    meta.setDynamicWildcardField("wildcard");
    assertEquals("wildcard", meta.getDynamicWildcardField());

    // Test dynamicExcludeWildcardField
    meta.setDynamicExcludeWildcardField("exclude");
    assertEquals("exclude", meta.getDynamicExcludeWildcardField());
  }

  @Test
  void stRowLimit() {
    meta.setRowLimit(100);
    assertEquals(100, meta.getRowLimit());

    meta.setRowLimit(0);
    assertEquals(0, meta.getRowLimit());

    meta.setRowLimit(-1);
    assertEquals(-1, meta.getRowLimit());
  }

  @Test
  void testSupportsErrorHandling() {
    assertTrue(meta.supportsErrorHandling());
  }

  @Test
  void testConstants() {
    assertNotNull(GetFileNamesMeta.RequiredFilesDesc);
    assertNotNull(GetFileNamesMeta.RequiredFilesCode);
    assertEquals(2, GetFileNamesMeta.RequiredFilesDesc.length);
    assertEquals(2, GetFileNamesMeta.RequiredFilesCode.length);
    assertEquals("N", GetFileNamesMeta.RequiredFilesCode[0]);
    assertEquals("Y", GetFileNamesMeta.RequiredFilesCode[1]);
  }
}
