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
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
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

/** Test class for GetFileNames edge cases and error conditions */
class GetFileNamesEdgeCasesTest {

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
  void testFileItemWithNullValues() {
    FileItem fileItem = new FileItem(null, null, null, null, null);

    assertNull(fileItem.getFileName());
    assertNull(fileItem.getFileMask());
    assertNull(fileItem.getExcludeFileMask());
    assertEquals("N", fileItem.getFileRequired());
    assertEquals("N", fileItem.getIncludeSubFolders());
  }

  @Test
  void testFileItemWithEmptyStrings() {
    FileItem fileItem = new FileItem("", "", "", "", "");

    assertEquals("", fileItem.getFileName());
    assertEquals("", fileItem.getFileMask());
    assertEquals("", fileItem.getExcludeFileMask());
    assertEquals("N", fileItem.getFileRequired());
    assertEquals("N", fileItem.getIncludeSubFolders());
  }

  @Test
  void testFileItemEqualityWithNulls() {
    FileItem fileItem1 = new FileItem("test.txt", null, null, "Y", "N");
    FileItem fileItem2 = new FileItem("test.txt", null, null, "Y", "N");
    FileItem fileItem3 = new FileItem("test.txt", "*.txt", null, "Y", "N");

    assertEquals(fileItem1, fileItem2);
    assertNotEquals(fileItem1, fileItem3);
  }

  @Test
  void testFilterItemWithNullValue() {
    FilterItem filterItem = new FilterItem(null);

    assertNull(filterItem.getFileTypeFilterSelection());
    assertEquals(new FilterItem(null), filterItem);
    assertEquals(filterItem.hashCode(), new FilterItem(null).hashCode());
  }

  @Test
  void testFilterItemWithEmptyString() {
    FilterItem filterItem = new FilterItem("");

    assertEquals("", filterItem.getFileTypeFilterSelection());
    assertEquals(new FilterItem(""), filterItem);
    assertEquals(filterItem.hashCode(), new FilterItem("").hashCode());
  }

  @Test
  void testGetFileNamesMetaWithEmptyLists() {
    meta.setFilesList(new ArrayList<>());
    meta.setFilterItemList(new ArrayList<>());

    assertTrue(meta.getFilesList().isEmpty());
    assertTrue(meta.getFilterItemList().isEmpty());
  }

  @Test
  void testGetFileNamesMetaWithNullLists() {
    meta.setFilesList(null);
    meta.setFilterItemList(null);

    assertNull(meta.getFilesList());
    assertNull(meta.getFilterItemList());
  }

  @Test
  void testGetFileNamesMetaWithLargeLists() {
    List<FileItem> largeFileList = new ArrayList<>();
    List<FilterItem> largeFilterList = new ArrayList<>();

    for (int i = 0; i < 1000; i++) {
      largeFileList.add(new FileItem("file" + i + ".txt", "*.txt", "*.tmp", "Y", "N"));
      largeFilterList.add(new FilterItem("FILES_ONLY"));
    }

    meta.setFilesList(largeFileList);
    meta.setFilterItemList(largeFilterList);

    assertEquals(1000, meta.getFilesList().size());
    assertEquals(1000, meta.getFilterItemList().size());
  }

  @Test
  void testGetFileNamesMetaWithSpecialCharacters() {
    String specialFileName = "file with spaces & symbols!@#$%^&*().txt";
    String specialMask = "*.txt";
    String specialExclude = "*.tmp";

    FileItem fileItem = new FileItem(specialFileName, specialMask, specialExclude, "Y", "N");

    assertEquals(specialFileName, fileItem.getFileName());
    assertEquals(specialMask, fileItem.getFileMask());
    assertEquals(specialExclude, fileItem.getExcludeFileMask());
  }

  @Test
  void testGetFileNamesMetaWithUnicodeCharacters() {
    String unicodeFileName = "файл_中文_한글.txt";
    String unicodeMask = "*.txt";

    FileItem fileItem = new FileItem(unicodeFileName, unicodeMask, "*.tmp", "Y", "N");

    assertEquals(unicodeFileName, fileItem.getFileName());
    assertEquals(unicodeMask, fileItem.getFileMask());
  }

  @Test
  void testGetFileNamesMetaWithVeryLongStrings() {
    String veryLongString = "a".repeat(1000);

    FileItem fileItem = new FileItem(veryLongString, veryLongString, veryLongString, "Y", "N");

    assertEquals(veryLongString, fileItem.getFileName());
    assertEquals(veryLongString, fileItem.getFileMask());
    assertEquals(veryLongString, fileItem.getExcludeFileMask());
  }

  @Test
  void testGetFileNamesMetaWithNegativeRowLimit() {
    meta.setRowLimit(-100);
    assertEquals(-100, meta.getRowLimit());
  }

  @Test
  void testGetFileNamesMetaWithMaxLongRowLimit() {
    meta.setRowLimit(Long.MAX_VALUE);
    assertEquals(Long.MAX_VALUE, meta.getRowLimit());
  }

  @Test
  void testGetFileNamesMetaWithZeroRowLimit() {
    meta.setRowLimit(0);
    assertEquals(0, meta.getRowLimit());
  }

  @Test
  void testGetFileNamesMetaWithAllBooleanCombinations() {
    // Test all possible combinations of boolean flags
    boolean[] flags = {true, false};

    for (boolean includeRowNumber : flags) {
      for (boolean fileField : flags) {
        for (boolean dynamicIncludeSubFolders : flags) {
          for (boolean addResultFile : flags) {
            for (boolean doNotFailIfNoFile : flags) {
              for (boolean raiseAnExceptionIfNoFile : flags) {
                meta.setIncludeRowNumber(includeRowNumber);
                meta.setFileField(fileField);
                meta.setDynamicIncludeSubFolders(dynamicIncludeSubFolders);
                meta.setAddResultFile(addResultFile);
                meta.setDoNotFailIfNoFile(doNotFailIfNoFile);
                meta.setRaiseAnExceptionIfNoFile(raiseAnExceptionIfNoFile);

                assertEquals(includeRowNumber, meta.isIncludeRowNumber());
                assertEquals(fileField, meta.isFileField());
                assertEquals(dynamicIncludeSubFolders, meta.isDynamicIncludeSubFolders());
                assertEquals(addResultFile, meta.isAddResultFile());
                assertEquals(doNotFailIfNoFile, meta.isDoNotFailIfNoFile());
                assertEquals(raiseAnExceptionIfNoFile, meta.isRaiseAnExceptionIfNoFile());
              }
            }
          }
        }
      }
    }
  }

  @Test
  void testGetFileNamesMetaWithEmptyStringFields() {
    meta.setRowNumberField("");
    meta.setDynamicFilenameField("");
    meta.setDynamicWildcardField("");
    meta.setDynamicExcludeWildcardField("");

    assertEquals("", meta.getRowNumberField());
    assertEquals("", meta.getDynamicFilenameField());
    assertEquals("", meta.getDynamicWildcardField());
    assertEquals("", meta.getDynamicExcludeWildcardField());
  }

  @Test
  void testGetFileNamesMetaWithNullStringFields() {
    meta.setRowNumberField(null);
    meta.setDynamicFilenameField(null);
    meta.setDynamicWildcardField(null);
    meta.setDynamicExcludeWildcardField(null);

    assertNull(meta.getRowNumberField());
    assertNull(meta.getDynamicFilenameField());
    assertNull(meta.getDynamicWildcardField());
    assertNull(meta.getDynamicExcludeWildcardField());
  }
}
