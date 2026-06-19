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
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.apache.hop.core.HopEnvironment;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.fileinput.FileInputList;
import org.apache.hop.core.fileinput.FileTypeFilter;
import org.apache.hop.core.logging.ILoggingObject;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.junit.rules.RestoreHopEngineEnvironmentExtension;
import org.apache.hop.pipeline.transform.RowAdapter;
import org.apache.hop.pipeline.transforms.mock.TransformMockHelper;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.api.io.TempDir;

/** Test class for GetFileNames transform */
class GetFileNamesTransformTest {

  @RegisterExtension
  static RestoreHopEngineEnvironmentExtension env = new RestoreHopEngineEnvironmentExtension();

  private TransformMockHelper<GetFileNamesMeta, GetFileNamesData> mockHelper;

  @BeforeEach
  void setUp() throws HopException {
    HopEnvironment.init();

    mockHelper =
        new TransformMockHelper<>("GetFileNames", GetFileNamesMeta.class, GetFileNamesData.class);
    when(mockHelper.logChannelFactory.create(any(), any(ILoggingObject.class)))
        .thenReturn(mockHelper.iLogChannel);
    when(mockHelper.pipeline.isRunning()).thenReturn(true);
  }

  @AfterEach
  void tearDown() {
    mockHelper.cleanUp();
  }

  @Test
  void testConstructor() {
    GetFileNames transform =
        new GetFileNames(
            mockHelper.transformMeta,
            mockHelper.iTransformMeta,
            mockHelper.iTransformData,
            0,
            mockHelper.pipelineMeta,
            mockHelper.pipeline);

    assertNotNull(transform);
    assertEquals(mockHelper.transformMeta, transform.getTransformMeta());
    assertEquals(mockHelper.iTransformMeta, transform.getMeta());
    assertEquals(mockHelper.iTransformData, transform.getData());
    assertEquals(0, transform.getCopyNr());
    assertEquals(mockHelper.pipelineMeta, transform.getPipelineMeta());
    assertEquals(mockHelper.pipeline, transform.getPipeline());
  }

  @Test
  void testInitWithFileFieldFalse() {
    // Setup meta for non-file field mode
    GetFileNamesMeta meta = new GetFileNamesMeta();
    meta.setFileField(false);
    meta.setRaiseAnExceptionIfNoFile(false);
    meta.setDefault();

    // Add a file item to the list
    meta.getFilesList().add(new FileItem("/tmp/test.txt", "*.txt", "*.tmp", "Y", "N"));

    GetFileNamesData data = new GetFileNamesData();

    GetFileNames transform =
        new GetFileNames(
            mockHelper.transformMeta, meta, data, 0, mockHelper.pipelineMeta, mockHelper.pipeline);

    // Mock the file list creation
    FileInputList mockFileList = mock(FileInputList.class);
    when(mockFileList.nrOfFiles()).thenReturn(1);
    when(mockFileList.getNonExistentFiles()).thenReturn(new ArrayList<>());
    when(mockFileList.getNonAccessibleFiles()).thenReturn(new ArrayList<>());

    GetFileNames spyTransform = spy(transform);
    // Mock the meta.getFileList method instead
    GetFileNamesMeta spyMeta = spy(meta);
    doReturn(mockFileList).when(spyMeta).getFileList(any());

    boolean result = spyTransform.init();

    assertTrue(result);
    assertNotNull(data.outputRowMeta);
    assertTrue(data.nrTransformFields > 0);
  }

  @Test
  void testInitWithFileFieldTrue() {
    // Setup meta for file field mode
    GetFileNamesMeta meta = new GetFileNamesMeta();
    meta.setFileField(true);
    meta.setRaiseAnExceptionIfNoFile(false);
    meta.setDefault();
    meta.setDynamicFilenameField("filename");

    GetFileNamesData data = new GetFileNamesData();

    GetFileNames transform =
        new GetFileNames(
            mockHelper.transformMeta, meta, data, 0, mockHelper.pipelineMeta, mockHelper.pipeline);

    boolean result = transform.init();

    assertTrue(result);
    assertNotNull(data.outputRowMeta);
    assertEquals(0, data.filessize);
  }

  @Test
  void testProcessRowWithNoInput() throws HopException {
    // Setup
    GetFileNamesMeta meta = new GetFileNamesMeta();
    meta.setFileField(false);
    meta.setDefault();

    GetFileNamesData data = new GetFileNamesData();
    data.filessize = 0;

    GetFileNames transform =
        new GetFileNames(
            mockHelper.transformMeta, meta, data, 0, mockHelper.pipelineMeta, mockHelper.pipeline);

    // Add empty row set (no rows)
    transform.addRowSetToInputRowSets(mockHelper.getMockInputRowSet());

    // Execute
    boolean result = transform.processRow();

    // Verify
    assertFalse(result); // Should return false when no more input
  }

  @Test
  void testDispose() {
    GetFileNamesMeta meta = new GetFileNamesMeta();
    GetFileNamesData data = new GetFileNamesData();

    GetFileNames transform =
        new GetFileNames(
            mockHelper.transformMeta, meta, data, 0, mockHelper.pipelineMeta, mockHelper.pipeline);

    // Test dispose with null file
    transform.dispose();
    // Should not throw exception

    // Test dispose with mock file
    data.file = mock(org.apache.commons.vfs2.FileObject.class);
    transform.dispose();
    // Should not throw exception
  }

  @Test
  void testBuildEmptyRow() {
    GetFileNamesMeta meta = new GetFileNamesMeta();
    GetFileNamesData data = new GetFileNamesData();
    data.outputRowMeta = new org.apache.hop.core.row.RowMeta();
    data.outputRowMeta.addValueMeta(new org.apache.hop.core.row.value.ValueMetaString("field1"));
    data.outputRowMeta.addValueMeta(new org.apache.hop.core.row.value.ValueMetaString("field2"));

    GetFileNames transform =
        new GetFileNames(
            mockHelper.transformMeta, meta, data, 0, mockHelper.pipelineMeta, mockHelper.pipeline);

    // Use reflection to access private method
    try {
      java.lang.reflect.Method method = GetFileNames.class.getDeclaredMethod("buildEmptyRow");
      method.setAccessible(true);
      Object[] row = (Object[]) method.invoke(transform);

      assertNotNull(row);
      // The output row meta has more fields than just the 2 we added due to the transform's default
      // fields
      assertTrue(row.length >= 2);
      assertNull(row[0]);
      assertNull(row[1]);
    } catch (Exception e) {
      fail("Failed to access buildEmptyRow method: " + e.getMessage());
    }
  }

  @Test
  void testConstants() {
    // Use reflection to access private constants
    try {
      java.lang.reflect.Field field1 = GetFileNames.class.getDeclaredField("CONST_LOG_NO_FILE");
      field1.setAccessible(true);
      String constLogNoFile = (String) field1.get(null);
      assertEquals("GetFileNames.Log.NoFile", constLogNoFile);

      java.lang.reflect.Field field2 =
          GetFileNames.class.getDeclaredField("CONST_ERROR_FINDING_FIELD");
      field2.setAccessible(true);
      String constErrorFindingField = (String) field2.get(null);
      assertEquals("GetFileNames.Log.ErrorFindingField", constErrorFindingField);

      java.lang.reflect.Field field3 =
          GetFileNames.class.getDeclaredField("CONST_COULD_NOT_FIND_FIELD");
      field3.setAccessible(true);
      String constCouldNotFindField = (String) field3.get(null);
      assertEquals("GetFileNames.Exception.CouldnotFindField", constCouldNotFindField);
    } catch (Exception e) {
      fail("Failed to access constants: " + e.getMessage());
    }
  }

  // ---------------------------------------------------------------------------
  // Runtime processRow coverage against a real folder tree:
  //   tempDir/
  //     file-a.txt  file-b.txt  data.csv
  //     folder-one/  folder-two/
  // ---------------------------------------------------------------------------

  @TempDir private Path tempDir;

  private final List<Object[]> captured = new ArrayList<>();
  private IRowMeta capturedMeta;

  private static void buildTree(Path root) throws IOException {
    Files.createDirectories(root.resolve("folder-one"));
    Files.createDirectories(root.resolve("folder-two"));
    Files.writeString(root.resolve("file-a.txt"), "alpha");
    Files.writeString(root.resolve("file-b.txt"), "beta");
    Files.writeString(root.resolve("data.csv"), "c,d");
  }

  private GetFileNamesMeta folderMeta(FileTypeFilter filter, String mask) {
    GetFileNamesMeta meta = new GetFileNamesMeta();
    meta.getFilterItemList().add(new FilterItem(filter.toString()));
    meta.getFilesList().add(new FileItem(tempDir.toAbsolutePath().toString(), mask, "", "N", "N"));
    meta.setRaiseAnExceptionIfNoFile(false);
    meta.setAddResultFile(false);
    return meta;
  }

  private GetFileNames runStatic(GetFileNamesMeta meta) throws HopException {
    captured.clear();
    capturedMeta = null;
    GetFileNamesData data = new GetFileNamesData();
    GetFileNames transform =
        new GetFileNames(
            mockHelper.transformMeta, meta, data, 0, mockHelper.pipelineMeta, mockHelper.pipeline);
    transform.addRowListener(
        new RowAdapter() {
          @Override
          public void rowWrittenEvent(IRowMeta rowMeta, Object[] row) {
            capturedMeta = rowMeta;
            captured.add(row);
          }
        });
    assertTrue(transform.init(), "init() should succeed");
    while (transform.processRow()) {
      // drain every output row
    }
    return transform;
  }

  private Set<String> capturedValues(String field) {
    int idx = capturedMeta.indexOfValue(field);
    Set<String> values = new HashSet<>();
    for (Object[] row : captured) {
      values.add((String) row[idx]);
    }
    return values;
  }

  @Test
  void staticFolderAllFilesEmitsRowPerEntry() throws Exception {
    buildTree(tempDir);
    runStatic(folderMeta(FileTypeFilter.FILES_AND_FOLDERS, ""));

    assertEquals(5, captured.size());
    Set<String> types = capturedValues("type");
    assertTrue(types.contains("file"), "files should be present");
    assertTrue(types.contains("folder"), "folders should be present");
    Set<String> names = capturedValues("short_filename");
    assertTrue(names.contains("file-a.txt"));
    assertTrue(names.contains("folder-one"));
  }

  @Test
  void staticFolderOnlyFoldersEmitsFolderRows() throws Exception {
    buildTree(tempDir);
    runStatic(folderMeta(FileTypeFilter.ONLY_FOLDERS, ""));

    assertEquals(2, captured.size(), "only the two sub-folders are expected");
    for (String type : capturedValues("type")) {
      assertEquals("folder", type);
    }
  }

  @Test
  void rowLimitStopsOutput() throws Exception {
    buildTree(tempDir);
    GetFileNamesMeta meta = folderMeta(FileTypeFilter.FILES_AND_FOLDERS, "");
    meta.setRowLimit(2);
    runStatic(meta);

    assertEquals(2, captured.size());
  }

  @Test
  void includeRowNumberAddsIncrementingField() throws Exception {
    buildTree(tempDir);
    GetFileNamesMeta meta = folderMeta(FileTypeFilter.ONLY_FILES, "");
    meta.setIncludeRowNumber(true);
    meta.setRowNumberField("rownr");
    runStatic(meta);

    assertEquals(3, captured.size());
    int idx = capturedMeta.indexOfValue("rownr");
    assertTrue(idx >= 0, "row number field should be present");
    Set<Long> numbers = new HashSet<>();
    for (Object[] row : captured) {
      numbers.add(((Number) row[idx]).longValue());
    }
    assertTrue(numbers.contains(1L) && numbers.contains(2L) && numbers.contains(3L));
  }

  @Test
  void addResultFileRegistersResultFiles() throws Exception {
    buildTree(tempDir);
    GetFileNamesMeta meta = folderMeta(FileTypeFilter.ONLY_FILES, "");
    meta.setAddResultFile(true);
    GetFileNames transform = runStatic(meta);

    assertEquals(3, captured.size());
    assertEquals(
        3, transform.getResultFiles().size(), "each file should be added as a result file");
  }

  @Test
  void initFailsWhenNoFilesAndRaiseException() {
    // Empty temp folder, no matching files, raise-an-exception on -> init should fail.
    GetFileNamesMeta meta = folderMeta(FileTypeFilter.ONLY_FILES, "");
    meta.setRaiseAnExceptionIfNoFile(true);
    meta.setDoNotFailIfNoFile(false);
    GetFileNamesData data = new GetFileNamesData();
    GetFileNames transform =
        new GetFileNames(
            mockHelper.transformMeta, meta, data, 0, mockHelper.pipelineMeta, mockHelper.pipeline);

    assertFalse(transform.init());
  }

  @Test
  void initSucceedsWhenNoFilesButDoNotFail() {
    GetFileNamesMeta meta = folderMeta(FileTypeFilter.ONLY_FILES, "");
    meta.setRaiseAnExceptionIfNoFile(true);
    meta.setDoNotFailIfNoFile(true);
    GetFileNamesData data = new GetFileNamesData();
    GetFileNames transform =
        new GetFileNames(
            mockHelper.transformMeta, meta, data, 0, mockHelper.pipelineMeta, mockHelper.pipeline);

    assertTrue(transform.init());
  }
}
