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

import org.apache.commons.io.IOUtils;
import org.apache.commons.vfs2.FileObject;
import org.apache.commons.vfs2.FileSystemException;
import org.apache.commons.vfs2.FileSystemManager;
import org.apache.commons.vfs2.VFS;
import org.apache.hop.core.Const;
import org.apache.hop.core.HopClientEnvironment;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.fileinput.FileInputList;
import org.apache.hop.core.plugins.PluginRegistry;
import org.apache.hop.core.plugins.TransformPluginType;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.core.row.value.ValueMetaBinary;
import org.apache.hop.core.row.value.ValueMetaString;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.junit.rules.RestoreHopEngineEnvironment;
import org.apache.hop.pipeline.Pipeline;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.engines.local.LocalPipelineEngine;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.junit.*;
import org.mockito.Mockito;

import java.io.UnsupportedEncodingException;
import java.lang.reflect.Field;
import java.nio.charset.Charset;

import static org.junit.Assert.*;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyInt;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;

public class LoadFileInputTest {
  @ClassRule public static RestoreHopEngineEnvironment env = new RestoreHopEngineEnvironment();

  private FileSystemManager fs;
  private String filesPath;

  private String pipelineName;
  private PipelineMeta pipelineMeta;
  private Pipeline pipeline;

  private FileInputList fileInputList;

  private LoadFileInput loadFileInput;
  private LoadFileInputMeta loadFileInputMeta;
  private LoadFileInputData loadFileInputData;
  private LoadFileInputField loadFileInputField;
  private static String wasEncoding;

  @BeforeClass
  public static void setupBeforeClass() throws HopException {
    if (Const.isWindows()) {
      wasEncoding = System.getProperty("file.encoding");
      fiddleWithDefaultCharset("utf8");
    }
    HopClientEnvironment.init();
  }

  @AfterClass
  public static void teardownAfterClass() {
    if (wasEncoding != null) {
      fiddleWithDefaultCharset(wasEncoding);
    }
  }

  // Yeah, I don't like it much either, but it lets me set file.encoding after
  // the VM has fired up. Remove this code when the backlog ticket BACKLOG-20800 gets fixed.
  private static void fiddleWithDefaultCharset(String fiddleValue) {
    try {
      Class<Charset> charSet = Charset.class;
      Field defaultCharsetFld = charSet.getDeclaredField("defaultCharset");
      defaultCharsetFld.setAccessible(true);
      defaultCharsetFld.set(null, Charset.forName(fiddleValue));
    } catch (Exception ex) {
      System.out.println("*** Fiddling with Charset class failed");
    }
  }

  @Before
  public void setup() throws FileSystemException {
    fs = VFS.getManager();
    filesPath = '/' + this.getClass().getPackage().getName().replace('.', '/') + "/files/";

    pipelineName = "LoadFileInput";
    pipelineMeta = new PipelineMeta();
    pipelineMeta.setName(pipelineName);
    pipeline = new LocalPipelineEngine(pipelineMeta);

    loadFileInputMeta = spy(new LoadFileInputMeta());
    fileInputList = new FileInputList();
    Mockito.doReturn(fileInputList)
        .when(loadFileInputMeta)
        .getFiles(any(IVariables.class));
    String transformId =
        PluginRegistry.getInstance().getPluginId(TransformPluginType.class, loadFileInputMeta);
    TransformMeta transformMeta = new TransformMeta(transformId, "Load File Input", loadFileInputMeta);
    pipelineMeta.addTransform(transformMeta);

    loadFileInputData = new LoadFileInputData();

    int transformCopyNr = 0;

    loadFileInput =
        new LoadFileInput(
            transformMeta,
            loadFileInputMeta,
            loadFileInputData,
            transformCopyNr,
            pipelineMeta,
            pipeline);

    assertSame(loadFileInputMeta, transformMeta.getTransform());

    loadFileInputField = new LoadFileInputField();
    loadFileInputMeta.setInputFields(new LoadFileInputField[] {loadFileInputField});
    loadFileInput.init();
  }

  private FileObject getFile(final String filename) {
    try {
      return fs.resolveFile(this.getClass().getResource(filesPath + filename));
    } catch (Exception e) {
      throw new RuntimeException("fail. " + e.getMessage(), e);
    }
  }

  @Test
  public void testOpenNextFile_noFiles() {
    assertFalse(loadFileInputMeta.isIgnoreEmptyFile()); // ensure default value

    assertFalse(loadFileInput.openNextFile());
  }

  @Test
  public void testOpenNextFile_noFiles_ignoreEmpty() {
    loadFileInputMeta.setIgnoreEmptyFile(true);

    assertFalse(loadFileInput.openNextFile());
  }

  @Test
  public void testOpenNextFile_0() {
    assertFalse(loadFileInputMeta.isIgnoreEmptyFile()); // ensure default value

    fileInputList.addFile(getFile("input0.txt"));

    assertTrue(loadFileInput.openNextFile());
    assertFalse(loadFileInput.openNextFile());
  }

  @Test
  public void testOpenNextFile_0_ignoreEmpty() {
    loadFileInputMeta.setIgnoreEmptyFile(true);

    fileInputList.addFile(getFile("input0.txt"));

    assertFalse(loadFileInput.openNextFile());
  }

  @Test
  public void testOpenNextFile_000() {
    assertFalse(loadFileInputMeta.isIgnoreEmptyFile()); // ensure default value

    fileInputList.addFile(getFile("input0.txt"));
    fileInputList.addFile(getFile("input0.txt"));
    fileInputList.addFile(getFile("input0.txt"));

    assertTrue(loadFileInput.openNextFile());
    assertTrue(loadFileInput.openNextFile());
    assertTrue(loadFileInput.openNextFile());
    assertFalse(loadFileInput.openNextFile());
  }

  @Test
  public void testOpenNextFile_000_ignoreEmpty() {
    loadFileInputMeta.setIgnoreEmptyFile(true);

    fileInputList.addFile(getFile("input0.txt"));
    fileInputList.addFile(getFile("input0.txt"));
    fileInputList.addFile(getFile("input0.txt"));

    assertFalse(loadFileInput.openNextFile());
  }

  @Test
  public void testOpenNextFile_10() {
    assertFalse(loadFileInputMeta.isIgnoreEmptyFile()); // ensure default value

    fileInputList.addFile(getFile("input1.txt"));
    fileInputList.addFile(getFile("input0.txt"));

    assertTrue(loadFileInput.openNextFile());
    assertTrue(loadFileInput.openNextFile());
    assertFalse(loadFileInput.openNextFile());
  }

  @Test
  public void testOpenNextFile_10_ignoreEmpty() {
    loadFileInputMeta.setIgnoreEmptyFile(true);

    fileInputList.addFile(getFile("input1.txt"));
    fileInputList.addFile(getFile("input0.txt"));

    assertTrue(loadFileInput.openNextFile());
    assertFalse(loadFileInput.openNextFile());
  }

  @Test
  public void testOpenNextFile_01() {
    assertFalse(loadFileInputMeta.isIgnoreEmptyFile()); // ensure default value

    fileInputList.addFile(getFile("input0.txt"));
    fileInputList.addFile(getFile("input1.txt"));

    assertTrue(loadFileInput.openNextFile());
    assertTrue(loadFileInput.openNextFile());
    assertFalse(loadFileInput.openNextFile());
  }

  @Test
  public void testOpenNextFile_01_ignoreEmpty() {
    loadFileInputMeta.setIgnoreEmptyFile(true);

    fileInputList.addFile(getFile("input0.txt"));
    fileInputList.addFile(getFile("input1.txt"));

    assertTrue(loadFileInput.openNextFile());
    assertFalse(loadFileInput.openNextFile());
  }

  @Test
  public void testOpenNextFile_010() {
    assertFalse(loadFileInputMeta.isIgnoreEmptyFile()); // ensure default value

    fileInputList.addFile(getFile("input0.txt"));
    fileInputList.addFile(getFile("input1.txt"));
    fileInputList.addFile(getFile("input0.txt"));

    assertTrue(loadFileInput.openNextFile());
    assertTrue(loadFileInput.openNextFile());
    assertTrue(loadFileInput.openNextFile());
    assertFalse(loadFileInput.openNextFile());
  }

  @Test
  public void testOpenNextFile_010_ignoreEmpty() {
    loadFileInputMeta.setIgnoreEmptyFile(true);

    fileInputList.addFile(getFile("input0.txt"));
    fileInputList.addFile(getFile("input1.txt"));
    fileInputList.addFile(getFile("input0.txt"));

    assertTrue(loadFileInput.openNextFile());
    assertFalse(loadFileInput.openNextFile());
  }

  @Test
  public void testGetOneRow() throws Exception {
    // string without specified encoding
    fileInputList.addFile(getFile("input1.txt"));

    assertNotNull(loadFileInput.getOneRow());
    assertEquals("input1 - not empty", new String(loadFileInput.getData().filecontent));
  }

  @Test
  public void testUTF8Encoding() throws HopException, FileSystemException {
    loadFileInputMeta.setIncludeFilename(true);
    loadFileInputMeta.setFilenameField("filename");
    loadFileInputMeta.setIncludeRowNumber(true);
    loadFileInputMeta.setRowNumberField("rownumber");
    loadFileInputMeta.setShortFileNameField("shortname");
    loadFileInputMeta.setExtensionField("extension");
    loadFileInputMeta.setPathField("path");
    loadFileInputMeta.setIsHiddenField("hidden");
    loadFileInputMeta.setLastModificationDateField("lastmodified");
    loadFileInputMeta.setUriField("uri");
    loadFileInputMeta.setRootUriField("root uri");

    // string with UTF-8 encoding
    loadFileInputMeta.setEncoding("UTF-8");
    fileInputList.addFile(getFile("UTF-8.txt"));
    Object[] result = loadFileInput.getOneRow();
    assertEquals(" UTF-8 string ÕÕÕ€ ", result[0]);
    assertEquals(1L, result[2]);
    assertEquals("UTF-8.txt", result[3]);
    assertEquals("txt", result[4]);
    assertEquals(false, result[6]);
    assertEquals(getFile("UTF-8.txt").getURL().toString(), result[8]);
    assertEquals(getFile("UTF-8.txt").getName().getRootURI(), result[9]);
  }

  @Test
  public void testUTF8TrimLeft() throws HopException {
    loadFileInputMeta.setEncoding("UTF-8");
    loadFileInputField.setTrimType(IValueMeta.TRIM_TYPE_LEFT);
    fileInputList.addFile(getFile("UTF-8.txt"));
    assertEquals("UTF-8 string ÕÕÕ€ ", loadFileInput.getOneRow()[0]);
  }

  @Test
  public void testUTF8TrimRight() throws HopException {
    loadFileInputMeta.setEncoding("UTF-8");
    loadFileInputField.setTrimType(IValueMeta.TRIM_TYPE_RIGHT);
    fileInputList.addFile(getFile("UTF-8.txt"));
    assertEquals(" UTF-8 string ÕÕÕ€", loadFileInput.getOneRow()[0]);
  }

  @Test
  public void testUTF8Trim() throws HopException {
    loadFileInputMeta.setEncoding("UTF-8");
    loadFileInputField.setTrimType(IValueMeta.TRIM_TYPE_BOTH);
    fileInputList.addFile(getFile("UTF-8.txt"));
    assertEquals("UTF-8 string ÕÕÕ€", loadFileInput.getOneRow()[0]);
  }

  @Test
  public void testWindowsEncoding() throws HopException {
    loadFileInputMeta.setEncoding("Windows-1252");
    loadFileInputField.setTrimType(IValueMeta.TRIM_TYPE_NONE);
    fileInputList.addFile(getFile("Windows-1252.txt"));
    assertEquals(" Windows-1252 string ÕÕÕ€ ", loadFileInput.getOneRow()[0]);
  }

  @Test
  public void testWithNoEncoding() throws HopException, UnsupportedEncodingException {
    // string with Windows-1252 encoding but with no encoding set
    loadFileInputMeta.setEncoding(null);
    fileInputList.addFile(getFile("Windows-1252.txt"));
    assertNotEquals(" Windows-1252 string ÕÕÕ€ ", loadFileInput.getOneRow()[0]);
    assertEquals(
        " Windows-1252 string ÕÕÕ€ ",
        new String(loadFileInput.getData().filecontent, "Windows-1252"));
  }

  @Test
  public void testByteArray() throws Exception {
    IRowMeta mockedRowMetaInterface = mock(IRowMeta.class);
    loadFileInput.getData().outputRowMeta = mockedRowMetaInterface;
    loadFileInput.getData().convertRowMeta = mockedRowMetaInterface;
    Mockito.doReturn(new ValueMetaString()).when(mockedRowMetaInterface).getValueMeta(anyInt());

    // byte array
    Mockito.doReturn(new ValueMetaBinary()).when(mockedRowMetaInterface).getValueMeta(anyInt());
    loadFileInputMeta.setEncoding("UTF-8");
    fileInputList.addFile(getFile("hop.jpg"));
    loadFileInputField = new LoadFileInputField();
    loadFileInputField.setType(IValueMeta.TYPE_BINARY);
    loadFileInputMeta.setInputFields(new LoadFileInputField[] {loadFileInputField});

    assertNotNull(loadFileInput.getOneRow());
    assertArrayEquals(
        IOUtils.toByteArray(getFile("hop.jpg").getContent().getInputStream()),
        loadFileInput.getData().filecontent);
  }

  @Test
  public void testCopyOrCloneArrayFromLoadFileWithSmallerSizedReadRowArray() {
    int size = 5;
    Object[] rowData = new Object[size];
    Object[] readrow = new Object[size - 1];
    LoadFileInput loadFileInput = mock(LoadFileInput.class);

    Mockito.when(loadFileInput.copyOrCloneArrayFromLoadFile(rowData, readrow)).thenCallRealMethod();

    assertEquals(5, loadFileInput.copyOrCloneArrayFromLoadFile(rowData, readrow).length);
  }

  @Test
  public void testCopyOrCloneArrayFromLoadFileWithBiggerSizedReadRowArray() {
    int size = 5;
    Object[] rowData = new Object[size];
    Object[] readrow = new Object[size + 1];
    LoadFileInput loadFileInput = mock(LoadFileInput.class);

    Mockito.when(loadFileInput.copyOrCloneArrayFromLoadFile(rowData, readrow)).thenCallRealMethod();

    assertEquals(6, loadFileInput.copyOrCloneArrayFromLoadFile(rowData, readrow).length);
  }

  @Test
  public void testCopyOrCloneArrayFromLoadFileWithSameSizedReadRowArray() {
    int size = 5;
    Object[] rowData = new Object[size];
    Object[] readrow = new Object[size];
    LoadFileInput loadFileInput = mock(LoadFileInput.class);

    Mockito.when(loadFileInput.copyOrCloneArrayFromLoadFile(rowData, readrow)).thenCallRealMethod();

    assertEquals(5, loadFileInput.copyOrCloneArrayFromLoadFile(rowData, readrow).length);
  }
}
