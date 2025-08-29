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

package org.apache.hop.pipeline.transforms.excelwriter;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.common.io.Files;
import java.io.File;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import org.apache.commons.vfs2.FileObject;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.logging.ILoggingObject;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.core.row.value.ValueMetaBigNumber;
import org.apache.hop.core.row.value.ValueMetaBinary;
import org.apache.hop.core.row.value.ValueMetaDate;
import org.apache.hop.core.row.value.ValueMetaInteger;
import org.apache.hop.core.row.value.ValueMetaInternetAddress;
import org.apache.hop.core.row.value.ValueMetaNumber;
import org.apache.hop.core.row.value.ValueMetaString;
import org.apache.hop.core.row.value.ValueMetaTimestamp;
import org.apache.hop.pipeline.transforms.mock.TransformMockHelper;
import org.apache.hop.utils.TestUtils;
import org.apache.poi.hssf.usermodel.HSSFSheet;
import org.apache.poi.hssf.usermodel.HSSFWorkbook;
import org.apache.poi.ss.usermodel.Row;
import org.apache.poi.ss.usermodel.Workbook;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

public class ExcelWriterTransformTest {

  private static final String SHEET_NAME = "Sheet1";
  private static final String XLS = "xls";
  private static final String DOT_XLS = '.' + XLS;
  private static final String XLSX = "xlsx";
  private static final String DOT_XLSX = '.' + XLSX;
  private static final String EMPTY_STRING = "";

  private Workbook wb;
  private TransformMockHelper<ExcelWriterTransformMeta, ExcelWriterTransformData> mockHelper;
  private ExcelWriterTransform transform;

  private ExcelWriterTransformMeta metaMock;
  private ExcelWriterTransformData dataMock;
  ExcelWriterFileField fieldMock;

  private File templateFile;

  @BeforeEach
  public void setUp() throws Exception {
    String path = TestUtils.createRamFile(getClass().getSimpleName() + "/testXLSProtect.xls");
    FileObject xlsFile = TestUtils.getFileObject(path);
    wb = createWorkbook(xlsFile);
    mockHelper =
        new TransformMockHelper<>(
            "Excel Writer Test", ExcelWriterTransformMeta.class, ExcelWriterTransformData.class);
    when(mockHelper.logChannelFactory.create(any(), any(ILoggingObject.class)))
        .thenReturn(mockHelper.iLogChannel);

    metaMock = mock(ExcelWriterTransformMeta.class);

    fieldMock = mock(ExcelWriterFileField.class);
    doReturn(fieldMock).when(metaMock).getFile();

    ExcelWriterTemplateField templateMock = mock(ExcelWriterTemplateField.class);
    doReturn(templateMock).when(metaMock).getTemplate();

    dataMock = new ExcelWriterTransformData();
    ExcelWriterWorkbookDefinition workbookDefinition =
        new ExcelWriterWorkbookDefinition("string", null, null, null, 0, 0);
    dataMock.currentWorkbookDefinition = workbookDefinition;
    dataMock.usedFiles.add(workbookDefinition);

    transform =
        spy(
            new ExcelWriterTransform(
                mockHelper.transformMeta,
                metaMock,
                dataMock,
                0,
                mockHelper.pipelineMeta,
                mockHelper.pipeline));

    assertTrue(transform.init());
  }

  @AfterEach
  public void cleanUp() {
    mockHelper.cleanUp();
  }

  @Test
  public void testProtectSheet() throws Exception {

    transform.protectSheet(wb.getSheet(SHEET_NAME), "aa");
    assertTrue(wb.getSheet(SHEET_NAME).getProtect());
  }

  @Test
  public void testMaxSheetNameLength() {

    transform =
        spy(
            new ExcelWriterTransform(
                mockHelper.transformMeta,
                metaMock,
                dataMock,
                0,
                mockHelper.pipelineMeta,
                mockHelper.pipeline));

    // Return a 32 character name
    when(metaMock.getFile().getSheetname()).thenReturn("12345678901234567890123456789012");

    transform.init();

    try {
      transform.prepareNextOutputFile(any(Object[].class));
      // An exception should have been thrown!
      fail();
    } catch (HopException e) {
      String content = e.getMessage();

      // We expected this error message, the sheet name is too long for Excel
      assertTrue(content.contains("12345678901234567890123456789012"));
    }
  }

  @Test
  public void testPrepareNextOutputFile() throws Exception {
    assertTrue(transform.init());
    File outDir = Files.createTempDir();
    String testFileOut = outDir.getAbsolutePath() + File.separator + "test.xlsx";
    when(transform.buildFilename(0)).thenReturn(testFileOut);
    when(metaMock.getTemplate().isTemplateEnabled()).thenReturn(true);
    when(metaMock.getFile().isStreamingData()).thenReturn(true);
    when(metaMock.isHeaderEnabled()).thenReturn(true);
    when(metaMock.getFile().getExtension()).thenReturn(XLSX);
    dataMock.createNewFile = true;
    dataMock.realTemplateFileName = getClass().getResource("template_test.xlsx").getFile();
    dataMock.realSheetname = SHEET_NAME;

    transform.prepareNextOutputFile(any(Object[].class));
  }

  @Test
  public void testWriteUsingTemplateWithFormatting() throws Exception {

    String path = Files.createTempDir().getAbsolutePath() + File.separator + "formatted.xlsx";

    dataMock.fieldnrs = new int[] {0};
    dataMock.linkfieldnrs = new int[] {-1};
    dataMock.commentfieldnrs = new int[] {-1};
    dataMock.createNewFile = true;
    dataMock.realTemplateFileName =
        getClass().getResource("template_with_formatting.xlsx").getFile();
    dataMock.realSheetname = "TicketData";
    dataMock.inputRowMeta = mock(IRowMeta.class);

    List<ExcelWriterOutputField> fields = new ArrayList<>();
    fields.add(new ExcelWriterOutputField());

    IValueMeta vmi = mock(ValueMetaInteger.class);
    when(vmi.getType()).thenReturn(IValueMeta.TYPE_INTEGER);
    when(vmi.getName()).thenReturn("name");
    when(vmi.getNumber(any())).thenReturn(12.0);

    when(metaMock.getTemplate().isTemplateEnabled()).thenReturn(true);
    when(metaMock.getFile().isStreamingData()).thenReturn(false);
    when(metaMock.isHeaderEnabled()).thenReturn(false);
    when(metaMock.getFile().getExtension()).thenReturn(XLSX);
    when(metaMock.getOutputFields()).thenReturn(fields);

    when(dataMock.inputRowMeta.size()).thenReturn(10);
    when(dataMock.inputRowMeta.getValueMeta(anyInt())).thenReturn(vmi);

    when(transform.buildFilename(0)).thenReturn(path);
    dataMock.usedFiles.add(dataMock.currentWorkbookDefinition);
    transform.prepareNextOutputFile(any(Object[].class));

    dataMock.currentWorkbookDefinition.setPosY(1);
    dataMock.currentWorkbookDefinition.setSheet(spy(dataMock.currentWorkbookDefinition.getSheet()));
    transform.writeNextLine(dataMock.currentWorkbookDefinition, new Object[] {12});

    verify(dataMock.currentWorkbookDefinition.getSheet(), times(0)).createRow(1);
    verify(dataMock.currentWorkbookDefinition.getSheet()).getRow(1);
  }

  @Test
  public void testWriteUsingTemplateWithFormatting_Streaming() throws Exception {

    String path =
        Files.createTempDir().getAbsolutePath() + File.separator + "formatted_streaming.xlsx";

    dataMock.fieldnrs = new int[] {0};
    dataMock.linkfieldnrs = new int[] {-1};
    dataMock.commentfieldnrs = new int[] {-1};
    dataMock.createNewFile = true;
    dataMock.realTemplateFileName =
        getClass().getResource("template_with_formatting_streaming.xlsx").getFile();
    dataMock.realSheetname = "Data";
    dataMock.inputRowMeta = mock(IRowMeta.class);

    List<ExcelWriterOutputField> fields = new ArrayList<>();
    fields.add(new ExcelWriterOutputField());

    IValueMeta vmi = mock(ValueMetaInteger.class);
    when(vmi.getType()).thenReturn(IValueMeta.TYPE_INTEGER);
    when(vmi.getName()).thenReturn("name");
    when(vmi.getNumber(any())).thenReturn(12.0);

    when(metaMock.getTemplate().isTemplateEnabled()).thenReturn(true);
    when(metaMock.getFile().isStreamingData()).thenReturn(true);
    when(metaMock.getStartingCell()).thenReturn("A2");

    when(metaMock.isHeaderEnabled()).thenReturn(false);
    when(metaMock.getFile().getExtension()).thenReturn(XLSX);
    when(metaMock.getOutputFields()).thenReturn(fields);

    when(dataMock.inputRowMeta.size()).thenReturn(10);
    when(dataMock.inputRowMeta.getValueMeta(anyInt())).thenReturn(vmi);

    when(transform.buildFilename(0)).thenReturn(path);
    dataMock.usedFiles.add(dataMock.currentWorkbookDefinition);
    transform.prepareNextOutputFile(any(Object[].class));

    dataMock.currentWorkbookDefinition.setPosY(1);
    dataMock.currentWorkbookDefinition.setSheet(spy(dataMock.currentWorkbookDefinition.getSheet()));
    transform.writeNextLine(dataMock.currentWorkbookDefinition, new Object[] {12});

    verify(dataMock.currentWorkbookDefinition.getSheet(), times(1)).createRow(1);
    verify(dataMock.currentWorkbookDefinition.getSheet()).getRow(1);
  }

  @Test
  public void testValueBigNumber() throws Exception {

    IValueMeta vmi = mock(ValueMetaBigNumber.class, new DefaultAnswerThrowsException());
    Object vObj = new Object();
    doReturn(IValueMeta.TYPE_BIGNUMBER).when(vmi).getType();
    doReturn("value_bigNumber").when(vmi).getName();
    doReturn(Double.MAX_VALUE).when(vmi).getNumber(any());

    testBaseXlsx(vmi, vObj, false, false);
  }

  @Test
  public void testValueBinary() throws Exception {

    IValueMeta vmi = mock(ValueMetaBinary.class, new DefaultAnswerThrowsException());
    Object vObj = new Object();
    doReturn(IValueMeta.TYPE_BINARY).when(vmi).getType();
    doReturn("value_binary").when(vmi).getName();
    doReturn("a1b2c3d4e5f6g7h8i9j0").when(vmi).getString(any());

    testBaseXlsx(vmi, vObj, false, false);
  }

  @Test
  public void testValueBoolean() throws Exception {

    IValueMeta vmi = mock(ValueMetaInteger.class, new DefaultAnswerThrowsException());
    Object vObj = new Object();
    doReturn(IValueMeta.TYPE_BOOLEAN).when(vmi).getType();
    doReturn("value_bool").when(vmi).getName();
    doReturn(Boolean.FALSE).when(vmi).getBoolean(any());

    testBaseXlsx(vmi, vObj, false, false);
  }

  @Test
  public void testValueDate() throws Exception {

    IValueMeta vmi = mock(ValueMetaDate.class);
    Object vObj = new Object();
    doReturn(IValueMeta.TYPE_DATE).when(vmi).getType();
    doReturn("value_date").when(vmi).getName();
    doReturn(new Date()).when(vmi).getDate(any());

    testBaseXlsx(vmi, vObj, false, false);
  }

  @Test
  public void testValueInteger() throws Exception {

    IValueMeta vmi = mock(ValueMetaInteger.class, new DefaultAnswerThrowsException());
    Object vObj = new Object();
    doReturn(IValueMeta.TYPE_INTEGER).when(vmi).getType();
    doReturn("value_integer").when(vmi).getName();
    doReturn(Double.MAX_VALUE).when(vmi).getNumber(any());

    testBaseXlsx(vmi, vObj, false, false);
  }

  @Test
  public void testValueInternetAddress() throws Exception {

    IValueMeta vmi = mock(ValueMetaInternetAddress.class, new DefaultAnswerThrowsException());
    Object vObj = new Object();
    doReturn(IValueMeta.TYPE_INET).when(vmi).getType();
    doReturn("value_internetAddress").when(vmi).getName();
    doReturn("127.0.0.1").when(vmi).getString(any());

    testBaseXlsx(vmi, vObj, false, false);
  }

  @Test
  public void testValueNumber() throws Exception {

    IValueMeta vmi = mock(ValueMetaNumber.class, new DefaultAnswerThrowsException());
    Object vObj = new Object();
    doReturn(IValueMeta.TYPE_NUMBER).when(vmi).getType();
    doReturn("value_number").when(vmi).getName();
    doReturn(Double.MIN_VALUE).when(vmi).getNumber(any());

    testBaseXlsx(vmi, vObj, false, false);
  }

  @Test
  public void testValueString() throws Exception {

    IValueMeta vmi = mock(ValueMetaString.class, new DefaultAnswerThrowsException());
    Object vObj = new Object();
    doReturn(IValueMeta.TYPE_STRING).when(vmi).getType();
    doReturn("value_string").when(vmi).getName();
    doReturn("a_string").when(vmi).getString(any());

    testBaseXlsx(vmi, vObj, false, false);
  }

  @Test
  public void testValueTimestamp() throws Exception {

    IValueMeta vmi = mock(ValueMetaTimestamp.class);
    Object vObj = new Object();
    doReturn(IValueMeta.TYPE_TIMESTAMP).when(vmi).getType();
    doReturn("value_timestamp").when(vmi).getName();
    doReturn("127.0.0.1").when(vmi).getString(vObj);

    testBaseXlsx(vmi, vObj, false, false);
  }

  @Test
  public void test_Xlsx_Stream_NoTemplate() throws Exception {

    IValueMeta vmi = mock(ValueMetaInternetAddress.class, new DefaultAnswerThrowsException());
    Object vObj = new Object();
    doReturn(IValueMeta.TYPE_INET).when(vmi).getType();
    doReturn("value_internetAddress").when(vmi).getName();
    doReturn("127.0.0.1").when(vmi).getString(vObj);

    testBaseXlsx(vmi, vObj, true, false);
  }

  @Test
  public void test_Xlsx_NoStream_NoTemplate() throws Exception {

    IValueMeta vmi = mock(ValueMetaInternetAddress.class, new DefaultAnswerThrowsException());
    Object vObj = new Object();
    doReturn(IValueMeta.TYPE_INET).when(vmi).getType();
    doReturn("value_internetAddress").when(vmi).getName();
    doReturn("127.0.0.1").when(vmi).getString(vObj);

    testBaseXlsx(vmi, vObj, false, false);
  }

  @Test
  public void test_Xlsx_Stream_Template() throws Exception {

    IValueMeta vmi = mock(ValueMetaInternetAddress.class, new DefaultAnswerThrowsException());
    Object vObj = new Object();
    doReturn(IValueMeta.TYPE_INET).when(vmi).getType();
    doReturn("value_internetAddress").when(vmi).getName();
    doReturn("127.0.0.1").when(vmi).getString(vObj);

    testBaseXlsx(vmi, vObj, true, true);
  }

  @Test
  public void test_Xlsx_NoStream_Template() throws Exception {

    IValueMeta vmi = mock(ValueMetaInternetAddress.class, new DefaultAnswerThrowsException());
    Object vObj = new Object();
    doReturn(IValueMeta.TYPE_INET).when(vmi).getType();
    doReturn("value_internetAddress").when(vmi).getName();
    doReturn("127.0.0.1").when(vmi).getString(vObj);

    testBaseXlsx(vmi, vObj, false, true);
  }

  @Test
  public void test_Xls_NoTemplate() throws Exception {

    IValueMeta vmi = mock(ValueMetaTimestamp.class, new DefaultAnswerThrowsException());
    Object vObj = new Object();
    doReturn(IValueMeta.TYPE_INET).when(vmi).getType();
    doReturn("value_internetAddress").when(vmi).getName();
    doReturn("127.0.0.1").when(vmi).getString(vObj);

    testBaseXls(vmi, vObj, false);
  }

  @Test
  public void test_Xls_Template() throws Exception {

    IValueMeta vmi = mock(ValueMetaTimestamp.class, new DefaultAnswerThrowsException());
    Object vObj = new Object();
    doReturn(IValueMeta.TYPE_INET).when(vmi).getType();
    doReturn("value_internetAddress").when(vmi).getName();
    doReturn("127.0.0.1").when(vmi).getString(vObj);

    testBaseXls(vmi, vObj, true);
  }

  /**
   * The base for testing if a field of a specific type is correctly handled for an XLSX.
   *
   * @param vmi {@link IValueMeta}'s instance to be used
   * @param vObj the {@link Object} to be used as the value
   * @param isStreaming if it's to use streaming
   * @param isTemplateEnabled if it's to use a template
   */
  private void testBaseXlsx(
      IValueMeta vmi, Object vObj, boolean isStreaming, boolean isTemplateEnabled)
      throws Exception {
    testBase(vmi, vObj, XLSX, DOT_XLSX, isStreaming, isTemplateEnabled);
  }

  /**
   * The base for testing if a field of a specific type is correctly handled for an XLS.
   *
   * @param vmi {@link IValueMeta}'s instance to be used
   * @param vObj the {@link Object} to be used as the value
   * @param isTemplateEnabled if it's to use a template
   */
  private void testBaseXls(IValueMeta vmi, Object vObj, boolean isTemplateEnabled)
      throws Exception {

    testBase(vmi, vObj, XLS, DOT_XLS, false, isTemplateEnabled);
  }

  /**
   * The base for testing if a field of a specific type is correctly handled.
   *
   * @param vmi {@link IValueMeta}'s instance to be used
   * @param vObj the {@link Object} to be used as the value
   * @param extension the extension to be used
   * @param isStreaming if it's to use streaming
   * @param isTemplateEnabled if it's to use a template
   */
  private void testBase(
      IValueMeta vmi,
      Object vObj,
      String extension,
      String dotExtension,
      boolean isStreaming,
      boolean isTemplateEnabled)
      throws Exception {

    Object[] vObjArr = {vObj};
    assertTrue(transform.init());
    File tempFile = File.createTempFile(extension, dotExtension);
    tempFile.deleteOnExit();
    String path = tempFile.getAbsolutePath();

    if (isTemplateEnabled) {
      dataMock.realTemplateFileName =
          getClass().getResource("template_test" + dotExtension).getFile();
    }

    dataMock.fieldnrs = new int[] {0};
    dataMock.linkfieldnrs = new int[] {-1};
    dataMock.commentfieldnrs = new int[] {-1};
    dataMock.createNewFile = true;
    dataMock.realSheetname = SHEET_NAME;
    dataMock.inputRowMeta = mock(IRowMeta.class);

    when(transform.buildFilename(0)).thenReturn(path);
    when(metaMock.getTemplate().isTemplateEnabled()).thenReturn(isTemplateEnabled);
    when(metaMock.getFile().isStreamingData()).thenReturn(isStreaming);
    when(metaMock.isHeaderEnabled()).thenReturn(false);
    when(metaMock.getFile().getExtension()).thenReturn(extension);
    List<ExcelWriterOutputField> fields = new ArrayList<>();
    fields.add(new ExcelWriterOutputField());
    doReturn(fields).when(metaMock).getOutputFields();

    when(dataMock.inputRowMeta.size()).thenReturn(1);
    when(dataMock.inputRowMeta.getValueMeta(anyInt())).thenReturn(vmi);

    transform.prepareNextOutputFile(any(Object[].class));

    assertNull(dataMock.currentWorkbookDefinition.getSheet().getRow(1));

    // Unfortunately HSSFSheet is final and cannot be mocked, so we'll skip some validations
    dataMock.currentWorkbookDefinition.setPosY(1);
    if (null != dataMock.currentWorkbookDefinition.getSheet()
        && !(dataMock.currentWorkbookDefinition.getSheet() instanceof HSSFSheet)) {
      dataMock.currentWorkbookDefinition.setSheet(
          spy(dataMock.currentWorkbookDefinition.getSheet()));
    }

    transform.writeNextLine(dataMock.currentWorkbookDefinition, vObjArr);

    if (null != dataMock.currentWorkbookDefinition.getSheet()
        && !(dataMock.currentWorkbookDefinition.getSheet() instanceof HSSFSheet)) {
      verify(transform)
          .writeField(
              eq(dataMock.currentWorkbookDefinition),
              eq(vObj),
              eq(vmi),
              eq(fields.get(0)),
              any(Row.class),
              eq(0),
              any(),
              eq(0),
              eq(Boolean.FALSE));

      verify(dataMock.currentWorkbookDefinition.getSheet()).createRow(anyInt());
      verify(dataMock.currentWorkbookDefinition.getSheet()).getRow(1);
    }

    assertNotNull(dataMock.currentWorkbookDefinition.getSheet().getRow(1));
  }

  /**
   * Class to be used when mocking an Object so that, if not explicitly specified, any method called
   * will throw an exception.
   */
  private static class DefaultAnswerThrowsException implements Answer<Object> {
    @Override
    public Object answer(InvocationOnMock invocation) throws Throwable {
      throw new RuntimeException(
          "This method (" + invocation.getMethod() + ") shouldn't have been called.");
    }
  }

  private Workbook createWorkbook(FileObject file) throws Exception {
    Workbook wb = null;
    OutputStream os = null;
    try {
      os = file.getContent().getOutputStream();
      wb = new HSSFWorkbook();
      wb.createSheet(SHEET_NAME);
      wb.write(os);
    } finally {
      os.flush();
      os.close();
    }
    return wb;
  }
}
