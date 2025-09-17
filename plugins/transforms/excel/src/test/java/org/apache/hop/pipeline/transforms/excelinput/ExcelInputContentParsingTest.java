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

package org.apache.hop.pipeline.transforms.excelinput;

import static org.junit.jupiter.api.Assertions.assertEquals;

import org.apache.hop.core.Const;
import org.apache.hop.junit.rules.RestoreHopEngineEnvironmentExtension;
import org.apache.poi.openxml4j.util.ZipSecureFile;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

class ExcelInputContentParsingTest extends BaseExcelParsingTest {
  @RegisterExtension
  static RestoreHopEngineEnvironmentExtension env = new RestoreHopEngineEnvironmentExtension();

  private static final String[] CNST_3_SHEET_NAME_ARRAY = {"Sheet1", "Sheet2", "Sheet3"};
  private static final String[] CNST_1_SHEET_NAME_ARRAY = {"Sheet1"};
  private static final int[] CNST_3_SHEET_START_ROW_ARRAY = {23, 3, 7};
  private static final int[] CNST_3_ZERO_INT_ARRAY = {0, 0, 0};
  private static final int[] CNST_1_ZERO_INT_ARRAY = {0};
  private static final int TEST_ROW_LIMIT_SINGLE_SHEET = 10;
  private static final int TEST_ROW_LIMIT_MULTIPLE_SHEET = 20;

  @Override
  @BeforeEach
  void before() {
    super.before();

    System.clearProperty(Const.HOP_ZIP_MAX_ENTRY_SIZE);
    System.clearProperty(Const.HOP_ZIP_MAX_TEXT_SIZE);
    System.clearProperty(Const.HOP_ZIP_MIN_INFLATE_RATIO);
  }

  @Test
  void testXLS() throws Exception {
    meta.setSpreadSheetType(SpreadSheetType.POI);
    init("sample.xls");

    setFields(new ExcelInputField("f1", -1, -1), new ExcelInputField("f2", -1, -1));

    process();

    check(new Object[][] {{"test", null}, {"test", "test"}});
  }

  @Test
  void testXLSX() throws Exception {
    meta.setSpreadSheetType(SpreadSheetType.POI);
    init("sample.xlsx");

    setFields(new ExcelInputField("f1", -1, -1), new ExcelInputField("f2", -1, -1));

    process();

    check(new Object[][] {{"test", null}, {"test", "test"}});
  }

  @Test
  void testXLSXStream() throws Exception {
    meta.setSpreadSheetType(SpreadSheetType.SAX_POI);
    init("sample.xlsx");

    setFields(new ExcelInputField("f1", -1, -1), new ExcelInputField("f2", -1, -1));

    process();

    check(new Object[][] {{"test", null}, {"test", "test"}});
  }

  @Test
  void testODS24() throws Exception {
    meta.setSpreadSheetType(SpreadSheetType.ODS);
    init("sample-2.4.ods");

    setFields(new ExcelInputField("f1", -1, -1), new ExcelInputField("f2", -1, -1));

    process();

    check(new Object[][] {{"test", null}, {"test", "test"}});
  }

  @Test
  void testODS341() throws Exception {
    meta.setSpreadSheetType(SpreadSheetType.ODS);
    init("sample-3.4.1.ods");

    setFields(new ExcelInputField("f1", -1, -1), new ExcelInputField("f2", -1, -1));

    process();

    check(
        new Object[][] {
          {"AAABBC", "Nissan"}, {"AAABBC", "Nissan"}, {"AAABBC", "Nissan"}, {"AAABBC", "Nissan"}
        });
  }

  @Test
  void testZipBombConfiguration_Default() throws Exception {

    // First set some random values
    Long bogusMaxEntrySize = 1000L;
    ZipSecureFile.setMaxEntrySize(bogusMaxEntrySize);
    Long bogusMaxTextSize = 1000L;
    ZipSecureFile.setMaxTextSize(bogusMaxTextSize);
    Double bogusMinInflateRatio = 0.5d;
    ZipSecureFile.setMinInflateRatio(bogusMinInflateRatio);

    // Verify that the bogus values were set
    assertEquals(bogusMaxEntrySize, (Long) ZipSecureFile.getMaxEntrySize());
    assertEquals(bogusMaxTextSize, (Long) ZipSecureFile.getMaxTextSize());
    assertEquals(bogusMinInflateRatio, (Double) ZipSecureFile.getMinInflateRatio());

    // Initializing the ExcelInput transform should make the new values to be set
    meta.setSpreadSheetType(SpreadSheetType.SAX_POI);
    init("Balance_Type_Codes.xlsx");

    // Verify that the default values were used
    assertEquals(Const.HOP_ZIP_MAX_ENTRY_SIZE_DEFAULT, (Long) ZipSecureFile.getMaxEntrySize());
    assertEquals(Const.HOP_ZIP_MAX_TEXT_SIZE_DEFAULT, (Long) ZipSecureFile.getMaxTextSize());
    assertEquals(
        Const.HOP_ZIP_MIN_INFLATE_RATIO_DEFAULT, (Double) ZipSecureFile.getMinInflateRatio());
  }

  @Test
  void testZipBombConfiguration() throws Exception {
    Long maxEntrySizeVal = 3L * 1024 * 1024 * 1024;
    Long maxTextSizeVal = 2L * 1024 * 1024 * 1024;
    Double minInflateRatioVal = 0.123d;

    // First set the property values
    System.setProperty(Const.HOP_ZIP_MAX_ENTRY_SIZE, maxEntrySizeVal.toString());
    System.setProperty(Const.HOP_ZIP_MAX_TEXT_SIZE, maxTextSizeVal.toString());
    System.setProperty(Const.HOP_ZIP_MIN_INFLATE_RATIO, minInflateRatioVal.toString());
    // ExcelInput excelInput = new ExcelInput( null, null, 0, null, null );

    // Initializing the ExcelInput transform should make the new values to be set

    meta.setSpreadSheetType(SpreadSheetType.SAX_POI);
    init("Balance_Type_Codes.xlsx");

    // Verify that the setted values were used
    assertEquals(maxEntrySizeVal, (Long) ZipSecureFile.getMaxEntrySize());
    assertEquals(maxTextSizeVal, (Long) ZipSecureFile.getMaxTextSize());
    assertEquals(minInflateRatioVal, (Double) ZipSecureFile.getMinInflateRatio());
  }

  @Test
  void testXLSXCompressionRatioIsBig() throws Exception {

    // For this zip to be correctly handed, we need to allow a lower inflate ratio
    Double minInflateRatio = 0.007d;
    System.setProperty(Const.HOP_ZIP_MIN_INFLATE_RATIO, minInflateRatio.toString());

    meta.setSpreadSheetType(SpreadSheetType.SAX_POI);
    init("Balance_Type_Codes.xlsx");

    // Verify that the minimum allowed inflate ratio is the expected
    assertEquals(minInflateRatio, (Double) ZipSecureFile.getMinInflateRatio());

    setFields(new ExcelInputField("FIST ID", -1, -1), new ExcelInputField("SOURCE SYSTEM", -1, -1));

    process();

    checkErrors();
    checkContent(new Object[][] {{"FIST0200", "ACM"}});
  }

  protected void testLimitOption(
      int rowLimit,
      boolean startsWithHeader,
      int[] startRowArr,
      int[] startColumnArr,
      String[] sheetNameArr)
      throws Exception {

    // Common stuff
    meta.setSpreadSheetType(SpreadSheetType.SAX_POI);

    setFields(new ExcelInputField("COL", -1, -1));
    meta.setRowLimit(rowLimit);

    // Set scenario parameters
    meta.setStartsWithHeader(startsWithHeader);
    for (int i = 0; i < startRowArr.length; i++) {
      ExcelInputMeta.EISheet sheet = new ExcelInputMeta.EISheet();
      sheet.setName(sheetNameArr[i]);
      sheet.setStartColumn(startColumnArr[i]);
      sheet.setStartRow(startRowArr[i]);
      meta.getSheets().add(sheet);
    }

    init("pdi-17765.xlsx");

    // Process
    process();

    // Check
    checkErrors();
    assertEquals(rowLimit, rows.size(), "Wrong row count");
  }

  protected void testLimitOptionSingleSheet(
      int rowLimit, boolean startsWithHeader, int startRow, Object firstResult, Object lastResult)
      throws Exception {

    testLimitOption(
        TEST_ROW_LIMIT_SINGLE_SHEET,
        startsWithHeader,
        new int[] {startRow},
        CNST_1_ZERO_INT_ARRAY,
        CNST_1_SHEET_NAME_ARRAY);

    // Checks
    assertEquals(TEST_ROW_LIMIT_SINGLE_SHEET, rows.size(), "Wrong row count");
    assertEquals(firstResult, rows.get(0)[0], "Wrong first result");
    assertEquals(lastResult, rows.get(TEST_ROW_LIMIT_SINGLE_SHEET - 1)[0], "Wrong last result");
  }

  @Test
  void testLimitOptionSingleSheet_Header_StartRow0() throws Exception {
    String firstResult = "1.0";
    String lastResult = "10.0";
    testLimitOptionSingleSheet(TEST_ROW_LIMIT_SINGLE_SHEET, true, 0, firstResult, lastResult);
  }

  @Test
  void testLimitOptionSingleSheet_NoHeader_StartRow0() throws Exception {
    String firstResult = "col";
    String lastResult = "9.0";
    testLimitOptionSingleSheet(TEST_ROW_LIMIT_SINGLE_SHEET, false, 0, firstResult, lastResult);
  }

  @Test
  void testLimitOptionSingleSheet_Header_StartRow5() throws Exception {
    String firstResult = "6.0";
    String lastResult = "15.0";
    testLimitOptionSingleSheet(TEST_ROW_LIMIT_SINGLE_SHEET, true, 5, firstResult, lastResult);
  }

  @Test
  void testLimitOptionSingleSheet_NoHeader_StartRow5() throws Exception {
    String firstResult = "5.0";
    String lastResult = "14.0";
    testLimitOptionSingleSheet(TEST_ROW_LIMIT_SINGLE_SHEET, false, 5, firstResult, lastResult);
  }

  @Test
  void testLimitOptionSingleSheet_Header_StartRow12() throws Exception {
    String firstResult = "13.0";
    String lastResult = "22.0";
    testLimitOptionSingleSheet(TEST_ROW_LIMIT_SINGLE_SHEET, true, 12, firstResult, lastResult);
  }

  @Test
  void testLimitOptionSingleSheet_NoHeader_StartRow12() throws Exception {
    String firstResult = "12.0";
    String lastResult = "21.0";
    testLimitOptionSingleSheet(TEST_ROW_LIMIT_SINGLE_SHEET, false, 12, firstResult, lastResult);
  }

  @Test
  void testLimitOptionMultipleSheets_Header_StartRow0() throws Exception {
    String firstResult = "1.0";
    String lastResult = "20.0";
    testLimitOption(
        TEST_ROW_LIMIT_MULTIPLE_SHEET,
        true,
        CNST_3_ZERO_INT_ARRAY,
        CNST_3_ZERO_INT_ARRAY,
        CNST_3_SHEET_NAME_ARRAY);

    // Checks
    assertEquals(firstResult, rows.get(0)[0], "Wrong first result");
    assertEquals(lastResult, rows.get(TEST_ROW_LIMIT_MULTIPLE_SHEET - 1)[0], "Wrong last result");
  }

  @Test
  void testLimitOptionMultipleSheets_NoHeader_StartRow0() throws Exception {
    String firstResult = "col";
    String lastResult = "19.0";
    testLimitOption(
        TEST_ROW_LIMIT_MULTIPLE_SHEET,
        false,
        CNST_3_ZERO_INT_ARRAY,
        CNST_3_ZERO_INT_ARRAY,
        CNST_3_SHEET_NAME_ARRAY);

    // Checks
    assertEquals(firstResult, rows.get(0)[0], "Wrong first result");
    assertEquals(lastResult, rows.get(TEST_ROW_LIMIT_MULTIPLE_SHEET - 1)[0], "Wrong last result");
  }

  @Test
  void testLimitOptionMultipleSheets_Header_StartRowX() throws Exception {
    String firstResult = "24.0";
    String lastResult = "132.0";
    testLimitOption(
        TEST_ROW_LIMIT_MULTIPLE_SHEET,
        true,
        CNST_3_SHEET_START_ROW_ARRAY,
        CNST_3_ZERO_INT_ARRAY,
        CNST_3_SHEET_NAME_ARRAY);

    // Checks
    assertEquals(firstResult, rows.get(0)[0], "Wrong first result");
    assertEquals(lastResult, rows.get(TEST_ROW_LIMIT_MULTIPLE_SHEET - 1)[0], "Wrong last result");
  }

  @Test
  void testLimitOptionMultipleSheets_NoHeader_StartRowX() throws Exception {
    String firstResult = "23.0";
    String lastResult = "102.0";
    testLimitOption(
        TEST_ROW_LIMIT_MULTIPLE_SHEET,
        false,
        CNST_3_SHEET_START_ROW_ARRAY,
        CNST_3_ZERO_INT_ARRAY,
        CNST_3_SHEET_NAME_ARRAY);

    // Checks
    assertEquals(firstResult, rows.get(0)[0], "Wrong first result");
    assertEquals(lastResult, rows.get(TEST_ROW_LIMIT_MULTIPLE_SHEET - 1)[0], "Wrong last result");
  }
}
