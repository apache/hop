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
import static org.junit.jupiter.api.Assertions.assertNull;

import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.text.SimpleDateFormat;
import java.util.Date;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.junit.rules.RestoreHopEngineEnvironmentExtension;
import org.apache.poi.hssf.usermodel.HSSFWorkbook;
import org.apache.poi.ss.usermodel.Cell;
import org.apache.poi.ss.usermodel.CellStyle;
import org.apache.poi.ss.usermodel.Font;
import org.apache.poi.ss.usermodel.Row;
import org.apache.poi.ss.usermodel.Sheet;
import org.apache.poi.ss.usermodel.Workbook;
import org.apache.poi.xssf.streaming.SXSSFWorkbook;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.api.io.TempDir;

/**
 * Reads workbooks that mix cell types, date styles and formulas, and checks the values and the
 * conversions to the field types, for every way the transform reads an Excel file.
 */
class ExcelInputCellTypesTest extends BaseExcelParsingTest {
  @RegisterExtension
  static RestoreHopEngineEnvironmentExtension env = new RestoreHopEngineEnvironmentExtension();

  @TempDir Path folder;

  private static final int DATA_ROWS = 4;

  @Test
  void xlsWithPoi() throws Exception {
    readAndCheck(write(new HSSFWorkbook(), "types.xls"), SpreadSheetType.POI);
  }

  @Test
  void xlsxWithPoi() throws Exception {
    readAndCheck(write(new XSSFWorkbook(), "types.xlsx"), SpreadSheetType.POI);
  }

  @Test
  void xlsxWithStreaming() throws Exception {
    readAndCheck(write(new XSSFWorkbook(), "types.xlsx"), SpreadSheetType.SAX_POI);
  }

  /** A workbook written by the streaming writer keeps its strings inline in the sheet. */
  @Test
  void inlineStringsWithStreaming() throws Exception {
    checkInlineStrings(SpreadSheetType.SAX_POI);
  }

  @Test
  void inlineStringsWithPoi() throws Exception {
    checkInlineStrings(SpreadSheetType.POI);
  }

  private void checkInlineStrings(SpreadSheetType type) throws Exception {
    Path file = folder.resolve("inline.xlsx");
    try (SXSSFWorkbook workbook = new SXSSFWorkbook()) {
      Sheet sheet = workbook.createSheet("data");
      String[] values = {"text", "plain", " padded ", "under_xscore", "a_x0041_b", "x_", "€ é"};
      for (int r = 0; r < values.length; r++) {
        sheet.createRow(r).createCell(0).setCellValue(values[r]);
        sheet.getRow(r).createCell(1).setCellValue(r);
      }
      try (OutputStream out = Files.newOutputStream(file)) {
        workbook.write(out);
      }
    }

    meta.setSpreadSheetType(type);
    meta.setStartsWithHeader(true);
    addSheet();
    setFields(field("text", "String", null), field("nr", "Integer", null));
    open(file);
    process();

    // "_x0041_" is how the file format writes the character with code 0x41
    check(
        new Object[][] {
          {"plain", 1L},
          {" padded ", 2L},
          {"under_xscore", 3L},
          {"aAb", 4L},
          {"x_", 5L},
          {"€ é", 6L}
        });
  }

  private Path write(Workbook workbook, String name) throws Exception {
    Path file = folder.resolve(name);
    try (workbook) {
      short isoDate = workbook.createDataFormat().getFormat("yyyy-mm-dd");
      CellStyle date1 = workbook.createCellStyle();
      date1.setDataFormat(isoDate);
      // a second style with the same number format
      CellStyle date2 = workbook.createCellStyle();
      date2.setDataFormat(isoDate);
      Font bold = workbook.createFont();
      bold.setBold(true);
      date2.setFont(bold);
      CellStyle dateTime = workbook.createCellStyle();
      dateTime.setDataFormat(workbook.createDataFormat().getFormat("dd/mm/yyyy hh:mm"));
      CellStyle decimals = workbook.createCellStyle();
      decimals.setDataFormat(workbook.createDataFormat().getFormat("0.00"));

      Sheet sheet = workbook.createSheet("data");
      Row header = sheet.createRow(0);
      String[] names = {
        "int",
        "num",
        "numText",
        "date",
        "date2",
        "dateTime",
        "dateText",
        "numDate",
        "text",
        "textInt",
        "bool",
        "boolText",
        "fNum",
        "fText",
        "fDate",
        "fBool",
        "mixed"
      };
      for (int i = 0; i < names.length; i++) {
        header.createCell(i).setCellValue(names[i]);
      }

      for (int r = 1; r <= DATA_ROWS; r++) {
        Row row = sheet.createRow(r);
        if (r == 3) {
          // a row with only the first and the last cell
          row.createCell(0).setCellValue(r);
          row.createCell(16).setCellValue("only");
          continue;
        }
        row.createCell(0).setCellValue(r);
        Cell num = row.createCell(1);
        num.setCellValue(r + 0.5);
        num.setCellStyle(decimals);
        row.createCell(2).setCellValue(1000 * r + 0.5);
        Cell d1 = row.createCell(3);
        d1.setCellValue(date("2024-03-1" + r + " 00:00"));
        d1.setCellStyle(date1);
        Cell d2 = row.createCell(4);
        d2.setCellValue(date("2023-11-0" + r + " 00:00"));
        d2.setCellStyle(date2);
        Cell d3 = row.createCell(5);
        d3.setCellValue(date("2024-07-0" + r + " 13:45"));
        d3.setCellStyle(dateTime);
        Cell d4 = row.createCell(6);
        d4.setCellValue(date("2022-01-2" + r + " 00:00"));
        d4.setCellStyle(r == 1 ? date1 : date2);
        row.createCell(7).setCellValue(86_400_000d * r);
        row.createCell(8).setCellValue("  text " + r + "  ");
        row.createCell(9).setCellValue("4" + r);
        row.createCell(10).setCellValue(r % 2 == 0);
        row.createCell(11).setCellValue(r % 2 == 1);
        row.createCell(12).setCellFormula("A" + (r + 1) + "*2");
        row.createCell(13).setCellFormula("\"f\"&A" + (r + 1));
        Cell fDate = row.createCell(14);
        fDate.setCellFormula("D" + (r + 1) + "+1");
        fDate.setCellStyle(date1);
        row.createCell(15).setCellFormula("A" + (r + 1) + ">1");
        // one column, a different cell type in every row
        Cell mixed = row.createCell(16);
        switch (r) {
          case 1 -> mixed.setCellValue(7.25);
          case 2 -> {
            mixed.setCellValue(date("2021-05-06 00:00"));
            mixed.setCellStyle(date2);
          }
          default -> mixed.setCellValue(true);
        }
      }
      workbook.getCreationHelper().createFormulaEvaluator().evaluateAll();
      try (OutputStream out = Files.newOutputStream(file)) {
        workbook.write(out);
      }
    }
    return file;
  }

  private void readAndCheck(Path file, SpreadSheetType type) throws Exception {
    meta.setSpreadSheetType(type);
    meta.setStartsWithHeader(true);
    addSheet();

    ExcelInputField numText = field("numText", "String", "#,##0.00");
    numText.setDecimalSymbol(".");
    numText.setGroupSymbol(",");
    ExcelInputField text = field("text", "String", null);
    text.setTrimType(IValueMeta.TrimType.BOTH);
    ExcelInputField mixed = field("mixed", "String", null);

    setFields(
        field("int", "Integer", null),
        field("num", "Number", null),
        numText,
        field("date", "Date", null),
        field("date2", "Date", null),
        field("dateTime", "Date", null),
        field("dateText", "String", "yyyy/MM/dd"),
        field("numDate", "Date", null),
        text,
        field("textInt", "Integer", "#"),
        field("bool", "Boolean", null),
        field("boolText", "String", null),
        field("fNum", "Number", null),
        field("fText", "String", null),
        field("fDate", "Date", null),
        field("fBool", "Boolean", null),
        mixed);
    open(file);
    process();

    checkErrors();
    assertEquals(DATA_ROWS, rows.size());
    for (int r = 1; r <= DATA_ROWS; r++) {
      Object[] row = rows.get(r - 1);
      String at = type + " row " + r + " ";
      assertEquals((long) r, row[0], at + "int");
      if (r == 3) {
        for (int i = 1; i < 16; i++) {
          assertNull(row[i], at + "column " + i);
        }
        assertEquals("only", row[16], at + "mixed");
        continue;
      }
      assertEquals(r + 0.5, row[1], at + "num");
      assertEquals(r + ",000.50", row[2], at + "numText");
      assertEquals(date("2024-03-1" + r + " 00:00"), row[3], at + "date");
      assertEquals(date("2023-11-0" + r + " 00:00"), row[4], at + "date2");
      assertEquals(date("2024-07-0" + r + " 13:45"), row[5], at + "dateTime");
      assertEquals("2022/01/2" + r, row[6], at + "dateText");
      assertEquals(new Date(86_400_000L * r), row[7], at + "numDate");
      assertEquals("text " + r, row[8], at + "text");
      assertEquals(40L + r, row[9], at + "textInt");
      assertEquals(r % 2 == 0, row[10], at + "bool");
      assertEquals(r % 2 == 1 ? "Y" : "N", row[11], at + "boolText");
      assertEquals(r * 2d, row[12], at + "fNum");
      assertEquals("f" + r, row[13], at + "fText");
      assertEquals(date("2024-03-1" + (r + 1) + " 00:00"), row[14], at + "fDate");
      assertEquals(r > 1, row[15], at + "fBool");
    }
    assertEquals("7.25", rows.get(0)[16], type + " mixed number");
    assertEquals("2021/05/06 00:00:00.000", rows.get(1)[16], type + " mixed date");
    assertEquals("Y", rows.get(3)[16], type + " mixed boolean");
  }

  private void addSheet() {
    ExcelInputMeta.EISheet sheet = new ExcelInputMeta.EISheet();
    sheet.setName("data");
    sheet.setStartRow(0);
    sheet.setStartColumn(0);
    meta.getSheets().add(sheet);
  }

  private void open(Path file) throws Exception {
    ExcelInputMeta.EIFile input = new ExcelInputMeta.EIFile();
    input.setName(file.toString());
    input.setMask("");
    input.setExcludeMask("");
    input.setRequired("Y");
    input.setIncludeSubFolders("N");
    meta.getFiles().add(input);

    transform =
        new ExcelInput(transformMeta, meta, new ExcelInputData(), 1, pipelineMeta, pipeline);
    transform.init();
    transform.addRowListener(rowListener);
  }

  private static ExcelInputField field(String name, String type, String format) {
    ExcelInputField field = new ExcelInputField(name, -1, -1);
    field.setType(type);
    field.setFormat(format);
    return field;
  }

  private static Date date(String text) throws Exception {
    return new SimpleDateFormat("yyyy-MM-dd HH:mm").parse(text);
  }
}
