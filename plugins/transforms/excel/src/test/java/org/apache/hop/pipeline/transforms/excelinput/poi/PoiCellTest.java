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

package org.apache.hop.pipeline.transforms.excelinput.poi;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

import java.util.Date;
import java.util.TimeZone;
import java.util.stream.Stream;
import org.apache.hop.core.spreadsheet.IKCell;
import org.apache.hop.core.spreadsheet.KCellType;
import org.apache.poi.hssf.usermodel.HSSFWorkbook;
import org.apache.poi.ss.usermodel.Cell;
import org.apache.poi.ss.usermodel.CellStyle;
import org.apache.poi.ss.usermodel.DateUtil;
import org.apache.poi.ss.usermodel.Font;
import org.apache.poi.ss.usermodel.FormulaError;
import org.apache.poi.ss.usermodel.Row;
import org.apache.poi.ss.usermodel.Sheet;
import org.apache.poi.ss.usermodel.Workbook;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

/** The cell type and value that the POI reader gives for every kind of cell. */
class PoiCellTest {

  static Stream<Workbook> workbooks() {
    return Stream.of(new HSSFWorkbook(), new XSSFWorkbook());
  }

  @ParameterizedTest
  @MethodSource("workbooks")
  void plainCells(Workbook workbook) throws Exception {
    try (workbook) {
      Row row = workbook.createSheet("s").createRow(4);
      row.createCell(0).setCellValue(true);
      row.createCell(1).setCellValue(12.5);
      row.createCell(2).setCellValue("text");
      row.createCell(3).setBlank();
      row.createCell(4).setCellErrorValue(FormulaError.DIV0.getCode());

      check(row.getCell(0), KCellType.BOOLEAN, true, "true");
      check(row.getCell(1), KCellType.NUMBER, 12.5, "12.5");
      check(row.getCell(2), KCellType.LABEL, "text", "text");
      check(row.getCell(3), KCellType.EMPTY, null, null);
      check(row.getCell(4), KCellType.EMPTY, null, null);
      assertEquals(4, new PoiCell(row.getCell(0)).getRow());
    }
  }

  /** A number is a date when its style has a date format, and only then. */
  @ParameterizedTest
  @MethodSource("workbooks")
  void datesFollowTheCellStyle(Workbook workbook) throws Exception {
    try (workbook) {
      CellStyle date = workbook.createCellStyle();
      date.setDataFormat(workbook.createDataFormat().getFormat("yyyy-mm-dd"));
      // same number format, another style
      CellStyle boldDate = workbook.createCellStyle();
      boldDate.setDataFormat(date.getDataFormat());
      Font bold = workbook.createFont();
      bold.setBold(true);
      boldDate.setFont(bold);
      // a built-in date format
      CellStyle builtIn = workbook.createCellStyle();
      builtIn.setDataFormat((short) 14);
      CellStyle decimals = workbook.createCellStyle();
      decimals.setDataFormat(workbook.createDataFormat().getFormat("0.00"));
      CellStyle boldOnly = workbook.createCellStyle();
      boldOnly.setFont(bold);

      Sheet sheet = workbook.createSheet("s");
      double serial = 45000.25;
      CellStyle[] styles = {date, decimals, boldDate, null, builtIn, boldOnly, date, decimals};
      boolean[] isDate = {true, false, true, false, true, false, true, false};
      // two passes: the answer for a style must not depend on the cells read before it
      for (int pass = 0; pass < 2; pass++) {
        for (int i = 0; i < styles.length; i++) {
          Cell cell = sheet.createRow(pass * 10 + i).createCell(0);
          cell.setCellValue(serial + i);
          if (styles[i] != null) {
            cell.setCellStyle(styles[i]);
          }
          if (isDate[i]) {
            check(cell, KCellType.DATE, shifted(DateUtil.getJavaDate(serial + i)), null);
          } else {
            check(cell, KCellType.NUMBER, serial + i, Double.toString(serial + i));
          }
        }
      }

      // a negative number is not a date, whatever the style says
      Cell negative = sheet.createRow(30).createCell(0);
      negative.setCellValue(-3.5);
      negative.setCellStyle(date);
      check(negative, KCellType.NUMBER, -3.5, "-3.5");
      // and the style is still a date style for the next cell
      Cell next = sheet.createRow(31).createCell(0);
      next.setCellValue(serial);
      next.setCellStyle(date);
      check(next, KCellType.DATE, shifted(DateUtil.getJavaDate(serial)), null);
    }
  }

  @ParameterizedTest
  @MethodSource("workbooks")
  void formulaCells(Workbook workbook) throws Exception {
    try (workbook) {
      CellStyle date = workbook.createCellStyle();
      date.setDataFormat(workbook.createDataFormat().getFormat("dd/mm/yyyy"));

      Row row = workbook.createSheet("s").createRow(0);
      row.createCell(0).setCellValue(45000);
      row.createCell(1).setCellFormula("A1*2");
      row.createCell(2).setCellFormula("\"a\"&\"b\"");
      row.createCell(3).setCellFormula("A1>1");
      Cell dateFormula = row.createCell(4);
      dateFormula.setCellFormula("A1+1");
      dateFormula.setCellStyle(date);
      row.createCell(5).setCellFormula("1/0");
      row.createCell(6).setCellFormula("IF(A1>1,\"\",\"x\")");
      workbook.getCreationHelper().createFormulaEvaluator().evaluateAll();

      check(row.getCell(1), KCellType.NUMBER_FORMULA, 90000d, "90000.0");
      check(row.getCell(2), KCellType.STRING_FORMULA, "ab", "ab");
      check(row.getCell(3), KCellType.BOOLEAN_FORMULA, true, "true");
      check(row.getCell(4), KCellType.DATE_FORMULA, shifted(DateUtil.getJavaDate(45001)), null);
      check(row.getCell(5), KCellType.EMPTY, null, null);
      check(row.getCell(6), KCellType.STRING_FORMULA, "", "");
    }
  }

  /** The type and the value are the same however often they are asked for. */
  private static void check(Cell cell, KCellType type, Object value, String contents) {
    IKCell poiCell =
        new PoiSheet(cell.getSheet()).getCell(cell.getColumnIndex(), cell.getRowIndex());
    for (int i = 0; i < 2; i++) {
      assertEquals(type, poiCell.getType());
      if (value == null) {
        assertNull(poiCell.getValue());
        assertNull(poiCell.getContents());
      } else {
        assertEquals(value, poiCell.getValue());
        // the reader gives a date as a java.sql.Date: its text is the day only
        assertEquals(
            contents == null ? new java.sql.Date(((Date) value).getTime()).toString() : contents,
            poiCell.getContents());
      }
    }
    IKCell[] row = new PoiSheet(cell.getSheet()).getRow(cell.getRowIndex());
    assertEquals(type, row[cell.getColumnIndex()].getType());
  }

  /** The reader gives a date with the offset of the time zone added to it. */
  private static Date shifted(Date date) {
    return new Date(date.getTime() + TimeZone.getDefault().getOffset(date.getTime()));
  }
}
