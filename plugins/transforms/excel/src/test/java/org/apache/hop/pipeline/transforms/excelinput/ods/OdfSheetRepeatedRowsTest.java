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

package org.apache.hop.pipeline.transforms.excelinput.ods;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;

import java.io.File;
import java.io.InputStream;
import java.nio.file.Files;
import java.util.GregorianCalendar;
import org.apache.hop.core.spreadsheet.IKCell;
import org.apache.hop.core.spreadsheet.IKSheet;
import org.apache.hop.core.spreadsheet.KCellType;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;
import org.odftoolkit.odfdom.doc.OdfSpreadsheetDocument;
import org.odftoolkit.odfdom.doc.table.OdfTable;
import org.odftoolkit.odfdom.doc.table.OdfTableRow;

/**
 * Spreadsheet applications write the trailing empty rows of a sheet as a single row element with a
 * large table:number-rows-repeated attribute. Asking ODFDOM for a cell outside of the used range
 * makes it grow the table, which materializes every one of those repeated rows and does not
 * complete in any reasonable time. The "get fields from header row" button probes one column past
 * the last one to find the end of the header, so it used to hang on such files.
 */
class OdfSheetRepeatedRowsTest {

  private static final String SHEET_NAME = "Sheet1";

  /** Number of trailing empty rows, as a spreadsheet application would declare them. */
  private static final int REPEATED_EMPTY_ROWS = 1048573;

  @TempDir File tempDir;

  @Test
  @Timeout(value = 60, threadMode = Timeout.ThreadMode.SEPARATE_THREAD)
  void getCellOutsideUsedRangeReturnsNullInsteadOfExpandingTheSheet() throws Exception {
    File file = createSheetWithRepeatedEmptyRows();

    try (InputStream input = Files.newInputStream(file.toPath())) {
      OdfWorkbook workbook = new OdfWorkbook(input, "UTF-8");
      IKSheet sheet = workbook.getSheet(SHEET_NAME);

      // the repeated empty rows are not part of the used range
      assertEquals(2, sheet.getRows());

      // one column past the last one: this is the probe that used to hang
      assertNull(sheet.getCell(2, 0));
      assertNull(sheet.getCell(2, 1));

      // one row past the last one
      assertNull(sheet.getCell(0, 2));

      // negative indexes are not valid either
      assertNull(sheet.getCell(-1, 0));
      assertNull(sheet.getCell(0, -1));

      workbook.close();
    }
  }

  @Test
  @Timeout(value = 60, threadMode = Timeout.ThreadMode.SEPARATE_THREAD)
  void getCellInsideUsedRangeStillReadsHeaderAndData() throws Exception {
    File file = createSheetWithRepeatedEmptyRows();

    try (InputStream input = Files.newInputStream(file.toPath())) {
      OdfWorkbook workbook = new OdfWorkbook(input, "UTF-8");
      IKSheet sheet = workbook.getSheet(SHEET_NAME);

      IKCell header = sheet.getCell(0, 0);
      assertNotNull(header);
      assertEquals("DATE_OF_IMPORT", header.getContents());

      IKCell below = sheet.getCell(0, 1);
      assertNotNull(below);
      assertEquals(KCellType.DATE, below.getType());

      assertEquals("STRING_OF_IMPORT", sheet.getCell(1, 0).getContents());
      assertEquals(KCellType.LABEL, sheet.getCell(1, 1).getType());

      workbook.close();
    }
  }

  /**
   * Builds a two column sheet with a header row, one data row and a large block of empty rows
   * stored as a single repeated row element.
   */
  private File createSheetWithRepeatedEmptyRows() throws Exception {
    File file = new File(tempDir, "repeated-rows.ods");
    try (OdfSpreadsheetDocument document = OdfSpreadsheetDocument.newSpreadsheetDocument()) {
      OdfTable table = document.getTableList().get(0);
      table.setTableName(SHEET_NAME);

      table.getCellByPosition(0, 0).setStringValue("DATE_OF_IMPORT");
      table.getCellByPosition(1, 0).setStringValue("STRING_OF_IMPORT");
      table
          .getCellByPosition(0, 1)
          .setDateValue(new GregorianCalendar(2014, GregorianCalendar.DECEMBER, 26));
      table.getCellByPosition(1, 1).setStringValue("12-26-2019");

      OdfTableRow trailing = table.appendRow();
      trailing.getOdfElement().setTableNumberRowsRepeatedAttribute(REPEATED_EMPTY_ROWS);

      document.save(file);
    }
    return file;
  }
}
