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

import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.spreadsheet.IKCell;
import org.apache.hop.core.spreadsheet.IKWorkbook;
import org.apache.hop.core.variables.Variables;
import org.apache.hop.pipeline.transforms.excelinput.ods.OdfSheet;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class OdfSheetTest {

  private IKWorkbook ods341;
  private IKWorkbook ods24;

  @BeforeEach
  void init() throws HopException {
    ods341 =
        WorkbookFactory.getWorkbook(
            SpreadSheetType.ODS,
            this.getClass().getResource("files/sample-3.4.1.ods").getPath(),
            null,
            new Variables());
    ods24 =
        WorkbookFactory.getWorkbook(
            SpreadSheetType.ODS,
            this.getClass().getResource("files/sample-2.4.ods").getPath(),
            null,
            new Variables());
  }

  @Test
  void testRowColumnsCount() {

    String sameRowWidthSheet = "SameRowWidth";
    String diffRowWidthSheet = "DifferentRowWidth";

    checkRowCount(
        (OdfSheet) ods341.getSheet(sameRowWidthSheet), 3, "Row count mismatch for ODF v3.4.1");
    checkRowCount(
        (OdfSheet) ods24.getSheet(sameRowWidthSheet), 2, "Row count mismatch for ODF v2.4");
    checkRowCount(
        (OdfSheet) ods341.getSheet(diffRowWidthSheet), 3, "Row count mismatch for ODF v3.4.1");
    checkRowCount(
        (OdfSheet) ods24.getSheet(diffRowWidthSheet), 2, "Row count mismatch for ODF v2.4");

    checkCellCount(
        (OdfSheet) ods341.getSheet(sameRowWidthSheet), 15, "Cell count mismatch for ODF v3.4.1");
    checkCellCount(
        (OdfSheet) ods24.getSheet(sameRowWidthSheet), 1, "Cell count mismatch for ODF v2.4");
    checkCellCount(
        (OdfSheet) ods341.getSheet(diffRowWidthSheet),
        new int[] {15, 15, 12},
        "Cell count mismatch for ODF v3.4.1");
    checkCellCount(
        (OdfSheet) ods24.getSheet(diffRowWidthSheet),
        new int[] {3, 2},
        "Cell count mismatch for ODF v2.4");
  }

  private void checkRowCount(OdfSheet sheet, int expected, String failMsg) {
    int actual = sheet.getRows();
    assertEquals(expected, actual, failMsg);
  }

  private void checkCellCount(OdfSheet sheet, int expected, String failMsg) {
    int rowNo = sheet.getRows();
    for (int i = 0; i < rowNo; i++) {
      IKCell[] row = sheet.getRow(i);
      assertEquals(expected, row.length, failMsg + "; Row content: " + rowToString(row));
    }
  }

  private void checkCellCount(OdfSheet sheet, int[] expected, String failMsg) {
    int rowNo = sheet.getRows();
    assertEquals(expected.length, rowNo, "Row count mismatch");
    for (int i = 0; i < rowNo; i++) {
      IKCell[] row = sheet.getRow(i);
      assertEquals(expected[i], row.length, failMsg + "; Row content: " + rowToString(row));
    }
  }

  private String rowToString(IKCell[] row) {
    if (row == null || row.length == 0) {
      return "";
    }
    String result = cellToStr(row[0]);
    for (int j = 1; j < row.length; j++) {
      result += "," + cellToStr(row[j]);
    }
    return result;
  }

  private String cellToStr(IKCell cell) {
    String result = "null";
    if (cell != null) {
      result = cell.getContents();
    }
    return result;
  }
}
