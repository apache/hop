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

package org.apache.hop.pipeline.transforms.excelwriter.ods;

import org.apache.hop.core.util.Utils;
import org.odftoolkit.odfdom.doc.OdfSpreadsheetDocument;
import org.odftoolkit.odfdom.doc.table.OdfTable;
import org.odftoolkit.odfdom.doc.table.OdfTableCell;
import org.odftoolkit.odfdom.dom.OdfDocumentNamespace;
import org.w3c.dom.Element;

final class OdsFormulaHelper {

  private OdsFormulaHelper() {}

  static void applyFormula(OdfTableCell cell, String excelFormula) throws Exception {
    if (cell == null || Utils.isEmpty(excelFormula)) {
      return;
    }
    cell.setFormula(OdsFormulaConverter.toOdfFormula(excelFormula));
  }

  /**
   * Clears cached formula results so ODF spreadsheet applications recalculate on open. ODF has no
   * direct equivalent to POI's force-formula-recalculation flag.
   */
  static void prepareForRecalculation(OdfSpreadsheetDocument document) throws Exception {
    for (OdfTable table : document.getTableList()) {
      int rows = table.getRowCount();
      int cols = table.getColumnCount();
      for (int row = 0; row < rows; row++) {
        for (int col = 0; col < cols; col++) {
          OdfTableCell cell = table.getCellByPosition(col, row);
          if (cell == null || Utils.isEmpty(cell.getFormula())) {
            continue;
          }
          clearCachedFormulaResult(cell);
        }
      }
    }
  }

  private static void clearCachedFormulaResult(OdfTableCell cell) {
    Element element = cell.getOdfElement();
    String officeNs = OdfDocumentNamespace.OFFICE.getUri();
    removeAttributeIfPresent(element, officeNs, "value");
    removeAttributeIfPresent(element, officeNs, "string-value");
    removeAttributeIfPresent(element, officeNs, "boolean-value");
    removeAttributeIfPresent(element, officeNs, "date-value");
    removeAttributeIfPresent(element, officeNs, "time-value");
    cell.removeTextContent();
  }

  private static void removeAttributeIfPresent(
      Element element, String namespace, String localName) {
    if (element.hasAttributeNS(namespace, localName)) {
      element.removeAttributeNS(namespace, localName);
    }
  }
}
