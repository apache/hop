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
import org.odftoolkit.odfdom.dom.element.OdfStylableElement;
import org.odftoolkit.odfdom.dom.element.dc.DcCreatorElement;
import org.odftoolkit.odfdom.dom.element.office.OfficeAnnotationElement;
import org.odftoolkit.odfdom.dom.element.table.TableTableCellElement;
import org.odftoolkit.odfdom.dom.element.text.TextAElement;
import org.odftoolkit.odfdom.dom.element.text.TextPElement;
import org.odftoolkit.odfdom.dom.style.OdfStyleFamily;
import org.odftoolkit.odfdom.dom.style.props.OdfTextProperties;
import org.odftoolkit.odfdom.incubator.doc.office.OdfOfficeAutomaticStyles;
import org.odftoolkit.odfdom.incubator.doc.style.OdfStyle;

final class OdsStyleHelper {

  private static final String HYPERLINK_TEXT_STYLE = "HopOdsHyperlink";

  private OdsStyleHelper() {}

  static boolean cellHadContent(OdfTableCell cell) {
    if (cell == null) {
      return false;
    }
    if (!Utils.isEmpty(cell.getDisplayText())) {
      return true;
    }
    return !Utils.isEmpty(cell.getStyleName());
  }

  static void copyStyle(OdfTableCell target, OdfTableCell source) {
    if (target == null || source == null || target == source) {
      return;
    }
    String styleName = source.getStyleName();
    if (!Utils.isEmpty(styleName)) {
      asStylable(target).setStyleName(styleName);
    }
  }

  static void applyFormat(OdfTableCell cell, String format) {
    if (cell == null || Utils.isEmpty(format)) {
      return;
    }
    cell.setFormatString(OdsFormatConverter.toOdfFormat(format));
  }

  static OdfTableCell getCellFromReference(
      OdfSpreadsheetDocument document, OdfTable defaultTable, String reference) {
    if (Utils.isEmpty(reference)) {
      return null;
    }
    org.apache.poi.ss.util.CellReference cellRef =
        new org.apache.poi.ss.util.CellReference(reference);
    OdfTable table = defaultTable;
    String sheetName = cellRef.getSheetName();
    if (!Utils.isEmpty(sheetName)) {
      table = document.getTableByName(sheetName);
    }
    if (table == null) {
      return null;
    }
    return table.getCellByPosition(cellRef.getCol(), cellRef.getRow());
  }

  static void applyHyperlink(
      OdfSpreadsheetDocument document,
      OdfTable table,
      OdfTableCell cell,
      String link,
      String displayText)
      throws Exception {
    if (cell == null || Utils.isEmpty(link)) {
      return;
    }
    String href = toOdfHref(link);
    String text = Utils.isEmpty(displayText) ? link : displayText;
    String linkStyleName = getOrCreateHyperlinkTextStyle(document, table);

    cell.removeTextContent();
    TableTableCellElement cellElement = (TableTableCellElement) cell.getOdfElement();
    TextPElement paragraph = cellElement.newTextPElement();
    TextAElement anchor = paragraph.newTextAElement(href, linkStyleName);
    anchor.setXlinkTypeAttribute("simple");
    anchor.setTextContent(text);
  }

  static void applyComment(OdfTableCell cell, String author, String comment) {
    if (cell == null || Utils.isEmpty(comment)) {
      return;
    }
    TableTableCellElement cellElement = (TableTableCellElement) cell.getOdfElement();
    OfficeAnnotationElement annotation = cellElement.newOfficeAnnotationElement();
    if (!Utils.isEmpty(author)) {
      DcCreatorElement creator = annotation.newDcCreatorElement();
      creator.setTextContent(author);
    }
    TextPElement paragraph = annotation.newTextPElement();
    paragraph.setTextContent(comment);
  }

  private static String toOdfHref(String link) {
    if (link.startsWith("http:") || link.startsWith("https:") || link.startsWith("ftp:")) {
      return link;
    }
    if (link.startsWith("mailto:")) {
      return link;
    }
    if (link.startsWith("'")) {
      return link.substring(1);
    }
    return link;
  }

  private static OdfStylableElement asStylable(OdfTableCell cell) {
    return (OdfStylableElement) cell.getOdfElement();
  }

  private static String getOrCreateHyperlinkTextStyle(
      OdfSpreadsheetDocument document, OdfTable table) throws Exception {
    OdfTableCell anchorCell = table.getCellByPosition(0, 0);
    OdfOfficeAutomaticStyles automaticStyles =
        ((OdfStylableElement) anchorCell.getOdfElement()).getAutomaticStyles();
    OdfStyle existing = automaticStyles.getStyle(HYPERLINK_TEXT_STYLE, OdfStyleFamily.Text);
    if (existing != null) {
      return HYPERLINK_TEXT_STYLE;
    }
    OdfStyle style = automaticStyles.newStyle(OdfStyleFamily.Text);
    style.setStyleNameAttribute(HYPERLINK_TEXT_STYLE);
    style.setProperty(OdfTextProperties.Color, "#0000ff");
    style.setProperty(OdfTextProperties.TextUnderlineStyle, "solid");
    style.setProperty(OdfTextProperties.TextUnderlineType, "single");
    return HYPERLINK_TEXT_STYLE;
  }
}
