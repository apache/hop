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
 *
 */

package org.apache.hop.ui.core.widgets;

import java.io.OutputStream;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.vfs2.FileObject;
import org.apache.hop.core.gui.plugin.GuiPlugin;
import org.apache.hop.core.gui.plugin.toolbar.GuiToolbarElement;
import org.apache.hop.core.logging.LogChannel;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.core.row.value.ValueMetaInteger;
import org.apache.hop.core.row.value.ValueMetaString;
import org.apache.hop.core.vfs.HopVfs;
import org.apache.hop.ui.core.dialog.BaseDialog;
import org.apache.hop.ui.core.dialog.ErrorDialog;
import org.apache.hop.ui.core.dialog.MessageBox;
import org.apache.hop.ui.core.widget.ColumnInfo;
import org.apache.hop.ui.core.widget.TableView;
import org.apache.hop.ui.util.EnvironmentUtils;
import org.apache.poi.ss.usermodel.BorderStyle;
import org.apache.poi.ss.usermodel.Cell;
import org.apache.poi.ss.usermodel.CellStyle;
import org.apache.poi.ss.usermodel.Row;
import org.apache.poi.xssf.usermodel.XSSFCellStyle;
import org.apache.poi.xssf.usermodel.XSSFCreationHelper;
import org.apache.poi.xssf.usermodel.XSSFFont;
import org.apache.poi.xssf.usermodel.XSSFSheet;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;
import org.eclipse.swt.SWT;
import org.eclipse.swt.graphics.Cursor;
import org.eclipse.swt.widgets.Shell;
import org.eclipse.swt.widgets.TableItem;

@GuiPlugin
public class TableViewExportToExcelToolbarButton {

  private static final String ID_TOOLBAR_EXPORT_EXCEL = "tableview-toolbar-30000-export-to-excel";

  @GuiToolbarElement(
      root = TableView.ID_TOOLBAR,
      id = ID_TOOLBAR_EXPORT_EXCEL,
      toolTip = "i18n::ExcelWidget.ExportToolbarButton.ToolTip",
      separator = true,
      image = "excelwriter.svg")
  public static void copyToNewGoogleDocsSpreadsheet(TableView tableView) {
    Shell shell = tableView.getShell();
    Cursor oldCursor = shell.getCursor();
    shell.setCursor(shell.getDisplay().getSystemCursor(SWT.CURSOR_WAIT));

    try (XSSFWorkbook workbook = new XSSFWorkbook()) {
      XSSFSheet sheet = workbook.createSheet("Apache Hop data export");

      // Header style
      XSSFCellStyle headerStyle = workbook.createCellStyle();
      headerStyle.setBorderBottom(BorderStyle.DOUBLE); // single line border
      XSSFFont font = workbook.createFont();
      font.setBold(true);
      headerStyle.setFont(font);

      // Write the header
      //
      int rowNr = 0;
      Row row = sheet.createRow(rowNr++);
      int colNr = 0;
      Cell cell = row.createCell(colNr++);
      cell.setCellValue("#");
      cell.setCellStyle(headerStyle);
      for (ColumnInfo columnInfo : tableView.getColumns()) {
        cell = row.createCell(colNr++);
        cell.setCellValue(columnInfo.getName());
        cell.setCellStyle(headerStyle);
      }

      // Write the data to the sheet
      //
      ValueMetaString cellValueMeta = new ValueMetaString("cell");

      Map<Integer, CellStyle> columnStyles = new HashMap<>();
      XSSFCreationHelper creationHelper = workbook.getCreationHelper();

      for (TableItem item : tableView.getNonEmptyItems()) {
        row = sheet.createRow(rowNr++);
        colNr = 0;
        for (int i = 0; i < tableView.getColumns().length + 1; i++) {
          CellStyle cellStyle = columnStyles.get(i);
          boolean storeStyle = false;
          IValueMeta valueMeta;
          if (i == 0) {
            valueMeta = new ValueMetaInteger("#");
            valueMeta.setConversionMask("#");
          } else {
            valueMeta = tableView.getColumns()[i - 1].getValueMeta();
          }
          String string = item.getText(i);
          cell = row.createCell(colNr++);

          if (StringUtils.isEmpty(string) || "<null>".equals(string)) {
            continue;
          }

          if (valueMeta == null) {
            cell.setCellValue(string);
          } else {
            // Convert back from String to the original data type
            //
            cellValueMeta.setConversionMask(valueMeta.getConversionMask());
            Object object = valueMeta.convertData(cellValueMeta, string);

            switch (valueMeta.getType()) {
              case IValueMeta.TYPE_INTEGER:
                cell.setCellValue((double) ((Long) object));
                break;
              case IValueMeta.TYPE_NUMBER:
                cell.setCellValue((Double) object);
                break;
              case IValueMeta.TYPE_DATE:
                cell.setCellValue((Date) object);
                if (cellStyle == null) {
                  storeStyle = true;
                  cellStyle = workbook.createCellStyle();
                  cellStyle.setDataFormat(
                      creationHelper.createDataFormat().getFormat("yyyy/m/d h:mm:ss"));
                }
                break;
              case IValueMeta.TYPE_BOOLEAN:
                cell.setCellValue((Boolean) object);
                break;
              default:
                cell.setCellValue(string);
                break;
            }
            if (cellStyle != null) {
              cell.setCellStyle(cellStyle);
              if (storeStyle) {
                columnStyles.put(i, cellStyle);
              }
            }
          }
        }
      }

      // Now auto-size the columns
      //
      for (int column = 0; column < tableView.getColumns().length + 1; column++) {
        sheet.autoSizeColumn(column);
      }

      FileObject fileObject;

      if (EnvironmentUtils.getInstance().isWeb()) {
        LogChannel.UI.logBasic("Asking where to save the Excel file...");
        String filename =
            BaseDialog.presentFileDialog(
                shell, new String[] {"*.xlsx"}, new String[] {"Excel XLSX files"}, true);
        if (StringUtils.isEmpty(filename)) {
          return;
        }
        fileObject = HopVfs.getFileObject(filename);
      } else {
        // Just create a temporary file
        //
        fileObject =
            HopVfs.createTempFile(
                "apache-hop-table-export", ".xlsx", System.getProperty("java.io.tmpdir"));
      }

      String filename = HopVfs.getFilename(fileObject);
      LogChannel.UI.logBasic("Saving to file: " + filename);

      // Write the spreadsheet to the file
      //
      try (OutputStream outputStream = HopVfs.getOutputStream(fileObject, false)) {
        workbook.write(outputStream);
      }

      shell.setCursor(oldCursor);
      EnvironmentUtils.getInstance().openUrl(filename);
      if (EnvironmentUtils.getInstance().isWeb()) {
        MessageBox messageBox = new MessageBox(shell, SWT.ICON_INFORMATION | SWT.OK);
        messageBox.setText("File written");
        messageBox.setMessage("The Excel file was written to: " + filename);
        messageBox.open();
      }
    } catch (Throwable e) {
      shell.setCursor(oldCursor);
      new ErrorDialog(shell, "Error", "Error exporting rows to a new Excel file", e);
    }
  }
}
