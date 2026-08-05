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

import java.io.BufferedWriter;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.vfs2.FileObject;
import org.apache.hop.core.Const;
import org.apache.hop.core.gui.plugin.GuiPlugin;
import org.apache.hop.core.gui.plugin.toolbar.GuiToolbarElement;
import org.apache.hop.core.logging.LogChannel;
import org.apache.hop.core.vfs.HopVfs;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.ui.core.dialog.BaseDialog;
import org.apache.hop.ui.core.dialog.EnterSelectionDialog;
import org.apache.hop.ui.core.dialog.ErrorDialog;
import org.apache.hop.ui.core.dialog.MessageBox;
import org.apache.hop.ui.core.widget.ColumnInfo;
import org.apache.hop.ui.core.widget.TableView;
import org.apache.hop.ui.util.EnvironmentUtils;
import org.eclipse.swt.SWT;
import org.eclipse.swt.graphics.Cursor;
import org.eclipse.swt.widgets.Shell;
import org.eclipse.swt.widgets.TableItem;

@GuiPlugin
public class TableViewExportToCsvToolbarButton {

  private static final Class<?> PKG = TableViewExportToCsvToolbarButton.class;

  private static final String ID_TOOLBAR_EXPORT_CSV = "tableview-toolbar-30010-export-to-csv";

  private static final char CSV_SEPARATOR = ',';
  private static final char CSV_ENCLOSURE = '"';

  @GuiToolbarElement(
      root = TableView.ID_TOOLBAR,
      id = ID_TOOLBAR_EXPORT_CSV,
      toolTip = "i18n::CsvWidget.ExportToolbarButton.ToolTip",
      separator = false,
      image = "textfileoutput.svg")
  public static void exportToCsv(TableView tableView) {
    Shell shell = tableView.getShell();
    ColumnInfo[] allColumns = tableView.getColumns();

    // Build the list of column names for selection
    //
    String[] columnNames = new String[allColumns.length];
    for (int i = 0; i < allColumns.length; i++) {
      columnNames[i] = allColumns[i].getName();
    }

    // Show a multi-select dialog to let the user choose which columns to export
    // By default all columns are selected
    //
    EnterSelectionDialog selectionDialog =
        new EnterSelectionDialog(
            shell,
            columnNames,
            BaseMessages.getString(PKG, "CsvWidget.ExportColumnSelection.Title"),
            BaseMessages.getString(PKG, "CsvWidget.ExportColumnSelection.Message"));
    selectionDialog.setMulti(true);
    int[] allIndices = new int[columnNames.length];
    for (int i = 0; i < allIndices.length; i++) {
      allIndices[i] = i;
    }
    selectionDialog.setSelectedNrs(allIndices);

    if (selectionDialog.open() == null) {
      // User cancelled
      return;
    }

    int[] selectedIndices = selectionDialog.getSelectionIndeces();
    if (selectedIndices == null || selectedIndices.length == 0) {
      return;
    }

    // Build a list of selected ColumnInfo entries for convenience
    //
    List<ColumnInfo> selectedColumns = new ArrayList<>();
    for (int idx : selectedIndices) {
      selectedColumns.add(allColumns[idx]);
    }

    Cursor oldCursor = shell.getCursor();
    shell.setCursor(shell.getDisplay().getSystemCursor(SWT.CURSOR_WAIT));

    try {
      FileObject fileObject;

      if (EnvironmentUtils.getInstance().isWeb()) {
        LogChannel.UI.logBasic("Asking where to save the CSV file...");
        String filename =
            BaseDialog.presentFileDialog(
                shell, new String[] {"*.csv"}, new String[] {"CSV files"}, true);
        if (StringUtils.isEmpty(filename)) {
          shell.setCursor(oldCursor);
          return;
        }
        fileObject = HopVfs.getFileObject(filename);
      } else {
        // Just create a temporary file
        //
        fileObject =
            HopVfs.createTempFile(
                "apache-hop-table-export", ".csv", System.getProperty("java.io.tmpdir"));
      }

      String filename = HopVfs.getFilename(fileObject);
      LogChannel.UI.logBasic("Saving to file: " + filename);

      try (OutputStream outputStream = HopVfs.getOutputStream(fileObject, false);
          BufferedWriter writer =
              new BufferedWriter(new OutputStreamWriter(outputStream, StandardCharsets.UTF_8))) {

        // Write the header (# row number column + selected columns)
        //
        writeCsvField(writer, "#");
        for (ColumnInfo columnInfo : selectedColumns) {
          writer.write(CSV_SEPARATOR);
          writeCsvField(writer, columnInfo.getName());
        }
        writer.write(Const.CR);

        // Write the data rows
        //
        for (TableItem item : tableView.getNonEmptyItems()) {
          // Write the row number column (#)
          //
          String rowNumString = item.getText(0);
          if (StringUtils.isEmpty(rowNumString) || "<null>".equals(rowNumString)) {
            writeCsvField(writer, "");
          } else {
            writeCsvField(writer, rowNumString);
          }

          // Write only the selected columns
          //
          for (int selectedIdx : selectedIndices) {
            // selectedIdx is 0-based into allColumns[]; TableItem column is selectedIdx + 1
            //
            int tableItemCol = selectedIdx + 1;
            String string = item.getText(tableItemCol);
            writer.write(CSV_SEPARATOR);
            if (StringUtils.isEmpty(string) || "<null>".equals(string)) {
              writeCsvField(writer, "");
            } else {
              writeCsvField(writer, string);
            }
          }
          writer.write(Const.CR);
        }

        writer.flush();
      }

      shell.setCursor(oldCursor);
      EnvironmentUtils.getInstance().openUrl(filename);
      if (EnvironmentUtils.getInstance().isWeb()) {
        MessageBox messageBox = new MessageBox(shell, SWT.ICON_INFORMATION | SWT.OK);
        messageBox.setText("File written");
        messageBox.setMessage("The CSV file was written to: " + filename);
        messageBox.open();
      }
    } catch (Throwable e) {
      shell.setCursor(oldCursor);
      new ErrorDialog(shell, "Error", "Error exporting rows to a new CSV file", e);
    }
  }

  /**
   * Write a single CSV field with double-quote enclosure. Internal double quotes are escaped by
   * doubling them (RFC 4180).
   */
  private static void writeCsvField(BufferedWriter writer, String value) throws Exception {
    writer.write(CSV_ENCLOSURE);
    if (value != null) {
      for (int i = 0; i < value.length(); i++) {
        char c = value.charAt(i);
        if (c == CSV_ENCLOSURE) {
          // Escape enclosure by writing it twice
          //
          writer.write(CSV_ENCLOSURE);
          writer.write(CSV_ENCLOSURE);
        } else {
          writer.write(c);
        }
      }
    }
    writer.write(CSV_ENCLOSURE);
  }
}
