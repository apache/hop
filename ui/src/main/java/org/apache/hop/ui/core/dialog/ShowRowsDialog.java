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

package org.apache.hop.ui.core.dialog;

import java.util.List;
import org.apache.hop.core.Const;
import org.apache.hop.core.config.HopConfig;
import org.apache.hop.core.exception.HopValueException;
import org.apache.hop.core.logging.LogChannel;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.core.util.Utils;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.ui.core.PropsUi;
import org.apache.hop.ui.core.gui.GuiResource;
import org.apache.hop.ui.core.gui.WindowProperty;
import org.apache.hop.ui.core.widget.ColumnInfo;
import org.apache.hop.ui.core.widget.TableView;
import org.eclipse.swt.SWT;
import org.eclipse.swt.layout.FormAttachment;
import org.eclipse.swt.layout.FormData;
import org.eclipse.swt.layout.FormLayout;
import org.eclipse.swt.widgets.Label;
import org.eclipse.swt.widgets.MessageBox;
import org.eclipse.swt.widgets.Shell;
import org.eclipse.swt.widgets.TableItem;

/**
 * Read-only dialog that shows a static set of rows with a custom title and header message.
 *
 * <p>Use this when the caller already has rows in memory and only needs to display them. For
 * transform preview (streaming, "get more rows", pause/stop, logging text), use {@link
 * PreviewRowsDialog} instead.
 *
 * <p>Column headers are sortable. Cells hold their full values — the grid only shortens long or
 * multi-line text when drawing it — so copying, exporting and sorting all work on complete values.
 */
public final class ShowRowsDialog {

  private static final Class<?> PKG = ShowRowsDialog.class;

  private static final boolean AVOID_BINARY_IN_HEX =
      Const.toBoolean(
          HopConfig.readStringVariable(Const.HOP_BINARY_FIELDS_AVOID_HEX_PREVIEW, "false"));

  private final Shell parentShell;
  private final IVariables variables;
  private final String title;
  private final String message;
  private final IRowMeta rowMeta;
  private final List<Object[]> rows;

  private Shell shell;
  private TableView tableView;
  private int lineNr;

  public ShowRowsDialog(
      Shell parent,
      IVariables variables,
      String title,
      String message,
      IRowMeta rowMeta,
      List<Object[]> rows) {
    this.parentShell = parent;
    this.variables = variables;
    this.title = title;
    this.message = message;
    this.rowMeta = rowMeta;
    this.rows = rows;
  }

  public void open() {
    if (parentShell == null || parentShell.isDisposed()) {
      return;
    }
    if (rowMeta == null || Utils.isEmpty(rows)) {
      MessageBox messageBox =
          new MessageBox(parentShell, SWT.OK | SWT.ICON_INFORMATION | SWT.APPLICATION_MODAL);
      messageBox.setText(Utils.isEmpty(title) ? "" : title);
      messageBox.setMessage(BaseMessages.getString(PKG, "ShowRowsDialog.NoRows.Message"));
      messageBox.open();
      return;
    }

    shell = new Shell(parentShell, SWT.DIALOG_TRIM | SWT.RESIZE | SWT.MAX | SWT.MIN);
    PropsUi.setLook(shell);
    shell.setImage(GuiResource.getInstance().getImageHopUi());
    shell.setText(Utils.isEmpty(title) ? "" : title);

    FormLayout formLayout = new FormLayout();
    formLayout.marginWidth = PropsUi.getFormMargin();
    formLayout.marginHeight = PropsUi.getFormMargin();
    shell.setLayout(formLayout);

    int margin = PropsUi.getMargin();

    String header = Utils.isEmpty(message) ? "" : message;
    header +=
        " " + BaseMessages.getString(PKG, "ShowRowsDialog.NrRows", Integer.toString(rows.size()));

    Label messageLabel = new Label(shell, SWT.LEFT);
    messageLabel.setText(header.trim());
    PropsUi.setLook(messageLabel);
    FormData fdMessage = new FormData();
    fdMessage.left = new FormAttachment(0, 0);
    fdMessage.right = new FormAttachment(100, 0);
    fdMessage.top = new FormAttachment(0, margin);
    messageLabel.setLayoutData(fdMessage);

    tableView = buildTableView(margin, messageLabel);
    populateRows();
    RowPreviewSupport.installCellTooltips(tableView, rowMeta);

    BaseDialog.defaultShellHandling(shell, c -> close(), c -> close());
  }

  private TableView buildTableView(int margin, Label messageLabel) {
    ColumnInfo[] columns = new ColumnInfo[rowMeta.size()];
    for (int i = 0; i < rowMeta.size(); i++) {
      IValueMeta valueMeta = rowMeta.getValueMeta(i);
      columns[i] =
          new ColumnInfo(valueMeta.getName(), ColumnInfo.COLUMN_TYPE_TEXT, valueMeta.isNumeric());
      RowPreviewSupport.applyColumnMeta(columns[i], valueMeta);
    }

    TableView view =
        new TableView(
            variables,
            shell,
            SWT.BORDER | SWT.FULL_SELECTION | SWT.MULTI,
            columns,
            0,
            null,
            PropsUi.getInstance());
    view.setShowingBlueNullValues(true);
    // Data rows, not configuration: draw long / multi-line values shortened.
    view.setShortenDisplayedValues(true);
    // Column sorting is enabled: items carry their full values, so a sort reorders complete rows.
    view.setSortable(true);

    FormData fdTable = new FormData();
    fdTable.left = new FormAttachment(0, 0);
    fdTable.top = new FormAttachment(messageLabel, margin);
    fdTable.right = new FormAttachment(100, 0);
    fdTable.bottom = new FormAttachment(100, -margin);
    view.setLayoutData(fdTable);
    return view;
  }

  private void populateRows() {
    lineNr = 0;
    for (int i = 0; i < rows.size(); i++) {
      TableItem item;
      if (i == 0) {
        item = tableView.table.getItem(0);
      } else {
        item = new TableItem(tableView.table, SWT.NONE);
      }
      fillRow(item, rows.get(i));
    }
    if (!tableView.isDisposed()) {
      tableView.optWidth(true, 200);
    }
  }

  private void fillRow(TableItem item, Object[] row) {
    if (row == null) {
      return;
    }

    lineNr++;
    String rowNumber;
    try {
      rowNumber = tableView.getNumberColumn().getValueMeta().getString((long) lineNr);
    } catch (Exception e) {
      rowNumber = Integer.toString(lineNr);
    }
    item.setText(0, rowNumber);

    for (int column = 0; column < rowMeta.size(); column++) {
      IValueMeta valueMeta = rowMeta.getValueMeta(column);
      String displayValue;
      try {
        displayValue = RowPreviewSupport.formatCell(valueMeta, row[column], AVOID_BINARY_IN_HEX);
      } catch (HopValueException | ArrayIndexOutOfBoundsException e) {
        new LogChannel(PKG).logError("Unable to format cell value", e);
        displayValue = null;
      }

      if (displayValue != null) {
        // The cell holds the shortened, single-line text while the grid keeps the full value
        // aside, so what is copied, expanded or read back out of the table stays complete.
        tableView.setCellValue(item, column + 1, displayValue);
        item.setForeground(column + 1, GuiResource.getInstance().getColorBlack());
      } else {
        item.setText(column + 1, "<null>");
        item.setForeground(column + 1, GuiResource.getInstance().getColorBlue());
      }
    }
  }

  private void close() {
    if (shell == null || shell.isDisposed()) {
      return;
    }
    PropsUi.getInstance().setScreen(new WindowProperty(shell));
    shell.dispose();
  }

  static String formatColumnMetaTooltip(IValueMeta valueMeta) {
    return RowPreviewSupport.formatColumnMetaTooltip(valueMeta);
  }
}
