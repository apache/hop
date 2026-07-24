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
import org.apache.commons.codec.binary.Hex;
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
import org.eclipse.swt.custom.TableEditor;
import org.eclipse.swt.graphics.Point;
import org.eclipse.swt.graphics.Rectangle;
import org.eclipse.swt.layout.FillLayout;
import org.eclipse.swt.layout.FormAttachment;
import org.eclipse.swt.layout.FormData;
import org.eclipse.swt.layout.FormLayout;
import org.eclipse.swt.widgets.Label;
import org.eclipse.swt.widgets.MessageBox;
import org.eclipse.swt.widgets.Shell;
import org.eclipse.swt.widgets.TableItem;
import org.eclipse.swt.widgets.Text;

/**
 * Read-only dialog that shows a static set of rows with a custom title and header message.
 *
 * <p>Use this when the caller already has rows in memory and only needs to display them. For
 * transform preview (streaming, "get more rows", pause/stop, logging text), use {@link
 * PreviewRowsDialog} instead.
 */
public final class ShowRowsDialog {

  private static final Class<?> PKG = ShowRowsDialog.class;

  private static final int MAX_BINARY_STRING_PREVIEW_SIZE =
      PreviewRowsDialog.MAX_BINARY_STRING_PREVIEW_SIZE;

  private static final boolean AVOID_BINARY_IN_HEX =
      Const.toBoolean(
          HopConfig.readStringVariable(Const.HOP_BINARY_FIELDS_AVOID_HEX_PREVIEW, "false"));

  /** Caps for the floating full-value box: it grows to fit the value, then scrolls beyond these. */
  private static final int MAX_OVERLAY_WIDTH = 600;

  private static final int MAX_OVERLAY_HEIGHT = 400;

  private final Shell parentShell;
  private final IVariables variables;
  private final String title;
  private final String message;
  private final IRowMeta rowMeta;
  private final List<Object[]> rows;

  private Shell shell;
  private TableView tableView;
  private int lineNr;

  /** Read-only text field overlaid on the clicked cell so its value can be selected in place. */
  private TableEditor cellEditor;

  private Text cellEditorText;

  /** Lightweight floating box showing the full value of an overflowing cell (Ctrl+Space style). */
  private Shell valueOverlay;

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
    setupCellSelection();

    BaseDialog.defaultShellHandling(shell, c -> close(), c -> close());
  }

  private TableView buildTableView(int margin, Label messageLabel) {
    ColumnInfo[] columns = new ColumnInfo[rowMeta.size()];
    for (int i = 0; i < rowMeta.size(); i++) {
      IValueMeta valueMeta = rowMeta.getValueMeta(i);
      columns[i] =
          new ColumnInfo(valueMeta.getName(), ColumnInfo.COLUMN_TYPE_TEXT, valueMeta.isNumeric());
      columns[i].setToolTip(valueMeta.toStringMeta());
      columns[i].setValueMeta(valueMeta);
      columns[i].setImage(GuiResource.getInstance().getImage(valueMeta));
      columns[i].setReadOnly(true);
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
    // Rows are kept in load order so a cell's visual position maps straight back to the buffer that
    // holds its full value (see getFullCellString). Sorting rebuilds the table items from their
    // truncated display text, breaking that mapping, so it is disabled here.
    view.setSortable(false);

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
        if (valueMeta.isBinary()) {
          byte[] bytes = valueMeta.getBinary(row[column]);
          if (bytes == null) {
            displayValue = null;
          } else {
            displayValue =
                AVOID_BINARY_IN_HEX ? valueMeta.getString(bytes) : Hex.encodeHexString(bytes);
            if (displayValue != null && displayValue.length() > MAX_BINARY_STRING_PREVIEW_SIZE) {
              displayValue = displayValue.substring(0, MAX_BINARY_STRING_PREVIEW_SIZE);
            }
          }
        } else {
          displayValue = valueMeta.getString(row[column]);
        }
      } catch (HopValueException | ArrayIndexOutOfBoundsException e) {
        new LogChannel(PKG).logError("Unable to format cell value", e);
        displayValue = null;
      }

      if (displayValue != null) {
        item.setText(column + 1, TableView.formatCellValueForDisplay(displayValue));
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
    if (cellEditor != null) {
      cellEditor.dispose();
    }
    PropsUi.getInstance().setScreen(new WindowProperty(shell));
    shell.dispose();
  }

  /**
   * Cells only show a truncated, single-lined value for performance (see {@link
   * TableView#formatCellValueForDisplay}). Clicking a cell drops a read-only text field on it (like
   * the inline editor of an editable grid) so its full value can be selected and copied in place;
   * double-clicking a cell shows its full, original content in a floating box (handy for long
   * strings, JSON and multi-line values).
   */
  private void setupCellSelection() {
    cellEditor = new TableEditor(tableView.table);
    cellEditor.grabHorizontal = true;
    cellEditor.horizontalAlignment = SWT.LEFT;
    tableView.table.addListener(
        SWT.MouseDown,
        event -> {
          // Leave Shift/Ctrl clicks to the table so row range/toggle selection keeps working.
          if (event.button == 1 && (event.stateMask & (SWT.SHIFT | SWT.MOD1)) == 0) {
            openCellEditor(new Point(event.x, event.y));
          }
        });
    tableView.table.addListener(
        SWT.MouseDoubleClick, event -> showFullCellValue(new Point(event.x, event.y)));
  }

  private void showFullCellValue(Point point) {
    CellRef ref = cellAt(point);
    if (ref != null) {
      expandCell(ref.bounds, ref.rowIndex, ref.columnIndex);
    }
  }

  /**
   * Single-click handler: drop a read-only text field on the clicked cell — the same cell-fitting
   * effect as an editable grid's inline editor — holding the full, untruncated value so it can be
   * selected and copied in place. Double-clicking the field expands it to the full-value box.
   */
  private void openCellEditor(Point point) {
    CellRef ref = cellAt(point);
    if (ref == null) {
      return;
    }
    String full = getFullCellString(ref.rowIndex, ref.columnIndex - 1);
    if (full == null) {
      return;
    }
    TableItem item = tableView.table.getItem(ref.rowIndex);

    if (cellEditorText != null && !cellEditorText.isDisposed()) {
      cellEditorText.dispose();
    }

    final Text field = new Text(tableView.table, SWT.SINGLE | SWT.READ_ONLY);
    PropsUi.setLook(field);
    field.setText(full);
    cellEditorText = field;
    final long openedAt = System.currentTimeMillis();

    // Escape or losing focus removes the field again.
    field.addListener(
        SWT.KeyDown,
        e -> {
          if (e.keyCode == SWT.ESC) {
            field.dispose();
          }
        });
    field.addListener(SWT.FocusOut, e -> field.dispose());

    // Double-clicking the field expands to the full-value box. The field is created on the first
    // click, so on some platforms the second click of a double-click lands on this fresh field as a
    // plain MouseDown; treat a click within the OS double-click time of it opening as that second
    // click too. Coordinates are captured so the box anchors to the same cell.
    final Rectangle cellBounds = ref.bounds;
    final int rowIndex = ref.rowIndex;
    final int columnIndex = ref.columnIndex;
    field.addListener(SWT.MouseDoubleClick, e -> expandCell(cellBounds, rowIndex, columnIndex));
    field.addListener(
        SWT.MouseDown,
        e -> {
          if (System.currentTimeMillis() - openedAt
              <= tableView.getDisplay().getDoubleClickTime()) {
            expandCell(cellBounds, rowIndex, columnIndex);
          }
        });

    cellEditor.setEditor(field, item, columnIndex);
    field.setFocus();
    field.selectAll();
  }

  /** Expand the given cell's full value into the floating, selectable value box. */
  private void expandCell(Rectangle cellBounds, int rowIndex, int columnIndex) {
    if (cellEditorText != null && !cellEditorText.isDisposed()) {
      cellEditorText.dispose();
    }
    // A double-click fires both a MouseDown (caught within the double-click window) and a
    // MouseDoubleClick; ignore the second while the box for this click is still up.
    if (valueOverlay != null && !valueOverlay.isDisposed()) {
      return;
    }
    String full = getFullCellString(rowIndex, columnIndex - 1);
    if (full != null) {
      showValueOverlay(cellBounds, full);
    }
  }

  /**
   * Resolve the data cell under a table-relative point, or null when the point isn't over a data
   * cell (the row-number column, empty space, or a row/column outside the backing buffer).
   */
  private CellRef cellAt(Point point) {
    if (tableView == null || tableView.isDisposed() || rows == null || rowMeta == null) {
      return null;
    }
    TableItem item = tableView.table.getItem(point);
    if (item == null) {
      return null;
    }
    int rowIndex = tableView.table.indexOf(item);
    if (rowIndex < 0 || rowIndex >= rows.size()) {
      return null;
    }
    // Column 0 is the row-number column; data columns start at 1. Find the one under the pointer.
    for (int i = 1; i < tableView.table.getColumnCount(); i++) {
      Rectangle b = item.getBounds(i);
      if (b.contains(point)) {
        return i - 1 < rowMeta.size() ? new CellRef(rowIndex, i, b) : null;
      }
    }
    return null;
  }

  /**
   * A located data cell: its row index into the buffer, its 1-based table column, and its bounds.
   */
  private static final class CellRef {
    private final int rowIndex;
    private final int columnIndex;
    private final Rectangle bounds;

    private CellRef(int rowIndex, int columnIndex, Rectangle bounds) {
      this.rowIndex = rowIndex;
      this.columnIndex = columnIndex;
      this.bounds = bounds;
    }
  }

  /**
   * Show the full cell value in a lightweight, non-modal multi-line text box anchored to the cell —
   * the same floating-shell idea as the Ctrl+Space variable helper. Dismisses on Escape or when it
   * loses focus.
   */
  private void showValueOverlay(Rectangle cellBounds, String value) {
    if (valueOverlay != null && !valueOverlay.isDisposed()) {
      valueOverlay.dispose();
    }

    Point location = tableView.table.toDisplay(cellBounds.x, cellBounds.y);

    // A resizable (but title-less) floating shell: light like the variable helper, yet the user can
    // drag its edges to make it bigger for a long value.
    final Shell overlay = new Shell(shell, SWT.RESIZE);
    overlay.setLayout(new FillLayout());

    final Text text =
        new Text(overlay, SWT.MULTI | SWT.WRAP | SWT.V_SCROLL | SWT.READ_ONLY | SWT.BORDER);
    PropsUi.setLook(text);
    text.setText(value);

    // Size the box to its content. Width comes from the value's natural (unwrapped) width, at least
    // the cell width and capped so it never sprawls across the screen; height is then measured
    // after
    // the value wraps to that width (minus the border + scrollbar gutter), so a value that spans
    // several lines gets a taller box instead of a scrollbar — we only scroll past the height cap.
    Point natural = text.computeSize(SWT.DEFAULT, SWT.DEFAULT);
    int contentWidth = Math.min(natural.x + 20, MAX_OVERLAY_WIDTH);
    int width = Math.max(cellBounds.width, contentWidth);
    int wrapWidth = Math.max(50, width - 24);
    Point wrapped = text.computeSize(wrapWidth, SWT.DEFAULT);
    int contentHeight = Math.min(wrapped.y + 8, MAX_OVERLAY_HEIGHT);
    int height = Math.max(cellBounds.height + 4, contentHeight);
    overlay.setSize(width, height);
    overlay.setLocation(location.x, location.y);

    // Dismiss on Escape.
    text.addListener(
        SWT.KeyDown,
        e -> {
          if (e.keyCode == SWT.ESC) {
            overlay.dispose();
          }
        });

    overlay.open();
    valueOverlay = overlay;

    // Grab focus after the current (double-click) event settles, so a trailing table focus event
    // can't immediately close the box. Pre-select the whole value so it's ready to copy. Only after
    // focus is settled inside the box do we arm click-away dismissal — via shell deactivation
    // rather
    // than a text focus-out, so grabbing a resize edge (which keeps the shell active) doesn't close
    // it.
    overlay
        .getDisplay()
        .asyncExec(
            () -> {
              if (text.isDisposed()) {
                return;
              }
              text.setFocus();
              text.selectAll();
              overlay.addListener(
                  SWT.Deactivate,
                  e -> {
                    if (!overlay.isDisposed()) {
                      overlay.dispose();
                    }
                  });
            });
  }

  /** Convert the raw buffer value at (rowIndex, column) to its full string form, no truncation. */
  private String getFullCellString(int rowIndex, int column) {
    Object[] row = rows.get(rowIndex);
    IValueMeta valueMeta = rowMeta.getValueMeta(column);
    try {
      if (valueMeta.isBinary()) {
        byte[] bytes = valueMeta.getBinary(row[column]);
        if (bytes == null) {
          return null;
        }
        return AVOID_BINARY_IN_HEX ? valueMeta.getString(bytes) : Hex.encodeHexString(bytes);
      }
      return valueMeta.getString(row[column]);
    } catch (HopValueException e) {
      new LogChannel(PKG).logError("Unable to format cell value", e);
      return null;
    }
  }
}
