/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hop.ui.core.dialog;

import java.util.ArrayList;
import java.util.List;
import org.apache.commons.codec.binary.Hex;
import org.apache.hop.core.Const;
import org.apache.hop.core.config.HopConfig;
import org.apache.hop.core.exception.HopValueException;
import org.apache.hop.core.logging.ILogChannel;
import org.apache.hop.core.logging.LogChannel;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.core.row.RowMeta;
import org.apache.hop.core.row.value.ValueMetaString;
import org.apache.hop.core.util.Utils;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.ui.core.PropsUi;
import org.apache.hop.ui.core.gui.GuiResource;
import org.apache.hop.ui.core.gui.WindowProperty;
import org.apache.hop.ui.core.widget.ColumnInfo;
import org.apache.hop.ui.core.widget.TableView;
import org.apache.hop.ui.hopgui.HopGui;
import org.apache.hop.ui.pipeline.transform.BaseTransformDialog;
import org.eclipse.swt.SWT;
import org.eclipse.swt.custom.TableEditor;
import org.eclipse.swt.events.KeyAdapter;
import org.eclipse.swt.events.KeyEvent;
import org.eclipse.swt.events.KeyListener;
import org.eclipse.swt.graphics.Point;
import org.eclipse.swt.graphics.Rectangle;
import org.eclipse.swt.layout.FillLayout;
import org.eclipse.swt.layout.FormAttachment;
import org.eclipse.swt.layout.FormData;
import org.eclipse.swt.layout.FormLayout;
import org.eclipse.swt.widgets.Button;
import org.eclipse.swt.widgets.Control;
import org.eclipse.swt.widgets.Label;
import org.eclipse.swt.widgets.Shell;
import org.eclipse.swt.widgets.TableItem;
import org.eclipse.swt.widgets.Text;

/**
 * Displays rows for transform preview: streaming updates, optional logging text, and actions such
 * as "get more rows" or stop. For a static set of rows with a custom title and message (no preview
 * actions), prefer {@link ShowRowsDialog}.
 */
public class PreviewRowsDialog {
  private static final Class<?> PKG = PreviewRowsDialog.class;

  public static final int MAX_BINARY_STRING_PREVIEW_SIZE = 1000000;
  public static final boolean PREVIEW_AVOID_BINARY_IN_HEX =
      Const.toBoolean(
          HopConfig.readStringVariable(Const.HOP_BINARY_FIELDS_AVOID_HEX_PREVIEW, "false"));

  /** Caps for the floating full-value box: it grows to fit the value, then scrolls beyond these. */
  private static final int MAX_OVERLAY_WIDTH = 600;

  private static final int MAX_OVERLAY_HEIGHT = 400;

  private String transformName;

  private Label wlFields;

  private TableView wFields;

  private Shell shell;

  /** Lightweight floating box showing the full value of an overflowing cell (Ctrl+Space style). */
  private Shell valueOverlay;

  /** Read-only text field overlaid on the clicked cell so its value can be selected in place. */
  private TableEditor cellEditor;

  private Text cellEditorText;

  private final List<Object[]> buffer;

  private String title;
  private String message;

  private Rectangle bounds;

  private int hscroll;
  private int vscroll;
  private int hmax;
  private int vmax;

  private final String loggingText;

  private boolean proposingToGetMoreRows;

  private boolean proposingToStop;

  private boolean askingForMoreRows;

  private boolean askingToStop;

  private IRowMeta rowMeta;

  private final IVariables variables;

  private final ILogChannel log;

  private boolean dynamic;

  private boolean waitingForRows;

  protected int lineNr;

  private int style = SWT.DIALOG_TRIM | SWT.RESIZE | SWT.MAX | SWT.MIN;

  private final Shell parentShell;

  private final List<IDialogClosedListener> dialogClosedListeners;

  private Control bottomButton;

  public PreviewRowsDialog(
      Shell parent,
      IVariables variables,
      int style,
      String transformName,
      IRowMeta rowMeta,
      List<Object[]> rowBuffer) {
    this(parent, variables, style, transformName, rowMeta, rowBuffer, null);
  }

  public PreviewRowsDialog(
      Shell parent,
      IVariables variables,
      int style,
      String transformName,
      IRowMeta rowMeta,
      List<Object[]> rowBuffer,
      String loggingText) {
    this.transformName = transformName;
    this.buffer = rowBuffer;
    this.loggingText = loggingText;
    this.rowMeta = rowMeta;
    this.variables = variables;
    this.parentShell = parent;
    this.style = (style != SWT.None) ? style : this.style;
    this.dialogClosedListeners = new ArrayList<>();

    bounds = null;
    hscroll = -1;
    vscroll = -1;
    title = null;
    message = null;

    this.log = new LogChannel("Row Preview");
  }

  public void setTitleMessage(String title, String message) {
    this.title = title;
    this.message = message;
  }

  public void open() {
    if (title == null) {
      title = BaseMessages.getString(PKG, "PreviewRowsDialog.Title");
    }
    if (message == null) {
      message = BaseMessages.getString(PKG, "PreviewRowsDialog.Header", transformName);
    }

    // Empty static preview: show a single modal on the real parent. Parenting a message to the
    // preview shell here caused odd modality (that shell is created but never opened).
    if (!dynamic && Utils.isEmpty(buffer)) {
      MessageBox mb =
          new MessageBox(parentShell, SWT.OK | SWT.ICON_INFORMATION | SWT.APPLICATION_MODAL);
      mb.setText(title);
      mb.setMessage(BaseMessages.getString(PKG, "PreviewRowsDialog.NoRows.Message"));
      mb.open();
      return;
    }

    shell = new Shell(parentShell, style);
    PropsUi.setLook(shell);
    shell.setImage(GuiResource.getInstance().getImageHopUi());

    FormLayout formLayout = new FormLayout();
    formLayout.marginWidth = PropsUi.getFormMargin();
    formLayout.marginHeight = PropsUi.getFormMargin();

    if (buffer != null) {
      message += " " + BaseMessages.getString(PKG, "PreviewRowsDialog.NrRows", "" + buffer.size());
    }

    shell.setLayout(formLayout);
    shell.setText(title);

    List<Button> buttons = new ArrayList<>();

    Button wClose = new Button(shell, SWT.PUSH);
    wClose.setText(BaseMessages.getString(PKG, "System.Button.Close"));
    wClose.addListener(SWT.Selection, e -> cancel());
    buttons.add(wClose);

    if (!Utils.isEmpty(loggingText)) {
      Button wLog = new Button(shell, SWT.PUSH);
      wLog.setText(BaseMessages.getString(PKG, "PreviewRowsDialog.Button.ShowLog"));
      wLog.addListener(SWT.Selection, e -> log());
      buttons.add(wLog);
    }

    if (proposingToStop) {
      Button wStop = new Button(shell, SWT.PUSH);
      wStop.setText(BaseMessages.getString(PKG, "PreviewRowsDialog.Button.Stop.Label"));
      wStop.setToolTipText(BaseMessages.getString(PKG, "PreviewRowsDialog.Button.Stop.ToolTip"));
      wStop.addListener(SWT.Selection, e -> cancel());
      buttons.add(wStop);
    }

    if (proposingToGetMoreRows) {
      Button wNext = new Button(shell, SWT.PUSH);
      wNext.setText(BaseMessages.getString(PKG, "PreviewRowsDialog.Button.Next.Label"));
      wNext.setToolTipText(BaseMessages.getString(PKG, "PreviewRowsDialog.Button.Next.ToolTip"));
      wNext.addListener(
          SWT.Selection,
          e -> {
            askingForMoreRows = true;
            close();
          });
      buttons.add(wNext);
    }

    if (proposingToGetMoreRows || proposingToStop) {
      wClose.setText(BaseMessages.getString(PKG, "PreviewRowsDialog.Button.Close.Label"));
      wClose.setToolTipText(BaseMessages.getString(PKG, "PreviewRowsDialog.Button.Close.ToolTip"));
    }

    // Position the buttons...
    bottomButton = buttons.getFirst();
    BaseTransformDialog.positionBottomButtons(
        shell, buttons.toArray(new Button[buttons.size()]), PropsUi.getMargin(), null);

    addFields();

    KeyListener escapeListener =
        new KeyAdapter() {
          @Override
          public void keyPressed(KeyEvent e) {
            if (e.keyCode == SWT.ESC) {
              cancel();
            }
          }
        };

    shell.addKeyListener(escapeListener);
    wFields.addKeyListener(escapeListener);
    wFields.table.addKeyListener(escapeListener);
    buttons.stream().forEach(b -> b.addKeyListener(escapeListener));

    getData();

    BaseDialog.defaultShellHandling(shell, c -> close(), c -> cancel());
  }

  private void cancel() {
    askingToStop = true;
    close();
  }

  private boolean addFields() {
    PropsUi props = PropsUi.getInstance();
    int margin = PropsUi.getMargin();

    if (wlFields == null) {
      wlFields = new Label(shell, SWT.LEFT);
      wlFields.setText(message);
      PropsUi.setLook(wlFields);
      FormData fdlFields = new FormData();
      fdlFields.left = new FormAttachment(0, 0);
      fdlFields.right = new FormAttachment(100, 0);
      fdlFields.top = new FormAttachment(0, margin);
      wlFields.setLayoutData(fdlFields);
    } else {
      wFields.dispose();
    }

    if (dynamic && rowMeta == null) {
      rowMeta = new RowMeta();
      rowMeta.addValueMeta(new ValueMetaString("<waiting for rows>"));
      waitingForRows = true;
    }
    ColumnInfo[] columns = new ColumnInfo[rowMeta.size()];
    for (int i = 0; i < rowMeta.size(); i++) {
      IValueMeta valueMeta = rowMeta.getValueMeta(i);
      columns[i] =
          new ColumnInfo(valueMeta.getName(), ColumnInfo.COLUMN_TYPE_TEXT, valueMeta.isNumeric());
      columns[i].setToolTip(valueMeta.toStringMeta());
      columns[i].setValueMeta(valueMeta);
      columns[i].setImage(GuiResource.getInstance().getImage(valueMeta));
      // A preview is a viewer: the grid's own editable inline editor must stay off. We drop our own
      // read-only field on the cell instead (see the MouseDown handler below), which also frees
      // double-click to pop up the full, untruncated value of a cell.
      columns[i].setReadOnly(true);
    }

    wFields =
        new TableView(
            variables, shell, SWT.BORDER | SWT.FULL_SELECTION | SWT.MULTI, columns, 0, null, props);
    wFields.setShowingBlueNullValues(true);
    // Rows are kept in load order so a cell's visual position maps straight back to the buffer that
    // holds its full value. Sorting would reorder items (and sort by the truncated display text),
    // breaking that mapping, so it is disabled here.
    wFields.setSortable(false);

    // Cells only show a truncated, single-lined value for performance. Click a cell to drop a
    // read-only text field on it (like the inline editor of an editable grid) so its value can be
    // selected and copied in place; double-click a cell to see its full, original content in a
    // floating box (handy for long strings, JSON and multi-line values).
    cellEditor = new TableEditor(wFields.table);
    cellEditor.grabHorizontal = true;
    cellEditor.horizontalAlignment = SWT.LEFT;
    wFields.table.addListener(
        SWT.MouseDown,
        event -> {
          // Leave Shift/Ctrl clicks to the table so row range/toggle selection keeps working.
          if (event.button == 1 && (event.stateMask & (SWT.SHIFT | SWT.MOD1)) == 0) {
            openCellEditor(new Point(event.x, event.y));
          }
        });
    wFields.table.addListener(
        SWT.MouseDoubleClick, event -> showFullCellValue(new Point(event.x, event.y)));

    FormData fdFields = new FormData();
    fdFields.left = new FormAttachment(0, 0);
    fdFields.top = new FormAttachment(wlFields, margin);
    fdFields.right = new FormAttachment(100, 0);
    fdFields.bottom = new FormAttachment(bottomButton, -2 * margin);
    wFields.setLayoutData(fdFields);

    if (dynamic) {
      shell.layout(true, true);
    }

    return false;
  }

  public void dispose() {
    if (cellEditor != null) {
      cellEditor.dispose();
    }
    PropsUi.getInstance().setScreen(new WindowProperty(shell));
    bounds = shell.getBounds();
    hscroll = wFields.getHorizontalBar().getSelection();
    vscroll = wFields.getVerticalBar().getSelection();
    shell.dispose();
  }

  /** Copy information from the meta-data input to the dialog fields. */
  private void getData() {
    synchronized (buffer) {
      shell
          .getDisplay()
          .asyncExec(
              () -> {
                lineNr = 0;
                for (int i = 0; i < buffer.size(); i++) {
                  TableItem item;
                  if (i == 0) {
                    item = wFields.table.getItem(i);
                  } else {
                    item = new TableItem(wFields.table, SWT.NONE);
                  }

                  Object[] row = buffer.get(i);

                  getDataForRow(item, row);
                }
                if (!wFields.isDisposed()) {
                  wFields.optWidth(true, 200);
                }
              });
    }
  }

  protected int getDataForRow(TableItem item, Object[] row) {
    int nrErrors = 0;

    if (row == null) { // no row to process
      return nrErrors;
    }

    // Display the correct line item...
    //
    String strNr;
    lineNr++;
    try {
      strNr = wFields.getNumberColumn().getValueMeta().getString((long) lineNr);
    } catch (Exception e) {
      strNr = Integer.toString(lineNr);
    }
    item.setText(0, strNr);

    for (int c = 0; c < rowMeta.size(); c++) {
      IValueMeta v = rowMeta.getValueMeta(c);
      String show;
      try {
        if (v.isBinary()) {
          byte[] bytes = v.getBinary(row[c]);
          if (bytes == null) {
            show = null;
          } else {
            if (PREVIEW_AVOID_BINARY_IN_HEX) {
              show = v.getString(bytes);
            } else {
              show = Hex.encodeHexString(bytes);
            }
            if (show.length() > MAX_BINARY_STRING_PREVIEW_SIZE) {
              // We want to limit the size of the strings during preview to keep all SWT widgets
              // happy.
              //
              show = show.substring(0, MAX_BINARY_STRING_PREVIEW_SIZE);
            }
          }
        } else {
          show = v.getString(row[c]);
        }

      } catch (HopValueException | ArrayIndexOutOfBoundsException e) {
        nrErrors++;
        if (nrErrors < 25) {
          log.logError(Const.getStackTracker(e));
        }
        show = null;
      }

      if (show != null) {
        item.setText(c + 1, TableView.formatCellValueForDisplay(show));
        item.setForeground(c + 1, GuiResource.getInstance().getColorBlack());
      } else {
        // Set null value
        item.setText(c + 1, "<null>");
        item.setForeground(c + 1, GuiResource.getInstance().getColorBlue());
      }
    }

    return nrErrors;
  }

  /**
   * When a data cell is double-clicked, show its full value in a lightweight box anchored to the
   * cell so it can be selected and copied. Overflowing values (long / multi-line) get a larger,
   * scrollable box; a short single value gets a small one. Does nothing for non-data cells.
   */
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
    TableItem item = wFields.table.getItem(ref.rowIndex);

    if (cellEditorText != null && !cellEditorText.isDisposed()) {
      cellEditorText.dispose();
    }

    final Text field = new Text(wFields.table, SWT.SINGLE | SWT.READ_ONLY);
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
          if (System.currentTimeMillis() - openedAt <= wFields.getDisplay().getDoubleClickTime()) {
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
    if (wFields == null || wFields.isDisposed() || buffer == null || rowMeta == null) {
      return null;
    }
    TableItem item = wFields.table.getItem(point);
    if (item == null) {
      return null;
    }
    int rowIndex = wFields.table.indexOf(item);
    if (rowIndex < 0 || rowIndex >= buffer.size()) {
      return null;
    }
    // Column 0 is the row-number column; data columns start at 1. Find the one under the pointer.
    for (int i = 1; i < wFields.table.getColumnCount(); i++) {
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

    Point location = wFields.table.toDisplay(cellBounds.x, cellBounds.y);

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
    Object[] row = buffer.get(rowIndex);
    IValueMeta valueMeta = rowMeta.getValueMeta(column);
    try {
      if (valueMeta.isBinary()) {
        byte[] bytes = valueMeta.getBinary(row[column]);
        if (bytes == null) {
          return null;
        }
        return PREVIEW_AVOID_BINARY_IN_HEX
            ? valueMeta.getString(bytes)
            : Hex.encodeHexString(bytes);
      }
      return valueMeta.getString(row[column]);
    } catch (HopValueException e) {
      log.logError(Const.getStackTracker(e));
      return null;
    }
  }

  private void close() {
    transformName = null;
    dispose();
  }

  /** Show the logging of the preview (in case errors occurred */
  private void log() {
    if (loggingText != null) {
      EnterTextDialog etd =
          new EnterTextDialog(
              shell,
              BaseMessages.getString(PKG, "PreviewRowsDialog.ShowLogging.Title"),
              BaseMessages.getString(PKG, "PreviewRowsDialog.ShowLogging.Message"),
              loggingText);
      etd.open();
    }
  }

  public boolean isDisposed() {
    return shell.isDisposed();
  }

  public Rectangle getBounds() {
    return bounds;
  }

  public void setBounds(Rectangle b) {
    bounds = b;
  }

  public int getHScroll() {
    return hscroll;
  }

  public void setHScroll(int s) {
    hscroll = s;
  }

  public int getVScroll() {
    return vscroll;
  }

  public void setVScroll(int s) {
    vscroll = s;
  }

  public int getHMax() {
    return hmax;
  }

  public void setHMax(int m) {
    hmax = m;
  }

  public int getVMax() {
    return vmax;
  }

  public void setVMax(int m) {
    vmax = m;
  }

  /**
   * @return true if the user is asking to grab the next rows with preview
   */
  public boolean isAskingForMoreRows() {
    return askingForMoreRows;
  }

  /**
   * @return true if the dialog is proposing to ask for more rows
   */
  public boolean isProposingToGetMoreRows() {
    return proposingToGetMoreRows;
  }

  /**
   * @param proposingToGetMoreRows Set to true if you want to display a button asking for more
   *     preview rows.
   */
  public void setProposingToGetMoreRows(boolean proposingToGetMoreRows) {
    this.proposingToGetMoreRows = proposingToGetMoreRows;
  }

  /**
   * @return the askingToStop
   */
  public boolean isAskingToStop() {
    return askingToStop;
  }

  /**
   * @return the proposingToStop
   */
  public boolean isProposingToStop() {
    return proposingToStop;
  }

  /**
   * @param proposingToStop the proposingToStop to set
   */
  public void setProposingToStop(boolean proposingToStop) {
    this.proposingToStop = proposingToStop;
  }

  public void setDynamic(boolean dynamic) {
    this.dynamic = dynamic;
  }

  public synchronized void addDataRow(final IRowMeta rowMeta, final Object[] rowData) {

    if (shell == null || shell.isDisposed()) {
      return;
    }

    HopGui.getInstance()
        .getDisplay()
        .syncExec(
            () -> {
              if (wFields.isDisposed()) {
                return;
              }

              if (waitingForRows) {
                PreviewRowsDialog.this.rowMeta = rowMeta;
                addFields();
              }

              TableItem item = new TableItem(wFields.table, SWT.NONE);
              getDataForRow(item, rowData);
              if (waitingForRows) {
                waitingForRows = false;
                wFields.removeEmptyRows();
                PreviewRowsDialog.this.rowMeta = rowMeta;
                if (wFields.table.getItemCount() < 10) {
                  wFields.optWidth(true);
                }
              }

              if (wFields.table.getItemCount() > PropsUi.getInstance().getDefaultPreviewSize()) {
                wFields.table.remove(0);
              }

              wFields.table.setTopIndex(wFields.table.getItemCount() - 1);
            });
  }

  public void addDialogClosedListener(IDialogClosedListener listener) {
    dialogClosedListeners.add(listener);
  }
}
