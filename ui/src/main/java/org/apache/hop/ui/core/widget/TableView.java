// CHECKSTYLE:FileLength:OFF
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

package org.apache.hop.ui.core.widget;

import org.apache.commons.lang.ArrayUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.hop.core.Condition;
import org.apache.hop.core.Const;
import org.apache.hop.core.Props;
import org.apache.hop.core.RowMetaAndData;
import org.apache.hop.core.exception.HopValueException;
import org.apache.hop.core.logging.LogChannel;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.core.row.RowMeta;
import org.apache.hop.core.row.value.ValueMetaFactory;
import org.apache.hop.core.row.value.ValueMetaInteger;
import org.apache.hop.core.row.value.ValueMetaNumber;
import org.apache.hop.core.row.value.ValueMetaString;
import org.apache.hop.core.undo.ChangeAction;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.ui.core.PropsUi;
import org.apache.hop.ui.core.dialog.EnterConditionDialog;
import org.apache.hop.ui.core.dialog.ErrorDialog;
import org.apache.hop.ui.core.gui.GuiResource;
import org.apache.hop.ui.hopgui.TextSizeUtilFacade;
import org.apache.hop.ui.util.EnvironmentUtils;
import org.eclipse.swt.SWT;
import org.eclipse.swt.SWTException;
import org.eclipse.swt.custom.CCombo;
import org.eclipse.swt.custom.TableEditor;
import org.eclipse.swt.dnd.Clipboard;
import org.eclipse.swt.dnd.DND;
import org.eclipse.swt.dnd.DragSource;
import org.eclipse.swt.dnd.DragSourceEvent;
import org.eclipse.swt.dnd.DragSourceListener;
import org.eclipse.swt.dnd.TextTransfer;
import org.eclipse.swt.dnd.Transfer;
import org.eclipse.swt.events.FocusAdapter;
import org.eclipse.swt.events.FocusEvent;
import org.eclipse.swt.events.KeyAdapter;
import org.eclipse.swt.events.KeyEvent;
import org.eclipse.swt.events.KeyListener;
import org.eclipse.swt.events.ModifyEvent;
import org.eclipse.swt.events.ModifyListener;
import org.eclipse.swt.events.MouseAdapter;
import org.eclipse.swt.events.MouseEvent;
import org.eclipse.swt.events.SelectionAdapter;
import org.eclipse.swt.events.SelectionEvent;
import org.eclipse.swt.events.SelectionListener;
import org.eclipse.swt.events.TraverseListener;
import org.eclipse.swt.graphics.Color;
import org.eclipse.swt.graphics.Point;
import org.eclipse.swt.graphics.Rectangle;
import org.eclipse.swt.layout.FormAttachment;
import org.eclipse.swt.layout.FormData;
import org.eclipse.swt.layout.FormLayout;
import org.eclipse.swt.widgets.Button;
import org.eclipse.swt.widgets.Composite;
import org.eclipse.swt.widgets.Control;
import org.eclipse.swt.widgets.Display;
import org.eclipse.swt.widgets.Event;
import org.eclipse.swt.widgets.Listener;
import org.eclipse.swt.widgets.Menu;
import org.eclipse.swt.widgets.MenuItem;
import org.eclipse.swt.widgets.MessageBox;
import org.eclipse.swt.widgets.ScrollBar;
import org.eclipse.swt.widgets.Table;
import org.eclipse.swt.widgets.TableColumn;
import org.eclipse.swt.widgets.TableItem;
import org.eclipse.swt.widgets.Text;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Hashtable;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Widget to display or modify data, displayed in a Table format.
 *
 * @author Matt
 * @since 27-05-2003
 */
public class TableView extends Composite {

  public interface ITableViewModifyListener {
    void moveRow(int position1, int position2);

    void insertRow(int rowIndex);

    void cellFocusLost(int rowIndex);

    void delete(int[] items);
  }

  private static final Class<?> PKG = TableView.class; // For Translator

  // define CANCEL_KEYS here so that RWT needs not to be imported.
  // HiromuHota/pentaho-kettle#123
  private static final String CANCEL_KEYS = "org.eclipse.rap.rwt.cancelKeys";

  private final Composite parent;
  private final ColumnInfo[] columns;
  private boolean readonly;
  private int buttonRowNr;
  private int buttonColNr;
  private String buttonContent;

  private boolean previousShift;
  private int selectionStart;

  public Table table;

  private TableEditor editor;
  private final TableColumn[] tableColumn;

  private final PropsUi props;

  private Control text;
  private CCombo cCombo;
  private ComboVar comboVar;
  private Button button;

  private TableItem activeTableItem;
  private int activeTableColumn;
  private int activeTableRow;

  private final KeyListener lsKeyText;
  private final KeyListener lsKeyCombo;
  private final FocusAdapter lsFocusText;
  private final FocusAdapter lsFocusCombo;
  private final ModifyListener lsModCombo;
  private final TraverseListener lsTraverse;
  private final Listener lsFocusInTabItem;

  private int sortField;
  private int sortFieldLast;
  private boolean sortingDescending;
  private Boolean sortingDescendingLast;
  private boolean sortable;
  private int lastRowCount;
  private boolean fieldChanged;

  private ModifyListener lsMod;
  private final ModifyListener lsUndo;
  private ModifyListener lsContent;
  private Clipboard clipboard;

  // private int last_carret_position;

  private ArrayList<ChangeAction> undo;
  private int undoPosition;
  private String[] beforeEdit;
  private final MenuItem miEditUndo;
  private final MenuItem miEditRedo;

  private static final String CLIPBOARD_DELIMITER = "\t";

  private Condition condition;
  private final Color defaultBackgroundColor;
  private final Map<String, Color> usedColors;
  private ColumnInfo numberColumn;
  protected int textWidgetCaretPosition;

  private final IVariables variables;

  private boolean showingBlueNullValues;
  private boolean showingConversionErrorsInline;
  private boolean isTextButton = false;
  private boolean addIndexColumn = true;

  private final Color nullTextColor;

  private ITableViewModifyListener tableViewModifyListener =
      new ITableViewModifyListener() {
        @Override
        public void moveRow(int position1, int position2) {}

        @Override
        public void insertRow(int rowIndex) {}

        @Override
        public void cellFocusLost(int rowIndex) {}

        @Override
        public void delete(int[] items) {}
      };

  public TableView(
      IVariables variables,
      Composite parent,
      int style,
      ColumnInfo[] columnInfo,
      int nrRows,
      ModifyListener lsm,
      PropsUi pr) {
    this(variables, parent, style, columnInfo, nrRows, false, lsm, pr);
  }

  public TableView(
      IVariables variables,
      Composite parent,
      int style,
      ColumnInfo[] columnInfo,
      int nrRows,
      boolean readOnly,
      ModifyListener lsm,
      PropsUi pr) {
    this(variables, parent, style, columnInfo, nrRows, readOnly, lsm, pr, true);
  }

  public TableView(
      IVariables variables,
      Composite parent,
      int style,
      ColumnInfo[] columnInfo,
      int nrRows,
      boolean readOnly,
      ModifyListener lsm,
      PropsUi pr,
      final boolean addIndexColumn) {
    this(variables, parent, style, columnInfo, nrRows, readOnly, lsm, pr, addIndexColumn, null);
  }

  public TableView(
      IVariables variables,
      Composite parent,
      int style,
      ColumnInfo[] columnInfo,
      int nrRows,
      boolean readOnly,
      ModifyListener lsm,
      PropsUi pr,
      final boolean addIndexColumn,
      Listener listener) {
    super(parent, SWT.NO_BACKGROUND | SWT.NO_FOCUS | SWT.NO_MERGE_PAINTS | SWT.NO_RADIO_GROUP);
    this.parent = parent;
    this.columns = columnInfo;
    this.props = pr;
    this.readonly = readOnly;
    this.clipboard = null;
    this.variables = variables;
    this.addIndexColumn = addIndexColumn;
    this.lsFocusInTabItem = listener;

    sortField = 0;
    sortFieldLast = -1;
    sortingDescending = false;
    sortingDescendingLast = null;

    nullTextColor = GuiResource.getInstance().getColorBlue();

    sortable = true;

    selectionStart = -1;
    previousShift = false;

    usedColors = new Hashtable<>();

    condition = null;

    lsMod = lsm;

    clearUndo();

    numberColumn = new ColumnInfo("#", ColumnInfo.COLUMN_TYPE_TEXT, true, true);
    IValueMeta numberColumnValueMeta = new ValueMetaNumber("#");
    numberColumnValueMeta.setConversionMask("####0.###");
    numberColumn.setValueMeta(numberColumnValueMeta);

    lsUndo = arg0 -> fieldChanged = true;

    FormLayout controlLayout = new FormLayout();
    controlLayout.marginLeft = 0;
    controlLayout.marginRight = 0;
    controlLayout.marginTop = 0;
    controlLayout.marginBottom = 0;

    setLayout(controlLayout);

    // Create table, add columns & rows...
    table = new Table(this, style | SWT.MULTI);
    props.setLook(table, Props.WIDGET_STYLE_TABLE);
    table.setLinesVisible(true);

    FormData fdTable = new FormData();
    fdTable.left = new FormAttachment(0, 0);
    fdTable.right = new FormAttachment(100, 0);
    fdTable.top = new FormAttachment(0, 0);
    fdTable.bottom = new FormAttachment(100, 0);
    table.setLayoutData(fdTable);

    tableColumn = new TableColumn[columns.length + 1];
    tableColumn[0] = new TableColumn(table, SWT.RIGHT);
    tableColumn[0].setResizable(true);
    tableColumn[0].setText("#");
    tableColumn[0].setWidth(addIndexColumn ? 25 : 0);
    tableColumn[0].setAlignment(SWT.RIGHT);

    for (int i = 0; i < columns.length; i++) {
      int allignment = columns[i].getAlignment();
      tableColumn[i + 1] = new TableColumn(table, allignment);
      tableColumn[i + 1].setResizable(true);
      if (columns[i].getName() != null) {
        tableColumn[i + 1].setText(columns[i].getName());
      }
      if (columns[i].getToolTip() != null) {
        tableColumn[i + 1].setToolTipText((columns[i].getToolTip()));
      }
      IValueMeta valueMeta = columns[i].getValueMeta();
      if (valueMeta != null && valueMeta.isNumeric()) {
        tableColumn[i + 1].setAlignment(SWT.RIGHT);
      }
      tableColumn[i + 1].pack();
    }

    table.setHeaderVisible(true);
    table.setLinesVisible(true);

    // Set the default values...
    if (nrRows > 0) {
      table.setItemCount(nrRows);
    } else {
      table.setItemCount(1);
    }

    // Get the background color of item 0, before anything happened with it,
    // that's the default color.
    defaultBackgroundColor = table.getItem(0).getBackground();

    setRowNums();

    // Set the sort sign on the first column. (0)
    table.setSortColumn(table.getColumn(sortField));
    table.setSortDirection(sortingDescending ? SWT.DOWN : SWT.UP);

    // create a ControlEditor field to edit the contents of a cell
    editor = new TableEditor(table);
    editor.grabHorizontal = true;
    editor.grabVertical = true;

    Menu mRow = new Menu(table);
    MenuItem miRowInsBef = new MenuItem(mRow, SWT.NONE);
    miRowInsBef.setText(
        OsHelper.customizeMenuitemText(
            BaseMessages.getString(PKG, "TableView.menu.InsertBeforeRow")));
    MenuItem miRowInsAft = new MenuItem(mRow, SWT.NONE);
    miRowInsAft.setText(
        OsHelper.customizeMenuitemText(
            BaseMessages.getString(PKG, "TableView.menu.InsertAfterRow")));
    new MenuItem(mRow, SWT.SEPARATOR);
    MenuItem miRowUp = new MenuItem(mRow, SWT.NONE);
    miRowUp.setText(
        OsHelper.customizeMenuitemText(BaseMessages.getString(PKG, "TableView.menu.MoveUp")));
    MenuItem miRowDown = new MenuItem(mRow, SWT.NONE);
    miRowDown.setText(
        OsHelper.customizeMenuitemText(BaseMessages.getString(PKG, "TableView.menu.MoveDown")));
    MenuItem miCol1 = new MenuItem(mRow, SWT.NONE);
    miCol1.setText(
        OsHelper.customizeMenuitemText(
            BaseMessages.getString(PKG, "TableView.menu.OptimalSizeWithHeader")));
    MenuItem miCol2 = new MenuItem(mRow, SWT.NONE);
    miCol2.setText(
        OsHelper.customizeMenuitemText(
            BaseMessages.getString(PKG, "TableView.menu.OptimalSizeWithoutHeader")));
    new MenuItem(mRow, SWT.SEPARATOR);
    MenuItem miClear = new MenuItem(mRow, SWT.NONE);
    miClear.setText(
        OsHelper.customizeMenuitemText(BaseMessages.getString(PKG, "TableView.menu.ClearAll")));
    new MenuItem(mRow, SWT.SEPARATOR);
    MenuItem miSelAll = new MenuItem(mRow, SWT.NONE);
    miSelAll.setText(
        OsHelper.customizeMenuitemText(BaseMessages.getString(PKG, "TableView.menu.SelectAll")));
    MenuItem miUnselAll = new MenuItem(mRow, SWT.NONE);
    miUnselAll.setText(
        OsHelper.customizeMenuitemText(
            BaseMessages.getString(PKG, "TableView.menu.ClearSelection")));
    MenuItem miFilter = new MenuItem(mRow, SWT.NONE);
    miFilter.setText(
        OsHelper.customizeMenuitemText(
            BaseMessages.getString(PKG, "TableView.menu.FilteredSelection")));
    new MenuItem(mRow, SWT.SEPARATOR);
    MenuItem miClipAll = new MenuItem(mRow, SWT.NONE);
    miClipAll.setText(
        OsHelper.customizeMenuitemText(
            BaseMessages.getString(PKG, "TableView.menu.CopyToClipboard")));
    MenuItem miPasteAll = new MenuItem(mRow, SWT.NONE);
    miPasteAll.setText(
        OsHelper.customizeMenuitemText(
            BaseMessages.getString(PKG, "TableView.menu.PasteFromClipboard")));
    MenuItem miCutAll = new MenuItem(mRow, SWT.NONE);
    miCutAll.setText(
        OsHelper.customizeMenuitemText(BaseMessages.getString(PKG, "TableView.menu.CutSelected")));
    MenuItem miDelAll = new MenuItem(mRow, SWT.NONE);
    miDelAll.setText(
        OsHelper.customizeMenuitemText(
            BaseMessages.getString(PKG, "TableView.menu.DeleteSelected")));
    MenuItem miKeep = new MenuItem(mRow, SWT.NONE);
    miKeep.setText(
        OsHelper.customizeMenuitemText(BaseMessages.getString(PKG, "TableView.menu.KeepSelected")));
    new MenuItem(mRow, SWT.SEPARATOR);
    MenuItem miCopyToAll = new MenuItem(mRow, SWT.NONE);
    miCopyToAll.setText(
        OsHelper.customizeMenuitemText(
            BaseMessages.getString(PKG, "TableView.menu.CopyFieldToAllRows")));
    new MenuItem(mRow, SWT.SEPARATOR);
    miEditUndo = new MenuItem(mRow, SWT.NONE);
    miEditRedo = new MenuItem(mRow, SWT.NONE);
    setUndoMenu();

    if (readonly) {
      miRowInsBef.setEnabled(false);
      miRowInsAft.setEnabled(false);
      miRowUp.setEnabled(false);
      miRowDown.setEnabled(false);
      miClear.setEnabled(false);
      miCopyToAll.setEnabled(false);
      miPasteAll.setEnabled(false);
      miDelAll.setEnabled(false);
      miCutAll.setEnabled(false);
      miKeep.setEnabled(false);
    }

    SelectionAdapter lsRowInsBef =
        new SelectionAdapter() {
          @Override
          public void widgetSelected(SelectionEvent e) {
            insertRowBefore();
          }
        };
    SelectionAdapter lsRowInsAft =
        new SelectionAdapter() {
          @Override
          public void widgetSelected(SelectionEvent e) {
            insertRowAfter();
          }
        };
    SelectionAdapter lsCol1 =
        new SelectionAdapter() {
          @Override
          public void widgetSelected(SelectionEvent e) {
            optWidth(true);
          }
        };
    SelectionAdapter lsCol2 =
        new SelectionAdapter() {
          @Override
          public void widgetSelected(SelectionEvent e) {
            optWidth(false);
          }
        };
    SelectionAdapter lsRowUp =
        new SelectionAdapter() {
          @Override
          public void widgetSelected(SelectionEvent e) {
            moveRows(-1);
          }
        };
    SelectionAdapter lsRowDown =
        new SelectionAdapter() {
          @Override
          public void widgetSelected(SelectionEvent e) {
            moveRows(+1);
          }
        };
    SelectionAdapter lsClear =
        new SelectionAdapter() {
          @Override
          public void widgetSelected(SelectionEvent e) {
            clearAll(true);
          }
        };
    SelectionAdapter lsClipAll =
        new SelectionAdapter() {
          @Override
          public void widgetSelected(SelectionEvent e) {
            clipSelected();
          }
        };
    SelectionAdapter lsCopyToAll =
        new SelectionAdapter() {
          @Override
          public void widgetSelected(SelectionEvent e) {
            copyToAll();
          }
        };
    SelectionAdapter lsSelAll =
        new SelectionAdapter() {
          @Override
          public void widgetSelected(SelectionEvent e) {
            selectAll();
          }
        };
    SelectionAdapter lsUnselAll =
        new SelectionAdapter() {
          @Override
          public void widgetSelected(SelectionEvent e) {
            unselectAll();
          }
        };
    SelectionAdapter lsPasteAll =
        new SelectionAdapter() {
          @Override
          public void widgetSelected(SelectionEvent e) {
            pasteSelected();
          }
        };
    SelectionAdapter lsCutAll =
        new SelectionAdapter() {
          @Override
          public void widgetSelected(SelectionEvent e) {
            cutSelected();
          }
        };
    SelectionAdapter lsDelAll =
        new SelectionAdapter() {
          @Override
          public void widgetSelected(SelectionEvent e) {
            delSelected();
          }
        };
    SelectionAdapter lsKeep =
        new SelectionAdapter() {
          @Override
          public void widgetSelected(SelectionEvent e) {
            keepSelected();
          }
        };
    SelectionAdapter lsFilter =
        new SelectionAdapter() {
          @Override
          public void widgetSelected(SelectionEvent e) {
            setFilter();
          }
        };
    SelectionAdapter lsEditUndo =
        new SelectionAdapter() {
          @Override
          public void widgetSelected(SelectionEvent e) {
            undoAction();
          }
        };
    SelectionAdapter lsEditRedo =
        new SelectionAdapter() {
          @Override
          public void widgetSelected(SelectionEvent e) {
            redoAction();
          }
        };

    miRowInsBef.addSelectionListener(lsRowInsBef);
    miRowInsAft.addSelectionListener(lsRowInsAft);
    miCol1.addSelectionListener(lsCol1);
    miCol2.addSelectionListener(lsCol2);
    miRowUp.addSelectionListener(lsRowUp);
    miRowDown.addSelectionListener(lsRowDown);
    miClear.addSelectionListener(lsClear);
    miClipAll.addSelectionListener(lsClipAll);
    miCopyToAll.addSelectionListener(lsCopyToAll);
    miSelAll.addSelectionListener(lsSelAll);
    miUnselAll.addSelectionListener(lsUnselAll);
    miPasteAll.addSelectionListener(lsPasteAll);
    miCutAll.addSelectionListener(lsCutAll);
    miDelAll.addSelectionListener(lsDelAll);
    miKeep.addSelectionListener(lsKeep);
    miFilter.addSelectionListener(lsFilter);
    miEditUndo.addSelectionListener(lsEditUndo);
    miEditRedo.addSelectionListener(lsEditRedo);

    table.setMenu(mRow);

    lsFocusText =
        new FocusAdapter() {
          @Override
          public void focusLost(FocusEvent e) {
            if (isWrongLostFocusEvent()) {
              return;
            }

            final Display d = Display.getCurrent();

            if (table.isDisposed()) {
              return;
            }
            final TableItem row = activeTableItem;
            if (row == null) {
              return;
            }
            final int colNr = activeTableColumn;
            final int rowNr = table.indexOf(row);
            final Control ftext = text;

            final String[] fBeforeEdit = beforeEdit;

            // Save the position of the caret for the focus-dropping popup-dialogs
            // The content is then in contentDestination
            textWidgetCaretPosition = getTextWidgetCaretPosition(colNr);

            final String value = getTextWidgetValue(colNr);

            final Runnable worker =
                () -> {
                  try {
                    if (row.isDisposed()) {
                      return;
                    }
                    row.setText(colNr, value);
                    ftext.dispose();

                    String[] afterEdit = getItemText(row);
                    checkChanged(
                        new String[][] {fBeforeEdit},
                        new String[][] {afterEdit},
                        new int[] {rowNr});
                  } catch (Exception ignored) {
                    // widget is disposed, ignore
                  }
                };

            // force the immediate update
            if (!row.isDisposed()) {
              row.setText(colNr, value);
            }

            if (columns[colNr - 1].getType() == ColumnInfo.COLUMN_TYPE_TEXT_BUTTON) {
              try {
                Thread.sleep(500);
              } catch (InterruptedException ignored) {
                // ignored
              }
              Runnable r = () -> d.asyncExec(worker);
              Thread t = new Thread(r);
              t.start();
            } else {
              worker.run();
            }
            tableViewModifyListener.cellFocusLost(rowNr);
          }

          /**
           * This is a workaround for SWT bug (see PDI-15268). Calling a context menu should be
           * ignored in SWT org.eclipse.swt.widgets.Control#gtk_event_after
           *
           * @return true if it is wrong event
           */
          private boolean isWrongLostFocusEvent() {
            Control controlGotFocus = Display.getCurrent().getCursorControl();
            return Const.isLinux() && (controlGotFocus == null || text.equals(controlGotFocus));
          }
        };
    lsFocusCombo =
        new FocusAdapter() {
          @Override
          public void focusLost(FocusEvent e) {
            TableItem row = activeTableItem;
            if (row == null) {
              return;
            }
            int colNr = activeTableColumn;
            int rowNr = table.indexOf(row);

            if (colNr > 0) {
              try {
                boolean usingVariables = columns[colNr - 1].isUsingVariables();
                if (usingVariables) {
                  row.setText(colNr, comboVar.getText());
                } else {
                  row.setText(colNr, cCombo.getText());
                }
              } catch (Exception exc) {
                // Eat widget disposed error
              }

              String[] afterEdit = getItemText(row);
              if (afterEdit != null) {
                checkChanged(
                    new String[][] {beforeEdit}, new String[][] {afterEdit}, new int[] {rowNr});
              }
            }
            tableViewModifyListener.cellFocusLost(rowNr);
          }
        };
    lsModCombo =
        e -> {
          TableItem row = activeTableItem;
          if (row == null) {
            return;
          }
          int colNr = activeTableColumn;
          int rowNr = table.indexOf(row);
          boolean usingVariables = columns[colNr - 1].isUsingVariables();
          if (usingVariables) {
            row.setText(colNr, comboVar.getText());
          } else {
            row.setText(colNr, cCombo.getText());
          }

          String[] afterEdit = getItemText(row);
          checkChanged(new String[][] {beforeEdit}, new String[][] {afterEdit}, new int[] {rowNr});
        };

    // Catch the keys pressed when editing a Text-field...
    lsKeyText =
        new KeyAdapter() {
          @Override
          public void keyPressed(KeyEvent e) {
            boolean right = false;
            boolean left = false;

            /*
             * left = e.keyCode == SWT.ARROW_LEFT && last_carret_position==0;
             *
             * if (text!=null && !text.isDisposed()) right = e.keyCode == SWT.ARROW_RIGHT &&
             * last_carret_position==text.getText().length();
             */

            // "ENTER": close the text editor and copy the data over
            // We edit the data after moving to another cell, only if editNextCell =
            // true;
            if (e.character == SWT.CR
                || e.keyCode == SWT.ARROW_DOWN
                || e.keyCode == SWT.ARROW_UP
                || e.keyCode == SWT.TAB
                || left
                || right) {
              if (activeTableItem == null) {
                return;
              }

              applyTextChange(activeTableItem, activeTableRow, activeTableColumn);

              int maxCols = table.getColumnCount();
              int maxRows = table.getItemCount();

              boolean editNextCell = false;
              if (e.keyCode == SWT.ARROW_DOWN && activeTableRow < maxRows - 1) {
                activeTableRow++;
                editNextCell = true;
              }
              if (e.keyCode == SWT.ARROW_UP && activeTableRow > 0) {
                activeTableRow--;
                editNextCell = true;
              }
              // TAB
              if ((e.keyCode == SWT.TAB && ((e.stateMask & SWT.SHIFT) == 0)) || right) {
                activeTableColumn++;
                editNextCell = true;
              }
              // Shift Tab
              if ((e.keyCode == SWT.TAB && ((e.stateMask & SWT.SHIFT) != 0)) || left) {
                activeTableColumn--;
                editNextCell = true;
              }
              if (activeTableColumn < 1) { // from SHIFT-TAB
                activeTableColumn = maxCols - 1;
                if (activeTableRow > 0) {
                  activeTableRow--;
                }
              }
              if (activeTableColumn >= maxCols) { // from TAB
                activeTableColumn = 1;
                activeTableRow++;
              }
              // Tab beyond last line: add a line to table!
              if (activeTableRow >= maxRows) {
                TableItem item = new TableItem(table, SWT.NONE, activeTableRow);
                item.setText(1, "");
                setRowNums();
              }

              activeTableItem = table.getItem(activeTableRow); // just to make sure!

              if (editNextCell) {
                edit(activeTableRow, activeTableColumn);
              } else {
                if (e.keyCode == SWT.ARROW_DOWN && activeTableRow == maxRows - 1) {
                  insertRowAfter();
                }
              }

            } else if (e.keyCode == SWT.ESC) {
              text.dispose();
              // setFocus();
              table.setFocus();
            }

            // last_carret_position = text.isDisposed()?-1:text.getCaretPosition();
          }
        };

    // Catch the keys pressed when editing a Combo field
    lsKeyCombo =
        new KeyAdapter() {
          @Override
          public void keyPressed(KeyEvent e) {
            comboKeyPressed(e);
          }
        };

    /*
     * It seems there is an other keyListener active to help control the cursor. There is support for keys like
     * LEFT/RIGHT/UP/DOWN/HOME/END/etc It presents us with a problem because we only get the position of the row/column
     * AFTER the other listener did it's workflow. Therefor we added global variables prev_rownr and prev_colnr
     */

    KeyListener lsKeyTable =
        new KeyAdapter() {
          @Override
          public void keyPressed(KeyEvent e) {
            if (activeTableItem == null) {
              return;
            }

            int maxcols = table.getColumnCount();
            int maxrows = table.getItemCount();

            boolean shift = (e.stateMask & SWT.SHIFT) != 0;
            if (!previousShift && shift || selectionStart < 0) {
              // Shift is pressed down: reset start of selection
              // No start of selection known? reset as well.
              selectionStart = activeTableRow;
            }
            previousShift = shift;
            boolean ctrl = ((e.stateMask & SWT.MOD1) != 0);

            // Move rows up or down shortcuts...
            if (!readonly && e.keyCode == SWT.ARROW_DOWN && ctrl) {
              moveRows(+1);
              e.doit = false;
              return;
            }

            if (!readonly && e.keyCode == SWT.ARROW_UP && ctrl) {
              moveRows(-1);
              e.doit = false;
              return;
            }

            // Select extra row down
            if (e.keyCode == SWT.ARROW_DOWN && shift) {
              activeTableRow++;
              if (activeTableRow >= maxrows) {
                activeTableRow = maxrows - 1;
              }

              selectRows(selectionStart, activeTableRow);
              // activeTableItem = table.getItem(activeTableRow);
              table.showItem(table.getItem(activeTableRow));
              e.doit = false;
              return;
            }

            // Select extra row up
            if (e.keyCode == SWT.ARROW_UP && shift) {
              activeTableRow--;
              if (activeTableRow < 0) {
                activeTableRow = 0;
              }

              selectRows(activeTableRow, selectionStart);
              // activeTableItem = table.getItem(activeTableRow);

              table.showItem(table.getItem(activeTableRow));
              e.doit = false;
              return;
            }

            // Select all rows until end
            if (e.keyCode == SWT.HOME && shift) {
              activeTableRow = 0;

              // Select all indices from "from_selection" to "row"
              //
              selectRows(selectionStart, activeTableRow);
              table.showItem(activeTableItem);
              e.doit = false;
              return;
            }

            // Select extra row up
            if (e.keyCode == SWT.END && shift) {
              activeTableRow = maxrows;

              selectRows(selectionStart, activeTableRow);
              table.showItem(activeTableItem);
              e.doit = false;
              return;
            }

            // Move cursor: set selection on the row in question.
            if ((e.keyCode == SWT.ARROW_DOWN && !shift)
                || (e.keyCode == SWT.ARROW_UP && !shift)
                || (e.keyCode == SWT.HOME && !shift)
                || (e.keyCode == SWT.END && !shift)) {
              switch (e.keyCode) {
                case SWT.ARROW_DOWN:
                  activeTableRow++;
                  if (activeTableRow >= maxrows) {
                    if (!readonly) {
                      insertRowAfter();
                    } else {
                      activeTableRow = maxrows - 1;
                    }
                  }
                  break;
                case SWT.ARROW_UP:
                  activeTableRow--;
                  if (activeTableRow < 0) {
                    activeTableRow = 0;
                  }
                  break;
                case SWT.HOME:
                  activeTableRow = 0;
                  break;
                case SWT.END:
                  activeTableRow = maxrows - 1;
                  break;
                default:
                  break;
              }
              setPosition(activeTableRow, activeTableColumn);
              table.deselectAll();
              table.select(activeTableRow);
              table.showItem(table.getItem(activeTableRow));
              e.doit = false;
              return;
            }

            // CTRL-A --> Select All lines
            if (e.keyCode == 'a' && ctrl) {
              e.doit = false;
              selectAll();
              return;
            }

            // ESC --> unselect all
            if (e.keyCode == SWT.ESC) {
              e.doit = false;
              unselectAll();
              selectRows(activeTableRow, activeTableRow);
              setFocus();
              // table.setFocus();
              return;
            }

            // CTRL-C --> Copy selected lines to clipboard
            if (e.keyCode == 'c' && ctrl) {
              e.doit = false;
              clipSelected();
              return;
            }

            // CTRL-K --> keep only selected lines
            if (!readonly && e.keyCode == 'k' && ctrl) {
              e.doit = false;
              keepSelected();
              return;
            }

            // CTRL-X --> Cut selected infomation...
            if (!readonly && e.keyCode == 'x' && ctrl) {
              e.doit = false;
              cutSelected();
              return;
            }

            // CTRL-V --> Paste selected infomation...
            if (!readonly && e.keyCode == 'v' && ctrl) {
              e.doit = false;
              pasteSelected();
              return;
            }

            // F3 --> optimal width including headers
            if (e.keyCode == SWT.F3) {
              e.doit = false;
              optWidth(true);
              return;
            }

            // DEL --> delete selected lines
            if (!readonly && e.keyCode == SWT.DEL) {
              e.doit = false;
              delSelected();
              return;
            }

            // F4 --> optimal width excluding headers
            if (e.keyCode == SWT.F4) {
              e.doit = false;
              optWidth(false);
              return;
            }

            // CTRL-Y --> redo action
            if (e.keyCode == 'y' && ctrl) {
              e.doit = false;
              redoAction();
              return;
            }

            // CTRL-Z --> undo action
            if (e.keyCode == 'z' && ctrl) {
              e.doit = false;
              undoAction();
              return;
            }

            // Return: edit the first field in the row.
            if (e.keyCode == SWT.CR || e.keyCode == SWT.ARROW_RIGHT || e.keyCode == SWT.TAB) {
              activeTableColumn = 1;
              edit(activeTableRow, activeTableColumn);
              e.doit = false;
              return;
            }

            if (activeTableColumn > 0) {
              boolean textChar =
                  (e.character >= 'a' && e.character <= 'z')
                      || (e.character >= 'A' && e.character <= 'Z')
                      || (e.character >= '0' && e.character <= '9')
                      || (e.character == ' ')
                      || (e.character == '_')
                      || (e.character == ',')
                      || (e.character == '.')
                      || (e.character == '+')
                      || (e.character == '-')
                      || (e.character == '*')
                      || (e.character == '/')
                      || (e.character == ';');

              // setSelection(row, rowNr, colNr);
              // character a-z, A-Z, 0-9: start typing...
              if (e.character == SWT.CR || e.keyCode == SWT.F2 || textChar) {
                boolean selectText = true;
                char extraChar = 0;

                if (textChar) {
                  extraChar = e.character;
                  selectText = false;
                }
                e.doit = false;
                edit(activeTableRow, activeTableColumn, selectText, extraChar);
              }
              if (e.character == SWT.TAB) {
                // TAB
                if (e.keyCode == SWT.TAB && ((e.stateMask & SWT.SHIFT) == 0)) {
                  activeTableColumn++;
                }
                // Shift Tab
                if (e.keyCode == SWT.TAB && ((e.stateMask & SWT.SHIFT) != 0)) {
                  activeTableColumn--;
                }
                if (activeTableColumn < 1) { // from SHIFT-TAB
                  activeTableColumn = maxcols - 1;
                  if (activeTableRow > 0) {
                    activeTableRow--;
                  }
                }
                if (activeTableColumn >= maxcols) { // from TAB
                  activeTableColumn = 1;
                  activeTableRow++;
                }
                // Tab beyond last line: add a line to table!
                if (activeTableRow >= maxrows) {
                  TableItem item = new TableItem(table, SWT.NONE, activeTableRow);
                  item.setText(1, "");
                  setRowNums();
                }
                // row = table.getItem(rowNr);
                e.doit = false;
                edit(activeTableRow, activeTableColumn);
              }
            }

            setFocus();
            table.setFocus();
          }
        };
    table.addKeyListener(lsKeyTable);

    // Table listens to the mouse:
    MouseAdapter lsMouseT =
        new MouseAdapter() {
          @Override
          public void mouseDown(MouseEvent event) {
            if (activeTableItem != null
                && !activeTableItem.isDisposed()
                && editor != null
                && editor.getEditor() != null
                && !editor.getEditor().isDisposed()) {
              if (activeTableColumn > 0) {
                switch (columns[activeTableColumn - 1].getType()) {
                  case ColumnInfo.COLUMN_TYPE_TEXT:
                    applyTextChange(activeTableItem, activeTableRow, activeTableColumn);
                    break;
                  case ColumnInfo.COLUMN_TYPE_CCOMBO:
                    applyComboChange(activeTableItem, activeTableRow, activeTableColumn);
                    break;
                }
              }
            }
            // if ( event.button == 1 ) {
            boolean rightClick = event.button == 3;
            if (event.button == 1 || rightClick) {
              boolean shift = (event.stateMask & SWT.SHIFT) != 0;
              boolean control = (event.stateMask & SWT.MOD1) != 0;
              if (!shift && !control) {
                Rectangle clientArea = table.getClientArea();
                Point pt = new Point(event.x, event.y);
                int index = table.getTopIndex();
                while (index < table.getItemCount()) {
                  boolean visible = false;
                  final TableItem item = table.getItem(index);
                  for (int i = 0; i < table.getColumnCount(); i++) {
                    Rectangle rect = item.getBounds(i);
                    if (rect.contains(pt)) {
                      activeTableItem = item;
                      activeTableColumn = i;
                      activeTableRow = index;

                      if (!rightClick) {
                        editSelected();
                      }
                      return;
                    } else {
                      if (i == table.getColumnCount() - 1
                          && // last column
                          pt.x > rect.x + rect.width
                          && // to the right
                          pt.y >= rect.y
                          && pt.y <= rect.y + rect.height // same
                      // height
                      // as this
                      // visible
                      // item
                      ) {
                        return; // don't do anything when clicking to the right of
                        // the grid.
                      }
                    }
                    if (!visible && rect.intersects(clientArea)) {
                      visible = true;
                    }
                  }
                  if (!visible) {
                    return;
                  }
                  index++;
                }
                if (rightClick) {
                  return;
                }
                // OK, so they clicked in the table and we did not go into the
                // invisible: below the last line!
                // Position on last row, 1st column and add a new line...
                setPosition(table.getItemCount() - 1, 1);
                insertRowAfter();
              }
            }
          }
        };

    table.addMouseListener(lsMouseT);

    // Add support for sorted columns!
    //
    final int nrcols = tableColumn.length;
    for (int i = 0; i < nrcols; i++) {
      final int colNr = i;
      tableColumn[i].addListener(
          SWT.Selection,
          e -> {
            // Sorting means: clear undo information!
            clearUndo();

            sortTable(colNr);
          });
    }

    lsTraverse = e -> e.doit = false;
    table.addTraverseListener(lsTraverse);
    table.setData(CANCEL_KEYS, new String[] {"TAB", "SHIFT+TAB"});

    // Clean up the clipboard
    addDisposeListener(
        e -> {
          if (clipboard != null) {
            clipboard.dispose();
            clipboard = null;
          }
        });

    // Drag & drop source!

    // Drag & Drop for table-viewer
    Transfer[] ttypes = new Transfer[] {TextTransfer.getInstance()};

    DragSource ddSource = new DragSource(table, DND.DROP_MOVE | DND.DROP_COPY);
    ddSource.setTransfer(ttypes);
    ddSource.addDragListener(
        new DragSourceListener() {
          @Override
          public void dragStart(DragSourceEvent event) {}

          @Override
          public void dragSetData(DragSourceEvent event) {
            event.data = "TableView" + Const.CR + getSelectedText();
          }

          @Override
          public void dragFinished(DragSourceEvent event) {}
        });

    table.layout();
    table.pack();

    optWidth(true);

    layout();
    pack();
  }

  private void comboKeyPressed(KeyEvent e) {

    // "ENTER": close the text editor and copy the data over
    //
    if (e.keyCode == SWT.CR || e.keyCode == SWT.TAB) {
      if (activeTableItem == null) {
        return;
      }

      applyComboChange(activeTableItem, activeTableRow, activeTableColumn);

      String[] afterEdit = getItemText(activeTableItem);
      checkChanged(
          new String[][] {beforeEdit}, new String[][] {afterEdit}, new int[] {activeTableRow});

      int maxCols = table.getColumnCount();
      int maxRows = table.getItemCount();

      boolean sel = false;
      // TAB
      if ((e.keyCode == SWT.TAB && ((e.stateMask & SWT.SHIFT) == 0))) {
        activeTableColumn++;
        sel = true;
      }
      // Shift Tab
      if ((e.keyCode == SWT.TAB && ((e.stateMask & SWT.SHIFT) != 0))) {
        activeTableColumn--;
        sel = true;
      }

      if (activeTableColumn < 1) { // from SHIFT-TAB
        activeTableColumn = maxCols - 1;
        if (activeTableRow > 0) {
          activeTableRow--;
        }
      }
      if (activeTableColumn >= maxCols) { // from TAB
        activeTableColumn = 1;
        activeTableRow++;
      }
      // Tab beyond last line: add a line to table!
      if (activeTableRow >= maxRows) {
        TableItem item = new TableItem(table, SWT.NONE, activeTableRow);
        item.setText(1, "");
        setRowNums();
      }
      if (sel) {
        edit(activeTableRow, activeTableColumn);
      }
      table.setFocus();
    }

    if (e.keyCode == SWT.ESC) {
      if (activeTableItem != null) {
        activeTableItem.setText(activeTableColumn, beforeEdit[activeTableColumn - 1]);
      }
      ColumnInfo columnInfo = columns[activeTableColumn-1];
      if (columnInfo.isUsingVariables()) {
        comboVar.setVisible( false );
      } else {
        cCombo.setVisible( false );
      }
      table.setFocus();
      e.doit = false;
    }
  }

  private void safelyDisposeControl(Control combo) {
    if (combo == null) {
      return;
    }
    synchronized (combo) {
      if (combo.isDisposed()) {
        return;
      }
      combo.dispose();
    }
  }

  protected String getTextWidgetValue(int colNr) {
    boolean b = columns[colNr - 1].isUsingVariables();
    if (b) {
      return ((TextVar) text).getText();
    } else {
      return ((Text) text).getText();
    }
  }

  protected int getTextWidgetCaretPosition(int colNr) {
    if (colNr >= 0) {
      boolean b = columns[colNr - 1].isUsingVariables();
      if (b) {
        return ((TextVar) text).getTextWidget().getCaretPosition();
      } else {
        return ((Text) text).getCaretPosition();
      }
    } else {
      return -1;
    }
  }

  public void sortTable(int colNr) {
    if (!sortable) {
      return;
    }

    if (sortField == colNr) {
      sortingDescending = (!sortingDescending);
    } else {
      sortField = colNr;
      sortingDescending = false;
    }

    sortTable(sortField, sortingDescending);
  }

  public void setSelection(int[] selectedItems) {
    table.select(selectedItems);
  }

  public void sortTable(int sortField, boolean sortingDescending) {
    boolean shouldRefresh = false;
    if (this.sortFieldLast == -1 && this.sortingDescendingLast == null) {
      // first time through, so update
      shouldRefresh = true;
      this.sortFieldLast = this.sortField;
      this.sortingDescendingLast = new Boolean(this.sortingDescending);

      this.sortField = sortField;
      this.sortingDescending = sortingDescending;
    }

    if (sortFieldLast != this.sortField) {
      this.sortFieldLast = this.sortField;
      this.sortField = sortField;
      shouldRefresh = true;
    }

    if (sortingDescendingLast != this.sortingDescending) {
      this.sortingDescendingLast = this.sortingDescending;
      this.sortingDescending = sortingDescending;
      shouldRefresh = true;
    }

    if (!shouldRefresh && table.getItemCount() == lastRowCount) {
      return;
    }

    removeEmptyRows();

    try {
      // First, get all info and put it in a Vector of Rows...
      TableItem[] items = table.getItems();
      List<Object[]> v = new ArrayList<>();

      // First create the row metadata for the grid
      //
      final IRowMeta rowMeta = new RowMeta();

      // First values are the color name + value!
      rowMeta.addValueMeta(new ValueMetaString("colorname"));
      rowMeta.addValueMeta(new ValueMetaInteger("color"));
      for (int j = 0; j < table.getColumnCount(); j++) {
        ColumnInfo colInfo;
        if (j > 0) {
          colInfo = columns[j - 1];
        } else {
          colInfo = numberColumn;
        }

        IValueMeta valueMeta = colInfo.getValueMeta();
        if (j == sortField) {
          valueMeta.setSortedDescending(sortingDescending);
        }

        rowMeta.addValueMeta(valueMeta);
      }

      final IRowMeta sourceRowMeta = rowMeta.cloneToType(IValueMeta.TYPE_STRING);
      final IRowMeta conversionRowMeta = rowMeta.clone();

      // Set it all to string...
      // Also set the storage value metadata: this will allow us to convert back
      // and forth without a problem.
      //
      for (int i = 0; i < sourceRowMeta.size(); i++) {
        IValueMeta sourceValueMeta = sourceRowMeta.getValueMeta(i);
        sourceValueMeta.setStorageType(IValueMeta.STORAGE_TYPE_NORMAL);

        IValueMeta conversionMetaData = conversionRowMeta.getValueMeta(i);
        conversionMetaData.setStorageType(IValueMeta.STORAGE_TYPE_NORMAL);

        // Meaning: this string comes from an Integer/Number/Date/etc.
        //
        sourceRowMeta.getValueMeta(i).setConversionMetadata(conversionMetaData);
      }

      // Now populate a list of data rows...
      //
      for (int i = 0; i < items.length; i++) {
        TableItem item = items[i];
        Object[] r = new Object[table.getColumnCount() + 2];

        // First values are the color name + value!
        Color bg = item.getBackground();
        if (!bg.equals(defaultBackgroundColor)) {
          String colorName = "bg " + bg.toString();
          r[0] = colorName;
          r[1] = new Long((bg.getRed() << 16) + (bg.getGreen() << 8) + (bg.getBlue()));
          // Save it in the used colors map!
          usedColors.put(colorName, bg);
        }

        for (int j = 0; j < table.getColumnCount(); j++) {
          String data = item.getText(j);
          if (GuiResource.getInstance().getColorBlue().equals(item.getForeground(j))) {
            data = null;
          }
          IValueMeta sourceValueMeta = sourceRowMeta.getValueMeta(j + 2);
          try {
            r[j + 2] = sourceValueMeta.convertDataUsingConversionMetaData(data);
          } catch (Exception e) {
            if (isShowingConversionErrorsInline()) {
              r[j + 2] = Const.getStackTracker(e);
            } else {
              throw e;
            }
          }
        }
        v.add(r);
      }

      final int[] sortIndex = new int[] {sortField + 2};

      // Sort the vector!
      Collections.sort(
          v,
          (r1, r2) -> {
            try {
              return conversionRowMeta.compare(r1, r2, sortIndex);
            } catch (HopValueException e) {
              throw new RuntimeException("Error comparing rows", e);
            }
          });

      // Clear the table
      table.removeAll();

      // Refill the table
      for (int i = 0; i < v.size(); i++) {
        Object[] r = v.get(i);
        TableItem item = new TableItem(table, SWT.NONE);

        String colorName = (String) r[0];
        Long colorValue = (Long) r[1];
        if (colorValue != null) {
          // Get it from the map
          //
          Color bg = usedColors.get(colorName);
          if (bg != null) {
            item.setBackground(bg);
          }
        }

        for (int j = 2; j < r.length; j++) {
          String string = conversionRowMeta.getString(r, j);
          if (showingBlueNullValues && string == null) {
            string = "<null>";
            item.setForeground(j - 2, nullTextColor);
          } else {
            item.setForeground(j - 2, GuiResource.getInstance().getColorBlack());
          }
          if (string != null) {
            item.setText(j - 2, string);
          }
        }
      }
      table.setSortColumn(table.getColumn(this.sortField));
      table.setSortDirection(sortingDescending ? SWT.DOWN : SWT.UP);

      lastRowCount = table.getItemCount();
    } catch (Exception e) {
      new ErrorDialog(
          this.getShell(),
          BaseMessages.getString(PKG, "TableView.ErrorDialog.title"),
          BaseMessages.getString(PKG, "TableView.ErrorDialog.description"),
          e);
    }
  }

  private void selectRows(int from, int to) {
    table.deselectAll();
    if (from == to) {
      table.select(from);
    } else {
      if (from > to) {
        table.select(to, from);
      } else {
        table.select(from, to);
      }
    }
  }

  private void applyTextChange(TableItem row, int rowNr, int colNr) {
    String textData = getTextWidgetValue(colNr);

    row.setText(colNr, textData);
    text.dispose();
    table.setFocus();

    tableViewModifyListener.cellFocusLost(rowNr);

    String[] afterEdit = getItemText(row);
    checkChanged(new String[][] {beforeEdit}, new String[][] {afterEdit}, new int[] {rowNr});

    selectionStart = -1;

    fireContentChangedListener(rowNr, colNr, textData);
  }

  /**
   * Inform the content listener that content changed.
   *
   * @param rowNr
   * @param colNr
   * @param textData
   */
  private void fireContentChangedListener(int rowNr, int colNr, String textData) {

    if (lsContent != null) {
      Event event = new Event();
      event.data = textData;
      event.widget = table;
      event.x = rowNr;
      event.y = colNr;

      lsContent.modifyText(new ModifyEvent(event));
    }
  }

  private void applyComboChange(TableItem row, int rowNr, int colNr) {
    String textData;
    boolean usingVariables = columns[colNr - 1].isUsingVariables();
    if (usingVariables) {
      textData = comboVar.getText();
    } else {
      textData = cCombo.getText();
    }
    row.setText(colNr, textData);
    if (usingVariables) {
      comboVar.setVisible(false);
    } else {
      cCombo.setVisible(false);
    }

    String[] afterEdit = getItemText(row);
    checkChanged(new String[][] {beforeEdit}, new String[][] {afterEdit}, new int[] {rowNr});

    selectionStart = -1;

    fireContentChangedListener(rowNr, colNr, textData);
  }

  public void addModifyListener(ModifyListener ls) {
    lsMod = ls;
  }

  public void setColumnInfo(int idx, ColumnInfo col) {
    columns[idx] = col;
  }

  public void setColumnText(int idx, String text) {
    TableColumn col = table.getColumn(idx);
    col.setText(text);
  }

  public void setColumnToolTip(int idx, String text) {
    columns[idx].setToolTip(text);
  }

  private void editSelected() {
    if (activeTableItem == null) {
      return;
    }

    if (activeTableColumn > 0) {
      edit(activeTableRow, activeTableColumn);
    } else {
      selectRows(activeTableRow, activeTableRow);
    }
  }

  private void checkChanged(String[][] before, String[][] after, int[] index) {
    // Did we change anything: if so, add undo information
    if (fieldChanged) {
      ChangeAction ta = new ChangeAction();
      ta.setChanged(before, after, index);
      addUndo(ta);
    }
  }

  private void setModified() {
    if (lsMod != null) {
      Event e = new Event();
      e.widget = this;
      lsMod.modifyText(new ModifyEvent(e));
    }
  }

  private void insertRowBefore() {
    if (readonly) {
      return;
    }

    TableItem row = activeTableItem;
    if (row == null) {
      return;
    }
    int rowNr = table.indexOf(row);

    insertRow(rowNr);
  }

  private void insertRowAfter() {
    if (readonly) {
      return;
    }

    TableItem row = activeTableItem;
    if (row == null) {
      return;
    }
    int rowNr = table.indexOf(row);

    insertRow(rowNr + 1);
  }

  private void insertRow(int ronr) {
    TableItem item = new TableItem(table, SWT.NONE, ronr);
    item.setText(1, "");

    // Add undo information
    ChangeAction ta = new ChangeAction();
    String[] str = getItemText(item);
    ta.setNew(new String[][] {str}, new int[] {ronr});
    addUndo(ta);

    setRowNums();

    edit(ronr, 1);
    tableViewModifyListener.insertRow(ronr);
  }

  public void clearAll() {
    clearAll(false);
  }

  public void clearAll(boolean ask) {
    int id = SWT.YES;
    if (ask) {
      MessageBox mb = new MessageBox(parent.getShell(), SWT.YES | SWT.NO | SWT.ICON_QUESTION);
      mb.setMessage(BaseMessages.getString(PKG, "TableView.MessageBox.ClearTable.message"));
      mb.setText(BaseMessages.getString(PKG, "TableView.MessageBox.ClearTable.title"));
      id = mb.open();
    }

    if (id == SWT.YES) {
      table.removeAll();
      new TableItem(table, SWT.NONE);
      if (!readonly) {
        parent.getDisplay().asyncExec(() -> edit(0, 1));
      }
      this.setModified(); // timh
    }
  }

  private void moveRows(int offset) {
    if ((offset != 1) && (offset != -1)) {
      return;
    }

    int[] selectionIndicies = table.getSelectionIndices();
    int selectedIndex = table.getSelectionIndex();

    // selectionIndicies is not guaranteed to be in any order so must sort
    // before using
    Arrays.sort(selectionIndicies);

    if (offset == 1) {
      if (selectionIndicies[selectionIndicies.length - 1] >= table.getItemCount() - 1) {
        // If the last row in the table is selected then don't move any rows
        // down
        return;
      }
      selectionIndicies = moveRowsDown(selectionIndicies);
    } else {
      if (selectionIndicies[0] == 0) {
        // If the first row in the table is selected then don't move any rows up
        return;
      }
      selectionIndicies = moveRowsUp(selectionIndicies);
    }

    activeTableRow = selectedIndex + offset;
    table.setSelection(activeTableRow);
    table.setSelection(selectionIndicies);
    activeTableItem = table.getItem(activeTableRow);
  }

  private int[] moveRowsDown(int[] selectionIndicies) {
    // Move the selected rows down starting with the lowest row
    for (int i = selectionIndicies.length - 1; i >= 0; i--) {
      int row = selectionIndicies[i];
      int newRow = row + 1;
      moveRow(row, newRow);
      ChangeAction ta = new ChangeAction();
      ta.setItemMove(new int[] {row}, new int[] {newRow});
      addUndo(ta);
      selectionIndicies[i] = newRow;
    }
    return selectionIndicies;
  }

  private int[] moveRowsUp(int[] selectionIndicies) {
    // Move the selected rows up starting with the highest row
    for (int i = 0; i < selectionIndicies.length; i++) {
      int row = selectionIndicies[i];
      int newRow = row - 1;
      moveRow(row, newRow);
      ChangeAction ta = new ChangeAction();
      ta.setItemMove(new int[] {row}, new int[] {newRow});
      addUndo(ta);
      selectionIndicies[i] = newRow;
    }
    return selectionIndicies;
  }

  private void moveRow(int from, int to) {
    TableItem rowfrom = table.getItem(from);
    TableItem rowto = table.getItem(to);

    // Grab the strings on that line...
    String[] strfrom = getItemText(rowfrom);
    String[] strto = getItemText(rowto);

    // Copy the content
    for (int i = 0; i < strfrom.length; i++) {
      rowfrom.setText(i + 1, strto[i]);
      rowto.setText(i + 1, strfrom[i]);
    }
    tableViewModifyListener.moveRow(from, to);

    setModified();
  }

  private void copyToAll() {
    TableItem row = activeTableItem;
    if (row == null || row.isDisposed()) {
      return;
    }
    int colNr = activeTableColumn;

    if (colNr == 0) {
      return;
    }

    String str = row.getText(colNr);

    // Get undo information: all columns
    int size = table.getItemCount();

    String[][] before = new String[size][];
    String[][] after = new String[size][];
    int[] index = new int[size];

    for (int i = 0; i < table.getItemCount(); i++) {
      TableItem item = table.getItem(i);

      index[i] = i;
      before[i] = getItemText(item);

      item.setText(colNr, str);

      after[i] = getItemText(item);
    }

    // Add the undo information!
    ChangeAction ta = new ChangeAction();
    ta.setChanged(before, after, index);
    addUndo(ta);
  }

  private void selectAll() {
    table.selectAll();
  }

  private void unselectAll() {
    table.deselectAll();
  }

  private void clipSelected() {
    if (clipboard != null) {
      clipboard.dispose();
      clipboard = null;
    }

    clipboard = new Clipboard(getDisplay());
    TextTransfer tran = TextTransfer.getInstance();

    String clip = getSelectedText();

    if (clip == null) {
      return;
    }

    clipboard.setContents(new String[] {clip}, new Transfer[] {tran});
  }

  private String getSelectedText() {
    String selection = "";

    for (int c = 1; c < table.getColumnCount(); c++) {
      TableColumn tc = table.getColumn(c);
      if (c > 1) {
        selection += CLIPBOARD_DELIMITER;
      }
      selection += tc.getText();
    }
    selection += Const.CR;

    TableItem[] items = table.getSelection();
    if (items.length == 0) {
      return null;
    }

    // Table.getSelection() of RWT are ordered reversely.
    // HiromuHota/pentaho-kettle#156
    if (EnvironmentUtils.getInstance().isWeb()) {
      ArrayUtils.reverse(items);
    }

    for (int r = 0; r < items.length; r++) {
      TableItem ti = items[r];
      for (int c = 1; c < table.getColumnCount(); c++) {
        ColumnInfo ci = columns[c - 1];
        if (c > 1) {
          selection += CLIPBOARD_DELIMITER;
        }
        String value = ti.getText(c);
        if (StringUtils.isNotEmpty(value)) {
          Color textColor = ti.getForeground(c);
          if (!nullTextColor.equals(textColor) || !"<null>".equals(value)) {
            selection += ti.getText(c);
          }
        }
      }
      selection += Const.CR;
    }
    return selection;
  }

  /*
   * Example: ----------------------------------------------------------------- Field in stream;Dimension field
   * TIME;TIME DATA_TYPE;DATA_TYPE MAP_TYPE;MAP_TYPE RESOLUTION;RESOLUTION START_TIME;START_TIME
   * -----------------------------------------------------------------
   *
   * !! Paste at the end of the table! --> Create new table item for every line
   */

  private int getCurrentRownr() {
    if (table.getItemCount() <= 1) {
      return 0;
    }

    TableItem row = activeTableItem;
    if (row == null) {
      return 0;
    }
    int rowNr = table.indexOf(row);

    if (rowNr < 0) {
      rowNr = 0;
    }

    return rowNr;
  }

  private void pasteSelected() {
    int rowNr = getCurrentRownr();

    if (clipboard != null) {
      clipboard.dispose();
      clipboard = null;
    }

    clipboard = new Clipboard(getDisplay());
    TextTransfer tran = TextTransfer.getInstance();

    String text = (String) clipboard.getContents(tran);

    if (text != null) {
      String[] lines = text.split(Const.CR);
      if (lines.length > 1) {
        // ALlocate complete paste grid!
        String[][] grid = new String[lines.length - 1][];
        int[] idx = new int[lines.length - 1];

        for (int i = 1; i < lines.length; i++) {
          grid[i - 1] = lines[i].split("\t");
          idx[i - 1] = rowNr + i;
          addItem(idx[i - 1], grid[i - 1]);
        }

        ChangeAction ta = new ChangeAction();
        ta.setNew(grid, idx);
        addUndo(ta);
      }
      if (rowNr == 0 && table.getItemCount() > rowNr + 1) {
        // Empty row at rowNr?
        // Remove it!

        if (isEmpty(rowNr, -1)) {
          table.remove(rowNr);
        }
      }
      setRowNums();
      unEdit();

      setModified();
    }
  }

  private void addItem(int pos, String[] str) {
    TableItem item = new TableItem(table, SWT.NONE, pos);
    for (int i = 0; i < str.length; i++) {
      item.setText(i + 1, str[i]);
    }
    setModified();
  }

  private void cutSelected() {
    clipSelected(); // copy selected lines to clipboard
    delSelected();
  }

  private void delSelected() {
    if (nrNonEmpty() == 0) {
      return;
    }

    // Which items do we delete?
    int[] items = table.getSelectionIndices();

    if (items.length == 0) {
      return;
    }

    // Save undo information
    String[][] before = new String[items.length][];
    for (int i = 0; i < items.length; i++) {
      TableItem ti = table.getItem(items[i]);
      before[i] = getItemText(ti);
    }

    ChangeAction ta = new ChangeAction();
    ta.setDelete(before, items);
    addUndo(ta);

    TableItem row = activeTableItem;
    if (row == null) {
      return;
    }
    int rowbefore = table.indexOf(row);

    // Delete selected items.
    table.remove(items);

    if (table.getItemCount() == 0) {
      TableItem item = new TableItem(table, SWT.NONE);
      // Save undo infomation!
      String[] stritem = getItemText(item);
      ta = new ChangeAction();
      ta.setNew(new String[][] {stritem}, new int[] {0});
      addUndo(ta);
    }

    // If the last row is gone, put the selection back on last-1!
    if (rowbefore >= table.getItemCount()) {
      rowbefore = table.getItemCount() - 1;
    }

    // After the delete, we put the cursor on the same row as before (if we can)
    if (rowbefore < table.getItemCount() && table.getItemCount() > 0) {
      setPosition(rowbefore, 1);
      table.setSelection(rowbefore);
      activeTableRow = rowbefore;
    }
    tableViewModifyListener.delete(items);
    setRowNums();

    setModified();
  }

  private void keepSelected() {
    // Which items are selected?
    int[] sels = table.getSelectionIndices();

    int size = table.getItemCount();

    // Which items do we delete?
    int[] items = new int[size - sels.length];

    if (items.length == 0) {
      return; // everything is selected: keep everything, do nothing.
    }

    // Set the item-indices to delete...
    int nr = 0;
    for (int i = 0; i < table.getItemCount(); i++) {
      boolean selected = false;
      for (int j = 0; j < sels.length && !selected; j++) {
        if (sels[j] == i) {
          selected = true;
        }
      }
      if (!selected) {
        items[nr] = i;
        nr++;
      }
    }

    // Save undo information
    String[][] before = new String[items.length][];
    for (int i = 0; i < items.length; i++) {
      TableItem ti = table.getItem(items[i]);
      before[i] = getItemText(ti);
    }

    ChangeAction ta = new ChangeAction();
    ta.setDelete(before, items);
    addUndo(ta);

    // Delete selected items.
    table.remove(items);

    if (table.getItemCount() == 0) {
      TableItem item = new TableItem(table, SWT.NONE);
      // Save undo infomation!
      String[] stritem = getItemText(item);
      ta = new ChangeAction();
      ta.setNew(new String[][] {stritem}, new int[] {0});
      addUndo(ta);
    }

    /*
     * try { table.getRow(); } catch(Exception e) // Index is too high: lower to last available value {
     * setPosition(table.getItemCount()-1, 1); }
     */

    setRowNums();

    setModified();
  }

  private void setPosition(int rowNr, int colNr) {
    activeTableColumn = colNr;
    activeTableRow = rowNr;
    if (rowNr >= 0) {
      activeTableItem = table.getItem(rowNr);
    }
  }

  public void edit(int rowNr, int colNr) {
    setPosition(rowNr, colNr);
    edit(rowNr, colNr, true, (char) 0);
  }

  private void edit(int rowNr, int colNr, boolean selectText, char extra) {
    selectionStart = -1;

    TableItem row = table.getItem(rowNr);

    Control oldEditor = editor.getEditor();
    if (oldEditor != null && !oldEditor.isDisposed()) {
      try {
        oldEditor.dispose();
      } catch (SWTException swte) {
        // Eat "Widget Is Disposed Exception" : did you ever!!!
      }
    }

    activeTableItem = table.getItem(activeTableRow); // just to make sure, clean
    // up afterwards.
    table.showItem(row);
    table.setSelection(new TableItem[] {row});

    if (columns.length == 0) {
      return;
    }

    switch (columns[colNr - 1].getType()) {
      case ColumnInfo.COLUMN_TYPE_TEXT:
        isTextButton = false;
        editText(row, rowNr, colNr, selectText, extra, columns[colNr - 1]);
        break;
      case ColumnInfo.COLUMN_TYPE_CCOMBO:
      case ColumnInfo.COLUMN_TYPE_FORMAT:
        editCombo(row, rowNr, colNr);
        break;
      case ColumnInfo.COLUMN_TYPE_BUTTON:
        editButton(row, rowNr, colNr);
        break;
      case ColumnInfo.COLUMN_TYPE_TEXT_BUTTON:
        if (columns[colNr - 1].shouldRenderTextVarButton()) {
          isTextButton = true;
        } else {
          isTextButton = false;
        }
        editText(row, rowNr, colNr, selectText, extra, columns[colNr - 1]);
        break;
      default:
        break;
    }
  }

  private String[] getItemText(TableItem row) {
    if (row.isDisposed()) {
      return null;
    }

    String[] retval = new String[table.getColumnCount() - 1];
    for (int i = 0; i < retval.length; i++) {
      retval[i] = row.getText(i + 1);
    }

    return retval;
  }

  private void editText(
      TableItem row,
      final int rowNr,
      final int colNr,
      boolean selectText,
      char extra,
      ColumnInfo columnInfo) {
    beforeEdit = getItemText(row);
    fieldChanged = false;

    ColumnInfo colinfo = columns[colNr - 1];

    if (colinfo.isReadOnly()) {
      return;
    }

    if (colinfo.getDisabledListener() != null) {
      boolean disabled = colinfo.getDisabledListener().isFieldDisabled(rowNr);
      if (disabled) {
        return;
      }
    }

    if (text != null && !text.isDisposed()) {
      text.dispose();
    }

    if (colinfo.getSelectionAdapter() != null) {
      Event e = new Event();
      e.widget = this;
      e.x = colNr;
      e.y = rowNr;
      columns[colNr - 1].getSelectionAdapter().widgetSelected(new SelectionEvent(e));
      return;
    }

    String content = row.getText(colNr) + (extra != 0 ? "" + extra : "");
    String tooltip = columns[colNr - 1].getToolTip();

    final boolean useVariables = columns[colNr - 1].isUsingVariables();
    final boolean passwordField = columns[colNr - 1].isPasswordField();

    final ModifyListener modifyListener = me -> setColumnWidthBasedOnTextField(colNr, useVariables);

    if (useVariables) {
      IGetCaretPosition getCaretPositionInterface =
          () -> ((TextVar) text).getTextWidget().getCaretPosition();

      // The text widget will be disposed when we get here
      // So we need to write to the table row
      //
      IInsertText insertTextInterface =
          (string, position) -> {
            StringBuilder buffer = new StringBuilder(table.getItem(rowNr).getText(colNr));
            buffer.insert(position, string);
            table.getItem(rowNr).setText(colNr, buffer.toString());
            int newPosition = position + string.length();
            edit(rowNr, colNr);
            ((TextVar) text).setSelection(newPosition);
            ((TextVar) text).showSelection();
            setColumnWidthBasedOnTextField(colNr, useVariables);
          };

      final TextVar textWidget;
      if (passwordField) {
        textWidget =
            new PasswordTextVar(
                variables, table, SWT.NONE, getCaretPositionInterface, insertTextInterface);
      } else if (isTextButton) {
        textWidget =
            new TextVarButton(
                variables,
                table,
                SWT.NONE,
                getCaretPositionInterface,
                insertTextInterface,
                columnInfo.getTextVarButtonSelectionListener());
      } else {
        textWidget =
            new TextVar(variables, table, SWT.NONE, getCaretPositionInterface, insertTextInterface);
      }

      text = textWidget;
      textWidget.setText(content);
      if (lsMod != null) {
        textWidget.addModifyListener(lsMod);
      }
      textWidget.addModifyListener(lsUndo);
      textWidget.setSelection(content.length());
      // last_carret_position = content.length();
      textWidget.addKeyListener(lsKeyText);
      // Make the column larger so we can still see the string we're entering...
      textWidget.addModifyListener(modifyListener);
      if (selectText) {
        textWidget.selectAll();
      }
      if (tooltip != null) {
        textWidget.setToolTipText(tooltip);
      } else {
        textWidget.setToolTipText("");
      }
      textWidget.addTraverseListener(lsTraverse);
      textWidget.setData(CANCEL_KEYS, new String[] {"TAB", "SHIFT+TAB"});
      textWidget.addFocusListener(lsFocusText);
    } else {
      Text textWidget = new Text(table, SWT.NONE);
      text = textWidget;
      textWidget.setText(content);
      if (lsMod != null) {
        textWidget.addModifyListener(lsMod);
      }
      textWidget.addModifyListener(lsUndo);
      textWidget.setSelection(content.length());
      // last_carret_position = content.length();
      textWidget.addKeyListener(lsKeyText);
      // Make the column larger so we can still see the string we're entering...
      textWidget.addModifyListener(modifyListener);
      if (selectText) {
        textWidget.selectAll();
      }
      if (tooltip != null) {
        textWidget.setToolTipText(tooltip);
      } else {
        textWidget.setToolTipText("");
      }
      textWidget.addTraverseListener(lsTraverse);
      textWidget.setData(CANCEL_KEYS, new String[] {"TAB", "SHIFT+TAB"});
      textWidget.addFocusListener(lsFocusText);
    }
    props.setLook(text, Props.WIDGET_STYLE_TABLE);

    int width = tableColumn[colNr].getWidth();
    int height = 30;

    editor.horizontalAlignment = SWT.LEFT;
    editor.grabHorizontal = true;

    // Open the text editor in the correct column of the selected row.
    editor.setEditor(text, row, colNr);

    text.setFocus();
    text.setSize(width, height);
    editor.layout();
  }

  private void setColumnWidthBasedOnTextField(final int colNr, final boolean useVariables) {
    if (!columns[colNr - 1].isAutoResize()) {
      return;
    }
    String str = getTextWidgetValue(colNr);

    int strmax = TextSizeUtilFacade.textExtent(str).x + 20;
    int colmax = tableColumn[colNr].getWidth();
    if (strmax > colmax) {
      if (!EnvironmentUtils.getInstance().isWeb()) {
        if (Const.isOSX() || Const.isLinux()) {
          strmax *= 1.4;
        }
      }
      tableColumn[colNr].setWidth(strmax + 30);

      // On linux, this causes the text to select everything...
      // This is because the focus is lost and re-gained. Nothing we can do
      // about it now.

      if (useVariables) {
        TextVar widget = (TextVar) text;
        int idx = widget.getTextWidget().getCaretPosition();
        widget.selectAll();
        widget.showSelection();
        widget.setSelection(0);
        widget.showSelection();
        widget.setSelection(idx);
      } else {
        Text widget = (Text) text;
        int idx = widget.getCaretPosition();
        widget.selectAll();
        widget.showSelection();
        widget.setSelection(0);
        widget.showSelection();
        widget.setSelection(idx);
      }
    }
  }

  private String[] getComboValues(TableItem row, ColumnInfo colinfo) {
    if (colinfo.getType() == ColumnInfo.COLUMN_TYPE_FORMAT) {
      int type = ValueMetaFactory.getIdForValueMeta(row.getText(colinfo.getFieldTypeColumn()));
      switch (type) {
        case IValueMeta.TYPE_DATE:
          return Const.getDateFormats();
        case IValueMeta.TYPE_INTEGER:
        case IValueMeta.TYPE_BIGNUMBER:
        case IValueMeta.TYPE_NUMBER:
          return Const.getNumberFormats();
        case IValueMeta.TYPE_STRING:
          return Const.getConversionFormats();
        default:
          return new String[0];
      }
    }
    return colinfo.getComboValues();
  }

  private void editCombo(TableItem item, int rowNr, int colNr) {
    beforeEdit = getItemText(item);
    fieldChanged = false;
    ColumnInfo columnInfo = columns[colNr - 1];

    if (columnInfo.isReadOnly() && columnInfo.getSelectionAdapter() != null) {
      return;
    }

    if (columnInfo.getDisabledListener() != null) {
      boolean disabled = columnInfo.getDisabledListener().isFieldDisabled(rowNr);
      if (disabled) {
        return;
      }
    }

    String[] opt = getComboValues(item, columnInfo);
    if (columnInfo.getComboValuesSelectionListener() != null) {
      opt = columnInfo.getComboValuesSelectionListener().getComboValues(item, rowNr, colNr);
    }

    final boolean useVariables = columnInfo.isUsingVariables();
    if (useVariables) {
      IGetCaretPosition getCaretPositionInterface = () -> 0;

      // Widget will be disposed when we get here
      // So we need to write to the table row
      //
      IInsertText insertTextInterface =
          (string, position) -> {
            StringBuilder buffer = new StringBuilder(table.getItem(rowNr).getText(colNr));
            buffer.insert(position, string);
            table.getItem(rowNr).setText(colNr, buffer.toString());
            edit(rowNr, colNr);
            setModified();
          };

      safelyDisposeControl(comboVar);

      comboVar =
          new ComboVar(
              variables,
              table,
              SWT.SINGLE | SWT.LEFT,
              getCaretPositionInterface,
              insertTextInterface);
      if (lsFocusInTabItem != null) {
        comboVar.getCComboWidget().addListener(SWT.FocusIn, lsFocusInTabItem);
      } else {
        comboVar.setItems(opt);
      }
      props.setLook(comboVar, Props.WIDGET_STYLE_TABLE);
      comboVar.addTraverseListener(lsTraverse);
      comboVar.setData(CANCEL_KEYS, new String[] {"TAB", "SHIFT+TAB"});
      comboVar.addModifyListener(lsModCombo);
      comboVar.addFocusListener(lsFocusCombo);

      comboVar.setText(item.getText(colNr));

      if (lsMod != null) {
        comboVar.addModifyListener(lsMod);
      }
      comboVar.addModifyListener(lsUndo);
      comboVar.setToolTipText(columnInfo.getToolTip() == null ? "" : columnInfo.getToolTip());
      comboVar.setVisible(true);
      comboVar.addKeyListener(lsKeyCombo);

      // Set the bottom so the combovar fits inside the table cell
      ((FormData) comboVar.getCComboWidget().getLayoutData()).bottom = new FormAttachment(100, 0);

      editor.horizontalAlignment = SWT.LEFT;
      editor.layout();

      // Open the text editor in the correct column of the selected row.
      editor.setEditor(comboVar, item, colNr);
      comboVar.setFocus();
      comboVar.layout();
    } else {
      safelyDisposeControl(cCombo);

      cCombo = new CCombo(table, columnInfo.isReadOnly() ? SWT.READ_ONLY : SWT.NONE);
      props.setLook(cCombo, Props.WIDGET_STYLE_TABLE);
      cCombo.addTraverseListener(lsTraverse);
      cCombo.setData(CANCEL_KEYS, new String[] {"TAB", "SHIFT+TAB"});
      cCombo.addModifyListener(lsModCombo);
      cCombo.addFocusListener(lsFocusCombo);

      cCombo.setItems(opt);
      cCombo.setVisibleItemCount(opt.length);
      cCombo.setText(item.getText(colNr));
      if (lsMod != null) {
        cCombo.addModifyListener(lsMod);
      }
      cCombo.addModifyListener(lsUndo);
      cCombo.setToolTipText(columnInfo.getToolTip() == null ? "" : columnInfo.getToolTip());
      cCombo.setVisible(true);
      cCombo.addKeyListener(lsKeyCombo);
      if (columnInfo.getSelectionAdapter() != null) {
        cCombo.addSelectionListener(columns[colNr - 1].getSelectionAdapter());
      }
      editor.horizontalAlignment = SWT.LEFT;
      editor.layout();

      // Open the text editor in the correct column of the selected row.
      editor.setEditor(cCombo, item, colNr);
      cCombo.setFocus();
      cCombo.layout();
    }
  }

  private void editButton(TableItem row, int rowNr, int colNr) {
    beforeEdit = getItemText(row);
    fieldChanged = false;

    ColumnInfo columnInfo = columns[colNr - 1];

    if (columnInfo.isReadOnly()) {
      return;
    }

    if (columnInfo.getDisabledListener() != null) {
      boolean disabled = columnInfo.getDisabledListener().isFieldDisabled(rowNr);
      if (disabled) {
        return;
      }
    }

    button = new Button(table, SWT.PUSH);
    props.setLook(button, Props.WIDGET_STYLE_TABLE);
    String buttonText = columns[colNr - 1].getButtonText();
    if (buttonText != null) {
      button.setText(buttonText);
    }
    button.setImage(GuiResource.getInstance().getImage("ui/images/edit.svg"));

    SelectionListener selAdpt = columnInfo.getSelectionAdapter();
    if (selAdpt != null) {
      button.addSelectionListener(selAdpt);
    }

    buttonRowNr = rowNr;
    buttonColNr = colNr;

    // button.addTraverseListener(lsTraverse);
    buttonContent = row.getText(colNr);

    String tooltip = columns[colNr - 1].getToolTip();
    if (tooltip != null) {
      button.setToolTipText(tooltip);
    } else {
      button.setToolTipText("");
    }
    button.addTraverseListener(lsTraverse); // hop to next field
    button.setData(CANCEL_KEYS, new String[] {"TAB", "SHIFT+TAB"});
    button.addTraverseListener(arg0 -> closeActiveButton());

    editor.horizontalAlignment = SWT.LEFT;
    editor.verticalAlignment = SWT.TOP;
    editor.grabHorizontal = false;
    editor.grabVertical = false;

    Point size = button.computeSize(SWT.DEFAULT, SWT.DEFAULT);
    editor.minimumWidth = size.x;
    editor.minimumHeight = size.y - 2;

    // setRowNums();
    editor.layout();

    // Open the text editor in the correct column of the selected row.
    editor.setEditor(button);

    button.setFocus();

    // if the button loses focus, destroy it...
    /*
     * button.addFocusListener(new FocusAdapter() { public void focusLost(FocusEvent e) { button.dispose(); } } );
     */
  }

  public void setRowNums() {
    for (int i = 0; i < table.getItemCount(); i++) {
      TableItem item = table.getItem(i);
      if (item != null) {
        String num = "" + (i + 1);
        // for(int j=num.length();j<3;j++) num="0"+num;
        if (!item.getText(0).equals(num)) {
          item.setText(0, num);
        }
      }
    }
  }

  public void optWidth(boolean header) {
    optWidth(header, 0);
  }

  public void optWidth(boolean header, int nrLines) {

    int extraForMargin;
    if (Const.isWindows()) {
      extraForMargin = (int) (PropsUi.getNativeZoomFactor() * 8);
    } else {
      extraForMargin = (int) (PropsUi.getNativeZoomFactor() * 5);
    }

    for (int c = 0; c < table.getColumnCount(); c++) {
      TableColumn tc = table.getColumn(c);
      int max = 0;
      if (header) {
        max = TextSizeUtilFacade.textExtent(tc.getText()).x;

        // Check if the column has a sorted mark set. In that case, we need the
        // header to be a bit wider...
        //
        if (c == sortField && sortable) {
          max += extraForMargin;
        }
      }
      Set<String> columnStrings = new HashSet<>();

      boolean haveToGetTexts = false;
      if (c > 0) {
        final ColumnInfo column = columns[c - 1];
        if (column != null) {
          switch (column.getType()) {
            case ColumnInfo.COLUMN_TYPE_TEXT:
              haveToGetTexts = true;
              break;
            case ColumnInfo.COLUMN_TYPE_CCOMBO:
            case ColumnInfo.COLUMN_TYPE_FORMAT:
              haveToGetTexts = true;
              if (column.getComboValues() != null) {
                for (String comboValue : columns[c - 1].getComboValues()) {
                  columnStrings.add(comboValue);
                }
              }
              break;
            case ColumnInfo.COLUMN_TYPE_BUTTON:
              columnStrings.add(column.getButtonText());
              break;
            default:
              break;
          }
        }
      } else {
        haveToGetTexts = true;
      }

      if (haveToGetTexts) {
        for (int r = 0; r < table.getItemCount() && (r < nrLines || nrLines <= 0); r++) {
          TableItem ti = table.getItem(r);
          if (ti != null) {
            columnStrings.add(ti.getText(c));
          }
        }
      }

      for (String str : columnStrings) {
        int len = TextSizeUtilFacade.textExtent(str == null ? "" : str).x;
        if (len > max) {
          max = len;
        }
      }

      try {
        max += extraForMargin;
        if (c > 0) {
          max += extraForMargin; // margins on both sides of the column
        }
        if (Const.isWindows() || Const.isLinux()) {
          max += extraForMargin;
        }

        // The line number column
        //
        if (c == 0) {
          if (tc.getWidth() != max) {
            tc.setWidth(max);
          }
        } else {
          int desiredWidth = columns[c - 1].getWidth();
          if (desiredWidth > 0) {
            if (tc.getWidth() != desiredWidth) {
              tc.setWidth(desiredWidth);
            }
          } else {
            if (tc.getWidth() != max) {
              tc.setWidth(max);
            }
          }
        }

        if (tc.getWidth() != max) {
          if (c > 0 && columns[c - 1].getWidth() > 0) {
            tc.setWidth(columns[c - 1].getWidth());
          } else {
            tc.setWidth(max);
          }
        }
      } catch (Exception e) {
        // Ignore errors
        LogChannel.UI.logError("error in TableView", e);
      }
    }
    if (table.isListening(SWT.Resize)) {
      Event resizeEvent = new Event();
      resizeEvent.widget = table;
      resizeEvent.type = SWT.Resize;
      resizeEvent.display = getDisplay();
      resizeEvent.setBounds(table.getBounds());
      table.notifyListeners(SWT.Resize, resizeEvent);
    }
    unEdit();
  }

  public void optimizeTableView() {
    removeEmptyRows();
    setRowNums();
    optWidth(true);
  }
  /*
   * Remove empty rows in the table...
   */
  public void removeEmptyRows() {
    removeEmptyRows(-1);
  }

  private boolean isEmpty(int rowNr, int colNr) {
    boolean empty = false;
    TableItem item = table.getItem(rowNr);
    if (item != null) {
      if (colNr >= 0) {
        String str = item.getText(colNr);
        if (str == null || str.length() == 0) {
          empty = true;
        }
      } else {
        empty = true;
        for (int j = 1; j < table.getColumnCount(); j++) {
          String str = item.getText(j);
          if (str != null && str.length() > 0) {
            empty = false;
          }
        }
      }
    }
    return empty;
  }

  public void removeEmptyRows(int column) {
    // Remove "empty" table items, where item.getText(1) is empty, length==0

    for (int i = table.getItemCount() - 1; i >= 0; i--) {
      if (isEmpty(i, column)) {
        table.remove(i);
      }
    }
    if (table.getItemCount() == 0) { // At least one empty row!

      new TableItem(table, SWT.NONE);
    }
  }

  private List<Integer> nonEmptyIndexes;

  public List<Integer> getNonEmptyIndexes() {
    return nonEmptyIndexes;
  }

  /**
   * Count non-empty rows in the table... IMPORTANT: always call this method before calling
   * getNonEmpty(int selnr): for performance reasons we cache the row indexes.
   *
   * @return the number of rows/table-items that are not empty
   */
  public int nrNonEmpty() {
    nonEmptyIndexes = new ArrayList<>();

    // Count only non-empty rows
    for (int i = 0; i < table.getItemCount(); i++) {
      if (!isEmpty(i, -1)) {
        nonEmptyIndexes.add(i);
      }
    }

    return nonEmptyIndexes.size();
  }

  /**
   * Return the row/table-item on the specified index. IMPORTANT: the indexes of the non-empty rows
   * are populated with a call to nrNonEmpty(). Make sure to call that first.
   *
   * @param index the index of the non-empty row/table-item
   * @return the requested non-empty row/table-item
   */
  public TableItem getNonEmpty(int index) {
    int nonEmptyIndex = nonEmptyIndexes.get(index);
    return table.getItem(nonEmptyIndex);
  }

  /**
   * Give back a list with all the non-empty rows in the table...
   *
   * @return the rows/table-items that are not empty
   */
  public List<TableItem> getNonEmptyItems() {
    List<TableItem> list = new ArrayList<>();

    // Count only non-empty rows
    for (int i = 0; i < table.getItemCount(); i++) {
      if (!isEmpty(i, -1)) {
        list.add(table.getItem(i));
      }
    }

    return list;
  }

  public int indexOfString(String str, int column) {
    int nrNonEmptyFields = nrNonEmpty();
    for (int i = 0; i < nrNonEmptyFields; i++) {
      String cmp = getNonEmpty(i).getText(column);
      if (str.equalsIgnoreCase(cmp)) {
        return i;
      }
    }
    return -1;
  }

  @Override
  public ScrollBar getHorizontalBar() {
    return table.getHorizontalBar();
  }

  @Override
  public ScrollBar getVerticalBar() {
    return table.getVerticalBar();
  }

  private void addUndo(ChangeAction ta) {
    while (undo.size() > undoPosition + 1 && undo.size() > 0) {
      int last = undo.size() - 1;
      undo.remove(last);
    }

    undo.add(ta);
    undoPosition++;

    while (undo.size() > props.getMaxUndo()) {
      undo.remove(0);
      undoPosition--;
    }

    setUndoMenu();
  }

  private void undoAction() {
    ChangeAction ta = previousUndo();
    if (ta == null) {
      return;
    }

    // Get the current cursor position
    int rowNr = getCurrentRownr();

    setUndoMenu(); // something changed: change the menu
    switch (ta.getType()) {
        //
        // NEW
        //

        // We created a table item: undo this...
      case NewTableRow:
        int[] idx = ta.getCurrentIndex();
        table.remove(idx);
        for (int i = 0; i < idx.length; i++) {
          if (idx[i] < rowNr) {
            rowNr--; // shift with the rest.
          }
        }
        // See if the table is empty, if so : undo again!!
        if (table.getItemCount() == 0) {
          undoAction();
        }
        setRowNums();
        break;

        //
        // DELETE
        //

        // un-Delete the rows at correct location: re-insert
      case DeleteTableRow:
        idx = ta.getCurrentIndex();
        String[][] str = (String[][]) ta.getCurrent();
        for (int i = 0; i < idx.length; i++) {
          addItem(idx[i], str[i]);

          if (idx[i] <= rowNr) {
            rowNr++;
          }
        }
        setRowNums();
        break;

        //
        // CHANGE
        //

        // Change the item back to the original row-value.
      case ChangeTableRow:
        idx = ta.getCurrentIndex();
        String[][] prev = (String[][]) ta.getPrevious();
        for (int x = 0; x < idx.length; x++) {
          TableItem item = table.getItem(idx[x]);
          for (int i = 0; i < prev[x].length; i++) {
            item.setText(i + 1, prev[x][i]);
          }
        }
        break;

        //
        // POSITION
        //
        // The position of a row has changed...
      case PositionTableRow:
        int[] curr = ta.getCurrentIndex();
        int[] prevIdx = ta.getPreviousIndex();
        for (int i = 0; i < curr.length; i++) {
          moveRow(prevIdx[i], curr[i]);
        }
        setRowNums();
        break;
      default:
        break;
    }

    if (rowNr >= table.getItemCount()) {
      rowNr = table.getItemCount() - 1;
    }
    if (rowNr < 0) {
      rowNr = 0;
    }

    // cursor.setSelection(rowNr, 0);
    selectRows(rowNr, rowNr);
  }

  private void redoAction() {
    ChangeAction ta = nextUndo();
    if (ta == null) {
      return;
    }

    // Get the current cursor position
    int rowNr = getCurrentRownr();

    setUndoMenu(); // something changed: change the menu
    switch (ta.getType()) {
        //
        // NEW
        //
      case NewTableRow:
        int[] idx = ta.getCurrentIndex();
        String[][] str = (String[][]) ta.getCurrent();
        for (int i = 0; i < idx.length; i++) {
          addItem(idx[i], str[i]);
          if (idx[i] <= rowNr) {
            rowNr++; // Shift cursor position with the new items...
          }
        }
        setRowNums();
        break;

        //
        // DELETE
        //
      case DeleteTableRow:
        idx = ta.getCurrentIndex();
        table.remove(idx);
        for (int i = 0; i < idx.length; i++) {
          if (idx[i] < rowNr) {
            rowNr--; // shift with the rest.
          }
        }
        // See if the table is empty, if so : undo again!!
        if (table.getItemCount() == 0) {
          undoAction();
        }
        setRowNums();
        break;

        //
        // CHANGE
        //

      case ChangeTableRow:
        idx = ta.getCurrentIndex();
        String[][] curr = (String[][]) ta.getCurrent();
        for (int x = 0; x < idx.length; x++) {
          TableItem item = table.getItem(idx[x]);
          for (int i = 0; i < curr[x].length; i++) {
            item.setText(i + 1, curr[x][i]);
          }
        }
        break;

        //
        // CHANGE POSITION
        //
      case PositionTableRow:
        int[] currIdx = ta.getCurrentIndex();
        int[] prev = ta.getPreviousIndex();
        for (int i = 0; i < currIdx.length; i++) {
          moveRow(currIdx[i], prev[i]);
        }
        setRowNums();
        break;

      default:
        break;
    }

    if (rowNr >= table.getItemCount()) {
      rowNr = table.getItemCount() - 1;
    }
    if (rowNr < 0) {
      rowNr = 0;
    }

    // cursor.setSelection(rowNr, 0);
    selectRows(rowNr, rowNr);
  }

  private void setUndoMenu() {
    ChangeAction prev = viewPreviousUndo();
    ChangeAction next = viewNextUndo();

    if (miEditUndo.isDisposed() || miEditRedo.isDisposed()) {
      return;
    }

    if (prev != null) {
      miEditUndo.setEnabled(true);
      miEditUndo.setText(
          OsHelper.customizeMenuitemText(
              BaseMessages.getString(PKG, "TableView.menu.Undo", prev.toString())));
    } else {
      miEditUndo.setEnabled(false);
      miEditUndo.setText(
          OsHelper.customizeMenuitemText(
              BaseMessages.getString(PKG, "TableView.menu.UndoNotAvailable")));
    }

    if (next != null) {
      miEditRedo.setEnabled(true);
      miEditRedo.setText(
          OsHelper.customizeMenuitemText(
              BaseMessages.getString(PKG, "TableView.menu.Redo", next.toString())));
    } else {
      miEditRedo.setEnabled(false);
      miEditRedo.setText(
          OsHelper.customizeMenuitemText(
              BaseMessages.getString(PKG, "TableView.menu.RedoNotAvailable")));
    }
  }

  // get previous undo, change position
  private ChangeAction previousUndo() {
    if (undo.isEmpty() || undoPosition < 0) {
      return null; // No undo left!
    }

    ChangeAction retval = undo.get(undoPosition);

    undoPosition--;

    return retval;
  }

  // View previous undo, don't change position
  private ChangeAction viewPreviousUndo() {
    if (undo.isEmpty() || undoPosition < 0) {
      return null; // No undo left!
    }

    ChangeAction retval = undo.get(undoPosition);

    return retval;
  }

  private ChangeAction nextUndo() {
    int size = undo.size();
    if (size == 0 || undoPosition >= size - 1) {
      return null; // no redo left...
    }

    undoPosition++;

    ChangeAction retval = undo.get(undoPosition);

    return retval;
  }

  private ChangeAction viewNextUndo() {
    int size = undo.size();
    if (size == 0 || undoPosition >= size - 1) {
      return null; // no redo left...
    }

    ChangeAction retval = undo.get(undoPosition + 1);

    return retval;
  }

  private void clearUndo() {
    undo = new ArrayList<>();
    undoPosition = -1;
  }

  private Point getButtonPosition() {
    return new Point(buttonColNr, buttonRowNr);
  }

  public String getButtonString() {
    return buttonContent;
  }

  public void setButtonString(String str) {
    Point p = getButtonPosition();
    TableItem item = table.getItem(p.y);
    item.setText(p.x, str);
  }

  public void closeActiveButton() {
    if (button != null && !button.isDisposed()) {
      button.dispose();
    }
  }

  public void unEdit() {
    if (text != null && !text.isDisposed()) {
      text.dispose();
      text = null;
    }
    if (comboVar != null && !comboVar.isDisposed()) {
      comboVar.dispose();
      comboVar = null;
    }
    if (cCombo != null && !cCombo.isDisposed()) {
      cCombo.dispose();
      cCombo = null;
    }
  }

  // Filtering...

  public void setFilter() {
    if (condition == null) {
      condition = new Condition();
    }
    IRowMeta f = getRowWithoutValues();
    EnterConditionDialog ecd = new EnterConditionDialog(parent.getShell(), SWT.NONE, f, condition);
    Condition cond = ecd.open();
    if (cond != null) {
      ArrayList<Integer> tokeep = new ArrayList<>();

      // Apply the condition to the TableView...
      int nr = table.getItemCount();
      for (int i = nr - 1; i >= 0; i--) {
        RowMetaAndData r = getRow(i);
        boolean keep = cond.evaluate(r.getRowMeta(), r.getData());
        if (keep) {
          tokeep.add(Integer.valueOf(i));
        }
      }

      int[] sels = new int[tokeep.size()];
      for (int i = 0; i < sels.length; i++) {
        sels[i] = (tokeep.get(i)).intValue();
      }

      table.setSelection(sels);
    }
  }

  public IRowMeta getRowWithoutValues() {
    IRowMeta f = new RowMeta();
    f.addValueMeta(new ValueMetaInteger("#"));
    for (int i = 0; i < columns.length; i++) {
      f.addValueMeta(new ValueMetaString(columns[i].getName()));
    }
    return f;
  }

  public RowMetaAndData getRow(int nr) {
    TableItem ti = table.getItem(nr);
    IRowMeta rowMeta = getRowWithoutValues();
    Object[] rowData = new Object[rowMeta.size()];

    rowData[0] = new Long(nr);
    for (int i = 1; i < rowMeta.size(); i++) {
      rowData[i] = ti.getText(i);
    }

    return new RowMetaAndData(rowMeta, rowData);
  }

  public int[] getSelectionIndices() {
    return table.getSelectionIndices();
  }

  public int getSelectionIndex() {
    return table.getSelectionIndex();
  }

  public void remove(int index) {
    table.remove(index);
    if (table.getItemCount() == 0) {
      new TableItem(table, SWT.NONE);
    }
  }

  public void remove(int[] index) {
    table.remove(index);
    if (table.getItemCount() == 0) {
      new TableItem(table, SWT.NONE);
    }
  }

  public String getItem(int rowNr, int colNr) {
    TableItem item = table.getItem(rowNr);
    if (item != null) {
      return item.getText(colNr);
    } else {
      return null;
    }
  }

  public void add(String... string) {
    TableItem item = new TableItem(table, SWT.NONE);
    for (int i = 0; i < string.length && i + 1 < table.getColumnCount(); i++) {
      if (string[i] != null) {
        item.setText(i + 1, string[i]);
      }
    }
  }

  public String[] getItem(int rowNr) {
    TableItem item = table.getItem(rowNr);
    if (item != null) {
      return getItemText(item);
    } else {
      return null;
    }
  }

  /**
   * Get all the strings from a certain column as an array
   *
   * @param colNr The column to return
   * @return the column values as a string array.
   */
  public String[] getItems(int colNr) {
    String[] retval = new String[table.getItemCount()];
    for (int i = 0; i < retval.length; i++) {
      TableItem item = table.getItem(i);
      retval[i] = item.getText(colNr + 1);
    }
    return retval;
  }

  public void removeAll() {
    table.removeAll();
    if (table.getItemCount() == 0) {
      new TableItem(table, SWT.NONE);
    }
  }

  public int getItemCount() {
    return table.getItemCount();
  }

  public void setText(String text, int colNr, int rowNr) {
    TableItem item = table.getItem(rowNr);
    item.setText(colNr, text);
  }

  /** @return Returns the readonly. */
  public boolean isReadonly() {
    return readonly;
  }

  /** @param readonly The readonly to set. */
  public void setReadonly(boolean readonly) {
    this.readonly = readonly;
  }

  /** @return the sortable */
  public boolean isSortable() {
    return sortable;
  }

  /** @param sortable the sortable to set */
  public void setSortable(boolean sortable) {
    this.sortable = sortable;

    if (!sortable) {
      table.setSortColumn(null);
    } else {
      table.setSortColumn(table.getColumn(sortField));
    }
  }

  public void setFocusOnFirstEditableField() {
    // Look for the first field that can be edited...
    int rowNr = 0;

    boolean gotOne = false;
    for (int colNr = 0; colNr < columns.length && !gotOne; colNr++) {
      if (!columns[colNr].isReadOnly()) {
        // edit this one...
        gotOne = true;
        activeTableItem = table.getItem(rowNr);
        activeTableColumn = colNr + 1;
        edit(rowNr, colNr + 1);
      }
    }
  }

  @Override
  public void dispose() {
    safelyDisposeControl(text);
    safelyDisposeControl(comboVar);
    safelyDisposeControl(cCombo);
    super.dispose();
  }

  /** @return the getSortField */
  public int getSortField() {
    return sortField;
  }

  /** @return the sortingDescending */
  public boolean isSortingDescending() {
    return sortingDescending;
  }

  /** @param sortingDescending the sortingDescending to set */
  public void setSortingDescending(boolean sortingDescending) {
    this.sortingDescending = sortingDescending;
  }

  public Table getTable() {
    return table;
  }

  /** @return the numberColumn */
  public ColumnInfo getNumberColumn() {
    return numberColumn;
  }

  /** @param numberColumn the numberColumn to set */
  public void setNumberColumn(ColumnInfo numberColumn) {
    this.numberColumn = numberColumn;
  }

  public TableEditor getEditor() {
    return editor;
  }

  public void setEditor(TableEditor editor) {
    this.editor = editor;
  }

  public void applyOSXChanges() {
    if (text != null && !text.isDisposed() && lsFocusText != null) {
      lsFocusText.focusLost(null);
    }
  }

  /** @return the showingBlueNullValues */
  public boolean isShowingBlueNullValues() {
    return showingBlueNullValues;
  }

  /** @param showingBlueNullValues the showingBlueNullValues to set */
  public void setShowingBlueNullValues(boolean showingBlueNullValues) {
    this.showingBlueNullValues = showingBlueNullValues;
  }

  /** @return the lsContent */
  public ModifyListener getContentListener() {
    return lsContent;
  }

  /** @param lsContent the lsContent to set */
  public void setContentListener(ModifyListener lsContent) {
    this.lsContent = lsContent;
  }

  /** @return the showingConversionErrorsInline */
  public boolean isShowingConversionErrorsInline() {
    return showingConversionErrorsInline;
  }

  /** @param showingConversionErrorsInline the showingConversionErrorsInline to set */
  public void setShowingConversionErrorsInline(boolean showingConversionErrorsInline) {
    this.showingConversionErrorsInline = showingConversionErrorsInline;
  }

  /**
   * Returns copy of columns array in order to prevent unintented modifications.
   *
   * @return columns array
   */
  public ColumnInfo[] getColumns() {
    return Arrays.copyOf(columns, columns.length);
  }

  public TableItem getActiveTableItem() {
    return activeTableItem;
  }

  public int getActiveTableColumn() {
    return activeTableColumn;
  }

  public void setTableViewModifyListener(ITableViewModifyListener tableViewModifyListener) {
    this.tableViewModifyListener = tableViewModifyListener;
  }

  public boolean hasIndexColumn() {
    return this.addIndexColumn;
  }
}
