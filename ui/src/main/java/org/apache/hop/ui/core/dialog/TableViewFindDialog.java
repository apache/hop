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

import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.ui.core.PropsUi;
import org.apache.hop.ui.core.gui.GuiResource;
import org.apache.hop.ui.core.gui.WindowProperty;
import org.apache.hop.ui.core.widget.TableView;
import org.apache.hop.ui.core.widget.TableViewFind;
import org.apache.hop.ui.pipeline.transform.BaseTransformDialog;
import org.eclipse.swt.SWT;
import org.eclipse.swt.layout.FormAttachment;
import org.eclipse.swt.layout.FormData;
import org.eclipse.swt.layout.FormLayout;
import org.eclipse.swt.widgets.Button;
import org.eclipse.swt.widgets.Label;
import org.eclipse.swt.widgets.Shell;
import org.eclipse.swt.widgets.Table;
import org.eclipse.swt.widgets.TableColumn;
import org.eclipse.swt.widgets.TableItem;
import org.eclipse.swt.widgets.Text;

/** Find-a-value dialog for a {@link TableView}. Stays open across Find first and Find next. */
public class TableViewFindDialog {
  private static final Class<?> PKG = TableViewFindDialog.class;

  private static String lastFind = "";
  private static boolean lastCaseSensitive;
  private static boolean lastRegex;

  private final Shell parent;
  private final TableView tableView;
  private final PropsUi props;

  private Shell shell;
  private Table wColumns;
  private Text wFind;
  private Button wCaseSensitive;
  private Button wRegex;
  private Label wlStatus;
  private TableViewFind.Hit lastHit;

  public TableViewFindDialog(Shell parent, TableView tableView) {
    this.parent = parent;
    this.tableView = tableView;
    this.props = PropsUi.getInstance();
  }

  public void open() {
    if (parent == null || parent.isDisposed() || tableView == null || tableView.isDisposed()) {
      return;
    }
    TableViewFind.Grid grid = tableView.captureFindGrid();
    if (grid == null || grid.columnNames() == null || grid.columnNames().length == 0) {
      return;
    }

    shell = new Shell(parent, SWT.DIALOG_TRIM | SWT.RESIZE | SWT.APPLICATION_MODAL | SWT.SHEET);
    PropsUi.setLook(shell);
    shell.setImage(GuiResource.getInstance().getImageSearch());
    shell.setText(BaseMessages.getString(PKG, "TableViewFindDialog.Shell.Title"));

    FormLayout formLayout = new FormLayout();
    formLayout.marginWidth = PropsUi.getFormMargin();
    formLayout.marginHeight = PropsUi.getFormMargin();
    shell.setLayout(formLayout);

    int margin = PropsUi.getMargin();

    Label wlColumns = new Label(shell, SWT.LEFT);
    PropsUi.setLook(wlColumns);
    wlColumns.setText(BaseMessages.getString(PKG, "TableViewFindDialog.Columns.Label"));
    FormData fdlColumns = new FormData();
    fdlColumns.left = new FormAttachment(0, 0);
    fdlColumns.top = new FormAttachment(0, 0);
    wlColumns.setLayoutData(fdlColumns);

    wColumns =
        new Table(shell, SWT.CHECK | SWT.BORDER | SWT.V_SCROLL | SWT.H_SCROLL | SWT.FULL_SELECTION);
    PropsUi.setLook(wColumns);
    wColumns.setHeaderVisible(false);
    TableColumn nameColumn = new TableColumn(wColumns, SWT.LEFT);
    int[] visual = grid.visualDataColumns();
    if (visual != null) {
      for (int dataColumn : visual) {
        if (dataColumn < 0 || dataColumn >= grid.columnNames().length) {
          continue;
        }
        TableItem item = new TableItem(wColumns, SWT.NONE);
        item.setText(grid.columnNames()[dataColumn]);
        item.setData(dataColumn);
        item.setChecked(true);
      }
    }
    wColumns.addListener(
        SWT.Resize,
        e -> {
          if (!nameColumn.isDisposed()) {
            nameColumn.setWidth(Math.max(wColumns.getClientArea().width, 1));
          }
        });
    FormData fdColumns = new FormData();
    fdColumns.left = new FormAttachment(0, 0);
    fdColumns.top = new FormAttachment(wlColumns, margin);
    fdColumns.right = new FormAttachment(40, 0);
    fdColumns.height = (int) Math.round(200 * PropsUi.getNativeZoomFactor());
    wColumns.setLayoutData(fdColumns);

    Button wAll = new Button(shell, SWT.PUSH);
    wAll.setText(BaseMessages.getString(PKG, "TableViewFindDialog.All.Label"));
    wAll.addListener(SWT.Selection, e -> setAllChecked(true));
    FormData fdAll = new FormData();
    fdAll.left = new FormAttachment(0, 0);
    fdAll.top = new FormAttachment(wColumns, margin);
    wAll.setLayoutData(fdAll);

    Button wNone = new Button(shell, SWT.PUSH);
    wNone.setText(BaseMessages.getString(PKG, "TableViewFindDialog.None.Label"));
    wNone.addListener(SWT.Selection, e -> setAllChecked(false));
    FormData fdNone = new FormData();
    fdNone.left = new FormAttachment(wAll, margin);
    fdNone.top = new FormAttachment(wAll, 0, SWT.CENTER);
    wNone.setLayoutData(fdNone);

    Label wlFind = new Label(shell, SWT.LEFT);
    PropsUi.setLook(wlFind);
    wlFind.setText(BaseMessages.getString(PKG, "TableViewFindDialog.Find.Label"));
    FormData fdlFind = new FormData();
    fdlFind.left = new FormAttachment(wColumns, margin);
    fdlFind.top = new FormAttachment(0, 0);
    wlFind.setLayoutData(fdlFind);

    wFind = new Text(shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    PropsUi.setLook(wFind);
    wFind.setData(BaseDialog.NO_DEFAULT_HANDLER, Boolean.TRUE);
    // Consume Enter here and cancel the traverse so the default Find next button does not run a
    // second search for the same key.
    wFind.addListener(
        SWT.Traverse,
        e -> {
          if (e.detail == SWT.TRAVERSE_RETURN) {
            e.doit = false;
            find(false);
          }
        });
    FormData fdFind = new FormData();
    fdFind.left = new FormAttachment(wlFind, margin);
    fdFind.top = new FormAttachment(wlFind, 0, SWT.CENTER);
    fdFind.right = new FormAttachment(100, 0);
    wFind.setLayoutData(fdFind);

    wCaseSensitive = new Button(shell, SWT.CHECK);
    PropsUi.setLook(wCaseSensitive);
    wCaseSensitive.setText(BaseMessages.getString(PKG, "TableViewFindDialog.CaseSensitive.Label"));
    FormData fdCase = new FormData();
    fdCase.left = new FormAttachment(wColumns, margin);
    fdCase.top = new FormAttachment(wFind, margin);
    fdCase.right = new FormAttachment(100, 0);
    wCaseSensitive.setLayoutData(fdCase);

    wRegex = new Button(shell, SWT.CHECK);
    PropsUi.setLook(wRegex);
    wRegex.setText(BaseMessages.getString(PKG, "TableViewFindDialog.RegularExpression.Label"));
    FormData fdRegex = new FormData();
    fdRegex.left = new FormAttachment(wColumns, margin);
    fdRegex.top = new FormAttachment(wCaseSensitive, margin);
    fdRegex.right = new FormAttachment(100, 0);
    wRegex.setLayoutData(fdRegex);

    wlStatus = new Label(shell, SWT.LEFT);
    PropsUi.setLook(wlStatus);
    FormData fdStatus = new FormData();
    fdStatus.left = new FormAttachment(0, 0);
    fdStatus.right = new FormAttachment(100, 0);
    fdStatus.top = new FormAttachment(wAll, margin);
    wlStatus.setLayoutData(fdStatus);

    Button wFindFirst = new Button(shell, SWT.PUSH);
    wFindFirst.setText(BaseMessages.getString(PKG, "TableViewFindDialog.FindFirst.Button"));
    wFindFirst.addListener(SWT.Selection, e -> find(true));

    Button wFindNext = new Button(shell, SWT.PUSH);
    wFindNext.setText(BaseMessages.getString(PKG, "TableViewFindDialog.FindNext.Button"));
    wFindNext.addListener(SWT.Selection, e -> find(false));

    Button wCancel = new Button(shell, SWT.PUSH);
    wCancel.setText(BaseMessages.getString(PKG, "System.Button.Cancel"));
    wCancel.addListener(SWT.Selection, e -> close());

    BaseTransformDialog.positionBottomButtons(
        shell, new Button[] {wFindFirst, wFindNext, wCancel}, margin, wlStatus);
    shell.setDefaultButton(wFindNext);

    wFind.setText(lastFind == null ? "" : lastFind);
    wCaseSensitive.setSelection(lastCaseSensitive);
    wRegex.setSelection(lastRegex);
    // A changed query, case flag, or regex flag must not resume after the previous hit.
    wFind.addListener(SWT.Modify, e -> lastHit = null);
    wCaseSensitive.addListener(SWT.Selection, e -> lastHit = null);
    wRegex.addListener(SWT.Selection, e -> lastHit = null);
    wFind.selectAll();
    wFind.setFocus();

    BaseDialog.defaultShellHandling(shell, c -> find(false), this::close, false);
  }

  private void setAllChecked(boolean checked) {
    if (wColumns == null || wColumns.isDisposed()) {
      return;
    }
    for (TableItem item : wColumns.getItems()) {
      item.setChecked(checked);
    }
  }

  private boolean[] included(int columnCount) {
    boolean[] included = new boolean[columnCount];
    if (wColumns == null || wColumns.isDisposed()) {
      return included;
    }
    for (TableItem item : wColumns.getItems()) {
      if (!item.getChecked()) {
        continue;
      }
      Object data = item.getData();
      if (data instanceof Integer index && index >= 0 && index < columnCount) {
        included[index] = true;
      }
    }
    return included;
  }

  private void find(boolean fromStart) {
    if (shell == null || shell.isDisposed()) {
      return;
    }
    if (tableView == null || tableView.isDisposed()) {
      close();
      return;
    }
    TableViewFind.Grid grid = tableView.captureFindGrid();
    if (grid == null || grid.columnNames() == null) {
      close();
      return;
    }

    String query = wFind.getText();
    boolean caseSensitive = wCaseSensitive.getSelection();
    boolean regex = wRegex.getSelection();
    lastFind = query;
    lastCaseSensitive = caseSensitive;
    lastRegex = regex;

    boolean continued = !fromStart && lastHit != null;
    int startRow;
    int startColumn;
    boolean inclusive;
    if (fromStart || lastHit == null) {
      if (fromStart) {
        startRow = 0;
        startColumn = -1;
      } else {
        startRow = grid.activeRow();
        startColumn = grid.activeDataColumn();
      }
      inclusive = true;
    } else {
      startRow = lastHit.row();
      startColumn = lastHit.dataColumn();
      inclusive = false;
    }

    TableViewFind.Result result =
        TableViewFind.find(
            grid.cellValues(),
            grid.visualDataColumns(),
            included(grid.columnNames().length),
            query,
            caseSensitive,
            regex,
            startRow,
            startColumn,
            inclusive);

    switch (result.status()) {
      case EMPTY_QUERY ->
          setStatus(BaseMessages.getString(PKG, "TableViewFindDialog.Status.Empty"));
      case INVALID_REGEX ->
          setStatus(
              BaseMessages.getString(
                  PKG,
                  "TableViewFindDialog.Status.InvalidRegex",
                  result.regexMessage() == null ? "" : result.regexMessage()));
      case NOT_FOUND ->
          setStatus(
              BaseMessages.getString(
                  PKG,
                  continued
                      ? "TableViewFindDialog.Status.NoMore"
                      : "TableViewFindDialog.Status.NotFound"));
      case FOUND -> showHit(grid, result.hit());
    }
  }

  private void showHit(TableViewFind.Grid grid, TableViewFind.Hit hit) {
    lastHit = hit;
    tableView.revealFoundCell(hit.row(), hit.dataColumn());
    String name = "";
    if (hit.dataColumn() >= 0 && hit.dataColumn() < grid.columnNames().length) {
      name = grid.columnNames()[hit.dataColumn()];
    }
    setStatus(
        BaseMessages.getString(
            PKG,
            "TableViewFindDialog.Status.Found",
            Integer.toString(hit.row() + 1),
            name == null ? "" : name));
    if (shell != null && !shell.isDisposed()) {
      shell.forceActive();
      wFind.setFocus();
    }
  }

  private void setStatus(String message) {
    if (wlStatus != null && !wlStatus.isDisposed()) {
      wlStatus.setText(message == null ? "" : message);
    }
  }

  private boolean close() {
    if (shell != null && !shell.isDisposed()) {
      props.setScreen(new WindowProperty(shell));
      shell.dispose();
    }
    return true;
  }
}
