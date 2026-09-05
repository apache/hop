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

package org.apache.hop.ui.hopgui.perspective.database;

import java.util.List;
import lombok.Getter;
import org.apache.hop.core.Const;
import org.apache.hop.core.exception.HopValueException;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.ui.core.FormDataBuilder;
import org.apache.hop.ui.core.PropsUi;
import org.apache.hop.ui.core.dialog.ShowRowsDialog;
import org.apache.hop.ui.core.gui.GuiResource;
import org.apache.hop.ui.core.widget.ColumnInfo;
import org.apache.hop.ui.core.widget.TableView;
import org.eclipse.swt.SWT;
import org.eclipse.swt.custom.CTabFolder;
import org.eclipse.swt.custom.CTabItem;
import org.eclipse.swt.layout.FormLayout;
import org.eclipse.swt.widgets.Button;
import org.eclipse.swt.widgets.Composite;
import org.eclipse.swt.widgets.Label;
import org.eclipse.swt.widgets.TableItem;
import org.eclipse.swt.widgets.Text;

/** Result sets and success/failure text under a SQL editor. */
public class DatabaseResultsPanel extends Composite {

  public static final Class<?> PKG = DatabasePerspective.class;

  @Getter
  public static class QueryResult {
    private final int statementNr;
    private final IRowMeta rowMeta;
    private final List<Object[]> rows;

    public QueryResult(int statementNr, IRowMeta rowMeta, List<Object[]> rows) {
      this.statementNr = statementNr;
      this.rowMeta = rowMeta;
      this.rows = rows;
    }
  }

  private final IVariables variables;
  private final CTabFolder folder;
  private final Text messages;
  private final Button openWindow;

  public DatabaseResultsPanel(Composite parent, IVariables variables, Runnable onClose) {
    super(parent, SWT.NONE);
    this.variables = variables;
    PropsUi.setLook(this);
    setLayout(new FormLayout());

    Label title = new Label(this, SWT.NONE);
    PropsUi.setLook(title);
    title.setText(BaseMessages.getString(PKG, "DatabasePerspective.Results.Title"));
    title.setLayoutData(new FormDataBuilder().top().left().result());

    Button close = new Button(this, SWT.PUSH);
    close.setText(BaseMessages.getString(PKG, "DatabasePerspective.Results.Close"));
    close.setToolTipText(BaseMessages.getString(PKG, "DatabasePerspective.Results.Close.Tooltip"));
    close.setLayoutData(new FormDataBuilder().top().right().result());
    close.addListener(SWT.Selection, e -> onClose.run());

    openWindow = new Button(this, SWT.PUSH);
    openWindow.setText(BaseMessages.getString(PKG, "DatabasePerspective.Results.Float"));
    openWindow.setToolTipText(
        BaseMessages.getString(PKG, "DatabasePerspective.Results.Float.Tooltip"));
    openWindow.setLayoutData(
        new FormDataBuilder().top().right(close, -PropsUi.getMargin()).result());
    openWindow.addListener(SWT.Selection, e -> openSelectedInWindow());
    openWindow.setEnabled(false);

    folder = new CTabFolder(this, SWT.BORDER);
    PropsUi.setLook(folder, PropsUi.WIDGET_STYLE_TAB);
    folder.setLayoutData(
        new FormDataBuilder().top(close, PropsUi.getMargin()).bottom().fullWidth().result());

    CTabItem messagesTab = new CTabItem(folder, SWT.NONE);
    messagesTab.setText(BaseMessages.getString(PKG, "DatabasePerspective.Results.Messages"));
    messages =
        new Text(folder, SWT.MULTI | SWT.READ_ONLY | SWT.H_SCROLL | SWT.V_SCROLL | SWT.BORDER);
    PropsUi.setLook(messages);
    messagesTab.setControl(messages);
    folder.setSelection(0);
    folder.addListener(SWT.Selection, e -> updateFloatEnablement());
  }

  public void show(List<QueryResult> queryResults, String messageText) {
    // Drop previous result-set tabs; keep the messages tab (index 0).
    CTabItem[] items = folder.getItems();
    for (int i = items.length - 1; i >= 1; i--) {
      CTabItem item = items[i];
      if (item.getControl() != null && !item.getControl().isDisposed()) {
        item.getControl().dispose();
      }
      item.dispose();
    }

    messages.setText(Const.NVL(messageText, ""));

    CTabItem firstResultTab = null;
    if (queryResults != null) {
      for (QueryResult result : queryResults) {
        CTabItem tab = new CTabItem(folder, SWT.NONE);
        tab.setText(
            BaseMessages.getString(
                PKG,
                "DatabasePerspective.Results.QueryTab",
                Integer.toString(result.statementNr),
                Integer.toString(result.rows == null ? 0 : result.rows.size())));
        tab.setData(result);
        tab.setControl(buildTable(result));
        if (firstResultTab == null) {
          firstResultTab = tab;
        }
      }
    }
    folder.setSelection(firstResultTab != null ? firstResultTab : folder.getItem(0));
    updateFloatEnablement();
    layout(true, true);
  }

  private void openSelectedInWindow() {
    QueryResult result = currentQueryResult();
    if (result == null || result.getRowMeta() == null || isDisposed()) {
      return;
    }
    List<Object[]> rows = result.getRows() == null ? List.of() : result.getRows();
    new ShowRowsDialog(
            getShell(),
            variables,
            BaseMessages.getString(
                PKG,
                "DatabasePerspective.Results.Float.Title",
                Integer.toString(result.getStatementNr())),
            BaseMessages.getString(PKG, "DatabasePerspective.Results.Float.Message"),
            result.getRowMeta(),
            rows)
        .open();
  }

  private QueryResult currentQueryResult() {
    if (folder == null || folder.isDisposed()) {
      return null;
    }
    CTabItem selected = folder.getSelection();
    if (selected == null || selected.isDisposed()) {
      return null;
    }
    Object data = selected.getData();
    return data instanceof QueryResult result ? result : null;
  }

  private void updateFloatEnablement() {
    if (openWindow == null || openWindow.isDisposed()) {
      return;
    }
    QueryResult result = currentQueryResult();
    openWindow.setEnabled(result != null && result.getRowMeta() != null);
  }

  private TableView buildTable(QueryResult result) {
    IRowMeta rowMeta = result.rowMeta;
    int columns = rowMeta == null ? 0 : rowMeta.size();
    ColumnInfo[] columnInfos = new ColumnInfo[columns];
    for (int i = 0; i < columns; i++) {
      IValueMeta valueMeta = rowMeta.getValueMeta(i);
      columnInfos[i] =
          new ColumnInfo(valueMeta.getName(), ColumnInfo.COLUMN_TYPE_TEXT, valueMeta.isNumeric());
      columnInfos[i].setValueMeta(valueMeta);
      columnInfos[i].setReadOnly(true);
      columnInfos[i].setImage(GuiResource.getInstance().getImage(valueMeta));
    }

    TableView view =
        new TableView(
            variables,
            folder,
            SWT.BORDER | SWT.FULL_SELECTION | SWT.MULTI,
            columnInfos,
            0,
            true,
            null,
            PropsUi.getInstance());
    view.setShowingBlueNullValues(true);
    view.setShortenDisplayedValues(true);
    view.setSortable(true);
    view.setReadonly(true);

    if (result.rows != null && rowMeta != null) {
      int lineNr = 0;
      for (int r = 0; r < result.rows.size(); r++) {
        TableItem item = r == 0 ? view.table.getItem(0) : new TableItem(view.table, SWT.NONE);
        lineNr++;
        item.setText(0, Integer.toString(lineNr));
        Object[] row = result.rows.get(r);
        if (row == null) {
          continue;
        }
        for (int c = 0; c < rowMeta.size(); c++) {
          String display;
          try {
            display = rowMeta.getValueMeta(c).getString(row[c]);
          } catch (HopValueException | ArrayIndexOutOfBoundsException e) {
            display = null;
          }
          if (display == null) {
            item.setText(c + 1, "<null>");
            item.setForeground(c + 1, GuiResource.getInstance().getColorBlue());
          } else {
            view.setCellValue(item, c + 1, display);
          }
        }
      }
    }
    if (!view.isDisposed()) {
      view.optWidth(true, 200);
    }
    return view;
  }
}
