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

package org.apache.hop.git.provider;

import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.List;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.ui.core.ConstUi;
import org.apache.hop.ui.core.PropsUi;
import org.apache.hop.ui.core.gui.GuiResource;
import org.apache.hop.ui.core.gui.WindowProperty;
import org.eclipse.swt.SWT;
import org.eclipse.swt.layout.FormAttachment;
import org.eclipse.swt.layout.FormData;
import org.eclipse.swt.layout.FormLayout;
import org.eclipse.swt.widgets.Button;
import org.eclipse.swt.widgets.Dialog;
import org.eclipse.swt.widgets.Label;
import org.eclipse.swt.widgets.Shell;
import org.eclipse.swt.widgets.Table;
import org.eclipse.swt.widgets.TableColumn;
import org.eclipse.swt.widgets.TableItem;
import org.eclipse.swt.widgets.Text;

/** Searchable table dialog for large Git provider lists (branches, repositories, ...). */
public class SelectGitListDialog extends Dialog {

  private static final Class<?> PKG = SelectGitListDialog.class;

  private final List<String> allItems;
  private final String shellTitle;
  private final String initialFilter;

  private Shell shell;
  private Text wFilter;
  private Table wTable;
  private Label wStatus;
  private Button wOk;

  private String selected;
  private boolean confirmed;

  public SelectGitListDialog(
      Shell parent, String shellTitle, List<String> items, String initialFilter) {
    super(parent, SWT.DIALOG_TRIM | SWT.APPLICATION_MODAL | SWT.RESIZE);
    this.shellTitle = shellTitle;
    this.allItems = items == null ? List.of() : new ArrayList<>(items);
    this.initialFilter = initialFilter == null ? "" : initialFilter;
  }

  public boolean open() {
    Shell parent = getParent();
    shell = new Shell(parent, SWT.DIALOG_TRIM | SWT.APPLICATION_MODAL | SWT.RESIZE);
    shell.setText(shellTitle);
    shell.setImage(
        GuiResource.getInstance()
            .getImage(
                "git.svg", PKG.getClassLoader(), ConstUi.SMALL_ICON_SIZE, ConstUi.SMALL_ICON_SIZE));

    PropsUi props = PropsUi.getInstance();
    int margin = PropsUi.getMargin();
    int middle = props.getMiddlePct();

    FormLayout formLayout = new FormLayout();
    formLayout.marginWidth = PropsUi.getFormMargin();
    formLayout.marginHeight = PropsUi.getFormMargin();
    shell.setLayout(formLayout);
    shell.setMinimumSize(520, 480);
    PropsUi.setLook(shell);

    Label wlFilter = new Label(shell, SWT.RIGHT);
    PropsUi.setLook(wlFilter);
    wlFilter.setText(BaseMessages.getString(PKG, "SelectGitListDialog.Filter.Label"));
    FormData fdlFilter = new FormData();
    fdlFilter.left = new FormAttachment(0, 0);
    fdlFilter.right = new FormAttachment(middle, -margin);
    fdlFilter.top = new FormAttachment(0, margin);
    wlFilter.setLayoutData(fdlFilter);

    wFilter = new Text(shell, SWT.SINGLE | SWT.BORDER);
    PropsUi.setLook(wFilter);
    wFilter.setText(initialFilter);
    FormData fdFilter = new FormData();
    fdFilter.left = new FormAttachment(middle, 0);
    fdFilter.right = new FormAttachment(100, -margin);
    fdFilter.top = new FormAttachment(wlFilter, 0, SWT.CENTER);
    wFilter.setLayoutData(fdFilter);
    wFilter.addModifyListener(e -> refreshTable());

    wTable = new Table(shell, SWT.BORDER | SWT.SINGLE | SWT.FULL_SELECTION | SWT.V_SCROLL);
    wTable.setHeaderVisible(true);
    wTable.setLinesVisible(true);
    PropsUi.setLook(wTable);
    FormData fdTable = new FormData();
    fdTable.left = new FormAttachment(0, margin);
    fdTable.right = new FormAttachment(100, -margin);
    fdTable.top = new FormAttachment(wFilter, margin * 2);
    fdTable.bottom = new FormAttachment(100, -margin * 5);
    wTable.setLayoutData(fdTable);

    TableColumn column = new TableColumn(wTable, SWT.LEFT);
    column.setText(BaseMessages.getString(PKG, "SelectGitListDialog.Table.Name"));
    column.setWidth(460);

    wTable.addListener(SWT.Selection, e -> updateOkButton());
    wTable.addListener(
        SWT.DefaultSelection,
        e -> {
          if (wTable.getSelectionIndex() >= 0) {
            ok();
          }
        });

    wStatus = new Label(shell, SWT.LEFT);
    PropsUi.setLook(wStatus);
    FormData fdStatus = new FormData();
    fdStatus.left = new FormAttachment(0, margin);
    fdStatus.right = new FormAttachment(100, -margin);
    fdStatus.top = new FormAttachment(wTable, margin);
    wStatus.setLayoutData(fdStatus);

    Button wCancel = new Button(shell, SWT.PUSH);
    wCancel.setText(BaseMessages.getString(PKG, "SelectGitListDialog.Cancel.Label"));
    PropsUi.setLook(wCancel);
    FormData fdCancel = new FormData();
    fdCancel.right = new FormAttachment(100, -margin);
    fdCancel.bottom = new FormAttachment(100, -margin);
    wCancel.setLayoutData(fdCancel);
    wCancel.addListener(SWT.Selection, e -> cancel());

    wOk = new Button(shell, SWT.PUSH);
    wOk.setText(BaseMessages.getString(PKG, "SelectGitListDialog.Ok.Label"));
    PropsUi.setLook(wOk);
    FormData fdOk = new FormData();
    fdOk.right = new FormAttachment(wCancel, -margin);
    fdOk.bottom = new FormAttachment(100, -margin);
    wOk.setLayoutData(fdOk);
    wOk.addListener(SWT.Selection, e -> ok());

    refreshTable();
    selectInitialMatch();

    shell.pack();
    shell.setSize(Math.max(shell.getSize().x, 560), Math.max(shell.getSize().y, 480));
    org.apache.hop.ui.pipeline.transform.BaseTransformDialog.setSize(shell);
    WindowProperty winprop = new WindowProperty(shell);
    winprop.setName(shellTitle);
    PropsUi.getInstance().setScreen(winprop);

    shell.open();
    while (!shell.isDisposed()) {
      if (!shell.getDisplay().readAndDispatch()) {
        shell.getDisplay().sleep();
      }
    }

    return confirmed;
  }

  public String getSelected() {
    return selected;
  }

  private void refreshTable() {
    String query = wFilter.getText().trim().toLowerCase();
    wTable.removeAll();
    int shown = 0;
    for (String item : allItems) {
      if (query.isEmpty() || item.toLowerCase().contains(query)) {
        TableItem tableItem = new TableItem(wTable, SWT.NONE);
        tableItem.setText(0, item);
        shown++;
      }
    }
    wStatus.setText(
        MessageFormat.format(
            BaseMessages.getString(PKG, "SelectGitListDialog.Status.Items"),
            shown,
            allItems.size()));
    updateOkButton();
  }

  private void selectInitialMatch() {
    if (initialFilter.isBlank()) {
      return;
    }
    for (int i = 0; i < wTable.getItemCount(); i++) {
      if (initialFilter.equals(wTable.getItem(i).getText(0))) {
        wTable.setSelection(i);
        break;
      }
    }
    updateOkButton();
  }

  private void updateOkButton() {
    wOk.setEnabled(wTable.getSelectionIndex() >= 0);
  }

  private void ok() {
    int index = wTable.getSelectionIndex();
    if (index < 0) {
      return;
    }
    selected = wTable.getItem(index).getText(0);
    confirmed = true;
    shell.dispose();
  }

  private void cancel() {
    confirmed = false;
    shell.dispose();
  }
}
