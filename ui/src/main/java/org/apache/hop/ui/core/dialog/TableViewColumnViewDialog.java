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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;
import org.apache.commons.lang3.StringUtils;
import org.apache.hop.core.Const;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.ui.core.PropsUi;
import org.apache.hop.ui.core.gui.GuiResource;
import org.apache.hop.ui.core.gui.HopNamespace;
import org.apache.hop.ui.core.gui.WindowProperty;
import org.apache.hop.ui.core.widget.ColumnInfo;
import org.apache.hop.ui.core.widget.TableView;
import org.apache.hop.ui.core.widget.TableViewColumnView;
import org.apache.hop.ui.core.widget.TableViewColumnViewManager;
import org.apache.hop.ui.pipeline.transform.BaseTransformDialog;
import org.eclipse.swt.SWT;
import org.eclipse.swt.custom.SashForm;
import org.eclipse.swt.layout.FormAttachment;
import org.eclipse.swt.layout.FormData;
import org.eclipse.swt.layout.FormLayout;
import org.eclipse.swt.layout.GridData;
import org.eclipse.swt.layout.GridLayout;
import org.eclipse.swt.widgets.Button;
import org.eclipse.swt.widgets.Composite;
import org.eclipse.swt.widgets.Dialog;
import org.eclipse.swt.widgets.Display;
import org.eclipse.swt.widgets.Label;
import org.eclipse.swt.widgets.Shell;

/** Create, edit, apply and persist named {@link TableView} column views. */
public class TableViewColumnViewDialog extends Dialog {
  private static final Class<?> PKG = TableViewColumnViewDialog.class;

  private final PropsUi props;
  private final TableView tableView;
  private final String[] tableColumnNames;
  private final Set<String> tableColumnNameSet;
  private final String group;

  private final Map<String, TableViewColumnView> views = new LinkedHashMap<>();

  private Shell shell;
  private org.eclipse.swt.widgets.List wViews;
  private org.eclipse.swt.widgets.List wAvailable;
  private org.eclipse.swt.widgets.List wShown;
  private Label wlStatus;

  public TableViewColumnViewDialog(Shell parent, TableView tableView) {
    super(parent, SWT.NONE);
    this.props = PropsUi.getInstance();
    this.tableView = tableView;
    this.tableColumnNames = columnNames(tableView);
    this.tableColumnNameSet = new HashSet<>(Arrays.asList(tableColumnNames));
    String namespace;
    try {
      namespace = HopNamespace.getNamespace();
    } catch (Exception e) {
      namespace = "hop-gui";
    }
    this.group = namespace;
  }

  public void open() {
    Shell parent = getParent();
    shell = new Shell(parent, SWT.DIALOG_TRIM | SWT.RESIZE | SWT.MAX | SWT.MIN);
    PropsUi.setLook(shell);
    shell.setImage(GuiResource.getInstance().getImageView());
    shell.setText(BaseMessages.getString(PKG, "TableViewColumnViewDialog.Title"));

    FormLayout formLayout = new FormLayout();
    formLayout.marginWidth = PropsUi.getFormMargin();
    formLayout.marginHeight = PropsUi.getFormMargin();
    shell.setLayout(formLayout);

    int margin = PropsUi.getMargin();

    Button wApply = new Button(shell, SWT.PUSH);
    wApply.setText(BaseMessages.getString(PKG, "TableViewColumnViewDialog.Apply.Label"));
    wApply.addListener(SWT.Selection, e -> apply());

    Button wShowAll = new Button(shell, SWT.PUSH);
    wShowAll.setText(BaseMessages.getString(PKG, "TableViewColumnViewDialog.ShowAll.Label"));
    wShowAll.addListener(SWT.Selection, e -> showAll());

    Button wSave = new Button(shell, SWT.PUSH);
    wSave.setText(BaseMessages.getString(PKG, "System.Button.Save"));
    wSave.addListener(SWT.Selection, e -> save());

    Button wClose = new Button(shell, SWT.PUSH);
    wClose.setText(BaseMessages.getString(PKG, "System.Button.Close"));
    wClose.addListener(SWT.Selection, e -> close());

    BaseTransformDialog.positionBottomButtons(
        shell, new Button[] {wApply, wShowAll, wSave, wClose}, margin, null);

    wlStatus = new Label(shell, SWT.LEFT);
    PropsUi.setLook(wlStatus);
    FormData fdStatus = new FormData();
    fdStatus.left = new FormAttachment(0, 0);
    fdStatus.right = new FormAttachment(100, 0);
    fdStatus.bottom = new FormAttachment(wApply, -margin);
    wlStatus.setLayoutData(fdStatus);

    Label wlViews = new Label(shell, SWT.LEFT);
    wlViews.setText(BaseMessages.getString(PKG, "TableViewColumnViewDialog.Views.Label"));
    PropsUi.setLook(wlViews);
    FormData fdlViews = new FormData();
    fdlViews.left = new FormAttachment(0, 0);
    fdlViews.top = new FormAttachment(0, 0);
    wlViews.setLayoutData(fdlViews);

    Composite viewButtons = new Composite(shell, SWT.NONE);
    PropsUi.setLook(viewButtons);
    GridLayout buttonLayout = new GridLayout(4, false);
    buttonLayout.marginWidth = 0;
    buttonLayout.marginHeight = 0;
    viewButtons.setLayout(buttonLayout);
    FormData fdViewButtons = new FormData();
    fdViewButtons.left = new FormAttachment(0, 0);
    fdViewButtons.top = new FormAttachment(wlViews, margin);
    fdViewButtons.right = new FormAttachment(100, 0);
    viewButtons.setLayoutData(fdViewButtons);

    Button wNew = new Button(viewButtons, SWT.PUSH);
    wNew.setText(BaseMessages.getString(PKG, "System.Button.New"));
    wNew.addListener(SWT.Selection, e -> newView());

    Button wRename = new Button(viewButtons, SWT.PUSH);
    wRename.setText(BaseMessages.getString(PKG, "TableViewColumnViewDialog.Rename.Label"));
    wRename.addListener(SWT.Selection, e -> renameView());

    Button wDelete = new Button(viewButtons, SWT.PUSH);
    wDelete.setText(BaseMessages.getString(PKG, "System.Button.Delete"));
    wDelete.addListener(SWT.Selection, e -> deleteView());

    Button wUseCurrent = new Button(viewButtons, SWT.PUSH);
    wUseCurrent.setText(BaseMessages.getString(PKG, "TableViewColumnViewDialog.UseCurrent.Label"));
    wUseCurrent.addListener(SWT.Selection, e -> useCurrentLayout());

    wViews = new org.eclipse.swt.widgets.List(shell, SWT.BORDER | SWT.SINGLE | SWT.V_SCROLL);
    PropsUi.setLook(wViews);
    FormData fdViews = new FormData();
    fdViews.left = new FormAttachment(0, 0);
    fdViews.top = new FormAttachment(viewButtons, margin);
    fdViews.right = new FormAttachment(100, 0);
    fdViews.height = (int) (120 * PropsUi.getNativeZoomFactor());
    wViews.setLayoutData(fdViews);
    wViews.addListener(SWT.Selection, e -> viewSelected());

    SashForm sash = new SashForm(shell, SWT.HORIZONTAL);
    FormData fdSash = new FormData();
    fdSash.left = new FormAttachment(0, 0);
    fdSash.top = new FormAttachment(wViews, margin);
    fdSash.right = new FormAttachment(100, 0);
    fdSash.bottom = new FormAttachment(wlStatus, -margin);
    sash.setLayoutData(fdSash);

    Composite left =
        createListPane(
            sash, BaseMessages.getString(PKG, "TableViewColumnViewDialog.Available.Label"));
    wAvailable = (org.eclipse.swt.widgets.List) left.getData("list");
    wAvailable.addListener(SWT.DefaultSelection, e -> addSelected());

    Composite middle = new Composite(sash, SWT.NONE);
    PropsUi.setLook(middle);
    middle.setLayout(new FormLayout());

    Composite gButtons = new Composite(middle, SWT.NONE);
    GridLayout grid = new GridLayout(1, true);
    gButtons.setLayout(grid);
    PropsUi.setLook(gButtons);

    Button wAddOne = createMoveButton(gButtons, " > ", this::addSelected);
    Button wAddAll = createMoveButton(gButtons, " >> ", this::addAll);
    Button wRemoveOne = createMoveButton(gButtons, " < ", this::removeSelected);
    Button wRemoveAll = createMoveButton(gButtons, " << ", this::removeAll);
    wAddOne.setToolTipText(BaseMessages.getString(PKG, "TableViewColumnViewDialog.AddOne.ToolTip"));
    wAddAll.setToolTipText(BaseMessages.getString(PKG, "TableViewColumnViewDialog.AddAll.ToolTip"));
    wRemoveOne.setToolTipText(
        BaseMessages.getString(PKG, "TableViewColumnViewDialog.RemoveOne.ToolTip"));
    wRemoveAll.setToolTipText(
        BaseMessages.getString(PKG, "TableViewColumnViewDialog.RemoveAll.ToolTip"));

    FormData fdButtons = new FormData();
    wAddAll.pack();
    fdButtons.left = new FormAttachment(50, -(wAddAll.getSize().x / 2) - 5);
    fdButtons.top = new FormAttachment(20, 0);
    gButtons.setLayoutData(fdButtons);

    Composite right =
        createListPane(sash, BaseMessages.getString(PKG, "TableViewColumnViewDialog.Shown.Label"));
    wShown = (org.eclipse.swt.widgets.List) right.getData("list");
    wShown.addListener(SWT.DefaultSelection, e -> removeSelected());

    Composite orderButtons = new Composite(right, SWT.NONE);
    PropsUi.setLook(orderButtons);
    GridLayout orderLayout = new GridLayout(2, true);
    orderLayout.marginWidth = 0;
    orderLayout.marginHeight = 0;
    orderButtons.setLayout(orderLayout);
    FormData fdOrder = new FormData();
    fdOrder.left = new FormAttachment(0, 0);
    fdOrder.right = new FormAttachment(100, 0);
    fdOrder.bottom = new FormAttachment(100, 0);
    orderButtons.setLayoutData(fdOrder);

    Button wUp = new Button(orderButtons, SWT.PUSH);
    wUp.setText(BaseMessages.getString(PKG, "TableViewColumnViewDialog.MoveUp.Label"));
    wUp.setLayoutData(new GridData(GridData.FILL_HORIZONTAL));
    wUp.addListener(SWT.Selection, e -> moveShown(-1));

    Button wDown = new Button(orderButtons, SWT.PUSH);
    wDown.setText(BaseMessages.getString(PKG, "TableViewColumnViewDialog.MoveDown.Label"));
    wDown.setLayoutData(new GridData(GridData.FILL_HORIZONTAL));
    wDown.addListener(SWT.Selection, e -> moveShown(1));

    ((FormData) wShown.getLayoutData()).bottom = new FormAttachment(orderButtons, -margin);

    sash.setWeights(new int[] {42, 16, 42});

    shell.addListener(SWT.Close, e -> close());

    loadViews();
    refreshColumnLists(java.util.List.of());
    updateStatus();

    BaseTransformDialog.setSize(shell);
    shell.open();

    Display display = shell.getDisplay();
    while (!shell.isDisposed()) {
      if (!display.readAndDispatch()) {
        display.sleep();
      }
    }
  }

  private Composite createListPane(SashForm sash, String title) {
    Composite pane = new Composite(sash, SWT.NONE);
    PropsUi.setLook(pane);
    pane.setLayout(new FormLayout());

    Label label = new Label(pane, SWT.LEFT);
    label.setText(title);
    PropsUi.setLook(label);
    FormData fdLabel = new FormData();
    fdLabel.left = new FormAttachment(0, 0);
    fdLabel.top = new FormAttachment(0, 0);
    label.setLayoutData(fdLabel);

    org.eclipse.swt.widgets.List list =
        new org.eclipse.swt.widgets.List(
            pane, SWT.BORDER | SWT.MULTI | SWT.V_SCROLL | SWT.H_SCROLL);
    PropsUi.setLook(list);
    FormData fdList = new FormData();
    fdList.left = new FormAttachment(0, 0);
    fdList.top = new FormAttachment(label, 0);
    fdList.right = new FormAttachment(100, 0);
    fdList.bottom = new FormAttachment(100, 0);
    list.setLayoutData(fdList);
    pane.setData("list", list);
    return pane;
  }

  private Button createMoveButton(Composite parent, String text, Runnable action) {
    Button button = new Button(parent, SWT.PUSH);
    button.setText(text);
    button.setLayoutData(new GridData(GridData.FILL_HORIZONTAL));
    button.addListener(SWT.Selection, e -> action.run());
    return button;
  }

  private void loadViews() {
    views.clear();
    for (TableViewColumnView view : TableViewColumnViewManager.list(group)) {
      views.put(view.getName(), view);
    }
    refreshViewList(null);
  }

  private void refreshViewList(String selectName) {
    String[] names = views.keySet().toArray(new String[0]);
    Arrays.sort(names, String.CASE_INSENSITIVE_ORDER);
    wViews.setItems(names);
    if (selectName != null) {
      int index = Const.indexOfString(selectName, names);
      if (index >= 0) {
        wViews.setSelection(index);
      }
    }
  }

  private void viewSelected() {
    String name = selectedViewName();
    if (name == null) {
      return;
    }
    TableViewColumnView view = views.get(name);
    java.util.List<String> shown =
        view != null && view.getColumnNames() != null
            ? new ArrayList<>(view.getColumnNames())
            : new ArrayList<>();
    refreshColumnLists(shown);
    updateStatus();
  }

  private void newView() {
    String name = promptName("");
    if (name == null) {
      return;
    }
    if (views.containsKey(name) && !confirmOverwrite(name)) {
      return;
    }
    TableViewColumnView view = new TableViewColumnView(name, new ArrayList<>());
    views.put(name, view);
    refreshViewList(name);
    refreshColumnLists(java.util.List.of());
    updateStatus();
  }

  private void renameView() {
    String oldName = selectedViewName();
    if (oldName == null) {
      return;
    }
    String newName = promptName(oldName);
    if (newName == null || newName.equals(oldName)) {
      return;
    }
    if (views.containsKey(newName) && !confirmOverwrite(newName)) {
      return;
    }
    TableViewColumnView view = views.remove(oldName);
    if (view == null) {
      view = new TableViewColumnView();
    }
    view.setName(newName);
    view.setColumnNames(currentShownColumns());
    views.remove(newName);
    views.put(newName, view);
    TableViewColumnViewManager.delete(group, oldName);
    TableViewColumnViewManager.save(group, view);
    refreshViewList(newName);
  }

  private void deleteView() {
    String name = selectedViewName();
    if (name == null) {
      return;
    }
    MessageBox box = new MessageBox(shell, SWT.YES | SWT.NO | SWT.ICON_QUESTION);
    box.setText(BaseMessages.getString(PKG, "TableViewColumnViewDialog.Delete.Title"));
    box.setMessage(BaseMessages.getString(PKG, "TableViewColumnViewDialog.Delete.Message", name));
    if ((box.open() & SWT.YES) == 0) {
      return;
    }
    views.remove(name);
    TableViewColumnViewManager.delete(group, name);
    refreshViewList(null);
    refreshColumnLists(java.util.List.of());
    updateStatus();
  }

  private void useCurrentLayout() {
    refreshColumnLists(tableView.getVisibleColumnNamesInOrder());
    updateStatus();
  }

  private void addSelected() {
    moveItems(wAvailable, wShown, wAvailable.getSelection());
  }

  private void addAll() {
    moveItems(wAvailable, wShown, wAvailable.getItems());
  }

  private void removeSelected() {
    moveItems(wShown, wAvailable, wShown.getSelection());
  }

  private void removeAll() {
    moveItems(wShown, wAvailable, wShown.getItems());
  }

  private void moveItems(
      org.eclipse.swt.widgets.List from, org.eclipse.swt.widgets.List to, String[] items) {
    for (String item : items) {
      if (from.indexOf(item) >= 0) {
        from.remove(item);
      }
      boolean addToAvailable = to == wAvailable && tableColumnNameSet.contains(item);
      boolean addToShown = to == wShown;
      if ((addToAvailable || addToShown) && to.indexOf(item) < 0) {
        to.add(item);
      }
    }
    updateStatus();
  }

  private void moveShown(int delta) {
    int[] selection = wShown.getSelectionIndices();
    if (selection.length != 1) {
      return;
    }
    int from = selection[0];
    int to = from + delta;
    if (to < 0 || to >= wShown.getItemCount()) {
      return;
    }
    String[] items = wShown.getItems();
    String moving = items[from];
    items[from] = items[to];
    items[to] = moving;
    wShown.setItems(items);
    wShown.setSelection(to);
  }

  private void apply() {
    java.util.List<String> shown = currentShownColumns();
    if (shown.isEmpty()) {
      MessageBox box = new MessageBox(shell, SWT.OK | SWT.ICON_INFORMATION);
      box.setText(BaseMessages.getString(PKG, "TableViewColumnViewDialog.Title"));
      box.setMessage(BaseMessages.getString(PKG, "TableViewColumnViewDialog.EmptyView.Message"));
      box.open();
      return;
    }
    if (!tableView.applyColumnView(shown)) {
      MessageBox box = new MessageBox(shell, SWT.OK | SWT.ICON_INFORMATION);
      box.setText(BaseMessages.getString(PKG, "TableViewColumnViewDialog.Title"));
      box.setMessage(BaseMessages.getString(PKG, "TableViewColumnViewDialog.NoMatch.Message"));
      box.open();
    }
  }

  private void showAll() {
    tableView.resetColumnView();
  }

  private void save() {
    String name = selectedViewName();
    if (name == null) {
      name = promptName("");
      if (name == null) {
        return;
      }
      if (views.containsKey(name) && !confirmOverwrite(name)) {
        return;
      }
    }
    TableViewColumnView view = views.computeIfAbsent(name, key -> new TableViewColumnView());
    view.setName(name);
    view.setColumnNames(currentShownColumns());
    TableViewColumnViewManager.save(group, view);
    views.put(name, view);
    refreshViewList(name);
  }

  private void close() {
    props.setScreen(new WindowProperty(shell));
    shell.dispose();
  }

  private void refreshColumnLists(java.util.List<String> shownNames) {
    java.util.List<String> shown = new ArrayList<>();
    Set<String> shownSet = new HashSet<>();
    for (String name : shownNames) {
      if (StringUtils.isEmpty(name) || shownSet.contains(name)) {
        continue;
      }
      shown.add(name);
      shownSet.add(name);
    }
    wShown.setItems(shown.toArray(new String[0]));

    java.util.List<String> available = new ArrayList<>();
    for (String name : tableColumnNames) {
      if (!shownSet.contains(name)) {
        available.add(name);
      }
    }
    wAvailable.setItems(available.toArray(new String[0]));
  }

  private void updateStatus() {
    java.util.List<String> shown = currentShownColumns();
    int matching = 0;
    int missing = 0;
    for (String name : shown) {
      if (tableColumnNameSet.contains(name)) {
        matching++;
      } else {
        missing++;
      }
    }
    String status =
        BaseMessages.getString(
            PKG,
            "TableViewColumnViewDialog.Status.Visible",
            Integer.toString(matching),
            Integer.toString(tableColumnNames.length));
    if (missing > 0) {
      status +=
          "  "
              + BaseMessages.getString(
                  PKG, "TableViewColumnViewDialog.Status.Missing", Integer.toString(missing));
    }
    wlStatus.setText(status);
  }

  private java.util.List<String> currentShownColumns() {
    return new ArrayList<>(Arrays.asList(wShown.getItems()));
  }

  private String selectedViewName() {
    String[] selection = wViews.getSelection();
    if (selection == null || selection.length == 0) {
      return null;
    }
    return selection[0];
  }

  private String promptName(String initial) {
    EnterStringDialog dialog =
        new EnterStringDialog(
            shell,
            Const.NVL(initial, ""),
            BaseMessages.getString(PKG, "TableViewColumnViewDialog.Name.Title"),
            BaseMessages.getString(PKG, "TableViewColumnViewDialog.Name.Message"));
    String name = dialog.open();
    if (name == null) {
      return null;
    }
    name = name.trim();
    if (name.isEmpty()) {
      MessageBox box = new MessageBox(shell, SWT.OK | SWT.ICON_INFORMATION);
      box.setText(BaseMessages.getString(PKG, "TableViewColumnViewDialog.Title"));
      box.setMessage(BaseMessages.getString(PKG, "TableViewColumnViewDialog.Name.Empty"));
      box.open();
      return null;
    }
    return name;
  }

  private boolean confirmOverwrite(String name) {
    MessageBox box = new MessageBox(shell, SWT.YES | SWT.NO | SWT.ICON_QUESTION);
    box.setText(BaseMessages.getString(PKG, "TableViewColumnViewDialog.Overwrite.Title"));
    box.setMessage(
        BaseMessages.getString(PKG, "TableViewColumnViewDialog.Overwrite.Message", name));
    return (box.open() & SWT.YES) != 0;
  }

  private static String[] columnNames(TableView tableView) {
    ColumnInfo[] columns = tableView.getColumns();
    String[] names = new String[columns.length];
    for (int i = 0; i < columns.length; i++) {
      names[i] = Const.NVL(columns[i].getName(), "");
    }
    return names;
  }
}
