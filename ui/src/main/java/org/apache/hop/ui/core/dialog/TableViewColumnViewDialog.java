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
import java.util.List;
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
import org.apache.hop.ui.core.widget.TableViewColumnFilter;
import org.apache.hop.ui.core.widget.TableViewColumnView;
import org.apache.hop.ui.core.widget.TableViewColumnViewManager;
import org.apache.hop.ui.core.widget.TableViewColumnViews;
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

/** Create, edit, apply and persist named {@link TableView} column views and column filters. */
public class TableViewColumnViewDialog extends Dialog {
  private static final Class<?> PKG = TableViewColumnViewDialog.class;

  private final PropsUi props;
  private final TableView tableView;
  private final String[] tableColumnNames;
  private final Set<String> tableColumnNameSet;
  private final String group;

  private final Map<String, TableViewColumnView> views = new LinkedHashMap<>();

  /** The filters the user is working on: those of the selected view, or ad-hoc ones. */
  private List<TableViewColumnFilter> currentFilters = new ArrayList<>();

  private Shell shell;
  private org.eclipse.swt.widgets.List wViews;
  private org.eclipse.swt.widgets.List wAvailable;
  private org.eclipse.swt.widgets.List wShown;
  private org.eclipse.swt.widgets.List wFilters;
  private Button wMoveUp;
  private Button wMoveDown;
  private Button wRemoveFilter;
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
    // The attachment below is the gap to the available-columns list. Do not add another inset.
    grid.marginWidth = 0;
    grid.marginHeight = 0;
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

    wMoveUp = new Button(gButtons, SWT.ARROW | SWT.UP);
    wMoveUp.setLayoutData(new GridData(GridData.FILL_HORIZONTAL));
    wMoveUp.setToolTipText(BaseMessages.getString(PKG, "TableViewColumnViewDialog.MoveUp.ToolTip"));
    wMoveUp.addListener(SWT.Selection, e -> moveShown(-1));

    wMoveDown = new Button(gButtons, SWT.ARROW | SWT.DOWN);
    wMoveDown.setLayoutData(new GridData(GridData.FILL_HORIZONTAL));
    wMoveDown.setToolTipText(
        BaseMessages.getString(PKG, "TableViewColumnViewDialog.MoveDown.ToolTip"));
    wMoveDown.addListener(SWT.Selection, e -> moveShown(1));

    Button wAddFilter = new Button(gButtons, SWT.PUSH);
    wAddFilter.setText(BaseMessages.getString(PKG, "TableViewColumnViewDialog.AddFilter.Label"));
    wAddFilter.setLayoutData(new GridData(GridData.FILL_HORIZONTAL));
    wAddFilter.setToolTipText(
        BaseMessages.getString(PKG, "TableViewColumnViewDialog.AddFilter.ToolTip"));
    wAddFilter.addListener(SWT.Selection, e -> addFilter());
    wAddFilter.setEnabled(tableView.isFiltersSupported());

    wRemoveFilter = new Button(gButtons, SWT.PUSH);
    wRemoveFilter.setText(
        BaseMessages.getString(PKG, "TableViewColumnViewDialog.RemoveFilter.Label"));
    wRemoveFilter.setLayoutData(new GridData(GridData.FILL_HORIZONTAL));
    wRemoveFilter.setToolTipText(
        BaseMessages.getString(PKG, "TableViewColumnViewDialog.RemoveFilter.ToolTip"));
    wRemoveFilter.addListener(SWT.Selection, e -> removeFilter());

    FormData fdButtons = new FormData();
    // The middle sash pane starts at the right side of the available-columns list.
    fdButtons.left = new FormAttachment(0, 2 * margin);
    fdButtons.top = new FormAttachment(10, 0);
    gButtons.setLayoutData(fdButtons);

    Composite right = new Composite(sash, SWT.NONE);
    PropsUi.setLook(right);
    right.setLayout(new FormLayout());

    Label wlShown = new Label(right, SWT.LEFT);
    wlShown.setText(BaseMessages.getString(PKG, "TableViewColumnViewDialog.Shown.Label"));
    PropsUi.setLook(wlShown);
    FormData fdlShown = new FormData();
    fdlShown.left = new FormAttachment(0, 0);
    fdlShown.top = new FormAttachment(0, 0);
    wlShown.setLayoutData(fdlShown);

    wShown =
        new org.eclipse.swt.widgets.List(
            right, SWT.BORDER | SWT.MULTI | SWT.V_SCROLL | SWT.H_SCROLL);
    PropsUi.setLook(wShown);
    FormData fdShown = new FormData();
    fdShown.left = new FormAttachment(0, 0);
    fdShown.top = new FormAttachment(wlShown, 0);
    fdShown.right = new FormAttachment(100, 0);
    // Only reach until half the height of the available columns list, the filters live below.
    fdShown.bottom = new FormAttachment(50, 0);
    wShown.setLayoutData(fdShown);
    wShown.addListener(SWT.DefaultSelection, e -> removeSelected());
    wShown.addListener(SWT.Selection, e -> updateFilterButtons());

    Label wlFilters = new Label(right, SWT.LEFT);
    wlFilters.setText(BaseMessages.getString(PKG, "TableViewColumnViewDialog.Filters.Label"));
    PropsUi.setLook(wlFilters);
    FormData fdlFilters = new FormData();
    fdlFilters.left = new FormAttachment(0, 0);
    fdlFilters.top = new FormAttachment(wShown, margin);
    wlFilters.setLayoutData(fdlFilters);

    wFilters =
        new org.eclipse.swt.widgets.List(
            right, SWT.BORDER | SWT.MULTI | SWT.V_SCROLL | SWT.H_SCROLL);
    PropsUi.setLook(wFilters);
    FormData fdFilters = new FormData();
    fdFilters.left = new FormAttachment(0, 0);
    fdFilters.top = new FormAttachment(wlFilters, 0);
    fdFilters.right = new FormAttachment(100, 0);
    fdFilters.bottom = new FormAttachment(100, 0);
    wFilters.setLayoutData(fdFilters);
    wFilters.addListener(SWT.Selection, e -> updateFilterButtons());

    sash.setWeights(new int[] {42, 16, 42});

    shell.addListener(SWT.Close, e -> close());

    loadViews();
    // A filter added from the column header is already applied. Start from it, and from every
    // column, so Save and Apply keep the grid instead of clearing the columns.
    currentFilters = copyFilters(tableView.getAppliedColumnFilters());
    refreshColumnLists(
        TableViewColumnViews.columnsInViewOnOpen(
            tableView.getVisibleColumnNamesInOrder(), tableColumnNames));
    refreshFiltersList();
    updateFilterButtons();
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
    List<String> shown =
        view != null && view.getColumnNames() != null
            ? new ArrayList<>(view.getColumnNames())
            : new ArrayList<>();
    currentFilters =
        view != null && view.getFilters() != null
            ? new ArrayList<>(view.getFilters())
            : new ArrayList<>();
    refreshColumnLists(shown);
    refreshFiltersList();
    updateFilterButtons();
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
    currentFilters = new ArrayList<>();
    refreshViewList(name);
    refreshColumnLists(List.of());
    refreshFiltersList();
    updateFilterButtons();
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
    view.setFilters(new ArrayList<>(currentFilters));
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
    currentFilters = new ArrayList<>();
    refreshColumnLists(List.of());
    refreshFiltersList();
    updateFilterButtons();
    updateStatus();
  }

  private void useCurrentLayout() {
    refreshColumnLists(tableView.getVisibleColumnNamesInOrder());
    currentFilters = copyFilters(tableView.getAppliedColumnFilters());
    refreshFiltersList();
    updateFilterButtons();
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
    updateFilterButtons();
    updateStatus();
  }

  /**
   * Move the selected columns of the "Columns in view" list one position up or down as a block,
   * keeping their relative order.
   *
   * @param delta -1 to move up, 1 to move down
   */
  private void moveShown(int delta) {
    int[] selection = wShown.getSelectionIndices();
    if (selection.length == 0) {
      return;
    }
    String[] items = wShown.getItems();
    boolean[] selected = new boolean[items.length];
    for (int index : selection) {
      selected[index] = true;
    }
    if (delta < 0) {
      for (int i = 0; i < items.length; i++) {
        if (selected[i] && i > 0 && !selected[i - 1]) {
          swap(items, i, i - 1);
          selected[i] = false;
          selected[i - 1] = true;
        }
      }
    } else {
      for (int i = items.length - 1; i >= 0; i--) {
        if (selected[i] && i < items.length - 1 && !selected[i + 1]) {
          swap(items, i, i + 1);
          selected[i] = false;
          selected[i + 1] = true;
        }
      }
    }
    wShown.setItems(items);
    int[] newSelection = new int[selection.length];
    int n = 0;
    for (int i = 0; i < selected.length; i++) {
      if (selected[i]) {
        newSelection[n++] = i;
      }
    }
    wShown.setSelection(newSelection);
    updateFilterButtons();
  }

  private void swap(String[] items, int i, int j) {
    String tmp = items[i];
    items[i] = items[j];
    items[j] = tmp;
  }

  private void apply() {
    List<String> shown = currentShownColumns();
    if (shown.isEmpty()) {
      MessageBox box = new MessageBox(shell, SWT.OK | SWT.ICON_INFORMATION);
      box.setText(BaseMessages.getString(PKG, "TableViewColumnViewDialog.Title"));
      box.setMessage(BaseMessages.getString(PKG, "TableViewColumnViewDialog.EmptyView.Message"));
      box.open();
      return;
    }
    if (!tableView.applyColumnView(shown, currentFilters)) {
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
    view.setFilters(new ArrayList<>(currentFilters));
    TableViewColumnViewManager.save(group, view);
    views.put(name, view);
    refreshViewList(name);
  }

  private void close() {
    props.setScreen(new WindowProperty(shell));
    shell.dispose();
  }

  private void refreshColumnLists(List<String> shownNames) {
    List<String> shown = new ArrayList<>();
    Set<String> shownSet = new HashSet<>();
    for (String name : shownNames) {
      if (StringUtils.isEmpty(name) || shownSet.contains(name)) {
        continue;
      }
      shown.add(name);
      shownSet.add(name);
    }
    wShown.setItems(shown.toArray(new String[0]));

    List<String> available = new ArrayList<>();
    for (String name : tableColumnNames) {
      if (!shownSet.contains(name)) {
        available.add(name);
      }
    }
    wAvailable.setItems(available.toArray(new String[0]));
  }

  /** Show the filters the user is working on as "Column : value1,value2". */
  private void refreshFiltersList() {
    String[] items = new String[currentFilters.size()];
    for (int i = 0; i < currentFilters.size(); i++) {
      items[i] = TableViewColumnViews.formatFilter(currentFilters.get(i));
    }
    wFilters.setItems(items);
  }

  private void updateFilterButtons() {
    boolean shownSelected = wShown.getSelectionCount() > 0;
    wMoveUp.setEnabled(shownSelected);
    wMoveDown.setEnabled(shownSelected);
    wRemoveFilter.setEnabled(tableView.isFiltersSupported() && wFilters.getSelectionCount() > 0);
  }

  /**
   * Add a filter on a column: pick the column (from the selection when a single column is selected,
   * with a dialog otherwise), then pick the values to show from the unique sorted values of that
   * column.
   */
  private void addFilter() {
    if (!tableView.isFiltersSupported()) {
      return;
    }
    String columnName = pickFilterColumn();
    if (columnName == null) {
      return;
    }
    int dataIndex = Const.indexOfString(columnName, tableColumnNames);
    if (dataIndex < 0) {
      return;
    }
    TableViewColumnFilter existing = filterFor(columnName);
    // Null means cancelled, or the column has nothing to filter on. An empty list means the user
    // wants no filter on this column at all.
    List<String> selected =
        tableView.promptColumnFilterValues(
            shell, dataIndex, existing == null ? List.of() : existing.getValues());
    if (selected == null) {
      return;
    }
    currentFilters.removeIf(filter -> filter != null && columnName.equals(filter.getColumnName()));
    if (!selected.isEmpty()) {
      currentFilters.add(new TableViewColumnFilter(columnName, selected));
    }
    refreshFiltersList();
    updateFilterButtons();
    updateStatus();
  }

  /** Remove the filters that are selected in the applied-filters list. */
  private void removeFilter() {
    int[] selection = wFilters.getSelectionIndices();
    if (selection.length == 0) {
      return;
    }
    boolean[] removed = new boolean[currentFilters.size()];
    for (int index : selection) {
      if (index >= 0 && index < removed.length) {
        removed[index] = true;
      }
    }
    List<TableViewColumnFilter> kept = new ArrayList<>();
    for (int i = 0; i < currentFilters.size(); i++) {
      if (!removed[i]) {
        kept.add(currentFilters.get(i));
      }
    }
    currentFilters = kept;
    refreshFiltersList();
    updateFilterButtons();
    updateStatus();
  }

  private TableViewColumnFilter filterFor(String columnName) {
    for (TableViewColumnFilter filter : currentFilters) {
      if (filter != null && columnName.equals(filter.getColumnName())) {
        return filter;
      }
    }
    return null;
  }

  private String pickFilterColumn() {
    String[] shownSelection = wShown.getSelection();
    if (shownSelection.length == 1 && tableColumnNameSet.contains(shownSelection[0])) {
      return shownSelection[0];
    }
    String[] availableSelection = wAvailable.getSelection();
    if (availableSelection.length == 1) {
      return availableSelection[0];
    }
    EnterSelectionDialog dialog =
        new EnterSelectionDialog(
            shell,
            tableColumnNames,
            BaseMessages.getString(PKG, "TableViewColumnViewDialog.Filter.Column.Title"),
            BaseMessages.getString(PKG, "TableViewColumnViewDialog.Filter.Column.Message"));
    return dialog.open();
  }

  private void updateStatus() {
    List<String> shown = currentShownColumns();
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
    if (!currentFilters.isEmpty()) {
      status +=
          "  "
              + BaseMessages.getString(
                  PKG,
                  "TableViewColumnViewDialog.Status.Filters",
                  Integer.toString(currentFilters.size()));
      status +=
          "  "
              + BaseMessages.getString(
                  PKG,
                  "TableViewColumnViewDialog.Status.Rows",
                  Integer.toString(tableView.getMatchingRowCount(currentFilters)),
                  Integer.toString(tableView.getUnfilteredRowCount()));
    }
    wlStatus.setText(status);
  }

  /** A private copy, so editing the dialog does not change the filter the table is showing. */
  private static List<TableViewColumnFilter> copyFilters(List<TableViewColumnFilter> filters) {
    List<TableViewColumnFilter> copy = new ArrayList<>();
    if (filters == null) {
      return copy;
    }
    for (TableViewColumnFilter filter : filters) {
      if (filter != null) {
        copy.add(new TableViewColumnFilter(filter.getColumnName(), filter.getValues()));
      }
    }
    return copy;
  }

  private List<String> currentShownColumns() {
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
