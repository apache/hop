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

package org.apache.hop.pipeline.transforms.dbproc;

import java.util.ArrayList;
import java.util.List;
import org.apache.hop.core.Const;
import org.apache.hop.core.database.Database;
import org.apache.hop.core.database.DatabaseMeta;
import org.apache.hop.core.exception.HopDatabaseException;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.core.row.value.ValueMetaFactory;
import org.apache.hop.core.util.Utils;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.apache.hop.ui.core.ConstUi;
import org.apache.hop.ui.core.PropsUi;
import org.apache.hop.ui.core.dialog.BaseDialog;
import org.apache.hop.ui.core.dialog.EnterSelectionDialog;
import org.apache.hop.ui.core.dialog.ErrorDialog;
import org.apache.hop.ui.core.dialog.MessageBox;
import org.apache.hop.ui.core.gui.GuiCompositeWidgets;
import org.apache.hop.ui.core.gui.GuiCompositeWidgetsAdapter;
import org.apache.hop.ui.core.widget.ColumnInfo;
import org.apache.hop.ui.core.widget.MetaSelectionLine;
import org.apache.hop.ui.core.widget.TableView;
import org.apache.hop.ui.core.widget.TextVar;
import org.apache.hop.ui.hopgui.BackgroundThreadFacade;
import org.apache.hop.ui.pipeline.transform.BaseTransformDialog;
import org.apache.hop.ui.pipeline.transform.ITableItemInsertListener;
import org.eclipse.swt.SWT;
import org.eclipse.swt.custom.CTabFolder;
import org.eclipse.swt.custom.CTabItem;
import org.eclipse.swt.layout.FormAttachment;
import org.eclipse.swt.layout.FormData;
import org.eclipse.swt.widgets.Button;
import org.eclipse.swt.widgets.Combo;
import org.eclipse.swt.widgets.Composite;
import org.eclipse.swt.widgets.Control;
import org.eclipse.swt.widgets.Shell;
import org.eclipse.swt.widgets.TableItem;
import org.eclipse.swt.widgets.Text;

public class DBProcDialog extends BaseTransformDialog {
  private static final Class<?> PKG = DBProcMeta.class;

  private final DBProcMeta input;
  private GuiCompositeWidgets widgets;

  private TableView wArguments;
  private ColumnInfo[] argumentColumns;
  private TableView wResultFields;
  private Button wGetResultFields;

  private final List<String> inputFields = new ArrayList<>();

  private CTabFolder tabFolder;
  private CTabItem fieldsTab;
  private CTabItem generalTab;
  private boolean adjustingTab;

  public DBProcDialog(
      Shell parent, IVariables variables, DBProcMeta transformMeta, PipelineMeta pipelineMeta) {
    super(parent, variables, transformMeta, pipelineMeta);
    input = transformMeta;
  }

  @Override
  public String open() {
    createShell(BaseMessages.getString(PKG, "DBProcDialog.Shell.Title"));

    changed = input.hasChanged();

    buildButtonBar().ok(e -> ok()).get(e -> get()).cancel(e -> cancel()).build();

    widgets =
        GuiCompositeWidgets.addScrolledComposite(
            shell,
            variables,
            wTransformName,
            wOk,
            DBProcMeta.GUI_PLUGIN_ELEMENT_PARENT_ID,
            input,
            this::beforeCreate);
    widgets.setWidgetsListener(
        new GuiCompositeWidgetsAdapter() {
          @Override
          public void widgetModified(
              GuiCompositeWidgets compositeWidgets, Control changedWidget, String widgetId) {
            if (DBProcMeta.WIDGET_RESULT_TYPE.equals(widgetId)) {
              updateFieldsTab();
            }
          }
        });

    populateArguments();
    populateResultFields();
    findTabs();
    updateFieldsTab();
    loadInputFieldNames();

    input.setChanged(changed);
    focusTransformName();
    BaseDialog.defaultShellHandling(shell, c -> ok(), c -> cancel());
    return transformName;
  }

  private void beforeCreate(GuiCompositeWidgets compositeWidgets) {
    widgets = compositeWidgets;
    compositeWidgets.registerExtraGroup(
        BaseMessages.getString(PKG, "DBProcDialog.Group.General"), "10", null, this::addFindButton);
    compositeWidgets.registerExtraGroup(
        BaseMessages.getString(PKG, "DBProcDialog.Group.Parameters"),
        "20",
        null,
        this::addArgumentsTable);
    compositeWidgets.registerExtraGroup(
        BaseMessages.getString(PKG, "DBProcDialog.Group.Fields"),
        "30",
        null,
        this::addResultFieldsTable);
  }

  private void addFindButton(Composite parent) {
    Control[] children = parent.getChildren();
    Control last = children.length == 0 ? null : children[children.length - 1];

    Button find = new Button(parent, SWT.PUSH);
    find.setText(BaseMessages.getString(PKG, "DBProcDialog.Finding.Button"));
    find.setToolTipText(BaseMessages.getString(PKG, "DBProcDialog.Finding.Tooltip"));
    PropsUi.setLook(find);
    find.addListener(SWT.Selection, e -> selectProcedure());
    FormData fdFind = new FormData();
    fdFind.right = new FormAttachment(100, 0);
    fdFind.top = last == null ? new FormAttachment(0, 0) : new FormAttachment(last, margin);
    find.setLayoutData(fdFind);
  }

  private void addArgumentsTable(Composite parent) {
    int nrRows = input.getArguments() == null ? 0 : input.getArguments().size();
    argumentColumns =
        new ColumnInfo[] {
          new ColumnInfo(
              BaseMessages.getString(PKG, "DBProcDialog.ColumnInfo.Name"),
              ColumnInfo.COLUMN_TYPE_CCOMBO,
              new String[] {""},
              false),
          new ColumnInfo(
              BaseMessages.getString(PKG, "DBProcDialog.ColumnInfo.Direction"),
              ColumnInfo.COLUMN_TYPE_CCOMBO,
              "IN",
              "OUT",
              "INOUT"),
          new ColumnInfo(
              BaseMessages.getString(PKG, "DBProcDialog.ColumnInfo.Type"),
              ColumnInfo.COLUMN_TYPE_CCOMBO,
              ValueMetaFactory.getValueMetaNames()),
        };
    wArguments =
        new TableView(
            variables,
            parent,
            SWT.BORDER | SWT.FULL_SELECTION | SWT.MULTI,
            argumentColumns,
            nrRows,
            null,
            props);
    FormData fdArguments = new FormData();
    fdArguments.left = new FormAttachment(0, 0);
    fdArguments.top = new FormAttachment(0, 0);
    fdArguments.right = new FormAttachment(100, 0);
    fdArguments.bottom = new FormAttachment(100, 0);
    wArguments.setLayoutData(fdArguments);
  }

  private void addResultFieldsTable(Composite parent) {
    wGetResultFields = new Button(parent, SWT.PUSH);
    wGetResultFields.setText(BaseMessages.getString(PKG, "DBProcDialog.GetResultFields.Button"));
    wGetResultFields.setToolTipText(
        BaseMessages.getString(PKG, "DBProcDialog.GetResultFields.Tooltip"));
    PropsUi.setLook(wGetResultFields);
    wGetResultFields.addListener(SWT.Selection, e -> getResultFields());
    FormData fdGet = new FormData();
    fdGet.top = new FormAttachment(0, 0);
    fdGet.right = new FormAttachment(100, 0);
    wGetResultFields.setLayoutData(fdGet);

    int nrRows = input.getResultFields() == null ? 0 : input.getResultFields().size();
    ColumnInfo[] columns =
        new ColumnInfo[] {
          new ColumnInfo(
              BaseMessages.getString(PKG, "DBProcDialog.ColumnInfo.Name"),
              ColumnInfo.COLUMN_TYPE_TEXT,
              false),
          new ColumnInfo(
              BaseMessages.getString(PKG, "DBProcDialog.ColumnInfo.Type"),
              ColumnInfo.COLUMN_TYPE_CCOMBO,
              ValueMetaFactory.getValueMetaNames(),
              true),
          new ColumnInfo(
              BaseMessages.getString(PKG, "DBProcDialog.ColumnInfo.Format"),
              ColumnInfo.COLUMN_TYPE_FORMAT,
              2),
          new ColumnInfo(
              BaseMessages.getString(PKG, "DBProcDialog.ColumnInfo.Length"),
              ColumnInfo.COLUMN_TYPE_TEXT,
              false),
          new ColumnInfo(
              BaseMessages.getString(PKG, "DBProcDialog.ColumnInfo.Precision"),
              ColumnInfo.COLUMN_TYPE_TEXT,
              false)
        };
    wResultFields =
        new TableView(
            variables,
            parent,
            SWT.BORDER | SWT.FULL_SELECTION | SWT.MULTI | SWT.V_SCROLL | SWT.H_SCROLL,
            columns,
            nrRows,
            null,
            props);
    FormData fdFields = new FormData();
    fdFields.left = new FormAttachment(0, 0);
    fdFields.top = new FormAttachment(wGetResultFields, margin);
    fdFields.right = new FormAttachment(100, 0);
    fdFields.bottom = new FormAttachment(100, 0);
    wResultFields.setLayoutData(fdFields);
  }

  private void findTabs() {
    tabFolder = findTabFolder(shell);
    if (tabFolder == null) {
      return;
    }
    String fieldsLabel = BaseMessages.getString(PKG, "DBProcDialog.Group.Fields");
    String generalLabel = BaseMessages.getString(PKG, "DBProcDialog.Group.General");
    for (CTabItem item : tabFolder.getItems()) {
      if (fieldsLabel.equals(item.getText())) {
        fieldsTab = item;
        fieldsTab.setToolTipText(BaseMessages.getString(PKG, "DBProcDialog.FieldsTab.Tooltip"));
      } else if (generalLabel.equals(item.getText())) {
        generalTab = item;
      }
    }
    tabFolder.addListener(
        SWT.Selection,
        e -> {
          if (adjustingTab || isRowResultType() || fieldsTab == null) {
            return;
          }
          if (tabFolder.getSelection() == fieldsTab) {
            adjustingTab = true;
            try {
              tabFolder.setSelection(generalTab != null ? generalTab : tabFolder.getItem(0));
            } finally {
              adjustingTab = false;
            }
          }
        });
  }

  private CTabFolder findTabFolder(Control control) {
    if (control instanceof CTabFolder folder) {
      return folder;
    }
    if (control instanceof Composite composite) {
      for (Control child : composite.getChildren()) {
        CTabFolder found = findTabFolder(child);
        if (found != null) {
          return found;
        }
      }
    }
    return null;
  }

  private void updateFieldsTab() {
    boolean rows = isRowResultType();
    setWidgetEnabled(DBProcMeta.WIDGET_RESULT_NAME, !rows);
    if (wResultFields != null && !wResultFields.isDisposed()) {
      wResultFields.setEnabled(rows);
    }
    if (wGetResultFields != null && !wGetResultFields.isDisposed()) {
      wGetResultFields.setEnabled(rows);
    }
    if (fieldsTab != null && !fieldsTab.isDisposed()) {
      fieldsTab.setForeground(rows ? null : shell.getDisplay().getSystemColor(SWT.COLOR_DARK_GRAY));
      if (fieldsTab.getControl() != null && !fieldsTab.getControl().isDisposed()) {
        fieldsTab.getControl().setEnabled(rows);
      }
      if (!rows && tabFolder != null && tabFolder.getSelection() == fieldsTab) {
        adjustingTab = true;
        try {
          tabFolder.setSelection(generalTab != null ? generalTab : tabFolder.getItem(0));
        } finally {
          adjustingTab = false;
        }
      }
    }
  }

  private boolean isRowResultType() {
    return DBProcMeta.RESULT_TYPE_ROW.equalsIgnoreCase(widgetText(DBProcMeta.WIDGET_RESULT_TYPE));
  }

  private void loadInputFieldNames() {
    BackgroundThreadFacade.start(
        () -> {
          TransformMeta transformMeta = pipelineMeta.findTransform(transformName);
          if (transformMeta == null) {
            return;
          }
          final List<String> names = new ArrayList<>();
          try {
            IRowMeta row = pipelineMeta.getPrevTransformFields(variables, transformMeta);
            if (row != null) {
              for (int i = 0; i < row.size(); i++) {
                names.add(row.getValueMeta(i).getName());
              }
            }
          } catch (HopException e) {
            logError(BaseMessages.getString(PKG, "System.Dialog.GetFieldsFailed.Message"));
            return;
          }
          if (shell.isDisposed()) {
            return;
          }
          shell
              .getDisplay()
              .asyncExec(
                  () -> {
                    if (shell.isDisposed() || argumentColumns == null) {
                      return;
                    }
                    inputFields.clear();
                    inputFields.addAll(names);
                    setComboBoxes();
                  });
        });
  }

  private void selectProcedure() {
    String connectionName = widgetText(DBProcMeta.WIDGET_CONNECTION);
    if (Utils.isEmpty(connectionName)) {
      showMessage(
          "DBProcDialog.InvalidConnection.DialogTitle",
          "DBProcDialog.InvalidConnection.DialogMessage",
          SWT.OK | SWT.ICON_ERROR);
      return;
    }
    DatabaseMeta databaseMeta = pipelineMeta.findDatabase(connectionName, variables);
    if (databaseMeta == null) {
      showMessage(
          "DBProcDialog.InvalidConnection.DialogTitle",
          "DBProcDialog.InvalidConnection.DialogMessage",
          SWT.OK | SWT.ICON_ERROR);
      return;
    }
    try (Database db = new Database(loggingObject, variables, databaseMeta)) {
      db.connect();
      String[] procs = db.getProcedures();
      if (procs != null && procs.length > 0) {
        EnterSelectionDialog esd =
            new EnterSelectionDialog(
                shell,
                procs,
                BaseMessages.getString(PKG, "DBProcDialog.EnterSelection.DialogTitle"),
                BaseMessages.getString(PKG, "DBProcDialog.EnterSelection.DialogMessage"));
        String procedure = esd.open();
        if (procedure != null) {
          setProcedureText(procedure);
        }
      } else {
        showMessage(
            "DBProcDialog.NoProceduresFound.DialogTitle",
            "DBProcDialog.NoProceduresFound.DialogMessage",
            SWT.OK | SWT.ICON_INFORMATION);
      }
    } catch (HopDatabaseException dbe) {
      new ErrorDialog(
          shell,
          BaseMessages.getString(PKG, "DBProcDialog.ErrorGettingProceduresList.DialogTitle"),
          BaseMessages.getString(PKG, "DBProcDialog.ErrorGettingProceduresList.DialogMessage"),
          dbe);
    }
  }

  private void getResultFields() {
    String connectionName = widgetText(DBProcMeta.WIDGET_CONNECTION);
    if (Utils.isEmpty(connectionName)) {
      showMessage(
          "DBProcDialog.InvalidConnection.DialogTitle",
          "DBProcDialog.InvalidConnection.DialogMessage",
          SWT.OK | SWT.ICON_ERROR);
      return;
    }
    DatabaseMeta databaseMeta = pipelineMeta.findDatabase(connectionName, variables);
    if (databaseMeta == null) {
      showMessage(
          "DBProcDialog.InvalidConnection.DialogTitle",
          "DBProcDialog.InvalidConnection.DialogMessage",
          SWT.OK | SWT.ICON_ERROR);
      return;
    }

    List<DBProcMeta.ProcArgument> arguments = new ArrayList<>();
    for (DBProcMeta.ProcArgument argument : readArguments()) {
      if (!Utils.isEmpty(argument.getName()) && !Utils.isEmpty(argument.getDirection())) {
        arguments.add(argument);
      }
    }
    String[] names = new String[arguments.size()];
    String[] directions = new String[arguments.size()];
    int[] types = new int[arguments.size()];
    for (int i = 0; i < arguments.size(); i++) {
      names[i] = arguments.get(i).getName();
      directions[i] = arguments.get(i).getDirection();
      types[i] = ValueMetaFactory.getIdForValueMeta(arguments.get(i).getType());
    }
    String procedure = variables.resolve(widgetText(DBProcMeta.WIDGET_PROCEDURE));

    try (Database db = new Database(loggingObject, variables, databaseMeta)) {
      db.connect();
      try {
        db.setAutoCommit(false);
      } catch (HopDatabaseException ignored) {
        // The driver does not allow auto-commit to be turned off. A procedure that commits
        // itself can still change data.
      }
      try {
        IRowMeta fields = db.getProcedureResultFields(procedure, names, directions, types);
        if (fields == null || fields.isEmpty()) {
          showMessage(
              "DBProcDialog.NoResultSet.DialogTitle",
              "DBProcDialog.NoResultSet.DialogMessage",
              SWT.OK | SWT.ICON_INFORMATION);
          return;
        }
        wResultFields.clearAll(false);
        for (IValueMeta valueMeta : fields.getValueMetaList()) {
          TableItem item = new TableItem(wResultFields.table, SWT.NONE);
          item.setText(1, Const.NVL(valueMeta.getName(), ""));
          item.setText(2, valueMeta.getTypeDesc());
          item.setText(3, Const.NVL(valueMeta.getConversionMask(), ""));
          item.setText(4, valueMeta.getLength() < 0 ? "" : Integer.toString(valueMeta.getLength()));
          item.setText(
              5, valueMeta.getPrecision() < 0 ? "" : Integer.toString(valueMeta.getPrecision()));
        }
        wResultFields.removeEmptyRows();
        wResultFields.setRowNums();
        wResultFields.optWidth(true);
      } finally {
        try {
          db.rollback();
        } catch (HopDatabaseException ignored) {
          // A procedure that commits itself cannot be rolled back by this probe.
        }
      }
    } catch (HopException e) {
      new ErrorDialog(
          shell,
          BaseMessages.getString(PKG, "DBProcDialog.FailedToGetResultFields.DialogTitle"),
          BaseMessages.getString(PKG, "DBProcDialog.FailedToGetResultFields.DialogMessage"),
          e);
    }
  }

  protected void setComboBoxes() {
    String[] fieldNames = ConstUi.sortFieldNames(inputFields);
    argumentColumns[0].setComboValues(fieldNames);
  }

  private void populateArguments() {
    if (wArguments == null || input.getArguments() == null) {
      return;
    }
    for (int i = 0; i < input.getArguments().size(); i++) {
      DBProcMeta.ProcArgument argument = input.getArguments().get(i);
      TableItem item = wArguments.table.getItem(i);
      item.setText(1, Const.NVL(argument.getName(), ""));
      item.setText(2, Const.NVL(argument.getDirection(), ""));
      item.setText(3, Const.NVL(argument.getType(), ""));
    }
    wArguments.optimizeTableView();
  }

  private void populateResultFields() {
    if (wResultFields == null || input.getResultFields() == null) {
      return;
    }
    for (int i = 0; i < input.getResultFields().size(); i++) {
      DBProcField field = input.getResultFields().get(i);
      TableItem item = wResultFields.table.getItem(i);
      item.setText(1, Const.NVL(field.getName(), ""));
      item.setText(2, Const.NVL(field.getType(), ""));
      item.setText(3, Const.NVL(field.getFormat(), ""));
      item.setText(4, field.getLength() < 0 ? "" : Integer.toString(field.getLength()));
      item.setText(5, field.getPrecision() < 0 ? "" : Integer.toString(field.getPrecision()));
    }
    wResultFields.optimizeTableView();
  }

  private List<DBProcMeta.ProcArgument> readArguments() {
    List<DBProcMeta.ProcArgument> arguments = new ArrayList<>();
    if (wArguments == null || wArguments.isDisposed()) {
      return arguments;
    }
    for (TableItem item : wArguments.getNonEmptyItems()) {
      DBProcMeta.ProcArgument argument = new DBProcMeta.ProcArgument();
      argument.setName(item.getText(1));
      argument.setDirection(item.getText(2));
      argument.setType(item.getText(3));
      arguments.add(argument);
    }
    return arguments;
  }

  private List<DBProcField> readResultFields() {
    List<DBProcField> fields = new ArrayList<>();
    if (wResultFields == null || wResultFields.isDisposed()) {
      return fields;
    }
    for (TableItem item : wResultFields.getNonEmptyItems()) {
      if (Utils.isEmpty(item.getText(1))) {
        continue;
      }
      DBProcField field = new DBProcField();
      field.setName(item.getText(1));
      field.setType(item.getText(2));
      field.setFormat(item.getText(3));
      field.setLength(Const.toInt(item.getText(4), -1));
      field.setPrecision(Const.toInt(item.getText(5), -1));
      fields.add(field);
    }
    return fields;
  }

  private String widgetText(String widgetId) {
    Control control = widgets.getWidgetsMap().get(widgetId);
    if (control == null || control.isDisposed()) {
      return "";
    }
    if (control instanceof MetaSelectionLine<?> line) {
      return line.getText();
    }
    if (control instanceof TextVar textVar) {
      return textVar.getText();
    }
    if (control instanceof Combo combo) {
      return combo.getText();
    }
    if (control instanceof Text text) {
      return text.getText();
    }
    return "";
  }

  private void setProcedureText(String procedure) {
    Control control = widgets.getWidgetsMap().get(DBProcMeta.WIDGET_PROCEDURE);
    if (control instanceof TextVar textVar && !textVar.isDisposed()) {
      textVar.setText(Const.NVL(procedure, ""));
    }
  }

  private void setWidgetEnabled(String widgetId, boolean enabled) {
    Control control = widgets.getWidgetsMap().get(widgetId);
    if (control != null && !control.isDisposed()) {
      control.setEnabled(enabled);
    }
    Control label = widgets.getLabelsMap().get(widgetId);
    if (label != null && !label.isDisposed()) {
      label.setEnabled(enabled);
    }
  }

  private void showMessage(String titleKey, String messageKey, int style) {
    MessageBox box = new MessageBox(shell, style);
    box.setText(BaseMessages.getString(PKG, titleKey));
    box.setMessage(BaseMessages.getString(PKG, messageKey));
    box.open();
  }

  private void cancel() {
    transformName = null;
    input.setChanged(changed);
    dispose();
  }

  private void ok() {
    if (Utils.isEmpty(wTransformName.getText())) {
      return;
    }
    String connectionName = widgetText(DBProcMeta.WIDGET_CONNECTION);
    widgets.getWidgetsContents(input, DBProcMeta.GUI_PLUGIN_ELEMENT_PARENT_ID);
    input.setArguments(readArguments());
    input.setResultFields(readResultFields());
    transformName = wTransformName.getText();
    input.setChanged();
    if (Utils.isEmpty(connectionName)) {
      showMessage(
          "DBProcDialog.InvalidConnection.DialogTitle",
          "DBProcDialog.InvalidConnection.DialogMessage",
          SWT.OK | SWT.ICON_ERROR);
    }
    dispose();
  }

  private void get() {
    try {
      IRowMeta r = pipelineMeta.getPrevTransformFields(variables, transformName);
      if (r != null && !r.isEmpty()) {
        ITableItemInsertListener listener =
            (tableItem, v) -> {
              tableItem.setText(2, "IN");
              return true;
            };
        BaseTransformDialog.getFieldsFromPrevious(
            r, wArguments, 1, new int[] {1}, new int[] {3}, -1, -1, listener);
      }
    } catch (HopException ke) {
      new ErrorDialog(
          shell,
          BaseMessages.getString(PKG, "DBProcDialog.FailedToGetFields.DialogTitle"),
          BaseMessages.getString(PKG, "DBProcDialog.FailedToGetFields.DialogMessage"),
          ke);
    }
  }
}
