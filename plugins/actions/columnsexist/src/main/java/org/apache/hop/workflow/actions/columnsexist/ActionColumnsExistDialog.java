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

package org.apache.hop.workflow.actions.columnsexist;

import java.util.ArrayList;
import java.util.List;
import org.apache.commons.lang.StringUtils;
import org.apache.hop.core.Const;
import org.apache.hop.core.database.Database;
import org.apache.hop.core.database.DatabaseMeta;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.util.Utils;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.ui.core.PropsUi;
import org.apache.hop.ui.core.database.dialog.DatabaseExplorerDialog;
import org.apache.hop.ui.core.dialog.BaseDialog;
import org.apache.hop.ui.core.dialog.EnterSelectionDialog;
import org.apache.hop.ui.core.dialog.ErrorDialog;
import org.apache.hop.ui.core.dialog.MessageBox;
import org.apache.hop.ui.core.widget.ColumnInfo;
import org.apache.hop.ui.core.widget.MetaSelectionLine;
import org.apache.hop.ui.core.widget.TableView;
import org.apache.hop.ui.core.widget.TextVar;
import org.apache.hop.ui.pipeline.transform.BaseTransformDialog;
import org.apache.hop.ui.workflow.action.ActionDialog;
import org.apache.hop.ui.workflow.dialog.WorkflowDialog;
import org.apache.hop.workflow.WorkflowMeta;
import org.apache.hop.workflow.action.IAction;
import org.apache.hop.workflow.actions.columnsexist.ActionColumnsExist.ColumnExist;
import org.eclipse.swt.SWT;
import org.eclipse.swt.events.ModifyEvent;
import org.eclipse.swt.events.ModifyListener;
import org.eclipse.swt.events.SelectionAdapter;
import org.eclipse.swt.events.SelectionEvent;
import org.eclipse.swt.layout.FormAttachment;
import org.eclipse.swt.layout.FormData;
import org.eclipse.swt.layout.FormLayout;
import org.eclipse.swt.widgets.Button;
import org.eclipse.swt.widgets.Event;
import org.eclipse.swt.widgets.Label;
import org.eclipse.swt.widgets.Shell;
import org.eclipse.swt.widgets.TableItem;
import org.eclipse.swt.widgets.Text;

/**
 * This dialog allows you to edit the Column Exists action settings. (select the connection and the
 * table to be checked) This entry type evaluates!
 */
public class ActionColumnsExistDialog extends ActionDialog {
  private static final Class<?> PKG = ActionColumnsExist.class;
  private static final String CONST_ERROR_TITLE = "System.Dialog.Error.Title";

  private Text wName;
  private MetaSelectionLine<DatabaseMeta> wConnection;
  private TextVar wTablename;
  private ActionColumnsExist action;
  private boolean changed;
  private TableView wFields;
  private TextVar wSchemaname;

  public ActionColumnsExistDialog(
      Shell parent, ActionColumnsExist action, WorkflowMeta workflowMeta, IVariables variables) {
    super(parent, workflowMeta, variables);
    this.action = action;
    if (this.action.getName() == null) {
      this.action.setName(BaseMessages.getString(PKG, "ActionColumnsExist.Name.Default"));
    }
  }

  @Override
  public IAction open() {
    shell = new Shell(getParent(), SWT.DIALOG_TRIM | SWT.MIN | SWT.MAX | SWT.RESIZE);
    PropsUi.setLook(shell);
    WorkflowDialog.setShellImage(shell, action);

    ModifyListener lsMod = (ModifyEvent e) -> action.setChanged();
    changed = action.hasChanged();

    FormLayout formLayout = new FormLayout();
    formLayout.marginWidth = PropsUi.getFormMargin();
    formLayout.marginHeight = PropsUi.getFormMargin();

    shell.setLayout(formLayout);
    shell.setText(BaseMessages.getString(PKG, "ActionColumnsExist.Title"));

    int middle = props.getMiddlePct();
    int margin = PropsUi.getMargin();

    Button wOk = new Button(shell, SWT.PUSH);
    wOk.setText(BaseMessages.getString(PKG, "System.Button.OK"));
    wOk.addListener(SWT.Selection, (Event e) -> ok());
    Button wCancel = new Button(shell, SWT.PUSH);
    wCancel.setText(BaseMessages.getString(PKG, "System.Button.Cancel"));
    wCancel.addListener(SWT.Selection, (Event e) -> cancel());
    BaseTransformDialog.positionBottomButtons(shell, new Button[] {wOk, wCancel}, margin, null);

    // Action name line
    Label wlName = new Label(shell, SWT.RIGHT);
    wlName.setText(BaseMessages.getString(PKG, "ActionColumnsExist.Name.Label"));
    PropsUi.setLook(wlName);
    FormData fdlName = new FormData();
    fdlName.left = new FormAttachment(0, 0);
    fdlName.right = new FormAttachment(middle, -margin);
    fdlName.top = new FormAttachment(0, margin);
    wlName.setLayoutData(fdlName);
    wName = new Text(shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    PropsUi.setLook(wName);
    wName.addModifyListener(lsMod);
    FormData fdName = new FormData();
    fdName.left = new FormAttachment(middle, 0);
    fdName.top = new FormAttachment(0, margin);
    fdName.right = new FormAttachment(100, 0);
    wName.setLayoutData(fdName);

    // Connection line
    wConnection = addConnectionLine(shell, wName, action.getDatabaseMeta(), lsMod);

    // Schema name line
    // Schema name
    Label wlSchemaname = new Label(shell, SWT.RIGHT);
    wlSchemaname.setText(BaseMessages.getString(PKG, "ActionColumnsExist.Schemaname.Label"));
    PropsUi.setLook(wlSchemaname);
    FormData fdlSchemaname = new FormData();
    fdlSchemaname.left = new FormAttachment(0, 0);
    fdlSchemaname.right = new FormAttachment(middle, -margin);
    fdlSchemaname.top = new FormAttachment(wConnection, 2 * margin);
    wlSchemaname.setLayoutData(fdlSchemaname);

    Button wbSchema = new Button(shell, SWT.PUSH | SWT.CENTER);
    PropsUi.setLook(wbSchema);
    wbSchema.setText(BaseMessages.getString(PKG, "System.Button.Browse"));
    FormData fdbSchema = new FormData();
    fdbSchema.top = new FormAttachment(wConnection, 2 * margin);
    fdbSchema.right = new FormAttachment(100, 0);
    wbSchema.setLayoutData(fdbSchema);
    wbSchema.addSelectionListener(
        new SelectionAdapter() {
          @Override
          public void widgetSelected(SelectionEvent e) {
            getSchemaNames();
          }
        });

    wSchemaname = new TextVar(variables, shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    PropsUi.setLook(wSchemaname);
    wSchemaname.setToolTipText(
        BaseMessages.getString(PKG, "ActionColumnsExist.Schemaname.Tooltip"));
    wSchemaname.addModifyListener(lsMod);
    FormData fdSchemaname = new FormData();
    fdSchemaname.left = new FormAttachment(middle, 0);
    fdSchemaname.top = new FormAttachment(wConnection, 2 * margin);
    fdSchemaname.right = new FormAttachment(wbSchema, -margin);
    wSchemaname.setLayoutData(fdSchemaname);

    // Table name line
    Label wlTablename = new Label(shell, SWT.RIGHT);
    wlTablename.setText(BaseMessages.getString(PKG, "ActionColumnsExist.Tablename.Label"));
    PropsUi.setLook(wlTablename);
    FormData fdlTablename = new FormData();
    fdlTablename.left = new FormAttachment(0, 0);
    fdlTablename.right = new FormAttachment(middle, -margin);
    fdlTablename.top = new FormAttachment(wbSchema, margin);
    wlTablename.setLayoutData(fdlTablename);

    Button wbTable = new Button(shell, SWT.PUSH | SWT.CENTER);
    PropsUi.setLook(wbTable);
    wbTable.setText(BaseMessages.getString(PKG, "System.Button.Browse"));
    FormData fdbTable = new FormData();
    fdbTable.right = new FormAttachment(100, 0);
    fdbTable.top = new FormAttachment(wbSchema, margin);
    wbTable.setLayoutData(fdbTable);
    wbTable.addSelectionListener(
        new SelectionAdapter() {
          @Override
          public void widgetSelected(SelectionEvent e) {
            getTableName();
          }
        });

    // Table name
    wTablename = new TextVar(variables, shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    PropsUi.setLook(wTablename);
    wTablename.addModifyListener(lsMod);
    FormData fdTablename = new FormData();
    fdTablename.left = new FormAttachment(middle, 0);
    fdTablename.top = new FormAttachment(wbSchema, margin);
    fdTablename.right = new FormAttachment(wbTable, -margin);
    wTablename.setLayoutData(fdTablename);

    Label wlFields = new Label(shell, SWT.NONE);
    wlFields.setText(BaseMessages.getString(PKG, "ActionColumnsExist.Fields.Label"));
    PropsUi.setLook(wlFields);
    FormData fdlFields = new FormData();
    fdlFields.left = new FormAttachment(0, 0);
    fdlFields.right = new FormAttachment(middle, -margin);
    fdlFields.top = new FormAttachment(wTablename, 3 * margin);
    wlFields.setLayoutData(fdlFields);

    // Get columns button
    Button wbGetColumns = new Button(shell, SWT.PUSH | SWT.CENTER);
    PropsUi.setLook(wbGetColumns);
    wbGetColumns.setText(BaseMessages.getString(PKG, "ActionColumnsExist.GetColums.Button"));
    wbGetColumns.setToolTipText(
        BaseMessages.getString(PKG, "ActionColumnsExist.GetColums.Tooltip"));
    FormData fdbGetColumns = new FormData();
    fdbGetColumns.right = new FormAttachment(100, 0);
    fdbGetColumns.top = new FormAttachment(wlFields, margin);
    wbGetColumns.setLayoutData(fdbGetColumns);

    int rows = action.getColumns().size();

    final int FieldsRows = rows;

    ColumnInfo[] colinf =
        new ColumnInfo[] {
          new ColumnInfo(
              BaseMessages.getString(PKG, "ActionColumnsExist.Fields.Argument.Label"),
              ColumnInfo.COLUMN_TYPE_TEXT,
              false),
        };

    colinf[0].setUsingVariables(true);
    colinf[0].setToolTip(BaseMessages.getString(PKG, "ActionColumnsExist.Fields.Column"));

    wFields =
        new TableView(
            variables,
            shell,
            SWT.BORDER | SWT.FULL_SELECTION | SWT.MULTI,
            colinf,
            FieldsRows,
            lsMod,
            props);

    FormData fdFields = new FormData();
    fdFields.left = new FormAttachment(0, 0);
    fdFields.top = new FormAttachment(wlFields, margin);
    fdFields.right = new FormAttachment(wbGetColumns, -margin);
    fdFields.bottom = new FormAttachment(wOk, -2 * margin);
    wFields.setLayoutData(fdFields);

    // Get list of columns name from the table...
    wbGetColumns.addListener(SWT.Selection, e -> getListColumns());

    getData();

    BaseDialog.defaultShellHandling(shell, c -> ok(), c -> cancel());

    return action;
  }

  private void getTableName() {
    String databaseName = wConnection.getText();
    if (StringUtils.isNotEmpty(databaseName)) {
      DatabaseMeta databaseMeta = wConnection.loadSelectedElement();
      if (databaseMeta != null) {
        DatabaseExplorerDialog std =
            new DatabaseExplorerDialog(
                shell, SWT.NONE, variables, databaseMeta, this.getWorkflowMeta().getDatabases());
        std.setSelectedSchemaAndTable(wSchemaname.getText(), wTablename.getText());
        if (std.open()) {
          wSchemaname.setText(Const.NVL(std.getSchemaName(), ""));
          wTablename.setText(Const.NVL(std.getTableName(), ""));
        }
      } else {
        MessageBox mb = new MessageBox(shell, SWT.OK | SWT.ICON_ERROR);
        mb.setMessage(
            BaseMessages.getString(PKG, "ActionColumnsExist.ConnectionError.DialogMessage"));
        mb.setText(BaseMessages.getString(PKG, CONST_ERROR_TITLE));
        mb.open();
      }
    }
  }

  /** Copy information from the meta-data input to the dialog fields. */
  public void getData() {
    if (action.getName() != null) {
      wName.setText(action.getName());
    }
    if (action.getTableName() != null) {
      wTablename.setText(action.getTableName());
    }

    if (action.getSchemaName() != null) {
      wSchemaname.setText(action.getSchemaName());
    }

    if (action.getDatabaseMeta() != null) {
      wConnection.setText(action.getDatabaseMeta().getName());
    }

    if (action.getColumns() != null) {
      for (int i = 0; i < action.getColumns().size(); i++) {
        ColumnExist column = action.getColumns().get(i);
        TableItem ti = wFields.table.getItem(i);
        ti.setText(1, column.getName());
      }
      wFields.setRowNums();
      wFields.optWidth(true);
    }

    wName.selectAll();
    wName.setFocus();
  }

  private void cancel() {
    action.setChanged(changed);
    action = null;
    dispose();
  }

  private void ok() {
    if (Utils.isEmpty(wName.getText())) {
      MessageBox mb = new MessageBox(shell, SWT.OK | SWT.ICON_ERROR);
      mb.setText(BaseMessages.getString(PKG, "System.TransformActionNameMissing.Title"));
      mb.setMessage(BaseMessages.getString(PKG, "System.ActionNameMissing.Msg"));
      mb.open();
      return;
    }

    action.setName(wName.getText());
    action.setDatabaseMeta(wConnection.loadSelectedElement());
    action.setTableName(wTablename.getText());
    action.setSchemaName(wSchemaname.getText());

    List<ColumnExist> columns = new ArrayList<>();

    int nrItems = wFields.nrNonEmpty();
    for (int i = 0; i < nrItems; i++) {
      String name = wFields.getNonEmpty(i).getText(1);
      if (name != null && !name.isEmpty()) {
        columns.add(new ColumnExist(name));
      }
    }
    action.setColumns(columns);

    dispose();
  }

  /** Get a list of columns */
  private void getListColumns() {
    if (!Utils.isEmpty(wTablename.getText())) {
      DatabaseMeta databaseMeta = wConnection.loadSelectedElement();
      if (databaseMeta != null) {
        try (Database database = new Database(loggingObject, variables, databaseMeta)) {
          database.connect();
          IRowMeta row =
              database.getTableFieldsMeta(
                  variables.resolve(wSchemaname.getText()),
                  variables.resolve(wTablename.getText()));
          if (row != null) {
            String[] available = row.getFieldNames();

            wFields.removeAll();
            for (String s : available) {
              wFields.add(s);
            }
            wFields.removeEmptyRows();
            wFields.setRowNums();
          } else {
            MessageBox mb = new MessageBox(shell, SWT.OK | SWT.ICON_ERROR);
            mb.setMessage(
                BaseMessages.getString(PKG, "ActionColumnsExist.GetListColumsNoRow.DialogMessage"));
            mb.setText(BaseMessages.getString(PKG, CONST_ERROR_TITLE));
            mb.open();
          }
        } catch (Exception e) {
          new ErrorDialog(
              shell,
              BaseMessages.getString(PKG, CONST_ERROR_TITLE),
              BaseMessages.getString(
                  PKG, "ActionColumnsExist.ConnectionError2.DialogMessage", wTablename.getText()),
              e);
        }
      }
    }
  }

  private void getSchemaNames() {
    if (wSchemaname.isDisposed()) {
      return;
    }
    DatabaseMeta databaseMeta = wConnection.loadSelectedElement();
    if (databaseMeta != null) {
      try (Database database = new Database(loggingObject, variables, databaseMeta)) {
        database.connect();
        String[] schemas = database.getSchemas();

        if (null != schemas && schemas.length > 0) {
          schemas = Const.sortStrings(schemas);
          EnterSelectionDialog dialog =
              new EnterSelectionDialog(
                  shell,
                  schemas,
                  BaseMessages.getString(
                      PKG, "System.Dialog.AvailableSchemas.Title", wConnection.getText()),
                  BaseMessages.getString(PKG, "System.Dialog.AvailableSchemas.Message"));
          String d = dialog.open();
          if (d != null) {
            wSchemaname.setText(Const.NVL(d, ""));
          }

        } else {
          MessageBox mb = new MessageBox(shell, SWT.OK | SWT.ICON_ERROR);
          mb.setMessage(
              BaseMessages.getString(PKG, "System.Dialog.AvailableSchemas.Empty.Message"));
          mb.setText(BaseMessages.getString(PKG, "System.Dialog.AvailableSchemas.Empty.Title"));
          mb.open();
        }
      } catch (Exception e) {
        new ErrorDialog(
            shell,
            BaseMessages.getString(PKG, CONST_ERROR_TITLE),
            BaseMessages.getString(PKG, "System.Dialog.AvailableSchemas.ConnectionError"),
            e);
      }
    }
  }
}
