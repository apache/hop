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

package org.apache.hop.workflow.actions.columnsexist;

import org.apache.commons.lang.StringUtils;
import org.apache.hop.core.Const;
import org.apache.hop.core.database.Database;
import org.apache.hop.core.database.DatabaseMeta;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.util.Utils;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.ui.core.database.dialog.DatabaseExplorerDialog;
import org.apache.hop.ui.core.dialog.EnterSelectionDialog;
import org.apache.hop.ui.core.dialog.ErrorDialog;
import org.apache.hop.ui.core.gui.WindowProperty;
import org.apache.hop.ui.core.widget.ColumnInfo;
import org.apache.hop.ui.core.widget.MetaSelectionLine;
import org.apache.hop.ui.core.widget.TableView;
import org.apache.hop.ui.core.widget.TextVar;
import org.apache.hop.ui.pipeline.transform.BaseTransformDialog;
import org.apache.hop.ui.workflow.action.ActionDialog;
import org.apache.hop.ui.workflow.dialog.WorkflowDialog;
import org.apache.hop.workflow.WorkflowMeta;
import org.apache.hop.workflow.action.IAction;
import org.apache.hop.workflow.action.IActionDialog;
import org.eclipse.swt.SWT;
import org.eclipse.swt.events.ModifyEvent;
import org.eclipse.swt.events.ModifyListener;
import org.eclipse.swt.events.SelectionAdapter;
import org.eclipse.swt.events.SelectionEvent;
import org.eclipse.swt.events.ShellAdapter;
import org.eclipse.swt.events.ShellEvent;
import org.eclipse.swt.layout.FormAttachment;
import org.eclipse.swt.layout.FormData;
import org.eclipse.swt.layout.FormLayout;
import org.eclipse.swt.widgets.Button;
import org.eclipse.swt.widgets.Display;
import org.eclipse.swt.widgets.Event;
import org.eclipse.swt.widgets.Label;
import org.eclipse.swt.widgets.MessageBox;
import org.eclipse.swt.widgets.Shell;
import org.eclipse.swt.widgets.TableItem;
import org.eclipse.swt.widgets.Text;

/**
 * This dialog allows you to edit the Column Exists action settings. (select the connection and the
 * table to be checked) This entry type evaluates!
 *
 * @author Samatar
 * @since 15-06-2008
 */
public class ActionColumnsExistDialog extends ActionDialog implements IActionDialog {
  private static final Class<?> PKG = ActionColumnsExist.class; // For Translator

  private Text wName;

  private MetaSelectionLine<DatabaseMeta> wConnection;

  private TextVar wTablename;

  private ActionColumnsExist action;

  private Shell shell;

  private boolean changed;

  private TableView wFields;

  private TextVar wSchemaname;

  public ActionColumnsExistDialog(Shell parent, IAction action, WorkflowMeta workflowMeta) {
    super(parent, workflowMeta);
    this.action = (ActionColumnsExist) action;
    if (this.action.getName() == null) {
      this.action.setName(BaseMessages.getString(PKG, "ActionColumnsExist.Name.Default"));
    }
  }

  @Override
  public IAction open() {
    Shell parent = getParent();
    Display display = parent.getDisplay();

    shell = new Shell(parent, SWT.DIALOG_TRIM | SWT.MIN | SWT.MAX | SWT.RESIZE);
    props.setLook(shell);
    WorkflowDialog.setShellImage(shell, action);

    ModifyListener lsMod = (ModifyEvent e) -> action.setChanged();
    changed = action.hasChanged();

    FormLayout formLayout = new FormLayout();
    formLayout.marginWidth = Const.FORM_MARGIN;
    formLayout.marginHeight = Const.FORM_MARGIN;

    shell.setLayout(formLayout);
    shell.setText(BaseMessages.getString(PKG, "ActionColumnsExist.Title"));

    int middle = props.getMiddlePct();
    int margin = Const.MARGIN;

    // Filename line
    Label wlName = new Label(shell, SWT.RIGHT);
    wlName.setText(BaseMessages.getString(PKG, "ActionColumnsExist.Name.Label"));
    props.setLook(wlName);
    FormData fdlName = new FormData();
    fdlName.left = new FormAttachment(0, 0);
    fdlName.right = new FormAttachment(middle, 0);
    fdlName.top = new FormAttachment(0, margin);
    wlName.setLayoutData(fdlName);
    wName = new Text(shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    props.setLook(wName);
    wName.addModifyListener(lsMod);
    FormData fdName = new FormData();
    fdName.left = new FormAttachment(middle, 0);
    fdName.top = new FormAttachment(0, margin);
    fdName.right = new FormAttachment(100, 0);
    wName.setLayoutData(fdName);

    // Connection line
    wConnection = addConnectionLine(shell, wName, action.getDatabase(), lsMod);

    // Schema name line
    // Schema name
    Label wlSchemaname = new Label(shell, SWT.RIGHT);
    wlSchemaname.setText(BaseMessages.getString(PKG, "ActionColumnsExist.Schemaname.Label"));
    props.setLook(wlSchemaname);
    FormData fdlSchemaname = new FormData();
    fdlSchemaname.left = new FormAttachment(0, 0);
    fdlSchemaname.right = new FormAttachment(middle, -margin);
    fdlSchemaname.top = new FormAttachment(wConnection, 2 * margin);
    wlSchemaname.setLayoutData(fdlSchemaname);

    Button wbSchema = new Button(shell, SWT.PUSH | SWT.CENTER);
    props.setLook(wbSchema);
    wbSchema.setText(BaseMessages.getString(PKG, "System.Button.Browse"));
    FormData fdbSchema = new FormData();
    fdbSchema.top = new FormAttachment(wConnection, 2 * margin);
    fdbSchema.right = new FormAttachment(100, 0);
    wbSchema.setLayoutData(fdbSchema);
    wbSchema.addSelectionListener(
        new SelectionAdapter() {
          public void widgetSelected(SelectionEvent e) {
            getSchemaNames();
          }
        });

    wSchemaname = new TextVar(variables, shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    props.setLook(wSchemaname);
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
    props.setLook(wlTablename);
    FormData fdlTablename = new FormData();
    fdlTablename.left = new FormAttachment(0, 0);
    fdlTablename.right = new FormAttachment(middle, -margin);
    fdlTablename.top = new FormAttachment(wbSchema, margin);
    wlTablename.setLayoutData(fdlTablename);

    Button wbTable = new Button(shell, SWT.PUSH | SWT.CENTER);
    props.setLook(wbTable);
    wbTable.setText(BaseMessages.getString(PKG, "System.Button.Browse"));
    FormData fdbTable = new FormData();
    fdbTable.right = new FormAttachment(100, 0);
    fdbTable.top = new FormAttachment(wbSchema, margin);
    wbTable.setLayoutData(fdbTable);
    wbTable.addSelectionListener(
        new SelectionAdapter() {
          public void widgetSelected(SelectionEvent e) {
            getTableName();
          }
        });

    // Table name
    wTablename = new TextVar(variables, shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    props.setLook(wTablename);
    wTablename.addModifyListener(lsMod);
    FormData fdTablename = new FormData();
    fdTablename.left = new FormAttachment(middle, 0);
    fdTablename.top = new FormAttachment(wbSchema, margin);
    fdTablename.right = new FormAttachment(wbTable, -margin);
    wTablename.setLayoutData(fdTablename);

    // Get columns button
    Button wbGetColumns = new Button(shell, SWT.PUSH | SWT.CENTER);
    props.setLook(wbGetColumns);
    wbGetColumns.setText(BaseMessages.getString(PKG, "ActionColumnsExist.GetColums.Button"));
    wbGetColumns.setToolTipText(
        BaseMessages.getString(PKG, "ActionColumnsExist.GetColums.Tooltip"));
    FormData fdbGetColumns = new FormData();
    fdbGetColumns.right = new FormAttachment(100, 0);
    fdbGetColumns.top = new FormAttachment(wTablename, 38);
    wbGetColumns.setLayoutData(fdbGetColumns);

    // Buttons to the right of the screen...
    // Delete
    Button wbdFilename = new Button(shell, SWT.PUSH | SWT.CENTER);
    props.setLook(wbdFilename);
    wbdFilename.setText(BaseMessages.getString(PKG, "ActionColumnsExist.FilenameDelete.Button"));
    wbdFilename.setToolTipText(
        BaseMessages.getString(PKG, "ActionColumnsExist.FilenameDelete.Tooltip"));
    FormData fdbdFilename = new FormData();
    fdbdFilename.right = new FormAttachment(100, 0);
    fdbdFilename.top = new FormAttachment(wbGetColumns, margin);
    wbdFilename.setLayoutData(fdbdFilename);

    Label wlFields = new Label(shell, SWT.NONE);
    wlFields.setText(BaseMessages.getString(PKG, "ActionColumnsExist.Fields.Label"));
    props.setLook(wlFields);
    FormData fdlFields = new FormData();
    fdlFields.left = new FormAttachment(0, 0);
    fdlFields.right = new FormAttachment(middle, -margin);
    fdlFields.top = new FormAttachment(wTablename, 3 * margin);
    wlFields.setLayoutData(fdlFields);

    int rows =
        action.getArguments() == null
            ? 1
            : (action.getArguments().length == 0 ? 0 : action.getArguments().length);

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
    fdFields.bottom = new FormAttachment(100, -50);
    wFields.setLayoutData(fdFields);

    // Delete files from the list of files...
    wbdFilename.addSelectionListener(
        new SelectionAdapter() {
          public void widgetSelected(SelectionEvent arg0) {
            int[] idx = wFields.getSelectionIndices();
            wFields.remove(idx);
            wFields.removeEmptyRows();
            wFields.setRowNums();
          }
        });

    // Delete files from the list of files...
    wbGetColumns.addSelectionListener(
        new SelectionAdapter() {
          public void widgetSelected(SelectionEvent arg0) {
            getListColumns();
          }
        });

    Button wOk = new Button(shell, SWT.PUSH);
    wOk.setText(BaseMessages.getString(PKG, "System.Button.OK"));
    FormData fd = new FormData();
    fd.right = new FormAttachment(50, -10);
    fd.bottom = new FormAttachment(100, 0);
    fd.width = 100;
    wOk.setLayoutData(fd);

    Button wCancel = new Button(shell, SWT.PUSH);
    wCancel.setText(BaseMessages.getString(PKG, "System.Button.Cancel"));
    fd = new FormData();
    fd.left = new FormAttachment(50, 10);
    fd.bottom = new FormAttachment(100, 0);
    fd.width = 100;
    wCancel.setLayoutData(fd);

    // Add listeners
    wCancel.addListener(SWT.Selection, (Event e) -> cancel());
    wOk.addListener(SWT.Selection, (Event e) -> ok());
    BaseTransformDialog.positionBottomButtons(shell, new Button[] {wOk, wCancel}, margin, wFields);
    SelectionAdapter lsDef =
        new SelectionAdapter() {
          public void widgetDefaultSelected(SelectionEvent e) {
            ok();
          }
        };

    wName.addSelectionListener(lsDef);
    wTablename.addSelectionListener(lsDef);

    // Detect X or ALT-F4 or something that kills this window...
    shell.addShellListener(
        new ShellAdapter() {
          public void shellClosed(ShellEvent e) {
            cancel();
          }
        });

    getData();

    BaseTransformDialog.setSize(shell);

    shell.open();
    props.setDialogSize(shell, "ActionColumnsExistDialogSize");
    while (!shell.isDisposed()) {
      if (!display.readAndDispatch()) {
        display.sleep();
      }
    }
    return action;
  }

  public void dispose() {
    WindowProperty winprop = new WindowProperty(shell);
    props.setScreen(winprop);
    shell.dispose();
  }

  private void getTableName() {
    String databaseName = wConnection.getText();
    if (StringUtils.isNotEmpty(databaseName)) {
      DatabaseMeta databaseMeta = this.getWorkflowMeta().findDatabase(databaseName);
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
        mb.setText(BaseMessages.getString(PKG, "System.Dialog.Error.Title"));
        mb.open();
      }
    }
  }

  /** Copy information from the meta-data input to the dialog fields. */
  public void getData() {
    if (action.getName() != null) {
      wName.setText(action.getName());
    }
    if (action.getTablename() != null) {
      wTablename.setText(action.getTablename());
    }

    if (action.getSchemaname() != null) {
      wSchemaname.setText(action.getSchemaname());
    }

    if (action.getDatabase() != null) {
      wConnection.setText(action.getDatabase().getName());
    }

    if (action.getArguments() != null) {
      for (int i = 0; i < action.getArguments().length; i++) {
        TableItem ti = wFields.table.getItem(i);
        if (action.getArguments()[i] != null) {
          ti.setText(1, action.getArguments()[i]);
        }
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
    action.setDatabase(getWorkflowMeta().findDatabase(wConnection.getText()));
    action.setTablename(wTablename.getText());
    action.setSchemaname(wSchemaname.getText());

    int nrItems = wFields.nrNonEmpty();
    int nr = 0;
    for (int i = 0; i < nrItems; i++) {
      String arg = wFields.getNonEmpty(i).getText(1);
      if (arg != null && arg.length() != 0) {
        nr++;
      }
    }
    String[] args = new String[nr];
    nr = 0;
    for (int i = 0; i < nrItems; i++) {
      String arg = wFields.getNonEmpty(i).getText(1);
      if (arg != null && arg.length() != 0) {
        args[nr] = arg;
        nr++;
      }
    }
    action.setArguments(args);

    dispose();
  }

  /** Get a list of columns */
  private void getListColumns() {
    if (!Utils.isEmpty(wTablename.getText())) {
      DatabaseMeta databaseMeta = getWorkflowMeta().findDatabase(wConnection.getText());
      if (databaseMeta != null) {
        Database database = new Database(loggingObject, variables, databaseMeta );
        try {
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
            mb.setText(BaseMessages.getString(PKG, "System.Dialog.Error.Title"));
            mb.open();
          }
        } catch (Exception e) {
          new ErrorDialog(
              shell,
              BaseMessages.getString(PKG, "System.Dialog.Error.Title"),
              BaseMessages.getString(
                  PKG, "ActionColumnsExist.ConnectionError2.DialogMessage", wTablename.getText()),
              e);
        } finally {
          database.disconnect();
        }
      }
    }
  }

  private void getSchemaNames() {
    if (wSchemaname.isDisposed()) {
      return;
    }
    DatabaseMeta databaseMeta = getWorkflowMeta().findDatabase(wConnection.getText());
    if (databaseMeta != null) {
      Database database = new Database(loggingObject, variables, databaseMeta );
      try {
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
            BaseMessages.getString(PKG, "System.Dialog.Error.Title"),
            BaseMessages.getString(PKG, "System.Dialog.AvailableSchemas.ConnectionError"),
            e);
      } finally {
        if (database != null) {
          database.disconnect();
          database = null;
        }
      }
    }
  }
}
