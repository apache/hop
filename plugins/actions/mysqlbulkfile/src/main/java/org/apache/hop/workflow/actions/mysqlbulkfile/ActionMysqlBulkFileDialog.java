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

package org.apache.hop.workflow.actions.mysqlbulkfile;

import org.apache.commons.lang.StringUtils;
import org.apache.hop.core.Const;
import org.apache.hop.core.database.Database;
import org.apache.hop.core.database.DatabaseMeta;
import org.apache.hop.core.exception.HopDatabaseException;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.util.Utils;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.ui.core.database.dialog.DatabaseExplorerDialog;
import org.apache.hop.ui.core.dialog.BaseDialog;
import org.apache.hop.ui.core.dialog.EnterSelectionDialog;
import org.apache.hop.ui.core.dialog.ErrorDialog;
import org.apache.hop.ui.core.gui.WindowProperty;
import org.apache.hop.ui.core.widget.MetaSelectionLine;
import org.apache.hop.ui.core.widget.TextVar;
import org.apache.hop.ui.pipeline.transform.BaseTransformDialog;
import org.apache.hop.ui.workflow.action.ActionDialog;
import org.apache.hop.ui.workflow.dialog.WorkflowDialog;
import org.apache.hop.workflow.WorkflowMeta;
import org.apache.hop.workflow.action.IAction;
import org.apache.hop.workflow.action.IActionDialog;
import org.eclipse.swt.SWT;
import org.eclipse.swt.custom.CCombo;
import org.eclipse.swt.events.SelectionAdapter;
import org.eclipse.swt.events.SelectionEvent;
import org.eclipse.swt.events.ShellAdapter;
import org.eclipse.swt.events.ShellEvent;
import org.eclipse.swt.layout.FormAttachment;
import org.eclipse.swt.layout.FormData;
import org.eclipse.swt.layout.FormLayout;
import org.eclipse.swt.widgets.Button;
import org.eclipse.swt.widgets.Display;
import org.eclipse.swt.widgets.Group;
import org.eclipse.swt.widgets.Label;
import org.eclipse.swt.widgets.MessageBox;
import org.eclipse.swt.widgets.Shell;
import org.eclipse.swt.widgets.Text;

/**
 * This dialog allows you to edit the MYSQL Bulk Load To a file action settings. (select the
 * connection and the table to be checked) This action evaluates!
 *
 * @author Samatar
 * @since 06-03-2006
 */
public class ActionMysqlBulkFileDialog extends ActionDialog implements IActionDialog {
  private static final Class<?> PKG = ActionMysqlBulkFile.class; // For Translator

  private static final String[] FILETYPES =
      new String[] {
        BaseMessages.getString(PKG, "ActionMysqlBulkFile.Filetype.Text"),
        BaseMessages.getString(PKG, "ActionMysqlBulkFile.Filetype.All")
      };

  private Text wName;

  private MetaSelectionLine<DatabaseMeta> wConnection;

  private TextVar wTableName;

  private TextVar wSchemaName;

  private ActionMysqlBulkFile action;

  private Shell shell;

  private TextVar wFilename;

  private Button wHighPriority;

  private TextVar wSeparator;

  private TextVar wEnclosed;

  private Button wOptionEnclosed;

  private TextVar wLineTerminated;

  // List Columns

  private TextVar wListColumn;

  private TextVar wLimitLines;

  private CCombo wIfFileExists;

  private CCombo wOutDumpValue;

  // Add File to result

  private Button wAddFileToResult;

  public ActionMysqlBulkFileDialog(
      Shell parent, IAction action, WorkflowMeta workflowMeta, IVariables variables) {
    super(parent, workflowMeta, variables);
    this.action = (ActionMysqlBulkFile) action;
    if (this.action.getName() == null) {
      this.action.setName(BaseMessages.getString(PKG, "ActionMysqlBulkFile.Name.Default"));
    }
  }

  public IAction open() {
    Shell parent = getParent();
    Display display = parent.getDisplay();

    shell = new Shell(parent, SWT.DIALOG_TRIM | SWT.MIN | SWT.MAX | SWT.RESIZE);
    props.setLook(shell);
    WorkflowDialog.setShellImage(shell, action);

    FormLayout formLayout = new FormLayout();
    formLayout.marginWidth = Const.FORM_MARGIN;
    formLayout.marginHeight = Const.FORM_MARGIN;

    shell.setLayout(formLayout);
    shell.setText(BaseMessages.getString(PKG, "ActionMysqlBulkFile.Title"));

    int middle = props.getMiddlePct();
    int margin = Const.MARGIN;

    // Buttons at the bottom of the dialog
    //
    Button wOk = new Button(shell, SWT.PUSH);
    wOk.setText(BaseMessages.getString(PKG, "System.Button.OK"));
    wOk.addListener(SWT.Selection, e -> ok());
    Button wCancel = new Button(shell, SWT.PUSH);
    wCancel.setText(BaseMessages.getString(PKG, "System.Button.Cancel"));
    wCancel.addListener(SWT.Selection, e -> cancel());
    BaseTransformDialog.positionBottomButtons(shell, new Button[] {wOk, wCancel}, margin, null);

    // Filename line
    Label wlName = new Label(shell, SWT.RIGHT);
    wlName.setText(BaseMessages.getString(PKG, "ActionMysqlBulkFile.Name.Label"));
    props.setLook(wlName);
    FormData fdlName = new FormData();
    fdlName.left = new FormAttachment(0, 0);
    fdlName.right = new FormAttachment(middle, 0);
    fdlName.top = new FormAttachment(0, margin);
    wlName.setLayoutData(fdlName);
    wName = new Text(shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    props.setLook(wName);
    FormData fdName = new FormData();
    fdName.left = new FormAttachment(middle, 0);
    fdName.top = new FormAttachment(wlName, 0, SWT.CENTER);
    fdName.right = new FormAttachment(100, 0);
    wName.setLayoutData(fdName);

    // Connection line
    wConnection = addConnectionLine(shell, wName, action.getDatabase(), null);

    // Schema name line
    // Schema name
    Label wlSchemaName = new Label(shell, SWT.RIGHT);
    wlSchemaName.setText(BaseMessages.getString(PKG, "ActionMysqlBulkFile.SchemaName.Label"));
    props.setLook(wlSchemaName);
    FormData fdlSchemaName = new FormData();
    fdlSchemaName.left = new FormAttachment(0, 0);
    fdlSchemaName.right = new FormAttachment(middle, 0);
    fdlSchemaName.top = new FormAttachment(wConnection, margin);
    wlSchemaName.setLayoutData(fdlSchemaName);

    wSchemaName = new TextVar(variables, shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    props.setLook(wSchemaName);
    wSchemaName.setToolTipText(
        BaseMessages.getString(PKG, "ActionMysqlBulkFile.SchemaName.Tooltip"));
    FormData fdSchemaName = new FormData();
    fdSchemaName.left = new FormAttachment(middle, 0);
    fdSchemaName.top = new FormAttachment(wlSchemaName, 0, SWT.CENTER);
    fdSchemaName.right = new FormAttachment(100, 0);
    wSchemaName.setLayoutData(fdSchemaName);

    // Table name line
    Label wlTableName = new Label(shell, SWT.RIGHT);
    wlTableName.setText(BaseMessages.getString(PKG, "ActionMysqlBulkFile.TableName.Label"));
    props.setLook(wlTableName);
    FormData fdlTableName = new FormData();
    fdlTableName.left = new FormAttachment(0, 0);
    fdlTableName.right = new FormAttachment(middle, 0);
    fdlTableName.top = new FormAttachment(wSchemaName, margin);
    wlTableName.setLayoutData(fdlTableName);

    Button wbTable = new Button(shell, SWT.PUSH | SWT.CENTER);
    props.setLook(wbTable);
    wbTable.setText(BaseMessages.getString(PKG, "System.Button.Browse"));
    FormData fdbTable = new FormData();
    fdbTable.right = new FormAttachment(100, 0);
    fdbTable.top = new FormAttachment(wlTableName, 0, SWT.CENTER);
    wbTable.setLayoutData(fdbTable);
    wbTable.addSelectionListener(
        new SelectionAdapter() {
          public void widgetSelected(SelectionEvent e) {
            getTableName();
          }
        });

    wTableName = new TextVar(variables, shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    props.setLook(wTableName);
    wTableName.setToolTipText(BaseMessages.getString(PKG, "ActionMysqlBulkFile.TableName.Tooltip"));
    FormData fdTablename = new FormData();
    fdTablename.left = new FormAttachment(middle, 0);
    fdTablename.top = new FormAttachment(wlTableName, 0, SWT.CENTER);
    fdTablename.right = new FormAttachment(wbTable, -margin);
    wTableName.setLayoutData(fdTablename);

    // Filename line
    //
    Label wlFilename = new Label(shell, SWT.RIGHT);
    wlFilename.setText(BaseMessages.getString(PKG, "ActionMysqlBulkFile.Filename.Label"));
    props.setLook(wlFilename);
    FormData fdlFilename = new FormData();
    fdlFilename.left = new FormAttachment(0, 0);
    fdlFilename.top = new FormAttachment(wTableName, margin);
    fdlFilename.right = new FormAttachment(middle, -margin);
    wlFilename.setLayoutData(fdlFilename);

    Button wbFilename = new Button(shell, SWT.PUSH | SWT.CENTER);
    props.setLook(wbFilename);
    wbFilename.setText(BaseMessages.getString(PKG, "System.Button.Browse"));
    FormData fdbFilename = new FormData();
    fdbFilename.right = new FormAttachment(100, 0);
    fdbFilename.top = new FormAttachment(wlFilename, 0, SWT.CENTER);
    // fdbFilename.height = 22;
    wbFilename.setLayoutData(fdbFilename);

    wFilename = new TextVar(variables, shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    props.setLook(wFilename);
    FormData fdFilename = new FormData();
    fdFilename.left = new FormAttachment(middle, 0);
    fdFilename.top = new FormAttachment(wlFilename, 0, SWT.CENTER);
    fdFilename.right = new FormAttachment(wbFilename, -margin);
    wFilename.setLayoutData(fdFilename);

    // Whenever something changes, set the tooltip to the expanded version:
    wFilename.addModifyListener(
        e -> wFilename.setToolTipText(variables.resolve(wFilename.getText())));

    wbFilename.addListener(
        SWT.Selection,
        e ->
            BaseDialog.presentFileDialog(
                shell,
                wFilename,
                variables,
                new String[] {"*.txt", "*.csv", "*"},
                FILETYPES,
                true));

    // High Priority ?
    //
    Label wlHighPriority = new Label(shell, SWT.RIGHT);
    wlHighPriority.setText(BaseMessages.getString(PKG, "ActionMysqlBulkFile.HighPriority.Label"));
    props.setLook(wlHighPriority);
    FormData fdlHighPriority = new FormData();
    fdlHighPriority.left = new FormAttachment(0, 0);
    fdlHighPriority.top = new FormAttachment(wFilename, margin);
    fdlHighPriority.right = new FormAttachment(middle, -margin);
    wlHighPriority.setLayoutData(fdlHighPriority);
    wHighPriority = new Button(shell, SWT.CHECK);
    props.setLook(wHighPriority);
    wHighPriority.setToolTipText(
        BaseMessages.getString(PKG, "ActionMysqlBulkFile.HighPriority.Tooltip"));
    FormData fdHighPriority = new FormData();
    fdHighPriority.left = new FormAttachment(middle, 0);
    fdHighPriority.top = new FormAttachment(wlHighPriority, 0, SWT.CENTER);
    fdHighPriority.right = new FormAttachment(100, 0);
    wHighPriority.setLayoutData(fdHighPriority);
    wHighPriority.addSelectionListener(
        new SelectionAdapter() {
          public void widgetSelected(SelectionEvent e) {
            action.setChanged();
          }
        });

    // Out Dump
    //
    Label wlOutDumpValue = new Label(shell, SWT.RIGHT);
    wlOutDumpValue.setText(BaseMessages.getString(PKG, "ActionMysqlBulkFile.OutDumpValue.Label"));
    props.setLook(wlOutDumpValue);
    FormData fdlOutDumpValue = new FormData();
    fdlOutDumpValue.left = new FormAttachment(0, 0);
    fdlOutDumpValue.right = new FormAttachment(middle, 0);
    fdlOutDumpValue.top = new FormAttachment(wlHighPriority, 2 * margin);
    wlOutDumpValue.setLayoutData(fdlOutDumpValue);
    wOutDumpValue = new CCombo(shell, SWT.SINGLE | SWT.READ_ONLY | SWT.BORDER);
    wOutDumpValue.add(BaseMessages.getString(PKG, "ActionMysqlBulkFile.OutFileValue.Label"));
    wOutDumpValue.add(BaseMessages.getString(PKG, "ActionMysqlBulkFile.DumpFileValue.Label"));
    wOutDumpValue.select(0); // +1: starts at -1
    props.setLook(wOutDumpValue);
    FormData fdOutDumpValue = new FormData();
    fdOutDumpValue.left = new FormAttachment(middle, 0);
    fdOutDumpValue.top = new FormAttachment(wlOutDumpValue, 0, SWT.CENTER);
    fdOutDumpValue.right = new FormAttachment(100, 0);
    wOutDumpValue.setLayoutData(fdOutDumpValue);
    wOutDumpValue.addListener(SWT.Selection, e -> dumpFile());

    // Separator
    //
    Label wlSeparator = new Label(shell, SWT.RIGHT);
    wlSeparator.setText(BaseMessages.getString(PKG, "ActionMysqlBulkFile.Separator.Label"));
    props.setLook(wlSeparator);
    FormData fdlSeparator = new FormData();
    fdlSeparator.left = new FormAttachment(0, 0);
    fdlSeparator.right = new FormAttachment(middle, 0);
    fdlSeparator.top = new FormAttachment(wOutDumpValue, margin);
    wlSeparator.setLayoutData(fdlSeparator);
    wSeparator = new TextVar(variables, shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    props.setLook(wSeparator);
    FormData fdSeparator = new FormData();
    fdSeparator.left = new FormAttachment(middle, 0);
    fdSeparator.top = new FormAttachment(wlSeparator, 0, SWT.CENTER);
    fdSeparator.right = new FormAttachment(100, 0);
    wSeparator.setLayoutData(fdSeparator);

    // Enclosed
    //
    Label wlEnclosed = new Label(shell, SWT.RIGHT);
    wlEnclosed.setText(BaseMessages.getString(PKG, "ActionMysqlBulkFile.Enclosed.Label"));
    props.setLook(wlEnclosed);
    FormData fdlEnclosed = new FormData();
    fdlEnclosed.left = new FormAttachment(0, 0);
    fdlEnclosed.right = new FormAttachment(middle, 0);
    fdlEnclosed.top = new FormAttachment(wSeparator, margin);
    wlEnclosed.setLayoutData(fdlEnclosed);
    wEnclosed = new TextVar(variables, shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    props.setLook(wEnclosed);
    FormData fdEnclosed = new FormData();
    fdEnclosed.left = new FormAttachment(middle, 0);
    fdEnclosed.top = new FormAttachment(wlEnclosed, 0, SWT.CENTER);
    fdEnclosed.right = new FormAttachment(100, 0);
    wEnclosed.setLayoutData(fdEnclosed);

    // Optionally enclosed ?
    //
    Label wlOptionEnclosed = new Label(shell, SWT.RIGHT);
    wlOptionEnclosed.setText(
        BaseMessages.getString(PKG, "ActionMysqlBulkFile.OptionEnclosed.Label"));
    props.setLook(wlOptionEnclosed);
    FormData fdlOptionEnclosed = new FormData();
    fdlOptionEnclosed.left = new FormAttachment(0, 0);
    fdlOptionEnclosed.top = new FormAttachment(wEnclosed, margin);
    fdlOptionEnclosed.right = new FormAttachment(middle, -margin);
    wlOptionEnclosed.setLayoutData(fdlOptionEnclosed);
    wOptionEnclosed = new Button(shell, SWT.CHECK);
    props.setLook(wOptionEnclosed);
    wOptionEnclosed.setToolTipText(
        BaseMessages.getString(PKG, "ActionMysqlBulkFile.OptionEnclosed.Tooltip"));
    FormData fdOptionEnclosed = new FormData();
    fdOptionEnclosed.left = new FormAttachment(middle, 0);
    fdOptionEnclosed.top = new FormAttachment(wlOptionEnclosed, 0, SWT.CENTER);
    fdOptionEnclosed.right = new FormAttachment(100, 0);
    wOptionEnclosed.setLayoutData(fdOptionEnclosed);
    wOptionEnclosed.addSelectionListener(
        new SelectionAdapter() {
          public void widgetSelected(SelectionEvent e) {
            action.setChanged();
          }
        });

    // Line terminated
    //
    Label wlLineTerminated = new Label(shell, SWT.RIGHT);
    wlLineTerminated.setText(
        BaseMessages.getString(PKG, "ActionMysqlBulkFile.LineTerminated.Label"));
    props.setLook(wlLineTerminated);
    FormData fdlLineTerminated = new FormData();
    fdlLineTerminated.left = new FormAttachment(0, 0);
    fdlLineTerminated.right = new FormAttachment(middle, 0);
    fdlLineTerminated.top = new FormAttachment(wlOptionEnclosed, 2 * margin);
    wlLineTerminated.setLayoutData(fdlLineTerminated);
    wLineTerminated = new TextVar(variables, shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    props.setLook(wLineTerminated);
    FormData fdLineterminated = new FormData();
    fdLineterminated.left = new FormAttachment(middle, 0);
    fdLineterminated.top = new FormAttachment(wlLineTerminated, 0, SWT.CENTER);
    fdLineterminated.right = new FormAttachment(100, 0);
    wLineTerminated.setLayoutData(fdLineterminated);

    // List of columns to save to a file
    //
    Label wlListColumn = new Label(shell, SWT.RIGHT);
    wlListColumn.setText(BaseMessages.getString(PKG, "ActionMysqlBulkFile.ListColumn.Label"));
    props.setLook(wlListColumn);
    FormData fdlListColumn = new FormData();
    fdlListColumn.left = new FormAttachment(0, 0);
    fdlListColumn.right = new FormAttachment(middle, 0);
    fdlListColumn.top = new FormAttachment(wLineTerminated, margin);
    wlListColumn.setLayoutData(fdlListColumn);
    Button wbListColumns = new Button(shell, SWT.PUSH | SWT.CENTER);
    props.setLook(wbListColumns);
    wbListColumns.setText(BaseMessages.getString(PKG, "System.Button.Edit"));
    FormData fdbListColumns = new FormData();
    fdbListColumns.right = new FormAttachment(100, 0);
    fdbListColumns.top = new FormAttachment(wlListColumn, 0, SWT.CENTER);
    wbListColumns.setLayoutData(fdbListColumns);
    wbListColumns.addListener(SWT.Selection, e -> getListColumns());
    wListColumn = new TextVar(variables, shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    props.setLook(wListColumn);
    wListColumn.setToolTipText(
        BaseMessages.getString(PKG, "ActionMysqlBulkFile.ListColumn.Tooltip"));
    FormData fdListColumn = new FormData();
    fdListColumn.left = new FormAttachment(middle, 0);
    fdListColumn.top = new FormAttachment(wlListColumn, 0, SWT.CENTER);
    fdListColumn.right = new FormAttachment(wbListColumns, -margin);
    wListColumn.setLayoutData(fdListColumn);

    // Number of lines to Limit
    //
    Label wlLimitLines = new Label(shell, SWT.RIGHT);
    wlLimitLines.setText(BaseMessages.getString(PKG, "ActionMysqlBulkFile.LimitLines.Label"));
    props.setLook(wlLimitLines);
    FormData fdlLimitLines = new FormData();
    fdlLimitLines.left = new FormAttachment(0, 0);
    fdlLimitLines.right = new FormAttachment(middle, 0);
    fdlLimitLines.top = new FormAttachment(wListColumn, margin);
    wlLimitLines.setLayoutData(fdlLimitLines);

    wLimitLines = new TextVar(variables, shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    props.setLook(wLimitLines);
    wLimitLines.setToolTipText(
        BaseMessages.getString(PKG, "ActionMysqlBulkFile.LimitLines.Tooltip"));
    FormData fdLimitlines = new FormData();
    fdLimitlines.left = new FormAttachment(middle, 0);
    fdLimitlines.top = new FormAttachment(wListColumn, margin);
    fdLimitlines.right = new FormAttachment(100, 0);
    wLimitLines.setLayoutData(fdLimitlines);

    // IF File Exists
    // If Output File exists
    Label wlIfFileExists = new Label(shell, SWT.RIGHT);
    wlIfFileExists.setText(BaseMessages.getString(PKG, "ActionMysqlBulkFile.IfFileExists.Label"));
    props.setLook(wlIfFileExists);
    FormData fdlIfFileExists = new FormData();
    fdlIfFileExists.left = new FormAttachment(0, 0);
    fdlIfFileExists.right = new FormAttachment(middle, 0);
    fdlIfFileExists.top = new FormAttachment(wLimitLines, margin);
    wlIfFileExists.setLayoutData(fdlIfFileExists);
    wIfFileExists = new CCombo(shell, SWT.SINGLE | SWT.READ_ONLY | SWT.BORDER);
    wIfFileExists.add(
        BaseMessages.getString(PKG, "ActionMysqlBulkFile.Create_NewFile_IfFileExists.Label"));
    wIfFileExists.add(
        BaseMessages.getString(PKG, "ActionMysqlBulkFile.Do_Nothing_IfFileExists.Label"));
    wIfFileExists.add(BaseMessages.getString(PKG, "ActionMysqlBulkFile.Fail_IfFileExists.Label"));
    wIfFileExists.select(2); // +1: starts at -1

    props.setLook(wIfFileExists);
    FormData fdIfFileExists = new FormData();
    fdIfFileExists.left = new FormAttachment(middle, 0);
    fdIfFileExists.top = new FormAttachment(wLimitLines, margin);
    fdIfFileExists.right = new FormAttachment(100, 0);
    wIfFileExists.setLayoutData(fdIfFileExists);

    fdIfFileExists = new FormData();
    fdIfFileExists.left = new FormAttachment(middle, 0);
    fdIfFileExists.top = new FormAttachment(wLimitLines, margin);
    fdIfFileExists.right = new FormAttachment(100, 0);
    wIfFileExists.setLayoutData(fdIfFileExists);

    // fileresult grouping?
    // ////////////////////////
    // START OF LOGGING GROUP///
    // /
    Group wFileResult = new Group(shell, SWT.SHADOW_NONE);
    props.setLook(wFileResult);
    wFileResult.setText(BaseMessages.getString(PKG, "ActionMysqlBulkFile.FileResult.Group.Label"));

    FormLayout groupLayout = new FormLayout();
    groupLayout.marginWidth = 10;
    groupLayout.marginHeight = 10;

    wFileResult.setLayout(groupLayout);

    // Add file to result
    Label wlAddFileToResult = new Label(wFileResult, SWT.RIGHT);
    wlAddFileToResult.setText(
        BaseMessages.getString(PKG, "ActionMysqlBulkFile.AddFileToResult.Label"));
    props.setLook(wlAddFileToResult);
    FormData fdlAddFileToResult = new FormData();
    fdlAddFileToResult.left = new FormAttachment(0, 0);
    fdlAddFileToResult.top = new FormAttachment(wIfFileExists, margin);
    fdlAddFileToResult.right = new FormAttachment(middle, -margin);
    wlAddFileToResult.setLayoutData(fdlAddFileToResult);
    wAddFileToResult = new Button(wFileResult, SWT.CHECK);
    props.setLook(wAddFileToResult);
    wAddFileToResult.setToolTipText(
        BaseMessages.getString(PKG, "ActionMysqlBulkFile.AddFileToResult.Tooltip"));
    FormData fdAddFileToResult = new FormData();
    fdAddFileToResult.left = new FormAttachment(middle, 0);
    fdAddFileToResult.top = new FormAttachment(wlAddFileToResult, 0, SWT.CENTER);
    fdAddFileToResult.right = new FormAttachment(100, 0);
    wAddFileToResult.setLayoutData(fdAddFileToResult);
    wAddFileToResult.addSelectionListener(
        new SelectionAdapter() {
          public void widgetSelected(SelectionEvent e) {
            action.setChanged();
          }
        });

    FormData fdFileResult = new FormData();
    fdFileResult.left = new FormAttachment(0, margin);
    fdFileResult.top = new FormAttachment(wIfFileExists, margin);
    fdFileResult.right = new FormAttachment(100, -margin);
    fdFileResult.bottom = new FormAttachment(wOk, -2 * margin);
    wFileResult.setLayoutData(fdFileResult);

    // ///////////////////////////////////////////////////////////
    // / END OF LOGGING GROUP
    // ///////////////////////////////////////////////////////////

    // Add listeners

    SelectionAdapter lsDef =
        new SelectionAdapter() {
          public void widgetDefaultSelected(SelectionEvent e) {
            ok();
          }
        };

    wName.addSelectionListener(lsDef);
    wTableName.addSelectionListener(lsDef);

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
    props.setDialogSize(shell, "ActionMysqlBulkFileDialogSize");
    while (!shell.isDisposed()) {
      if (!display.readAndDispatch()) {
        display.sleep();
      }
    }
    return action;
  }

  public void dumpFile() {

    action.setChanged();
    if (wOutDumpValue.getSelectionIndex() == 0) {
      wSeparator.setEnabled(true);
      wEnclosed.setEnabled(true);
      wLineTerminated.setEnabled(true);

    } else {
      wSeparator.setEnabled(false);
      wEnclosed.setEnabled(false);
      wLineTerminated.setEnabled(false);
    }
  }

  public void dispose() {
    WindowProperty winprop = new WindowProperty(shell);
    props.setScreen(winprop);
    shell.dispose();
  }

  /** Copy information from the meta-data input to the dialog fields. */
  public void getData() {
    wName.setText(Const.NVL(action.getName(), ""));
    if (action.getSchemaName() != null) {
      wTableName.setText(action.getSchemaName());
    }
    if (action.getTableName() != null) {
      wTableName.setText(action.getTableName());
    }
    if (action.getFilename() != null) {
      wFilename.setText(action.getFilename());
    }
    if (action.getSeparator() != null) {
      wSeparator.setText(action.getSeparator());
    }

    if (action.getEnclosed() != null) {
      wEnclosed.setText(action.getEnclosed());
    }
    wOptionEnclosed.setSelection(action.isOptionEnclosed());

    if (action.getLineTerminated() != null) {
      wLineTerminated.setText(action.getLineTerminated());
    }

    wHighPriority.setSelection(action.isHighPriority());
    wOptionEnclosed.setSelection(action.isOptionEnclosed());

    if (action.getLimitLines() != null) {
      wLimitLines.setText(action.getLimitLines());
    } else {
      wLimitLines.setText("0");
    }

    if (action.getListColumn() != null) {
      wListColumn.setText(action.getListColumn());
    }

    if (action.outDumpValue >= 0) {
      wOutDumpValue.select(action.outDumpValue);
    } else {
      wOutDumpValue.select(0); // NORMAL priority
    }

    if (action.ifFileExists >= 0) {
      wIfFileExists.select(action.ifFileExists);
    } else {
      wIfFileExists.select(2); // FAIL
    }

    if (action.getDatabase() != null) {
      wConnection.setText(action.getDatabase().getName());
    }

    wAddFileToResult.setSelection(action.isAddFileToResult());

    wName.selectAll();
    wName.setFocus();
  }

  private void cancel() {
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
    action.setSchemaName(wSchemaName.getText());
    action.setTableName(wTableName.getText());
    action.setFilename(wFilename.getText());
    action.setSeparator(wSeparator.getText());
    action.setEnclosed(wEnclosed.getText());
    action.setOptionEnclosed(wOptionEnclosed.getSelection());
    action.setLineTerminated(wLineTerminated.getText());

    action.setLimitLines(wLimitLines.getText());
    action.setListColumn(wListColumn.getText());

    action.outDumpValue = wOutDumpValue.getSelectionIndex();

    action.setHighPriority(wHighPriority.getSelection());
    action.ifFileExists = wIfFileExists.getSelectionIndex();

    action.setAddFileToResult(wAddFileToResult.getSelection());

    action.setChanged();
    dispose();
  }

  private void getTableName() {
    String databaseName = wConnection.getText();
    if (StringUtils.isNotEmpty(databaseName)) {
      DatabaseMeta databaseMeta = getWorkflowMeta().findDatabase(databaseName);
      if (databaseMeta != null) {
        DatabaseExplorerDialog std =
            new DatabaseExplorerDialog(
                shell, SWT.NONE, variables, databaseMeta, getWorkflowMeta().getDatabases());
        std.setSelectedSchemaAndTable(wSchemaName.getText(), wTableName.getText());
        if (std.open()) {
          wTableName.setText(Const.NVL(std.getTableName(), ""));
        }
      } else {
        MessageBox mb = new MessageBox(shell, SWT.OK | SWT.ICON_ERROR);
        mb.setMessage(
            BaseMessages.getString(PKG, "ActionMysqlBulkFile.ConnectionError2.DialogMessage"));
        mb.setText(BaseMessages.getString(PKG, "System.Dialog.Error.Title"));
        mb.open();
      }
    }
  }

  /** Get a list of columns, comma separated, allow the user to select from it. */
  private void getListColumns() {
    if (!Utils.isEmpty(wTableName.getText())) {
      DatabaseMeta databaseMeta = getWorkflowMeta().findDatabase(wConnection.getText());
      if (databaseMeta != null) {
        Database database = new Database(loggingObject, variables, databaseMeta);
        try {
          database.connect();
          IRowMeta row = database.getTableFieldsMeta(wSchemaName.getText(), wTableName.getText());
          String[] available = row.getFieldNames();

          String[] source = wListColumn.getText().split(",");
          for (int i = 0; i < source.length; i++) {
            source[i] = Const.trim(source[i]);
          }
          int[] idxSource = Const.indexsOfStrings(source, available);
          EnterSelectionDialog dialog =
              new EnterSelectionDialog(
                  shell,
                  available,
                  BaseMessages.getString(PKG, "ActionMysqlBulkFile.SelectColumns.Title"),
                  BaseMessages.getString(PKG, "ActionMysqlBulkFile.SelectColumns.Message"));
          dialog.setMulti(true);
          dialog.setAvoidQuickSearch();
          dialog.setSelectedNrs(idxSource);
          if (dialog.open() != null) {
            StringBuilder columns = new StringBuilder();
            int[] idx = dialog.getSelectionIndeces();
            for (int i = 0; i < idx.length; i++) {
              if (i > 0) {
                columns.append(", ");
              }
              columns.append(available[idx[i]]);
            }
            wListColumn.setText(columns.toString());
          }
        } catch (HopDatabaseException e) {
          new ErrorDialog(
              shell,
              BaseMessages.getString(PKG, "System.Dialog.Error.Title"),
              BaseMessages.getString(PKG, "ActionMysqlBulkFile.ConnectionError2.DialogMessage"),
              e);
        } finally {
          database.disconnect();
        }
      }
    }
  }
}
