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

package org.apache.hop.workflow.actions.mysqlbulkload;

import org.apache.commons.lang.StringUtils;
import org.apache.hop.core.Const;
import org.apache.hop.core.database.Database;
import org.apache.hop.core.database.DatabaseMeta;
import org.apache.hop.core.exception.HopDatabaseException;
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
import org.apache.hop.ui.core.widget.MetaSelectionLine;
import org.apache.hop.ui.core.widget.TextVar;
import org.apache.hop.ui.pipeline.transform.BaseTransformDialog;
import org.apache.hop.ui.workflow.action.ActionDialog;
import org.apache.hop.ui.workflow.dialog.WorkflowDialog;
import org.apache.hop.workflow.WorkflowMeta;
import org.apache.hop.workflow.action.IAction;
import org.eclipse.swt.SWT;
import org.eclipse.swt.custom.CCombo;
import org.eclipse.swt.events.ModifyListener;
import org.eclipse.swt.events.SelectionAdapter;
import org.eclipse.swt.events.SelectionEvent;
import org.eclipse.swt.layout.FormAttachment;
import org.eclipse.swt.layout.FormData;
import org.eclipse.swt.layout.FormLayout;
import org.eclipse.swt.widgets.Button;
import org.eclipse.swt.widgets.Group;
import org.eclipse.swt.widgets.Label;
import org.eclipse.swt.widgets.Shell;
import org.eclipse.swt.widgets.Text;

/** Dialog class for the MySqlBulkLoader. */
public class ActionMysqlBulkLoadDialog extends ActionDialog {
  private static final Class<?> PKG = ActionMysqlBulkLoad.class;

  private static final String[] FILETYPES =
      new String[] {
        BaseMessages.getString(PKG, "ActionMysqlBulkLoad.Filetype.Text"),
        BaseMessages.getString(PKG, "ActionMysqlBulkLoad.Filetype.All")
      };

  private Text wName;

  private MetaSelectionLine<DatabaseMeta> wConnection;

  private TextVar wSchemaname;

  private TextVar wTablename;

  private ActionMysqlBulkLoad action;

  private boolean changed;

  private TextVar wFilename;

  private Button wLocalInfile;

  private TextVar wSeparator;

  private TextVar wEnclosed;

  private TextVar wEscaped;

  private TextVar wLineterminated;

  private TextVar wLinestarted;

  private TextVar wListattribut;

  private TextVar wIgnorelines;

  private Button wReplaceData;

  private CCombo wPriorityValue;

  // Add File to result

  private Button wAddFileToResult;

  public ActionMysqlBulkLoadDialog(
      Shell parent, ActionMysqlBulkLoad action, WorkflowMeta workflowMeta, IVariables variables) {
    super(parent, workflowMeta, variables);
    this.action = action;
    if (this.action.getName() == null) {
      this.action.setName(BaseMessages.getString(PKG, "ActionMysqlBulkLoad.Name.Default"));
    }
  }

  @Override
  public IAction open() {

    shell = new Shell(getParent(), SWT.DIALOG_TRIM | SWT.MIN | SWT.MAX | SWT.RESIZE);
    PropsUi.setLook(shell);
    WorkflowDialog.setShellImage(shell, action);

    ModifyListener lsMod = e -> action.setChanged();
    changed = action.hasChanged();

    FormLayout formLayout = new FormLayout();
    formLayout.marginWidth = PropsUi.getFormMargin();
    formLayout.marginHeight = PropsUi.getFormMargin();

    shell.setLayout(formLayout);
    shell.setText(BaseMessages.getString(PKG, "ActionMysqlBulkLoad.Title"));

    int middle = props.getMiddlePct();
    int margin = PropsUi.getMargin();

    // Buttons go at the very bottom
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
    wlName.setText(BaseMessages.getString(PKG, "ActionMysqlBulkLoad.Name.Label"));
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
    DatabaseMeta databaseMeta = workflowMeta.findDatabase(action.getConnection(), variables);
    wConnection = addConnectionLine(shell, wName, databaseMeta, lsMod);

    // Schema name line
    // Schema name
    Label wlSchemaname = new Label(shell, SWT.RIGHT);
    wlSchemaname.setText(BaseMessages.getString(PKG, "ActionMysqlBulkLoad.Schemaname.Label"));
    PropsUi.setLook(wlSchemaname);
    FormData fdlSchemaname = new FormData();
    fdlSchemaname.left = new FormAttachment(0, 0);
    fdlSchemaname.right = new FormAttachment(middle, -margin);
    fdlSchemaname.top = new FormAttachment(wConnection, margin);
    wlSchemaname.setLayoutData(fdlSchemaname);

    wSchemaname = new TextVar(variables, shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    PropsUi.setLook(wSchemaname);
    wSchemaname.setToolTipText(
        BaseMessages.getString(PKG, "ActionMysqlBulkLoad.Schemaname.Tooltip"));
    wSchemaname.addModifyListener(lsMod);
    FormData fdSchemaname = new FormData();
    fdSchemaname.left = new FormAttachment(middle, 0);
    fdSchemaname.top = new FormAttachment(wConnection, margin);
    fdSchemaname.right = new FormAttachment(100, 0);
    wSchemaname.setLayoutData(fdSchemaname);

    // Table name line
    Label wlTablename = new Label(shell, SWT.RIGHT);
    wlTablename.setText(BaseMessages.getString(PKG, "ActionMysqlBulkLoad.Tablename.Label"));
    PropsUi.setLook(wlTablename);
    FormData fdlTablename = new FormData();
    fdlTablename.left = new FormAttachment(0, 0);
    fdlTablename.right = new FormAttachment(middle, -margin);
    fdlTablename.top = new FormAttachment(wSchemaname, margin);
    wlTablename.setLayoutData(fdlTablename);

    Button wbTable = new Button(shell, SWT.PUSH | SWT.CENTER);
    PropsUi.setLook(wbTable);
    wbTable.setText(BaseMessages.getString(PKG, "System.Button.Browse"));
    FormData fdbTable = new FormData();
    fdbTable.right = new FormAttachment(100, 0);
    fdbTable.top = new FormAttachment(wSchemaname, margin / 2);
    wbTable.setLayoutData(fdbTable);
    wbTable.addSelectionListener(
        new SelectionAdapter() {
          @Override
          public void widgetSelected(SelectionEvent e) {
            getTableName();
          }
        });

    wTablename = new TextVar(variables, shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    PropsUi.setLook(wTablename);
    wTablename.addModifyListener(lsMod);
    FormData fdTablename = new FormData();
    fdTablename.left = new FormAttachment(middle, 0);
    fdTablename.top = new FormAttachment(wSchemaname, margin);
    fdTablename.right = new FormAttachment(wbTable, -margin);
    wTablename.setLayoutData(fdTablename);

    // Filename line
    // File
    Label wlFilename = new Label(shell, SWT.RIGHT);
    wlFilename.setText(BaseMessages.getString(PKG, "ActionMysqlBulkLoad.Filename.Label"));
    PropsUi.setLook(wlFilename);
    FormData fdlFilename = new FormData();
    fdlFilename.left = new FormAttachment(0, 0);
    fdlFilename.top = new FormAttachment(wTablename, margin);
    fdlFilename.right = new FormAttachment(middle, -margin);
    wlFilename.setLayoutData(fdlFilename);

    Button wbFilename = new Button(shell, SWT.PUSH | SWT.CENTER);
    PropsUi.setLook(wbFilename);
    wbFilename.setText(BaseMessages.getString(PKG, "System.Button.Browse"));
    FormData fdbFilename = new FormData();
    fdbFilename.right = new FormAttachment(100, 0);
    fdbFilename.top = new FormAttachment(wTablename, 0);
    wbFilename.setLayoutData(fdbFilename);

    wFilename = new TextVar(variables, shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    PropsUi.setLook(wFilename);
    wFilename.addModifyListener(lsMod);
    FormData fdFilename = new FormData();
    fdFilename.left = new FormAttachment(middle, 0);
    fdFilename.top = new FormAttachment(wTablename, margin);
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

    // Local
    //
    Label wlLocalInfile = new Label(shell, SWT.RIGHT);
    wlLocalInfile.setText(BaseMessages.getString(PKG, "ActionMysqlBulkLoad.LocalInfile.Label"));
    PropsUi.setLook(wlLocalInfile);
    FormData fdlLocalInfile = new FormData();
    fdlLocalInfile.left = new FormAttachment(0, 0);
    fdlLocalInfile.top = new FormAttachment(wFilename, margin);
    fdlLocalInfile.right = new FormAttachment(middle, -margin);
    wlLocalInfile.setLayoutData(fdlLocalInfile);
    wLocalInfile = new Button(shell, SWT.CHECK);
    PropsUi.setLook(wLocalInfile);
    wLocalInfile.setToolTipText(
        BaseMessages.getString(PKG, "ActionMysqlBulkLoad.LocalInfile.Tooltip"));
    FormData fdLocalInfile = new FormData();
    fdLocalInfile.left = new FormAttachment(middle, 0);
    fdLocalInfile.top = new FormAttachment(wlLocalInfile, 0, SWT.CENTER);
    fdLocalInfile.right = new FormAttachment(100, 0);
    wLocalInfile.setLayoutData(fdLocalInfile);
    wLocalInfile.addSelectionListener(
        new SelectionAdapter() {
          @Override
          public void widgetSelected(SelectionEvent e) {
            action.setChanged();
          }
        });

    // Priority
    //
    Label wlPriorityValue = new Label(shell, SWT.RIGHT);
    wlPriorityValue.setText(BaseMessages.getString(PKG, "ActionMysqlBulkLoad.ProrityValue.Label"));
    PropsUi.setLook(wlPriorityValue);
    FormData fdlPriorityValue = new FormData();
    fdlPriorityValue.left = new FormAttachment(0, 0);
    fdlPriorityValue.right = new FormAttachment(middle, -margin);
    fdlPriorityValue.top = new FormAttachment(wlLocalInfile, 2 * margin);
    wlPriorityValue.setLayoutData(fdlPriorityValue);
    wPriorityValue = new CCombo(shell, SWT.SINGLE | SWT.READ_ONLY | SWT.BORDER);
    wPriorityValue.add(BaseMessages.getString(PKG, "ActionMysqlBulkLoad.NorProrityValue.Label"));
    wPriorityValue.add(BaseMessages.getString(PKG, "ActionMysqlBulkLoad.LowProrityValue.Label"));
    wPriorityValue.add(BaseMessages.getString(PKG, "ActionMysqlBulkLoad.ConProrityValue.Label"));
    wPriorityValue.select(0); // +1: starts at -1
    PropsUi.setLook(wPriorityValue);
    FormData fdPriorityValue = new FormData();
    fdPriorityValue.left = new FormAttachment(middle, 0);
    fdPriorityValue.top = new FormAttachment(wlLocalInfile, 2 * margin);
    fdPriorityValue.right = new FormAttachment(100, 0);
    wPriorityValue.setLayoutData(fdPriorityValue);

    // Separator
    // Separator
    Label wlSeparator = new Label(shell, SWT.RIGHT);
    wlSeparator.setText(BaseMessages.getString(PKG, "ActionMysqlBulkLoad.Separator.Label"));
    PropsUi.setLook(wlSeparator);
    FormData fdlSeparator = new FormData();
    fdlSeparator.left = new FormAttachment(0, 0);
    fdlSeparator.right = new FormAttachment(middle, -margin);
    fdlSeparator.top = new FormAttachment(wPriorityValue, margin);
    wlSeparator.setLayoutData(fdlSeparator);

    wSeparator = new TextVar(variables, shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    PropsUi.setLook(wSeparator);
    wSeparator.addModifyListener(lsMod);
    FormData fdSeparator = new FormData();
    fdSeparator.left = new FormAttachment(middle, 0);
    fdSeparator.top = new FormAttachment(wPriorityValue, margin);
    fdSeparator.right = new FormAttachment(100, 0);
    wSeparator.setLayoutData(fdSeparator);

    // enclosed
    // Enclosed
    Label wlEnclosed = new Label(shell, SWT.RIGHT);
    wlEnclosed.setText(BaseMessages.getString(PKG, "ActionMysqlBulkLoad.Enclosed.Label"));
    PropsUi.setLook(wlEnclosed);
    FormData fdlEnclosed = new FormData();
    fdlEnclosed.left = new FormAttachment(0, 0);
    fdlEnclosed.right = new FormAttachment(middle, -margin);
    fdlEnclosed.top = new FormAttachment(wSeparator, margin);
    wlEnclosed.setLayoutData(fdlEnclosed);

    wEnclosed = new TextVar(variables, shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    PropsUi.setLook(wEnclosed);
    wEnclosed.addModifyListener(lsMod);
    FormData fdEnclosed = new FormData();
    fdEnclosed.left = new FormAttachment(middle, 0);
    fdEnclosed.top = new FormAttachment(wSeparator, margin);
    fdEnclosed.right = new FormAttachment(100, 0);
    wEnclosed.setLayoutData(fdEnclosed);

    // escaped
    // Escaped
    Label wlEscaped = new Label(shell, SWT.RIGHT);
    wlEscaped.setText(BaseMessages.getString(PKG, "ActionMysqlBulkLoad.Escaped.Label"));
    PropsUi.setLook(wlEscaped);
    FormData fdlEscaped = new FormData();
    fdlEscaped.left = new FormAttachment(0, 0);
    fdlEscaped.right = new FormAttachment(middle, -margin);
    fdlEscaped.top = new FormAttachment(wEnclosed, margin);
    wlEscaped.setLayoutData(fdlEscaped);

    wEscaped = new TextVar(variables, shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    PropsUi.setLook(wEscaped);
    wEscaped.setToolTipText(BaseMessages.getString(PKG, "ActionMysqlBulkLoad.Escaped.Tooltip"));
    wEscaped.addModifyListener(lsMod);
    FormData fdEscaped = new FormData();
    fdEscaped.left = new FormAttachment(middle, 0);
    fdEscaped.top = new FormAttachment(wEnclosed, margin);
    fdEscaped.right = new FormAttachment(100, 0);
    wEscaped.setLayoutData(fdEscaped);

    // Line started
    // Line starting
    Label wlLinestarted = new Label(shell, SWT.RIGHT);
    wlLinestarted.setText(BaseMessages.getString(PKG, "ActionMysqlBulkLoad.Linestarted.Label"));
    PropsUi.setLook(wlLinestarted);
    FormData fdlLinestarted = new FormData();
    fdlLinestarted.left = new FormAttachment(0, 0);
    fdlLinestarted.right = new FormAttachment(middle, -margin);
    fdlLinestarted.top = new FormAttachment(wEscaped, margin);
    wlLinestarted.setLayoutData(fdlLinestarted);

    wLinestarted = new TextVar(variables, shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    PropsUi.setLook(wLinestarted);
    wLinestarted.addModifyListener(lsMod);
    FormData fdLinestarted = new FormData();
    fdLinestarted.left = new FormAttachment(middle, 0);
    fdLinestarted.top = new FormAttachment(wEscaped, margin);
    fdLinestarted.right = new FormAttachment(100, 0);
    wLinestarted.setLayoutData(fdLinestarted);

    // Line terminated
    // Line terminated
    Label wlLineterminated = new Label(shell, SWT.RIGHT);
    wlLineterminated.setText(
        BaseMessages.getString(PKG, "ActionMysqlBulkLoad.Lineterminated.Label"));
    PropsUi.setLook(wlLineterminated);
    FormData fdlLineterminated = new FormData();
    fdlLineterminated.left = new FormAttachment(0, 0);
    fdlLineterminated.right = new FormAttachment(middle, -margin);
    fdlLineterminated.top = new FormAttachment(wLinestarted, margin);
    wlLineterminated.setLayoutData(fdlLineterminated);

    wLineterminated = new TextVar(variables, shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    PropsUi.setLook(wLineterminated);
    wLineterminated.addModifyListener(lsMod);
    FormData fdLineterminated = new FormData();
    fdLineterminated.left = new FormAttachment(middle, 0);
    fdLineterminated.top = new FormAttachment(wLinestarted, margin);
    fdLineterminated.right = new FormAttachment(100, 0);
    wLineterminated.setLayoutData(fdLineterminated);

    // List of columns to set for
    // List Columns
    Label wlListattribut = new Label(shell, SWT.RIGHT);
    wlListattribut.setText(BaseMessages.getString(PKG, "ActionMysqlBulkLoad.Listattribut.Label"));
    PropsUi.setLook(wlListattribut);
    FormData fdlListattribut = new FormData();
    fdlListattribut.left = new FormAttachment(0, 0);
    fdlListattribut.right = new FormAttachment(middle, -margin);
    fdlListattribut.top = new FormAttachment(wLineterminated, margin);
    wlListattribut.setLayoutData(fdlListattribut);

    Button wbListattribut = new Button(shell, SWT.PUSH | SWT.CENTER);
    PropsUi.setLook(wbListattribut);
    wbListattribut.setText(BaseMessages.getString(PKG, "System.Button.Edit"));
    FormData fdbListattribut = new FormData();
    fdbListattribut.right = new FormAttachment(100, 0);
    fdbListattribut.top = new FormAttachment(wLineterminated, margin);
    wbListattribut.setLayoutData(fdbListattribut);
    wbListattribut.addSelectionListener(
        new SelectionAdapter() {
          @Override
          public void widgetSelected(SelectionEvent e) {
            getListColumns();
          }
        });

    wListattribut = new TextVar(variables, shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    PropsUi.setLook(wListattribut);
    wListattribut.setToolTipText(
        BaseMessages.getString(PKG, "ActionMysqlBulkLoad.Listattribut.Tooltip"));
    wListattribut.addModifyListener(lsMod);
    FormData fdListattribut = new FormData();
    fdListattribut.left = new FormAttachment(middle, 0);
    fdListattribut.top = new FormAttachment(wLineterminated, margin);
    fdListattribut.right = new FormAttachment(wbListattribut, -margin);
    wListattribut.setLayoutData(fdListattribut);

    // Replace data
    // Replace
    Label wlReplaceData = new Label(shell, SWT.RIGHT);
    wlReplaceData.setText(BaseMessages.getString(PKG, "ActionMysqlBulkLoad.Replacedata.Label"));
    PropsUi.setLook(wlReplaceData);
    FormData fdlReplaceData = new FormData();
    fdlReplaceData.left = new FormAttachment(0, 0);
    fdlReplaceData.top = new FormAttachment(wListattribut, margin);
    fdlReplaceData.right = new FormAttachment(middle, -margin);
    wlReplaceData.setLayoutData(fdlReplaceData);
    wReplaceData = new Button(shell, SWT.CHECK);
    PropsUi.setLook(wReplaceData);
    wReplaceData.setToolTipText(
        BaseMessages.getString(PKG, "ActionMysqlBulkLoad.Replacedata.Tooltip"));
    FormData fdReplaceData = new FormData();
    fdReplaceData.left = new FormAttachment(middle, 0);
    fdReplaceData.top = new FormAttachment(wlReplaceData, 0, SWT.CENTER);
    fdReplaceData.right = new FormAttachment(100, 0);
    wReplaceData.setLayoutData(fdReplaceData);
    wReplaceData.addSelectionListener(
        new SelectionAdapter() {
          @Override
          public void widgetSelected(SelectionEvent e) {
            action.setChanged();
          }
        });

    // Nbr of lines to ignore
    // Ignore First lines
    Label wlIgnorelines = new Label(shell, SWT.RIGHT);
    wlIgnorelines.setText(BaseMessages.getString(PKG, "ActionMysqlBulkLoad.Ignorelines.Label"));
    PropsUi.setLook(wlIgnorelines);
    FormData fdlIgnorelines = new FormData();
    fdlIgnorelines.left = new FormAttachment(0, 0);
    fdlIgnorelines.right = new FormAttachment(middle, -margin);
    fdlIgnorelines.top = new FormAttachment(wlReplaceData, 2 * margin);
    wlIgnorelines.setLayoutData(fdlIgnorelines);

    wIgnorelines = new TextVar(variables, shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    PropsUi.setLook(wIgnorelines);
    wIgnorelines.addModifyListener(lsMod);
    FormData fdIgnorelines = new FormData();
    fdIgnorelines.left = new FormAttachment(middle, 0);
    fdIgnorelines.top = new FormAttachment(wlReplaceData, 2 * margin);
    fdIgnorelines.right = new FormAttachment(100, 0);
    wIgnorelines.setLayoutData(fdIgnorelines);

    // fileresult grouping?
    // ////////////////////////
    // START OF FileResult GROUP///
    // /
    Group wFileResult = new Group(shell, SWT.SHADOW_NONE);
    PropsUi.setLook(wFileResult);
    wFileResult.setText(BaseMessages.getString(PKG, "ActionMysqlBulkLoad.FileResult.Group.Label"));

    FormLayout groupLayout = new FormLayout();
    groupLayout.marginWidth = 10;
    groupLayout.marginHeight = 10;

    wFileResult.setLayout(groupLayout);

    // Add file to result
    Label wlAddFileToResult = new Label(wFileResult, SWT.RIGHT);
    wlAddFileToResult.setText(
        BaseMessages.getString(PKG, "ActionMysqlBulkLoad.AddFileToResult.Label"));
    PropsUi.setLook(wlAddFileToResult);
    FormData fdlAddFileToResult = new FormData();
    fdlAddFileToResult.left = new FormAttachment(0, 0);
    fdlAddFileToResult.top = new FormAttachment(wIgnorelines, margin);
    fdlAddFileToResult.right = new FormAttachment(middle, -margin);
    wlAddFileToResult.setLayoutData(fdlAddFileToResult);
    wAddFileToResult = new Button(wFileResult, SWT.CHECK);
    PropsUi.setLook(wAddFileToResult);
    wAddFileToResult.setToolTipText(
        BaseMessages.getString(PKG, "ActionMysqlBulkLoad.AddFileToResult.Tooltip"));
    FormData fdAddFileToResult = new FormData();
    fdAddFileToResult.left = new FormAttachment(middle, 0);
    fdAddFileToResult.top = new FormAttachment(wlAddFileToResult, 0, SWT.CENTER);
    fdAddFileToResult.right = new FormAttachment(100, 0);
    wAddFileToResult.setLayoutData(fdAddFileToResult);
    wAddFileToResult.addSelectionListener(
        new SelectionAdapter() {
          @Override
          public void widgetSelected(SelectionEvent e) {
            action.setChanged();
          }
        });

    FormData fdFileResult = new FormData();
    fdFileResult.left = new FormAttachment(0, margin);
    fdFileResult.top = new FormAttachment(wIgnorelines, margin);
    fdFileResult.right = new FormAttachment(100, -margin);
    fdFileResult.bottom = new FormAttachment(wOk, -2 * margin);
    wFileResult.setLayoutData(fdFileResult);

    // ///////////////////////////////////////////////////////////
    // / END OF FilesResult GROUP
    // ///////////////////////////////////////////////////////////

    getData();

    BaseDialog.defaultShellHandling(shell, c -> ok(), c -> cancel());

    return action;
  }

  /** Copy information from the meta-data input to the dialog fields. */
  public void getData() {
    wName.setText(Const.nullToEmpty(action.getName()));
    if (action.getSchemaName() != null) {
      wSchemaname.setText(action.getSchemaName());
    }
    if (action.getTableName() != null) {
      wTablename.setText(action.getTableName());
    }
    if (action.getFileName() != null) {
      wFilename.setText(action.getFileName());
    }
    if (action.getSeparator() != null) {
      wSeparator.setText(action.getSeparator());
    }

    if (action.getEnclosed() != null) {
      wEnclosed.setText(action.getEnclosed());
    }

    if (action.getEscaped() != null) {
      wEscaped.setText(action.getEscaped());
    }
    if (action.getLineStarted() != null) {
      wLinestarted.setText(action.getLineStarted());
    }
    if (action.getLineTerminated() != null) {
      wLineterminated.setText(action.getLineTerminated());
    }

    wReplaceData.setSelection(action.isReplaceData());

    wLocalInfile.setSelection(action.isLocalInFile());

    if (action.getIgnoreLines() != null) {

      wIgnorelines.setText(action.getIgnoreLines());
    } else {
      wIgnorelines.setText("0");
    }

    if (action.getListAttribute() != null) {
      wListattribut.setText(action.getListAttribute());
    }

    if (action.prorityValue >= 0) {
      wPriorityValue.select(action.prorityValue);
    } else {
      wPriorityValue.select(0); // NORMAL priority
    }

    if (action.getConnection() != null) {
      wConnection.setText(action.getConnection());
    }

    wAddFileToResult.setSelection(action.isAddFileToResult());

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
    action.setConnection(wConnection.getText());
    action.setSchemaName(wSchemaname.getText());
    action.setTableName(wTablename.getText());
    action.setFileName(wFilename.getText());
    action.setSeparator(wSeparator.getText());
    action.setEnclosed(wEnclosed.getText());
    action.setEscaped(wEscaped.getText());
    action.setLineTerminated(wLineterminated.getText());
    action.setLineStarted(wLinestarted.getText());
    action.setReplaceData(wReplaceData.getSelection());
    action.setIgnoreLines(wIgnorelines.getText());
    action.setListAttribute(wListattribut.getText());
    action.prorityValue = wPriorityValue.getSelectionIndex();
    action.setLocalInFile(wLocalInfile.getSelection());

    action.setAddFileToResult(wAddFileToResult.getSelection());

    dispose();
  }

  private void getTableName() {
    String databaseName = wConnection.getText();
    if (StringUtils.isNotEmpty(databaseName)) {
      DatabaseMeta databaseMeta = getWorkflowMeta().findDatabase(databaseName, variables);
      if (databaseMeta != null) {
        DatabaseExplorerDialog std =
            new DatabaseExplorerDialog(
                shell, SWT.NONE, variables, databaseMeta, getWorkflowMeta().getDatabases());
        std.setSelectedSchemaAndTable(wSchemaname.getText(), wTablename.getText());
        if (std.open()) {
          wTablename.setText(Const.NVL(std.getTableName(), ""));
        }
      } else {
        MessageBox mb = new MessageBox(shell, SWT.OK | SWT.ICON_ERROR);
        mb.setMessage(
            BaseMessages.getString(PKG, "ActionMysqlBulkLoad.ConnectionError2.DialogMessage"));
        mb.setText(BaseMessages.getString(PKG, "System.Dialog.Error.Title"));
        mb.open();
      }
    }
  }

  /** Get a list of columns, comma separated, allow the user to select from it. */
  private void getListColumns() {
    if (!Utils.isEmpty(wTablename.getText())) {
      DatabaseMeta databaseMeta = getWorkflowMeta().findDatabase(wConnection.getText(), variables);
      if (databaseMeta != null) {
        try (Database database = new Database(loggingObject, variables, databaseMeta)) {
          database.connect();
          String schemaTable =
              databaseMeta.getQuotedSchemaTableCombination(
                  variables, wSchemaname.getText(), wTablename.getText());
          IRowMeta row = database.getTableFields(schemaTable);
          String[] available = row.getFieldNames();

          String[] source = wListattribut.getText().split(",");
          for (int i = 0; i < source.length; i++) {
            source[i] = Const.trim(source[i]);
          }
          int[] idxSource = Const.indexsOfStrings(source, available);
          EnterSelectionDialog dialog =
              new EnterSelectionDialog(
                  shell,
                  available,
                  BaseMessages.getString(PKG, "ActionMysqlBulkLoad.SelectColumns.Title"),
                  BaseMessages.getString(PKG, "ActionMysqlBulkLoad.SelectColumns.Message"));
          dialog.setMulti(true);
          dialog.setAvoidQuickSearch();
          dialog.setSelectedNrs(idxSource);
          if (dialog.open() != null) {
            String columns = "";
            int[] idx = dialog.getSelectionIndeces();
            for (int i = 0; i < idx.length; i++) {
              if (i > 0) {
                columns += ", ";
              }
              columns += available[idx[i]];
            }
            wListattribut.setText(columns);
          }
        } catch (HopDatabaseException e) {
          new ErrorDialog(
              shell,
              BaseMessages.getString(PKG, "System.Dialog.Error.Title"),
              BaseMessages.getString(PKG, "ActionMysqlBulkLoad.ConnectionError2.DialogMessage"),
              e);
        }
      }
    }
  }
}
