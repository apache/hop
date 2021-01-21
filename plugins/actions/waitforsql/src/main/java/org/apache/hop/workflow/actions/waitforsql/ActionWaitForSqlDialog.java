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

package org.apache.hop.workflow.actions.waitforsql;

import org.apache.commons.lang.StringUtils;
import org.apache.hop.core.Const;
import org.apache.hop.core.Props;
import org.apache.hop.core.database.Database;
import org.apache.hop.core.database.DatabaseMeta;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.core.util.Utils;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.ui.core.database.dialog.DatabaseExplorerDialog;
import org.apache.hop.ui.core.gui.WindowProperty;
import org.apache.hop.ui.core.widget.MetaSelectionLine;
import org.apache.hop.ui.core.widget.StyledTextComp;
import org.apache.hop.ui.core.widget.TextVar;
import org.apache.hop.ui.pipeline.transform.BaseTransformDialog;
import org.apache.hop.ui.workflow.action.ActionDialog;
import org.apache.hop.ui.workflow.dialog.WorkflowDialog;
import org.apache.hop.workflow.WorkflowMeta;
import org.apache.hop.workflow.action.IAction;
import org.apache.hop.workflow.action.IActionDialog;
import org.eclipse.swt.SWT;
import org.eclipse.swt.custom.CCombo;
import org.eclipse.swt.events.FocusAdapter;
import org.eclipse.swt.events.FocusEvent;
import org.eclipse.swt.events.KeyAdapter;
import org.eclipse.swt.events.KeyEvent;
import org.eclipse.swt.events.ModifyListener;
import org.eclipse.swt.events.MouseAdapter;
import org.eclipse.swt.events.MouseEvent;
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
import org.eclipse.swt.widgets.Listener;
import org.eclipse.swt.widgets.MessageBox;
import org.eclipse.swt.widgets.Shell;
import org.eclipse.swt.widgets.Text;

/**
 * This dialog allows you to edit the Wait for SQL action settings.
 *
 * @author Samatar
 * @since 27-10-2008
 */
public class ActionWaitForSqlDialog extends ActionDialog implements IActionDialog {
  private static final Class<?> PKG = ActionWaitForSql.class; // For Translator

  private Button wbTable, wbSqlTable;

  private Text wName;

  private MetaSelectionLine<DatabaseMeta> wConnection;

  private ActionWaitForSql action;

  private Shell shell;

  private boolean changed;

  private Label wlUseSubs;

  private Button wUseSubs;

  private Label wlAddRowsToResult;

  private Button wAddRowsToResult;

  private Button wcustomSql;

  private Label wlSql;

  private StyledTextComp wSql;

  private Label wlPosition;

  // Schema name
  private Label wlSchemaname;
  private TextVar wSchemaname;

  private Label wlTablename;
  private TextVar wTablename;

  private CCombo wSuccessCondition;

  private TextVar wRowsCountValue;

  private TextVar wMaximumTimeout;

  private TextVar wCheckCycleTime;

  private Button wSuccesOnTimeout;

  private Label wlClearResultList;
  private Button wClearResultList;

  public ActionWaitForSqlDialog(Shell parent, IAction action, WorkflowMeta workflowMeta) {
    super(parent, workflowMeta);
    this.action = (ActionWaitForSql) action;
    if (this.action.getName() == null) {
      this.action.setName(BaseMessages.getString(PKG, "ActionWaitForSQL.Name.Default"));
    }
  }

  public IAction open() {
    Shell parent = getParent();
    Display display = parent.getDisplay();

    shell = new Shell(parent, SWT.DIALOG_TRIM | SWT.MIN | SWT.MAX | SWT.RESIZE);
    props.setLook(shell);
    WorkflowDialog.setShellImage(shell, action);

    ModifyListener lsMod = e -> action.setChanged();
    changed = action.hasChanged();

    FormLayout formLayout = new FormLayout();
    formLayout.marginWidth = Const.FORM_MARGIN;
    formLayout.marginHeight = Const.FORM_MARGIN;

    shell.setLayout(formLayout);
    shell.setText(BaseMessages.getString(PKG, "ActionWaitForSQL.Title"));

    int middle = props.getMiddlePct();
    int margin = Const.MARGIN;

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

    BaseTransformDialog.positionBottomButtons(shell, new Button[] {wOk, wCancel}, margin, null);

    // Filename line
    Label wlName = new Label(shell, SWT.RIGHT);
    wlName.setText(BaseMessages.getString(PKG, "ActionWaitForSQL.Name.Label"));
    props.setLook(wlName);
    FormData fdlName = new FormData();
    fdlName.left = new FormAttachment(0, 0);
    fdlName.right = new FormAttachment(middle, -margin);
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
    wlSchemaname = new Label(shell, SWT.RIGHT);
    wlSchemaname.setText(BaseMessages.getString(PKG, "ActionWaitForSQL.Schemaname.Label"));
    props.setLook(wlSchemaname);
    FormData fdlSchemaname = new FormData();
    fdlSchemaname.left = new FormAttachment(0, 0);
    fdlSchemaname.right = new FormAttachment(middle, 0);
    fdlSchemaname.top = new FormAttachment(wConnection, margin);
    wlSchemaname.setLayoutData(fdlSchemaname);

    wSchemaname = new TextVar(variables, shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    props.setLook(wSchemaname);
    wSchemaname.setToolTipText(BaseMessages.getString(PKG, "ActionWaitForSQL.Schemaname.Tooltip"));
    wSchemaname.addModifyListener(lsMod);
    FormData fdSchemaname = new FormData();
    fdSchemaname.left = new FormAttachment(middle, 0);
    fdSchemaname.top = new FormAttachment(wConnection, margin);
    fdSchemaname.right = new FormAttachment(100, 0);
    wSchemaname.setLayoutData(fdSchemaname);

    // Table name line
    wlTablename = new Label(shell, SWT.RIGHT);
    wlTablename.setText(BaseMessages.getString(PKG, "ActionWaitForSQL.Tablename.Label"));
    props.setLook(wlTablename);
    FormData fdlTablename = new FormData();
    fdlTablename.left = new FormAttachment(0, 0);
    fdlTablename.right = new FormAttachment(middle, 0);
    fdlTablename.top = new FormAttachment(wSchemaname, margin);
    wlTablename.setLayoutData(fdlTablename);

    wbTable = new Button(shell, SWT.PUSH | SWT.CENTER);
    props.setLook(wbTable);
    wbTable.setText(BaseMessages.getString(PKG, "System.Button.Browse"));
    FormData fdbTable = new FormData();
    fdbTable.right = new FormAttachment(100, 0);
    fdbTable.top = new FormAttachment(wSchemaname, margin / 2);
    wbTable.setLayoutData(fdbTable);
    wbTable.addSelectionListener(
        new SelectionAdapter() {
          public void widgetSelected(SelectionEvent e) {
            getTableName();
          }
        });

    wTablename = new TextVar(variables, shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    props.setLook(wTablename);
    wTablename.setToolTipText(BaseMessages.getString(PKG, "ActionWaitForSQL.Tablename.Tooltip"));
    wTablename.addModifyListener(lsMod);
    FormData fdTablename = new FormData();
    fdTablename.left = new FormAttachment(middle, 0);
    fdTablename.top = new FormAttachment(wSchemaname, margin);
    fdTablename.right = new FormAttachment(wbTable, -margin);
    wTablename.setLayoutData(fdTablename);

    // ////////////////////////
    // START OF Success GROUP///
    // ///////////////////////////////
    Group wSuccessGroup = new Group(shell, SWT.SHADOW_NONE);
    props.setLook(wSuccessGroup);
    wSuccessGroup.setText(BaseMessages.getString(PKG, "ActionWaitForSQL.SuccessGroup.Group.Label"));

    FormLayout SuccessGroupLayout = new FormLayout();
    SuccessGroupLayout.marginWidth = 10;
    SuccessGroupLayout.marginHeight = 10;
    wSuccessGroup.setLayout(SuccessGroupLayout);

    // Success Condition
    Label wlSuccessCondition = new Label(wSuccessGroup, SWT.RIGHT);
    wlSuccessCondition.setText(
        BaseMessages.getString(PKG, "ActionWaitForSQL.SuccessCondition.Label"));
    props.setLook(wlSuccessCondition);
    FormData fdlSuccessCondition = new FormData();
    fdlSuccessCondition.left = new FormAttachment(0, -margin);
    fdlSuccessCondition.right = new FormAttachment(middle, -2 * margin);
    fdlSuccessCondition.top = new FormAttachment(0, margin);
    wlSuccessCondition.setLayoutData(fdlSuccessCondition);
    wSuccessCondition = new CCombo(wSuccessGroup, SWT.SINGLE | SWT.READ_ONLY | SWT.BORDER);
    wSuccessCondition.setItems(ActionWaitForSql.successConditionsDesc);
    wSuccessCondition.select(0); // +1: starts at -1

    props.setLook(wSuccessCondition);
    FormData fdSuccessCondition = new FormData();
    fdSuccessCondition.left = new FormAttachment(middle, -margin);
    fdSuccessCondition.top = new FormAttachment(0, margin);
    fdSuccessCondition.right = new FormAttachment(100, 0);
    wSuccessCondition.setLayoutData(fdSuccessCondition);
    wSuccessCondition.addSelectionListener(
        new SelectionAdapter() {
          public void widgetSelected(SelectionEvent e) {
            // activeSuccessCondition();

          }
        });

    // Success when number of errors less than
    Label wlRowsCountValue = new Label(wSuccessGroup, SWT.RIGHT);
    wlRowsCountValue.setText(BaseMessages.getString(PKG, "ActionWaitForSQL.RowsCountValue.Label"));
    props.setLook(wlRowsCountValue);
    FormData fdlRowsCountValue = new FormData();
    fdlRowsCountValue.left = new FormAttachment(0, -margin);
    fdlRowsCountValue.top = new FormAttachment(wSuccessCondition, margin);
    fdlRowsCountValue.right = new FormAttachment(middle, -2 * margin);
    wlRowsCountValue.setLayoutData(fdlRowsCountValue);

    wRowsCountValue =
        new TextVar(
            variables,
            wSuccessGroup,
            SWT.SINGLE | SWT.LEFT | SWT.BORDER,
            BaseMessages.getString(PKG, "ActionWaitForSQL.RowsCountValue.Tooltip"));
    props.setLook(wRowsCountValue);
    wRowsCountValue.addModifyListener(lsMod);
    FormData fdRowsCountValue = new FormData();
    fdRowsCountValue.left = new FormAttachment(middle, -margin);
    fdRowsCountValue.top = new FormAttachment(wSuccessCondition, margin);
    fdRowsCountValue.right = new FormAttachment(100, 0);
    wRowsCountValue.setLayoutData(fdRowsCountValue);

    // Maximum timeout
    Label wlMaximumTimeout = new Label(wSuccessGroup, SWT.RIGHT);
    wlMaximumTimeout.setText(BaseMessages.getString(PKG, "ActionWaitForSQL.MaximumTimeout.Label"));
    props.setLook(wlMaximumTimeout);
    FormData fdlMaximumTimeout = new FormData();
    fdlMaximumTimeout.left = new FormAttachment(0, -margin);
    fdlMaximumTimeout.top = new FormAttachment(wRowsCountValue, margin);
    fdlMaximumTimeout.right = new FormAttachment(middle, -2 * margin);
    wlMaximumTimeout.setLayoutData(fdlMaximumTimeout);
    wMaximumTimeout = new TextVar(variables, wSuccessGroup, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    props.setLook(wMaximumTimeout);
    wMaximumTimeout.setToolTipText(
        BaseMessages.getString(PKG, "ActionWaitForSQL.MaximumTimeout.Tooltip"));
    wMaximumTimeout.addModifyListener(lsMod);
    FormData fdMaximumTimeout = new FormData();
    fdMaximumTimeout.left = new FormAttachment(middle, -margin);
    fdMaximumTimeout.top = new FormAttachment(wRowsCountValue, margin);
    fdMaximumTimeout.right = new FormAttachment(100, 0);
    wMaximumTimeout.setLayoutData(fdMaximumTimeout);

    // Cycle time
    Label wlCheckCycleTime = new Label(wSuccessGroup, SWT.RIGHT);
    wlCheckCycleTime.setText(BaseMessages.getString(PKG, "ActionWaitForSQL.CheckCycleTime.Label"));
    props.setLook(wlCheckCycleTime);
    FormData fdlCheckCycleTime = new FormData();
    fdlCheckCycleTime.left = new FormAttachment(0, -margin);
    fdlCheckCycleTime.top = new FormAttachment(wMaximumTimeout, margin);
    fdlCheckCycleTime.right = new FormAttachment(middle, -2 * margin);
    wlCheckCycleTime.setLayoutData(fdlCheckCycleTime);
    wCheckCycleTime = new TextVar(variables, wSuccessGroup, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    props.setLook(wCheckCycleTime);
    wCheckCycleTime.setToolTipText(
        BaseMessages.getString(PKG, "ActionWaitForSQL.CheckCycleTime.Tooltip"));
    wCheckCycleTime.addModifyListener(lsMod);
    FormData fdCheckCycleTime = new FormData();
    fdCheckCycleTime.left = new FormAttachment(middle, -margin);
    fdCheckCycleTime.top = new FormAttachment(wMaximumTimeout, margin);
    fdCheckCycleTime.right = new FormAttachment(100, 0);
    wCheckCycleTime.setLayoutData(fdCheckCycleTime);

    // Success on timeout
    Label wlSuccesOnTimeout = new Label(wSuccessGroup, SWT.RIGHT);
    wlSuccesOnTimeout.setText(
        BaseMessages.getString(PKG, "ActionWaitForSQL.SuccessOnTimeout.Label"));
    props.setLook(wlSuccesOnTimeout);
    FormData fdlSuccesOnTimeout = new FormData();
    fdlSuccesOnTimeout.left = new FormAttachment(0, -margin);
    fdlSuccesOnTimeout.top = new FormAttachment(wCheckCycleTime, margin);
    fdlSuccesOnTimeout.right = new FormAttachment(middle, -2 * margin);
    wlSuccesOnTimeout.setLayoutData(fdlSuccesOnTimeout);
    wSuccesOnTimeout = new Button(wSuccessGroup, SWT.CHECK);
    props.setLook(wSuccesOnTimeout);
    wSuccesOnTimeout.setToolTipText(
        BaseMessages.getString(PKG, "ActionWaitForSQL.SuccessOnTimeout.Tooltip"));
    FormData fdSuccesOnTimeout = new FormData();
    fdSuccesOnTimeout.left = new FormAttachment(middle, -margin);
    fdSuccesOnTimeout.top = new FormAttachment(wCheckCycleTime, margin);
    fdSuccesOnTimeout.right = new FormAttachment(100, -margin);
    wSuccesOnTimeout.setLayoutData(fdSuccesOnTimeout);
    wSuccesOnTimeout.addSelectionListener(
        new SelectionAdapter() {
          public void widgetSelected(SelectionEvent e) {
            action.setChanged();
          }
        });

    FormData fdSuccessGroup = new FormData();
    fdSuccessGroup.left = new FormAttachment(0, margin);
    fdSuccessGroup.top = new FormAttachment(wbTable, margin);
    fdSuccessGroup.right = new FormAttachment(100, -margin);
    wSuccessGroup.setLayoutData(fdSuccessGroup);
    // ///////////////////////////////////////////////////////////
    // / END OF SuccessGroup GROUP
    // ///////////////////////////////////////////////////////////

    // ////////////////////////
    // START OF Custom GROUP///
    // ///////////////////////////////
    Group wCustomGroup = new Group(shell, SWT.SHADOW_NONE);
    props.setLook(wCustomGroup);
    wCustomGroup.setText(BaseMessages.getString(PKG, "ActionWaitForSQL.CustomGroup.Group.Label"));

    FormLayout CustomGroupLayout = new FormLayout();
    CustomGroupLayout.marginWidth = 10;
    CustomGroupLayout.marginHeight = 10;
    wCustomGroup.setLayout(CustomGroupLayout);

    // custom SQL?
    Label wlcustomSql = new Label(wCustomGroup, SWT.RIGHT);
    wlcustomSql.setText(BaseMessages.getString(PKG, "ActionWaitForSQL.customSQL.Label"));
    props.setLook(wlcustomSql);
    FormData fdlcustomSql = new FormData();
    fdlcustomSql.left = new FormAttachment(0, -margin);
    fdlcustomSql.top = new FormAttachment(wSuccessGroup, margin);
    fdlcustomSql.right = new FormAttachment(middle, -2 * margin);
    wlcustomSql.setLayoutData(fdlcustomSql);
    wcustomSql = new Button(wCustomGroup, SWT.CHECK);
    props.setLook(wcustomSql);
    wcustomSql.setToolTipText(BaseMessages.getString(PKG, "ActionWaitForSQL.customSQL.Tooltip"));
    FormData fdcustomSql = new FormData();
    fdcustomSql.left = new FormAttachment(middle, -margin);
    fdcustomSql.top = new FormAttachment(wSuccessGroup, margin);
    fdcustomSql.right = new FormAttachment(100, 0);
    wcustomSql.setLayoutData(fdcustomSql);
    wcustomSql.addSelectionListener(
        new SelectionAdapter() {
          public void widgetSelected(SelectionEvent e) {

            setCustomerSql();
            action.setChanged();
          }
        });
    // use Variable substitution?
    wlUseSubs = new Label(wCustomGroup, SWT.RIGHT);
    wlUseSubs.setText(BaseMessages.getString(PKG, "ActionWaitForSQL.UseVariableSubst.Label"));
    props.setLook(wlUseSubs);
    FormData fdlUseSubs = new FormData();
    fdlUseSubs.left = new FormAttachment(0, -margin);
    fdlUseSubs.top = new FormAttachment(wcustomSql, margin);
    fdlUseSubs.right = new FormAttachment(middle, -2 * margin);
    wlUseSubs.setLayoutData(fdlUseSubs);
    wUseSubs = new Button(wCustomGroup, SWT.CHECK);
    props.setLook(wUseSubs);
    wUseSubs.setToolTipText(
        BaseMessages.getString(PKG, "ActionWaitForSQL.UseVariableSubst.Tooltip"));
    FormData fdUseSubs = new FormData();
    fdUseSubs.left = new FormAttachment(middle, -margin);
    fdUseSubs.top = new FormAttachment(wcustomSql, margin);
    fdUseSubs.right = new FormAttachment(100, 0);
    wUseSubs.setLayoutData(fdUseSubs);
    wUseSubs.addSelectionListener(
        new SelectionAdapter() {
          public void widgetSelected(SelectionEvent e) {
            action.setChanged();
          }
        });

    // clear result rows ?
    wlClearResultList = new Label(wCustomGroup, SWT.RIGHT);
    wlClearResultList.setText(
        BaseMessages.getString(PKG, "ActionWaitForSQL.ClearResultList.Label"));
    props.setLook(wlClearResultList);
    FormData fdlClearResultList = new FormData();
    fdlClearResultList.left = new FormAttachment(0, -margin);
    fdlClearResultList.top = new FormAttachment(wUseSubs, margin);
    fdlClearResultList.right = new FormAttachment(middle, -2 * margin);
    wlClearResultList.setLayoutData(fdlClearResultList);
    wClearResultList = new Button(wCustomGroup, SWT.CHECK);
    props.setLook(wClearResultList);
    wClearResultList.setToolTipText(
        BaseMessages.getString(PKG, "ActionWaitForSQL.ClearResultList.Tooltip"));
    FormData fdClearResultList = new FormData();
    fdClearResultList.left = new FormAttachment(middle, -margin);
    fdClearResultList.top = new FormAttachment(wUseSubs, margin);
    fdClearResultList.right = new FormAttachment(100, 0);
    wClearResultList.setLayoutData(fdClearResultList);
    wClearResultList.addSelectionListener(
        new SelectionAdapter() {
          public void widgetSelected(SelectionEvent e) {
            action.setChanged();
          }
        });

    // add rows to result?
    wlAddRowsToResult = new Label(wCustomGroup, SWT.RIGHT);
    wlAddRowsToResult.setText(
        BaseMessages.getString(PKG, "ActionWaitForSQL.AddRowsToResult.Label"));
    props.setLook(wlAddRowsToResult);
    FormData fdlAddRowsToResult = new FormData();
    fdlAddRowsToResult.left = new FormAttachment(0, -margin);
    fdlAddRowsToResult.top = new FormAttachment(wClearResultList, margin);
    fdlAddRowsToResult.right = new FormAttachment(middle, -2 * margin);
    wlAddRowsToResult.setLayoutData(fdlAddRowsToResult);
    wAddRowsToResult = new Button(wCustomGroup, SWT.CHECK);
    props.setLook(wAddRowsToResult);
    wAddRowsToResult.setToolTipText(
        BaseMessages.getString(PKG, "ActionWaitForSQL.AddRowsToResult.Tooltip"));
    FormData fdAddRowsToResult = new FormData();
    fdAddRowsToResult.left = new FormAttachment(middle, -margin);
    fdAddRowsToResult.top = new FormAttachment(wClearResultList, margin);
    fdAddRowsToResult.right = new FormAttachment(100, 0);
    wAddRowsToResult.setLayoutData(fdAddRowsToResult);
    wAddRowsToResult.addSelectionListener(
        new SelectionAdapter() {
          public void widgetSelected(SelectionEvent e) {
            action.setChanged();
          }
        });

    wlPosition = new Label(wCustomGroup, SWT.NONE);
    props.setLook(wlPosition);
    FormData fdlPosition = new FormData();
    fdlPosition.left = new FormAttachment(0, 0);
    fdlPosition.right = new FormAttachment(100, 0);
    // fdlPosition.top= new FormAttachment(wSql , 0);
    fdlPosition.bottom = new FormAttachment(100, -margin);
    wlPosition.setLayoutData(fdlPosition);

    // Script line
    wlSql = new Label(wCustomGroup, SWT.NONE);
    wlSql.setText(BaseMessages.getString(PKG, "ActionWaitForSQL.Script.Label"));
    props.setLook(wlSql);
    FormData fdlSql = new FormData();
    fdlSql.left = new FormAttachment(0, 0);
    fdlSql.top = new FormAttachment(wAddRowsToResult, margin);
    wlSql.setLayoutData(fdlSql);

    wbSqlTable = new Button(wCustomGroup, SWT.PUSH | SWT.CENTER);
    props.setLook(wbSqlTable);
    wbSqlTable.setText(BaseMessages.getString(PKG, "ActionWaitForSQL.GetSQLAndSelectStatement"));
    FormData fdbSqlTable = new FormData();
    fdbSqlTable.right = new FormAttachment(100, 0);
    fdbSqlTable.top = new FormAttachment(wAddRowsToResult, margin);
    wbSqlTable.setLayoutData(fdbSqlTable);

    wSql =
        new StyledTextComp(
            action,
            wCustomGroup,
            SWT.MULTI | SWT.LEFT | SWT.BORDER | SWT.H_SCROLL | SWT.V_SCROLL
        );
    props.setLook(wSql, Props.WIDGET_STYLE_FIXED);
    wSql.addModifyListener(lsMod);
    FormData fdSql = new FormData();
    fdSql.left = new FormAttachment(0, 0);
    fdSql.top = new FormAttachment(wbSqlTable, margin);
    fdSql.right = new FormAttachment(100, -10);
    fdSql.bottom = new FormAttachment(wlPosition, -margin);
    wSql.setLayoutData(fdSql);

    wSql.addModifyListener(arg0 -> setPosition());

    wSql.addKeyListener(
        new KeyAdapter() {
          public void keyPressed(KeyEvent e) {
            setPosition();
          }

          public void keyReleased(KeyEvent e) {
            setPosition();
          }
        });
    wSql.addFocusListener(
        new FocusAdapter() {
          public void focusGained(FocusEvent e) {
            setPosition();
          }

          public void focusLost(FocusEvent e) {
            setPosition();
          }
        });
    wSql.addMouseListener(
        new MouseAdapter() {
          public void mouseDoubleClick(MouseEvent e) {
            setPosition();
          }

          public void mouseDown(MouseEvent e) {
            setPosition();
          }

          public void mouseUp(MouseEvent e) {
            setPosition();
          }
        });
    wSql.addModifyListener(lsMod);

    FormData fdCustomGroup = new FormData();
    fdCustomGroup.left = new FormAttachment(0, margin);
    fdCustomGroup.top = new FormAttachment(wSuccessGroup, margin);
    fdCustomGroup.right = new FormAttachment(100, -margin);
    fdCustomGroup.bottom = new FormAttachment(wOk, -margin);
    wCustomGroup.setLayoutData(fdCustomGroup);
    // ///////////////////////////////////////////////////////////
    // / END OF CustomGroup GROUP
    // ///////////////////////////////////////////////////////////

    // Add listeners
    Listener lsCancel = e -> cancel();
    Listener lsOk = e -> ok();
    Listener lsbSqlTable = e -> getSql();

    wCancel.addListener(SWT.Selection, lsCancel);
    wOk.addListener(SWT.Selection, lsOk);

    SelectionAdapter lsDef =
        new SelectionAdapter() {
          public void widgetDefaultSelected(SelectionEvent e) {
            ok();
          }
        };

    wbSqlTable.addListener(SWT.Selection, lsbSqlTable);
    wName.addSelectionListener(lsDef);

    // Detect X or ALT-F4 or something that kills this window...
    shell.addShellListener(
        new ShellAdapter() {
          public void shellClosed(ShellEvent e) {
            cancel();
          }
        });

    getData();
    setCustomerSql();
    BaseTransformDialog.setSize(shell);

    shell.open();
    props.setDialogSize(shell, "ActionWaitForSQLDialogSize");
    while (!shell.isDisposed()) {
      if (!display.readAndDispatch()) {
        display.sleep();
      }
    }
    return action;
  }

  private void getSql() {
    DatabaseMeta inf = getWorkflowMeta().findDatabase(wConnection.getText());
    if (inf != null) {
      DatabaseExplorerDialog std =
          new DatabaseExplorerDialog(
              shell, SWT.NONE, variables, inf, getWorkflowMeta().getDatabases());
      if (std.open()) {
        String sql =
            "SELECT *"
                + Const.CR
                + "FROM "
                + inf.getQuotedSchemaTableCombination(
                    variables, std.getSchemaName(), std.getTableName())
                + Const.CR;
        wSql.setText(sql);

        MessageBox yn = new MessageBox(shell, SWT.YES | SWT.NO | SWT.CANCEL | SWT.ICON_QUESTION);
        yn.setMessage(BaseMessages.getString(PKG, "ActionWaitForSQL.IncludeFieldNamesInSQL"));
        yn.setText(BaseMessages.getString(PKG, "ActionWaitForSQL.DialogCaptionQuestion"));
        int id = yn.open();
        switch (id) {
          case SWT.CANCEL:
            break;
          case SWT.NO:
            wSql.setText(sql);
            break;
          case SWT.YES:
            Database db = new Database(loggingObject, variables, inf );
            try {
              db.connect();
              IRowMeta fields = db.getQueryFields(sql, false);
              if (fields != null) {
                sql = "SELECT" + Const.CR;
                for (int i = 0; i < fields.size(); i++) {
                  IValueMeta field = fields.getValueMeta(i);
                  if (i == 0) {
                    sql += "  ";
                  } else {
                    sql += ", ";
                  }
                  sql += inf.quoteField(field.getName()) + Const.CR;
                }
                sql +=
                    "FROM "
                        + inf.getQuotedSchemaTableCombination(
                            variables, std.getSchemaName(), std.getTableName())
                        + Const.CR;
                wSql.setText(sql);
              } else {
                MessageBox mb = new MessageBox(shell, SWT.OK | SWT.ICON_ERROR);
                mb.setMessage(
                    BaseMessages.getString(PKG, "ActionWaitForSQL.ERROR_CouldNotRetrieveFields")
                        + Const.CR
                        + BaseMessages.getString(PKG, "ActionWaitForSQL.PerhapsNoPermissions"));
                mb.setText(BaseMessages.getString(PKG, "ActionWaitForSQL.DialogCaptionError2"));
                mb.open();
              }
            } catch (HopException e) {
              MessageBox mb = new MessageBox(shell, SWT.OK | SWT.ICON_ERROR);
              mb.setText(BaseMessages.getString(PKG, "ActionWaitForSQL.DialogCaptionError3"));
              mb.setMessage(
                  BaseMessages.getString(PKG, "ActionWaitForSQL.AnErrorOccurred")
                      + Const.CR
                      + e.getMessage());
              mb.open();
            } finally {
              db.disconnect();
            }
            break;
          default:
            break;
        }
      }
    } else {
      MessageBox mb = new MessageBox(shell, SWT.OK | SWT.ICON_ERROR);
      mb.setMessage(BaseMessages.getString(PKG, "ActionWaitForSQL.ConnectionNoLongerAvailable"));
      mb.setText(BaseMessages.getString(PKG, "ActionWaitForSQL.DialogCaptionError4"));
      mb.open();
    }
  }

  public void setPosition() {
    int lineNumber = wSql.getLineNumber();
    int columnNumber = wSql.getColumnNumber();
    wlPosition.setText( BaseMessages.getString( PKG, "ActionWaitForSQL.Position.Label", "" + lineNumber, "" + columnNumber ) );
  }

  private void setCustomerSql() {
    wlClearResultList.setEnabled(wcustomSql.getSelection());
    wClearResultList.setEnabled(wcustomSql.getSelection());
    wlSql.setEnabled(wcustomSql.getSelection());
    wSql.setEnabled(wcustomSql.getSelection());
    wlAddRowsToResult.setEnabled(wcustomSql.getSelection());
    wAddRowsToResult.setEnabled(wcustomSql.getSelection());
    wlUseSubs.setEnabled(wcustomSql.getSelection());
    wbSqlTable.setEnabled(wcustomSql.getSelection());
    wUseSubs.setEnabled(wcustomSql.getSelection());
    wbTable.setEnabled(!wcustomSql.getSelection());
    wTablename.setEnabled(!wcustomSql.getSelection());
    wlTablename.setEnabled(!wcustomSql.getSelection());
    wlSchemaname.setEnabled(!wcustomSql.getSelection());
    wSchemaname.setEnabled(!wcustomSql.getSelection());
  }

  public void dispose() {
    WindowProperty winprop = new WindowProperty(shell);
    props.setScreen(winprop);
    shell.dispose();
  }

  /** Copy information from the meta-data input to the dialog fields. */
  public void getData() {
    wName.setText(Const.nullToEmpty(action.getName()));

    if (action.getDatabase() != null) {
      wConnection.setText(action.getDatabase().getName());
    }

    wSchemaname.setText(Const.nullToEmpty(action.schemaName));
    wTablename.setText(Const.nullToEmpty(action.tableName));

    wSuccessCondition.setText(ActionWaitForSql.getSuccessConditionDesc(action.successCondition));
    wRowsCountValue.setText(Const.NVL(action.rowsCountValue, "0"));
    wcustomSql.setSelection(action.isCustomSql);
    wUseSubs.setSelection(action.isUseVars);
    wAddRowsToResult.setSelection(action.isAddRowsResult);
    wClearResultList.setSelection(action.isClearResultList);
    wSql.setText(Const.nullToEmpty(action.customSql));
    wMaximumTimeout.setText(Const.NVL(action.getMaximumTimeout(), ""));
    wCheckCycleTime.setText(Const.NVL(action.getCheckCycleTime(), ""));
    wSuccesOnTimeout.setSelection(action.isSuccessOnTimeout());

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
      mb.setMessage("Please give this action a name.");
      mb.setText("Enter the name of the action");
      mb.open();
      return;
    }
    action.setName(wName.getText());
    action.setDatabase(getWorkflowMeta().findDatabase(wConnection.getText()));

    action.schemaName = wSchemaname.getText();
    action.tableName = wTablename.getText();
    action.successCondition =
        ActionWaitForSql.getSuccessConditionByDesc(wSuccessCondition.getText());
    action.rowsCountValue = wRowsCountValue.getText();
    action.isCustomSql = wcustomSql.getSelection();
    action.isUseVars = wUseSubs.getSelection();
    action.isAddRowsResult = wAddRowsToResult.getSelection();
    action.isClearResultList = wClearResultList.getSelection();
    action.customSql = wSql.getText();
    action.setMaximumTimeout(wMaximumTimeout.getText());
    action.setCheckCycleTime(wCheckCycleTime.getText());
    action.setSuccessOnTimeout(wSuccesOnTimeout.getSelection());

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
        std.setSelectedSchemaAndTable(wSchemaname.getText(), wTablename.getText());
        if (std.open()) {
          wTablename.setText(Const.NVL(std.getTableName(), ""));
        }
      } else {
        MessageBox mb = new MessageBox(shell, SWT.OK | SWT.ICON_ERROR);
        mb.setMessage(
            BaseMessages.getString(PKG, "ActionWaitForSQL.ConnectionError2.DialogMessage"));
        mb.setText(BaseMessages.getString(PKG, "System.Dialog.Error.Title"));
        mb.open();
      }
    }
  }
}
