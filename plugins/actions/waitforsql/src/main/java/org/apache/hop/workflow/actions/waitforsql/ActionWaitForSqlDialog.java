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

package org.apache.hop.workflow.actions.waitforsql;

import java.util.Arrays;
import java.util.List;
import org.apache.hop.core.Const;
import org.apache.hop.core.Props;
import org.apache.hop.core.database.Database;
import org.apache.hop.core.database.DatabaseMeta;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.core.util.Utils;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.ui.core.PropsUi;
import org.apache.hop.ui.core.database.dialog.DatabaseExplorerDialog;
import org.apache.hop.ui.core.dialog.BaseDialog;
import org.apache.hop.ui.core.dialog.MessageBox;
import org.apache.hop.ui.core.widget.MetaSelectionLine;
import org.apache.hop.ui.core.widget.SQLStyledTextComp;
import org.apache.hop.ui.core.widget.StyledTextComp;
import org.apache.hop.ui.core.widget.TextComposite;
import org.apache.hop.ui.core.widget.TextVar;
import org.apache.hop.ui.util.EnvironmentUtils;
import org.apache.hop.ui.workflow.action.ActionDialog;
import org.apache.hop.workflow.WorkflowMeta;
import org.apache.hop.workflow.action.IAction;
import org.apache.hop.workflow.actions.waitforsql.ActionWaitForSql.SuccessCondition;
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
import org.eclipse.swt.layout.FormAttachment;
import org.eclipse.swt.layout.FormData;
import org.eclipse.swt.layout.FormLayout;
import org.eclipse.swt.widgets.Button;
import org.eclipse.swt.widgets.Group;
import org.eclipse.swt.widgets.Label;
import org.eclipse.swt.widgets.Shell;

/** This dialog allows you to edit the Wait for SQL action settings. */
public class ActionWaitForSqlDialog extends ActionDialog {
  private static final Class<?> PKG = ActionWaitForSql.class;

  private Button wbTable;
  private Button wbSqlTable;

  private MetaSelectionLine<DatabaseMeta> wConnection;

  private ActionWaitForSql action;

  private boolean changed;

  private Label wlUseSubs;

  private Button wUseSubs;

  private Label wlAddRowsToResult;

  private Button wAddRowsToResult;

  private Button wCustomSql;

  private Label wlSql;

  private TextComposite wSql;

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

  private Button wSuccessOnTimeout;

  private Label wlClearResultList;
  private Button wClearResultList;

  public ActionWaitForSqlDialog(
      Shell parent, ActionWaitForSql action, WorkflowMeta workflowMeta, IVariables variables) {
    super(parent, workflowMeta, variables);
    this.action = action;
    if (this.action.getName() == null) {
      this.action.setName(BaseMessages.getString(PKG, "ActionWaitForSQL.Name.Default"));
    }
  }

  @Override
  public IAction open() {
    createShell(BaseMessages.getString(PKG, "ActionWaitForSQL.Title"), action);
    buildButtonBar().ok(e -> ok()).cancel(e -> cancel()).build();

    ModifyListener lsMod = e -> action.setChanged();
    changed = action.hasChanged();

    // Connection line
    DatabaseMeta databaseMeta = workflowMeta.findDatabase(action.getConnection(), variables);
    wConnection = addConnectionLine(shell, wSpacer, databaseMeta, lsMod);
    wConnection.addListener(SWT.Selection, e -> getSqlReservedWords());

    // Schema name line
    wlSchemaname = new Label(shell, SWT.RIGHT);
    wlSchemaname.setText(BaseMessages.getString(PKG, "ActionWaitForSQL.Schemaname.Label"));
    PropsUi.setLook(wlSchemaname);
    FormData fdlSchemaname = new FormData();
    fdlSchemaname.left = new FormAttachment(0, 0);
    fdlSchemaname.right = new FormAttachment(middle, -margin);
    fdlSchemaname.top = new FormAttachment(wConnection, margin);
    wlSchemaname.setLayoutData(fdlSchemaname);

    wSchemaname = new TextVar(variables, shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    PropsUi.setLook(wSchemaname);
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
    PropsUi.setLook(wlTablename);
    FormData fdlTablename = new FormData();
    fdlTablename.left = new FormAttachment(0, 0);
    fdlTablename.right = new FormAttachment(middle, -margin);
    fdlTablename.top = new FormAttachment(wSchemaname, margin);
    wlTablename.setLayoutData(fdlTablename);

    wbTable = new Button(shell, SWT.PUSH | SWT.CENTER);
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
    PropsUi.setLook(wSuccessGroup);
    wSuccessGroup.setText(BaseMessages.getString(PKG, "ActionWaitForSQL.SuccessGroup.Group.Label"));

    FormLayout successGroupLayout = new FormLayout();
    successGroupLayout.marginWidth = 10;
    successGroupLayout.marginHeight = 10;
    wSuccessGroup.setLayout(successGroupLayout);

    // Success Condition
    Label wlSuccessCondition = new Label(wSuccessGroup, SWT.RIGHT);
    wlSuccessCondition.setText(
        BaseMessages.getString(PKG, "ActionWaitForSQL.SuccessCondition.Label"));
    PropsUi.setLook(wlSuccessCondition);
    FormData fdlSuccessCondition = new FormData();
    fdlSuccessCondition.left = new FormAttachment(0, -margin);
    fdlSuccessCondition.right = new FormAttachment(middle, -margin);
    fdlSuccessCondition.top = new FormAttachment(0, margin);
    wlSuccessCondition.setLayoutData(fdlSuccessCondition);
    wSuccessCondition = new CCombo(wSuccessGroup, SWT.SINGLE | SWT.READ_ONLY | SWT.BORDER);
    wSuccessCondition.setItems(SuccessCondition.getDescriptions());
    wSuccessCondition.select(0); // +1: starts at -1

    PropsUi.setLook(wSuccessCondition);
    FormData fdSuccessCondition = new FormData();
    fdSuccessCondition.left = new FormAttachment(middle, -margin);
    fdSuccessCondition.top = new FormAttachment(0, margin);
    fdSuccessCondition.right = new FormAttachment(100, 0);
    wSuccessCondition.setLayoutData(fdSuccessCondition);
    wSuccessCondition.addSelectionListener(
        new SelectionAdapter() {
          @Override
          public void widgetSelected(SelectionEvent e) {
            // disable selection event
          }
        });

    // Success when number of errors less than
    Label wlRowsCountValue = new Label(wSuccessGroup, SWT.RIGHT);
    wlRowsCountValue.setText(BaseMessages.getString(PKG, "ActionWaitForSQL.RowsCountValue.Label"));
    PropsUi.setLook(wlRowsCountValue);
    FormData fdlRowsCountValue = new FormData();
    fdlRowsCountValue.left = new FormAttachment(0, -margin);
    fdlRowsCountValue.top = new FormAttachment(wSuccessCondition, margin);
    fdlRowsCountValue.right = new FormAttachment(middle, -margin);
    wlRowsCountValue.setLayoutData(fdlRowsCountValue);

    wRowsCountValue =
        new TextVar(
            variables,
            wSuccessGroup,
            SWT.SINGLE | SWT.LEFT | SWT.BORDER,
            BaseMessages.getString(PKG, "ActionWaitForSQL.RowsCountValue.Tooltip"));
    PropsUi.setLook(wRowsCountValue);
    wRowsCountValue.addModifyListener(lsMod);
    FormData fdRowsCountValue = new FormData();
    fdRowsCountValue.left = new FormAttachment(middle, -margin);
    fdRowsCountValue.top = new FormAttachment(wSuccessCondition, margin);
    fdRowsCountValue.right = new FormAttachment(100, 0);
    wRowsCountValue.setLayoutData(fdRowsCountValue);

    // Maximum timeout
    Label wlMaximumTimeout = new Label(wSuccessGroup, SWT.RIGHT);
    wlMaximumTimeout.setText(BaseMessages.getString(PKG, "ActionWaitForSQL.MaximumTimeout.Label"));
    PropsUi.setLook(wlMaximumTimeout);
    FormData fdlMaximumTimeout = new FormData();
    fdlMaximumTimeout.left = new FormAttachment(0, -margin);
    fdlMaximumTimeout.top = new FormAttachment(wRowsCountValue, margin);
    fdlMaximumTimeout.right = new FormAttachment(middle, -margin);
    wlMaximumTimeout.setLayoutData(fdlMaximumTimeout);
    wMaximumTimeout = new TextVar(variables, wSuccessGroup, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    PropsUi.setLook(wMaximumTimeout);
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
    PropsUi.setLook(wlCheckCycleTime);
    FormData fdlCheckCycleTime = new FormData();
    fdlCheckCycleTime.left = new FormAttachment(0, -margin);
    fdlCheckCycleTime.top = new FormAttachment(wMaximumTimeout, margin);
    fdlCheckCycleTime.right = new FormAttachment(middle, -margin);
    wlCheckCycleTime.setLayoutData(fdlCheckCycleTime);
    wCheckCycleTime = new TextVar(variables, wSuccessGroup, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    PropsUi.setLook(wCheckCycleTime);
    wCheckCycleTime.setToolTipText(
        BaseMessages.getString(PKG, "ActionWaitForSQL.CheckCycleTime.Tooltip"));
    wCheckCycleTime.addModifyListener(lsMod);
    FormData fdCheckCycleTime = new FormData();
    fdCheckCycleTime.left = new FormAttachment(middle, -margin);
    fdCheckCycleTime.top = new FormAttachment(wMaximumTimeout, margin);
    fdCheckCycleTime.right = new FormAttachment(100, 0);
    wCheckCycleTime.setLayoutData(fdCheckCycleTime);

    // Success on timeout
    Label wlSuccessOnTimeout = new Label(wSuccessGroup, SWT.RIGHT);
    wlSuccessOnTimeout.setText(
        BaseMessages.getString(PKG, "ActionWaitForSQL.SuccessOnTimeout.Label"));
    PropsUi.setLook(wlSuccessOnTimeout);
    FormData fdlSuccessOnTimeout = new FormData();
    fdlSuccessOnTimeout.left = new FormAttachment(0, -margin);
    fdlSuccessOnTimeout.top = new FormAttachment(wCheckCycleTime, margin);
    fdlSuccessOnTimeout.right = new FormAttachment(middle, -margin);
    wlSuccessOnTimeout.setLayoutData(fdlSuccessOnTimeout);
    wSuccessOnTimeout = new Button(wSuccessGroup, SWT.CHECK);
    PropsUi.setLook(wSuccessOnTimeout);
    wSuccessOnTimeout.setToolTipText(
        BaseMessages.getString(PKG, "ActionWaitForSQL.SuccessOnTimeout.Tooltip"));
    FormData fdSuccessOnTimeout = new FormData();
    fdSuccessOnTimeout.left = new FormAttachment(middle, -margin);
    fdSuccessOnTimeout.top = new FormAttachment(wlSuccessOnTimeout, 0, SWT.CENTER);
    fdSuccessOnTimeout.right = new FormAttachment(100, -margin);
    wSuccessOnTimeout.setLayoutData(fdSuccessOnTimeout);
    wSuccessOnTimeout.addSelectionListener(
        new SelectionAdapter() {
          @Override
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
    PropsUi.setLook(wCustomGroup);
    wCustomGroup.setText(BaseMessages.getString(PKG, "ActionWaitForSQL.CustomGroup.Group.Label"));

    FormLayout customGroupLayout = new FormLayout();
    customGroupLayout.marginWidth = 10;
    customGroupLayout.marginHeight = 10;
    wCustomGroup.setLayout(customGroupLayout);

    // custom SQL?
    Label wlCustomSql = new Label(wCustomGroup, SWT.RIGHT);
    wlCustomSql.setText(BaseMessages.getString(PKG, "ActionWaitForSQL.customSQL.Label"));
    PropsUi.setLook(wlCustomSql);
    FormData fdlCustomSql = new FormData();
    fdlCustomSql.left = new FormAttachment(0, -margin);
    fdlCustomSql.top = new FormAttachment(wSuccessGroup, margin);
    fdlCustomSql.right = new FormAttachment(middle, -margin);
    wlCustomSql.setLayoutData(fdlCustomSql);
    wCustomSql = new Button(wCustomGroup, SWT.CHECK);
    PropsUi.setLook(wCustomSql);
    wCustomSql.setToolTipText(BaseMessages.getString(PKG, "ActionWaitForSQL.customSQL.Tooltip"));
    FormData fdCustomSql = new FormData();
    fdCustomSql.left = new FormAttachment(middle, -margin);
    fdCustomSql.top = new FormAttachment(wlCustomSql, 0, SWT.CENTER);
    fdCustomSql.right = new FormAttachment(100, 0);
    wCustomSql.setLayoutData(fdCustomSql);
    wCustomSql.addSelectionListener(
        new SelectionAdapter() {
          @Override
          public void widgetSelected(SelectionEvent e) {

            setCustomerSql();
            action.setChanged();
          }
        });
    // use Variable substitution?
    wlUseSubs = new Label(wCustomGroup, SWT.RIGHT);
    wlUseSubs.setText(BaseMessages.getString(PKG, "ActionWaitForSQL.UseVariableSubst.Label"));
    PropsUi.setLook(wlUseSubs);
    FormData fdlUseSubs = new FormData();
    fdlUseSubs.left = new FormAttachment(0, -margin);
    fdlUseSubs.top = new FormAttachment(wlCustomSql, margin);
    fdlUseSubs.right = new FormAttachment(middle, -margin);
    wlUseSubs.setLayoutData(fdlUseSubs);
    wUseSubs = new Button(wCustomGroup, SWT.CHECK);
    PropsUi.setLook(wUseSubs);
    wUseSubs.setToolTipText(
        BaseMessages.getString(PKG, "ActionWaitForSQL.UseVariableSubst.Tooltip"));
    FormData fdUseSubs = new FormData();
    fdUseSubs.left = new FormAttachment(middle, -margin);
    fdUseSubs.top = new FormAttachment(wlUseSubs, 0, SWT.CENTER);
    fdUseSubs.right = new FormAttachment(100, 0);
    wUseSubs.setLayoutData(fdUseSubs);
    wUseSubs.addSelectionListener(
        new SelectionAdapter() {
          @Override
          public void widgetSelected(SelectionEvent e) {
            action.setChanged();
          }
        });

    // clear result rows ?
    wlClearResultList = new Label(wCustomGroup, SWT.RIGHT);
    wlClearResultList.setText(
        BaseMessages.getString(PKG, "ActionWaitForSQL.ClearResultList.Label"));
    PropsUi.setLook(wlClearResultList);
    FormData fdlClearResultList = new FormData();
    fdlClearResultList.left = new FormAttachment(0, -margin);
    fdlClearResultList.top = new FormAttachment(wlUseSubs, margin);
    fdlClearResultList.right = new FormAttachment(middle, -margin);
    wlClearResultList.setLayoutData(fdlClearResultList);
    wClearResultList = new Button(wCustomGroup, SWT.CHECK);
    PropsUi.setLook(wClearResultList);
    wClearResultList.setToolTipText(
        BaseMessages.getString(PKG, "ActionWaitForSQL.ClearResultList.Tooltip"));
    FormData fdClearResultList = new FormData();
    fdClearResultList.left = new FormAttachment(middle, -margin);
    fdClearResultList.top = new FormAttachment(wlClearResultList, 0, SWT.CENTER);
    fdClearResultList.right = new FormAttachment(100, 0);
    wClearResultList.setLayoutData(fdClearResultList);
    wClearResultList.addSelectionListener(
        new SelectionAdapter() {
          @Override
          public void widgetSelected(SelectionEvent e) {
            action.setChanged();
          }
        });

    // add rows to result?
    wlAddRowsToResult = new Label(wCustomGroup, SWT.RIGHT);
    wlAddRowsToResult.setText(
        BaseMessages.getString(PKG, "ActionWaitForSQL.AddRowsToResult.Label"));
    PropsUi.setLook(wlAddRowsToResult);
    FormData fdlAddRowsToResult = new FormData();
    fdlAddRowsToResult.left = new FormAttachment(0, -margin);
    fdlAddRowsToResult.top = new FormAttachment(wClearResultList, margin);
    fdlAddRowsToResult.right = new FormAttachment(middle, -margin);
    wlAddRowsToResult.setLayoutData(fdlAddRowsToResult);
    wAddRowsToResult = new Button(wCustomGroup, SWT.CHECK);
    PropsUi.setLook(wAddRowsToResult);
    wAddRowsToResult.setToolTipText(
        BaseMessages.getString(PKG, "ActionWaitForSQL.AddRowsToResult.Tooltip"));
    FormData fdAddRowsToResult = new FormData();
    fdAddRowsToResult.left = new FormAttachment(middle, -margin);
    fdAddRowsToResult.top = new FormAttachment(wlAddRowsToResult, 0, SWT.CENTER);
    fdAddRowsToResult.right = new FormAttachment(100, 0);
    wAddRowsToResult.setLayoutData(fdAddRowsToResult);
    wAddRowsToResult.addSelectionListener(
        new SelectionAdapter() {
          @Override
          public void widgetSelected(SelectionEvent e) {
            action.setChanged();
          }
        });

    wlPosition = new Label(wCustomGroup, SWT.NONE);
    PropsUi.setLook(wlPosition);
    FormData fdlPosition = new FormData();
    fdlPosition.left = new FormAttachment(0, 0);
    fdlPosition.right = new FormAttachment(100, 0);
    fdlPosition.bottom = new FormAttachment(100, -margin);
    wlPosition.setLayoutData(fdlPosition);

    // Script line
    wlSql = new Label(wCustomGroup, SWT.NONE);
    wlSql.setText(BaseMessages.getString(PKG, "ActionWaitForSQL.Script.Label"));
    PropsUi.setLook(wlSql);
    FormData fdlSql = new FormData();
    fdlSql.left = new FormAttachment(0, 0);
    fdlSql.top = new FormAttachment(wAddRowsToResult, margin);
    wlSql.setLayoutData(fdlSql);

    wbSqlTable = new Button(wCustomGroup, SWT.PUSH | SWT.CENTER);
    PropsUi.setLook(wbSqlTable);
    wbSqlTable.setText(BaseMessages.getString(PKG, "ActionWaitForSQL.GetSQLAndSelectStatement"));
    FormData fdbSqlTable = new FormData();
    fdbSqlTable.right = new FormAttachment(100, 0);
    fdbSqlTable.top = new FormAttachment(wAddRowsToResult, margin);
    wbSqlTable.setLayoutData(fdbSqlTable);
    wbSqlTable.addListener(SWT.Selection, e -> getSql());

    wSql =
        EnvironmentUtils.getInstance().isWeb()
            ? new StyledTextComp(
                action,
                wCustomGroup,
                SWT.MULTI | SWT.LEFT | SWT.BORDER | SWT.H_SCROLL | SWT.V_SCROLL)
            : new SQLStyledTextComp(
                action,
                wCustomGroup,
                SWT.MULTI | SWT.LEFT | SWT.BORDER | SWT.H_SCROLL | SWT.V_SCROLL);
    wSql.addLineStyleListener(getSqlReservedWords());
    PropsUi.setLook(wSql, Props.WIDGET_STYLE_FIXED);
    wSql.addModifyListener(lsMod);
    FormData fdSql = new FormData();
    fdSql.left = new FormAttachment(0, 0);
    fdSql.top = new FormAttachment(wbSqlTable, margin);
    fdSql.right = new FormAttachment(100, 0);
    fdSql.bottom = new FormAttachment(wlPosition, -margin);
    fdSql.height = 200;
    wSql.setLayoutData(fdSql);

    wSql.addModifyListener(arg0 -> setPosition());

    wSql.addKeyListener(
        new KeyAdapter() {
          @Override
          public void keyPressed(KeyEvent e) {
            setPosition();
          }

          @Override
          public void keyReleased(KeyEvent e) {
            setPosition();
          }
        });
    wSql.addFocusListener(
        new FocusAdapter() {
          @Override
          public void focusGained(FocusEvent e) {
            setPosition();
          }

          @Override
          public void focusLost(FocusEvent e) {
            setPosition();
          }
        });
    wSql.addMouseListener(
        new MouseAdapter() {
          @Override
          public void mouseDoubleClick(MouseEvent e) {
            setPosition();
          }

          @Override
          public void mouseDown(MouseEvent e) {
            setPosition();
          }

          @Override
          public void mouseUp(MouseEvent e) {
            setPosition();
          }
        });
    wSql.addModifyListener(lsMod);

    FormData fdCustomGroup = new FormData();
    fdCustomGroup.left = new FormAttachment(0, margin);
    fdCustomGroup.top = new FormAttachment(wSuccessGroup, margin);
    fdCustomGroup.right = new FormAttachment(100, -margin);
    fdCustomGroup.bottom = new FormAttachment(wCancel, -margin);
    wCustomGroup.setLayoutData(fdCustomGroup);
    // ///////////////////////////////////////////////////////////
    // / END OF CustomGroup GROUP
    // ///////////////////////////////////////////////////////////

    getData();
    setCustomerSql();
    focusActionName();

    BaseDialog.defaultShellHandling(shell, c -> ok(), c -> cancel());

    return action;
  }

  private List<String> getSqlReservedWords() {
    // Do not search keywords when connection is empty
    if (Utils.isEmpty(wConnection.getText())) {
      return List.of();
    }

    // If connection is a variable that can't be resolved
    if (variables.resolve(wConnection.getText()).startsWith("${")) {
      return List.of();
    }

    DatabaseMeta databaseMeta = workflowMeta.findDatabase(wConnection.getText(), variables);
    return Arrays.stream(databaseMeta.getReservedWords()).toList();
  }

  private void getSql() {
    DatabaseMeta databaseMeta = this.workflowMeta.findDatabase(wConnection.getText(), variables);
    if (databaseMeta != null) {
      DatabaseExplorerDialog std =
          new DatabaseExplorerDialog(
              shell, SWT.NONE, variables, databaseMeta, getWorkflowMeta().getDatabases());
      if (std.open()) {
        String sql =
            "SELECT *"
                + Const.CR
                + "FROM "
                + databaseMeta.getQuotedSchemaTableCombination(
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
            Database db = new Database(loggingObject, variables, databaseMeta);
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
                  sql += databaseMeta.quoteField(field.getName()) + Const.CR;
                }
                sql +=
                    "FROM "
                        + databaseMeta.getQuotedSchemaTableCombination(
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
    wlPosition.setText(
        BaseMessages.getString(
            PKG, "ActionWaitForSQL.Position.Label", "" + lineNumber, "" + columnNumber));
  }

  private void setCustomerSql() {
    wlClearResultList.setEnabled(wCustomSql.getSelection());
    wClearResultList.setEnabled(wCustomSql.getSelection());
    wlSql.setEnabled(wCustomSql.getSelection());
    wSql.setEnabled(wCustomSql.getSelection());
    wlAddRowsToResult.setEnabled(wCustomSql.getSelection());
    wAddRowsToResult.setEnabled(wCustomSql.getSelection());
    wlUseSubs.setEnabled(wCustomSql.getSelection());
    wbSqlTable.setEnabled(wCustomSql.getSelection());
    wUseSubs.setEnabled(wCustomSql.getSelection());
    wbTable.setEnabled(!wCustomSql.getSelection());
    wTablename.setEnabled(!wCustomSql.getSelection());
    wlTablename.setEnabled(!wCustomSql.getSelection());
    wlSchemaname.setEnabled(!wCustomSql.getSelection());
    wSchemaname.setEnabled(!wCustomSql.getSelection());
  }

  /** Copy information from the meta-data input to the dialog fields. */
  public void getData() {
    wName.setText(Const.nullToEmpty(action.getName()));
    wConnection.setText(Const.nullToEmpty(action.getConnection()));
    wSchemaname.setText(Const.nullToEmpty(action.getSchemaName()));
    wTablename.setText(Const.nullToEmpty(action.getTableName()));
    wSuccessCondition.setText(action.getSuccessCondition().getDescription());
    wRowsCountValue.setText(Const.NVL(action.getRowsCountValue(), "0"));
    wCustomSql.setSelection(action.isCustomSqlEnabled());
    wUseSubs.setSelection(action.isUseVars());
    wAddRowsToResult.setSelection(action.isAddRowsResult());
    wClearResultList.setSelection(action.isClearResultList());
    wSql.setText(Const.nullToEmpty(action.getCustomSql()));
    wMaximumTimeout.setText(Const.nullToEmpty(action.getMaximumTimeout()));
    wCheckCycleTime.setText(Const.nullToEmpty(action.getCheckCycleTime()));
    wSuccessOnTimeout.setSelection(action.isSuccessOnTimeout());
  }

  @Override
  protected void onActionNameModified() {
    action.setChanged();
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
    action.setConnection(wConnection.getText());
    action.setSchemaName(wSchemaname.getText());
    action.setTableName(wTablename.getText());
    action.setSuccessCondition(SuccessCondition.lookupDescription(wSuccessCondition.getText()));
    action.setRowsCountValue(wRowsCountValue.getText());
    action.setCustomSqlEnabled(wCustomSql.getSelection());
    action.setUseVars(wUseSubs.getSelection());
    action.setAddRowsResult(wAddRowsToResult.getSelection());
    action.setClearResultList(wClearResultList.getSelection());
    action.setCustomSql(wSql.getText());
    action.setMaximumTimeout(wMaximumTimeout.getText());
    action.setCheckCycleTime(wCheckCycleTime.getText());
    action.setSuccessOnTimeout(wSuccessOnTimeout.getSelection());

    dispose();
  }

  private void getTableName() {
    DatabaseMeta databaseMeta = workflowMeta.findDatabase(wConnection.getText(), variables);
    if (databaseMeta != null) {
      DatabaseExplorerDialog std =
          new DatabaseExplorerDialog(
              shell, SWT.NONE, variables, databaseMeta, getWorkflowMeta().getDatabases());
      std.setSelectedSchemaAndTable(wSchemaname.getText(), wTablename.getText());
      if (std.open()) {
        wTablename.setText(Const.NVL(std.getTableName(), ""));
      }
    }
  }
}
