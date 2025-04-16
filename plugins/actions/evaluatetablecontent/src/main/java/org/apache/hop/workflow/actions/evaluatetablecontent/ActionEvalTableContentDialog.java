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

package org.apache.hop.workflow.actions.evaluatetablecontent;

import org.apache.commons.lang.StringUtils;
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
import org.apache.hop.ui.core.widget.StyledTextComp;
import org.apache.hop.ui.core.widget.TextVar;
import org.apache.hop.ui.pipeline.transform.BaseTransformDialog;
import org.apache.hop.ui.workflow.action.ActionDialog;
import org.apache.hop.ui.workflow.dialog.WorkflowDialog;
import org.apache.hop.workflow.WorkflowMeta;
import org.apache.hop.workflow.action.IAction;
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
import org.eclipse.swt.widgets.Text;

/**
 * This dialog allows you to edit the Table content evaluation action settings. (select the
 * connection and the table to evaluate)
 */
public class ActionEvalTableContentDialog extends ActionDialog {
  private static final Class<?> PKG = ActionEvalTableContent.class;

  private Button wbTable;
  private Button wbSqlTable;

  private Text wName;

  private MetaSelectionLine<DatabaseMeta> wConnection;

  private ActionEvalTableContent action;

  private boolean changed;

  private Label wlUseSubs;

  private Button wUseSubs;

  private Label wlClearResultList;

  private Button wClearResultList;

  private Label wlAddRowsToResult;

  private Button wAddRowsToResult;

  private Button wCustomSql;

  private Label wlSql;

  private StyledTextComp wSql;

  private Label wlPosition;

  // Schema name
  private Label wlSchemaName;
  private TextVar wSchemaName;

  private Label wlTableName;
  private TextVar wTableName;

  private CCombo wSuccessCondition;

  private TextVar wLimit;

  public ActionEvalTableContentDialog(
      Shell parent,
      ActionEvalTableContent action,
      WorkflowMeta workflowMeta,
      IVariables variables) {
    super(parent, workflowMeta, variables);
    this.action = action;
    if (this.action.getName() == null) {
      this.action.setName(BaseMessages.getString(PKG, "ActionEvalTableContent.Name.Default"));
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
    shell.setText(BaseMessages.getString(PKG, "ActionEvalTableContent.Title"));

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
    wlName.setText(BaseMessages.getString(PKG, "ActionEvalTableContent.Name.Label"));
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
    wConnection = addConnectionLine(shell, wName, action.getDatabase(), lsMod);

    // Schema name line
    wlSchemaName = new Label(shell, SWT.RIGHT);
    wlSchemaName.setText(BaseMessages.getString(PKG, "ActionEvalTableContent.SchemaName.Label"));
    PropsUi.setLook(wlSchemaName);
    FormData fdlSchemaName = new FormData();
    fdlSchemaName.left = new FormAttachment(0, 0);
    fdlSchemaName.right = new FormAttachment(middle, -margin);
    fdlSchemaName.top = new FormAttachment(wConnection, margin);
    wlSchemaName.setLayoutData(fdlSchemaName);

    wSchemaName = new TextVar(variables, shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    PropsUi.setLook(wSchemaName);
    wSchemaName.setToolTipText(
        BaseMessages.getString(PKG, "ActionEvalTableContent.SchemaName.Tooltip"));
    wSchemaName.addModifyListener(lsMod);
    FormData fdSchemaName = new FormData();
    fdSchemaName.left = new FormAttachment(middle, 0);
    fdSchemaName.top = new FormAttachment(wConnection, margin);
    fdSchemaName.right = new FormAttachment(100, 0);
    wSchemaName.setLayoutData(fdSchemaName);

    // Table name line
    wlTableName = new Label(shell, SWT.RIGHT);
    wlTableName.setText(BaseMessages.getString(PKG, "ActionEvalTableContent.TableName.Label"));
    PropsUi.setLook(wlTableName);
    FormData fdlTableName = new FormData();
    fdlTableName.left = new FormAttachment(0, 0);
    fdlTableName.right = new FormAttachment(middle, -margin);
    fdlTableName.top = new FormAttachment(wSchemaName, margin);
    wlTableName.setLayoutData(fdlTableName);

    wbTable = new Button(shell, SWT.PUSH | SWT.CENTER);
    PropsUi.setLook(wbTable);
    wbTable.setText(BaseMessages.getString(PKG, "System.Button.Browse"));
    FormData fdbTable = new FormData();
    fdbTable.right = new FormAttachment(100, 0);
    fdbTable.top = new FormAttachment(wSchemaName, margin / 2);
    wbTable.setLayoutData(fdbTable);
    wbTable.addSelectionListener(
        new SelectionAdapter() {
          @Override
          public void widgetSelected(SelectionEvent e) {
            getTableName();
          }
        });

    wTableName = new TextVar(variables, shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    PropsUi.setLook(wTableName);
    wTableName.setToolTipText(
        BaseMessages.getString(PKG, "ActionEvalTableContent.TableName.Tooltip"));
    wTableName.addModifyListener(lsMod);
    FormData fdTableName = new FormData();
    fdTableName.left = new FormAttachment(middle, 0);
    fdTableName.top = new FormAttachment(wSchemaName, margin);
    fdTableName.right = new FormAttachment(wbTable, -margin);
    wTableName.setLayoutData(fdTableName);

    // ////////////////////////
    // START OF Success GROUP///
    // ///////////////////////////////
    Group wSuccessGroup = new Group(shell, SWT.SHADOW_NONE);
    PropsUi.setLook(wSuccessGroup);
    wSuccessGroup.setText(
        BaseMessages.getString(PKG, "ActionEvalTableContent.SuccessGroup.Group.Label"));

    FormLayout successGroupLayout = new FormLayout();
    successGroupLayout.marginWidth = 10;
    successGroupLayout.marginHeight = 10;
    wSuccessGroup.setLayout(successGroupLayout);

    // Success Condition
    Label wlSuccessCondition = new Label(wSuccessGroup, SWT.RIGHT);
    wlSuccessCondition.setText(
        BaseMessages.getString(PKG, "ActionEvalTableContent.SuccessCondition.Label"));
    PropsUi.setLook(wlSuccessCondition);
    FormData fdlSuccessCondition = new FormData();
    fdlSuccessCondition.left = new FormAttachment(0, -margin);
    fdlSuccessCondition.right = new FormAttachment(middle, -2 * margin);
    fdlSuccessCondition.top = new FormAttachment(0, margin);
    wlSuccessCondition.setLayoutData(fdlSuccessCondition);
    wSuccessCondition = new CCombo(wSuccessGroup, SWT.SINGLE | SWT.READ_ONLY | SWT.BORDER);
    wSuccessCondition.setItems(ActionEvalTableContent.successConditionsDesc);
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
    Label wlLimit = new Label(wSuccessGroup, SWT.RIGHT);
    wlLimit.setText(BaseMessages.getString(PKG, "ActionEvalTableContent.Limit.Label"));
    PropsUi.setLook(wlLimit);
    FormData fdlLimit = new FormData();
    fdlLimit.left = new FormAttachment(0, -margin);
    fdlLimit.top = new FormAttachment(wSuccessCondition, margin);
    fdlLimit.right = new FormAttachment(middle, -2 * margin);
    wlLimit.setLayoutData(fdlLimit);

    wLimit =
        new TextVar(
            variables,
            wSuccessGroup,
            SWT.SINGLE | SWT.LEFT | SWT.BORDER,
            BaseMessages.getString(PKG, "ActionEvalTableContent.Limit.Tooltip"));
    PropsUi.setLook(wLimit);
    wLimit.addModifyListener(lsMod);
    FormData fdLimit = new FormData();
    fdLimit.left = new FormAttachment(middle, -margin);
    fdLimit.top = new FormAttachment(wSuccessCondition, margin);
    fdLimit.right = new FormAttachment(100, -margin);
    wLimit.setLayoutData(fdLimit);

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
    wCustomGroup.setText(
        BaseMessages.getString(PKG, "ActionEvalTableContent.CustomGroup.Group.Label"));

    FormLayout customGroupLayout = new FormLayout();
    customGroupLayout.marginWidth = 10;
    customGroupLayout.marginHeight = 10;
    wCustomGroup.setLayout(customGroupLayout);

    // custom Sql?
    Label wlCustomSql = new Label(wCustomGroup, SWT.RIGHT);
    wlCustomSql.setText(BaseMessages.getString(PKG, "ActionEvalTableContent.customSQL.Label"));
    PropsUi.setLook(wlCustomSql);
    FormData fdlCustomSql = new FormData();
    fdlCustomSql.left = new FormAttachment(0, -margin);
    fdlCustomSql.top = new FormAttachment(0, margin);
    fdlCustomSql.right = new FormAttachment(middle, -2 * margin);
    wlCustomSql.setLayoutData(fdlCustomSql);
    wCustomSql = new Button(wCustomGroup, SWT.CHECK);
    PropsUi.setLook(wCustomSql);
    wCustomSql.setToolTipText(
        BaseMessages.getString(PKG, "ActionEvalTableContent.customSQL.Tooltip"));
    FormData fdCustomSql = new FormData();
    fdCustomSql.left = new FormAttachment(middle, -margin);
    fdCustomSql.top = new FormAttachment(wlCustomSql, 0, SWT.CENTER);
    fdCustomSql.right = new FormAttachment(100, 0);
    wCustomSql.setLayoutData(fdCustomSql);
    wCustomSql.addSelectionListener(
        new SelectionAdapter() {
          @Override
          public void widgetSelected(SelectionEvent e) {

            setCustomSql();
            action.setChanged();
          }
        });
    // use Variable substitution?
    wlUseSubs = new Label(wCustomGroup, SWT.RIGHT);
    wlUseSubs.setText(BaseMessages.getString(PKG, "ActionEvalTableContent.UseVariableSubst.Label"));
    PropsUi.setLook(wlUseSubs);
    FormData fdlUseSubs = new FormData();
    fdlUseSubs.left = new FormAttachment(0, -margin);
    fdlUseSubs.top = new FormAttachment(wlCustomSql, 2 * margin);
    fdlUseSubs.right = new FormAttachment(middle, -2 * margin);
    wlUseSubs.setLayoutData(fdlUseSubs);
    wUseSubs = new Button(wCustomGroup, SWT.CHECK);
    PropsUi.setLook(wUseSubs);
    wUseSubs.setToolTipText(
        BaseMessages.getString(PKG, "ActionEvalTableContent.UseVariableSubst.Tooltip"));
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
        BaseMessages.getString(PKG, "ActionEvalTableContent.ClearResultList.Label"));
    PropsUi.setLook(wlClearResultList);
    FormData fdlClearResultList = new FormData();
    fdlClearResultList.left = new FormAttachment(0, -margin);
    fdlClearResultList.top = new FormAttachment(wlUseSubs, 2 * margin);
    fdlClearResultList.right = new FormAttachment(middle, -2 * margin);
    wlClearResultList.setLayoutData(fdlClearResultList);
    wClearResultList = new Button(wCustomGroup, SWT.CHECK);
    PropsUi.setLook(wClearResultList);
    wClearResultList.setToolTipText(
        BaseMessages.getString(PKG, "ActionEvalTableContent.ClearResultList.Tooltip"));
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
        BaseMessages.getString(PKG, "ActionEvalTableContent.AddRowsToResult.Label"));
    PropsUi.setLook(wlAddRowsToResult);
    FormData fdlAddRowsToResult = new FormData();
    fdlAddRowsToResult.left = new FormAttachment(0, -margin);
    fdlAddRowsToResult.top = new FormAttachment(wlClearResultList, 2 * margin);
    fdlAddRowsToResult.right = new FormAttachment(middle, -2 * margin);
    wlAddRowsToResult.setLayoutData(fdlAddRowsToResult);
    wAddRowsToResult = new Button(wCustomGroup, SWT.CHECK);
    PropsUi.setLook(wAddRowsToResult);
    wAddRowsToResult.setToolTipText(
        BaseMessages.getString(PKG, "ActionEvalTableContent.AddRowsToResult.Tooltip"));
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
    wlSql.setText(BaseMessages.getString(PKG, "ActionEvalTableContent.Script.Label"));
    PropsUi.setLook(wlSql);
    FormData fdlSql = new FormData();
    fdlSql.left = new FormAttachment(0, 0);
    fdlSql.top = new FormAttachment(wlAddRowsToResult, 2 * margin);
    wlSql.setLayoutData(fdlSql);

    wbSqlTable = new Button(wCustomGroup, SWT.PUSH | SWT.CENTER);
    PropsUi.setLook(wbSqlTable);
    wbSqlTable.setText(
        BaseMessages.getString(PKG, "ActionEvalTableContent.GetSQLAndSelectStatement"));
    FormData fdbSqlTable = new FormData();
    fdbSqlTable.right = new FormAttachment(100, 0);
    fdbSqlTable.top = new FormAttachment(wAddRowsToResult, margin);
    wbSqlTable.setLayoutData(fdbSqlTable);
    wbSqlTable.addListener(SWT.Selection, e -> getSql());

    wSql =
        new StyledTextComp(
            action, wCustomGroup, SWT.MULTI | SWT.LEFT | SWT.BORDER | SWT.H_SCROLL | SWT.V_SCROLL);
    PropsUi.setLook(wSql, Props.WIDGET_STYLE_FIXED);
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
    fdCustomGroup.bottom = new FormAttachment(wOk, -margin);
    wCustomGroup.setLayoutData(fdCustomGroup);
    // ///////////////////////////////////////////////////////////
    // / END OF CustomGroup GROUP
    // ///////////////////////////////////////////////////////////

    getData();
    setCustomSql();

    BaseDialog.defaultShellHandling(shell, c -> ok(), c -> cancel());

    return action;
  }

  private void getSql() {
    DatabaseMeta inf = getWorkflowMeta().findDatabase(wConnection.getText(), variables);
    if (inf != null) {
      DatabaseExplorerDialog std =
          new DatabaseExplorerDialog(
              shell, SWT.NONE, variables, inf, getWorkflowMeta().getDatabases());
      if (std.open()) {
        StringBuilder sql = new StringBuilder();
        sql.append("SELECT *")
            .append(Const.CR)
            .append("FROM ")
            .append(
                inf.getQuotedSchemaTableCombination(
                    variables, std.getSchemaName(), std.getTableName()))
            .append(Const.CR);
        wSql.setText(sql.toString());

        MessageBox yn = new MessageBox(shell, SWT.YES | SWT.NO | SWT.CANCEL | SWT.ICON_QUESTION);
        yn.setMessage(BaseMessages.getString(PKG, "ActionEvalTableContent.IncludeFieldNamesInSQL"));
        yn.setText(BaseMessages.getString(PKG, "ActionEvalTableContent.DialogCaptionQuestion"));
        int id = yn.open();
        switch (id) {
          case SWT.CANCEL:
            break;
          case SWT.NO:
            wSql.setText(sql.toString());
            break;
          case SWT.YES:
            try (Database db = new Database(loggingObject, variables, inf)) {
              db.connect();
              IRowMeta fields = db.getQueryFields(sql.toString(), false);
              if (fields != null) {
                sql.setLength(0);
                sql.append("SELECT").append(Const.CR);
                for (int i = 0; i < fields.size(); i++) {
                  IValueMeta field = fields.getValueMeta(i);
                  if (i == 0) {
                    sql.append("  ");
                  } else {
                    sql.append(", ");
                  }
                  sql.append(inf.quoteField(field.getName())).append(Const.CR);
                }
                sql.append("FROM ")
                    .append(
                        inf.getQuotedSchemaTableCombination(
                            variables, std.getSchemaName(), std.getTableName()))
                    .append(Const.CR);
                wSql.setText(sql.toString());
              } else {
                MessageBox mb = new MessageBox(shell, SWT.OK | SWT.ICON_ERROR);
                mb.setMessage(
                    BaseMessages.getString(
                            PKG, "ActionEvalTableContent.ERROR_CouldNotRetrieveFields")
                        + Const.CR
                        + BaseMessages.getString(
                            PKG, "ActionEvalTableContent.PerhapsNoPermissions"));
                mb.setText(
                    BaseMessages.getString(PKG, "ActionEvalTableContent.DialogCaptionError2"));
                mb.open();
              }
            } catch (HopException e) {
              MessageBox mb = new MessageBox(shell, SWT.OK | SWT.ICON_ERROR);
              mb.setText(BaseMessages.getString(PKG, "ActionEvalTableContent.DialogCaptionError3"));
              mb.setMessage(
                  BaseMessages.getString(PKG, "ActionEvalTableContent.AnErrorOccurred")
                      + Const.CR
                      + e.getMessage());
              mb.open();
            }
            break;
          default:
            break;
        }
      }
    } else {
      MessageBox mb = new MessageBox(shell, SWT.OK | SWT.ICON_ERROR);
      mb.setMessage(
          BaseMessages.getString(PKG, "ActionEvalTableContent.ConnectionNoLongerAvailable"));
      mb.setText(BaseMessages.getString(PKG, "ActionEvalTableContent.DialogCaptionError4"));
      mb.open();
    }
  }

  public void setPosition() {
    int lineNumber = wSql.getLineNumber();
    int columnNumber = wSql.getColumnNumber();
    wlPosition.setText(
        BaseMessages.getString(
            PKG, "ActionEvalTableContent.Position.Label", "" + lineNumber, "" + columnNumber));
  }

  private void setCustomSql() {
    wlSql.setEnabled(wCustomSql.getSelection());
    wSql.setEnabled(wCustomSql.getSelection());
    wlAddRowsToResult.setEnabled(wCustomSql.getSelection());
    wAddRowsToResult.setEnabled(wCustomSql.getSelection());
    wlClearResultList.setEnabled(wCustomSql.getSelection());
    wClearResultList.setEnabled(wCustomSql.getSelection());
    wlUseSubs.setEnabled(wCustomSql.getSelection());
    wbSqlTable.setEnabled(wCustomSql.getSelection());
    wUseSubs.setEnabled(wCustomSql.getSelection());
    wbTable.setEnabled(!wCustomSql.getSelection());
    wTableName.setEnabled(!wCustomSql.getSelection());
    wlTableName.setEnabled(!wCustomSql.getSelection());
    wlSchemaName.setEnabled(!wCustomSql.getSelection());
    wSchemaName.setEnabled(!wCustomSql.getSelection());
  }

  /** Copy information from the meta-data input to the dialog fields. */
  public void getData() {
    if (action.getName() != null) {
      wName.setText(action.getName());
    }

    if (action.getDatabase() != null) {
      wConnection.setText(action.getDatabase().getName());
    }

    if (action.getSchemaname() != null) {
      wSchemaName.setText(action.getSchemaname());
    }
    if (action.getTableName() != null) {
      wTableName.setText(action.getTableName());
    }

    wSuccessCondition.setText(
        ActionEvalTableContent.getSuccessConditionDesc(
            ActionEvalTableContent.getSuccessConditionByCode(action.getSuccessCondition())));
    if (action.getLimit() != null) {
      wLimit.setText(action.getLimit());
    } else {
      wLimit.setText("0");
    }

    wCustomSql.setSelection(action.isUseCustomSql());
    wUseSubs.setSelection(action.isUseVars());
    wClearResultList.setSelection(action.isClearResultList());
    wAddRowsToResult.setSelection(action.isAddRowsResult());
    if (action.getCustomSql() != null) {
      wSql.setText(action.getCustomSql());
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
      mb.setMessage("Please give this action a name.");
      mb.setText("Enter the name of the action");
      mb.open();
      return;
    }
    action.setName(wName.getText());
    action.setConnection(wConnection.getText());

    action.setSchemaname(wSchemaName.getText());
    action.setTableName(wTableName.getText());
    action.setSuccessCondition(
        ActionEvalTableContent.getSuccessConditionCode(
            ActionEvalTableContent.getSuccessConditionByDesc(wSuccessCondition.getText())));
    action.setLimit(wLimit.getText());
    action.setUseCustomSql(wCustomSql.getSelection());
    action.setUseVars(wUseSubs.getSelection());
    action.setAddRowsResult(wAddRowsToResult.getSelection());
    action.setClearResultList(wClearResultList.getSelection());

    action.setCustomSql(wSql.getText());
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
        std.setSelectedSchemaAndTable(wSchemaName.getText(), wTableName.getText());
        if (std.open()) {
          wTableName.setText(Const.NVL(std.getTableName(), ""));
        }
      } else {
        MessageBox mb = new MessageBox(shell, SWT.OK | SWT.ICON_ERROR);
        mb.setMessage(
            BaseMessages.getString(PKG, "ActionEvalTableContent.ConnectionError2.DialogMessage"));
        mb.setText(BaseMessages.getString(PKG, "System.Dialog.Error.Title"));
        mb.open();
      }
    }
  }
}
