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

package org.apache.hop.workflow.actions.dbt;

import org.apache.hop.core.Const;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.util.Utils;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.ui.core.PropsUi;
import org.apache.hop.ui.core.dialog.ErrorDialog;
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
import org.eclipse.swt.layout.FormAttachment;
import org.eclipse.swt.layout.FormData;
import org.eclipse.swt.layout.FormLayout;
import org.eclipse.swt.widgets.Button;
import org.eclipse.swt.widgets.Combo;
import org.eclipse.swt.widgets.Display;
import org.eclipse.swt.widgets.Label;
import org.eclipse.swt.widgets.Shell;
import org.eclipse.swt.widgets.TableItem;
import org.eclipse.swt.widgets.Text;

public class ActionDbtDialog extends ActionDialog implements IActionDialog {

  private static final Class<?> PKG = ActionDbt.class;

  private ActionDbt action;
  private boolean changed;

  private Text wName;
  private MetaSelectionLine<DbtProject> wProject;
  private Combo wOperation;
  private TextVar wTarget;
  private TextVar wSelect;
  private TextVar wExclude;
  private TextVar wThreads;
  private TextVar wTimeout;
  private Button wFullRefresh;
  private Button wEmitOpenLineage;
  private TableView wVars;
  private TableView wEnvVars;

  public ActionDbtDialog(
      Shell parent, IAction action, WorkflowMeta workflowMeta, IVariables variables) {
    super(parent, workflowMeta, variables);
    this.action = (ActionDbt) action;
    if (this.action.getName() == null) {
      this.action.setName(BaseMessages.getString(PKG, "ActionDbt.Name"));
    }
  }

  @Override
  public IAction open() {
    Shell parent = getParent();

    shell = new Shell(parent, SWT.DIALOG_TRIM | SWT.MIN | SWT.MAX | SWT.RESIZE);
    PropsUi.setLook(shell);

    // Create MetaSelectionLine BEFORE calling WorkflowDialog.setShellImage() to avoid NPE
    // The VFS files-cache gets consumed/cleared when loading the shell image,
    // so we need to initialize MetaSelectionLine (which also requires VFS) first.
    wProject =
        new MetaSelectionLine<>(
            variables,
            metadataProvider,
            DbtProject.class,
            shell,
            SWT.BORDER,
            BaseMessages.getString(PKG, "ActionDbtDialog.Project.Label"),
            null);
    PropsUi.setLook(wProject);

    // NOW it's safe to load the shell image
    WorkflowDialog.setShellImage(shell, action);

    changed = action.hasChanged();

    FormLayout formLayout = new FormLayout();
    formLayout.marginWidth = PropsUi.getFormMargin();
    formLayout.marginHeight = PropsUi.getFormMargin();
    shell.setLayout(formLayout);
    shell.setText(BaseMessages.getString(PKG, "ActionDbtDialog.Title"));

    int margin = PropsUi.getMargin();
    int middle = props.getMiddlePct();

    // Action name
    Label wlName = new Label(shell, SWT.RIGHT);
    wlName.setText(BaseMessages.getString(PKG, "ActionDbtDialog.ActionName.Label"));
    PropsUi.setLook(wlName);
    FormData fdlName = new FormData();
    fdlName.left = new FormAttachment(0, 0);
    fdlName.right = new FormAttachment(middle, -margin);
    fdlName.top = new FormAttachment(0, margin);
    wlName.setLayoutData(fdlName);
    wName = new Text(shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    PropsUi.setLook(wName);
    FormData fdName = new FormData();
    fdName.left = new FormAttachment(middle, 0);
    fdName.right = new FormAttachment(100, 0);
    fdName.top = new FormAttachment(0, margin);
    wName.setLayoutData(fdName);

    // dbt project (metadata reference)
    FormData fdProject = new FormData();
    fdProject.left = new FormAttachment(0, 0);
    fdProject.right = new FormAttachment(100, 0);
    fdProject.top = new FormAttachment(wName, margin);
    wProject.setLayoutData(fdProject);
    try {
      wProject.fillItems();
    } catch (HopException e) {
      new ErrorDialog(shell, "Error", "Error listing dbt projects", e);
    }

    // Operation
    wOperation =
        addCombo("ActionDbtDialog.Operation.Label", wProject, middle, margin, operationCodes());
    wTarget = addTextVar("ActionDbtDialog.Target.Label", wOperation, middle, margin);
    wSelect = addTextVar("ActionDbtDialog.Select.Label", wTarget, middle, margin);
    wExclude = addTextVar("ActionDbtDialog.Exclude.Label", wSelect, middle, margin);
    wThreads = addTextVar("ActionDbtDialog.Threads.Label", wExclude, middle, margin);
    wTimeout = addTextVar("ActionDbtDialog.Timeout.Label", wThreads, middle, margin);
    wFullRefresh = addCheck("ActionDbtDialog.FullRefresh.Label", wTimeout, middle, margin);
    wEmitOpenLineage =
        addCheck("ActionDbtDialog.EmitOpenLineage.Label", wFullRefresh, middle, margin);

    // Buttons (built first so the tables can anchor to them)
    Button wOk = new Button(shell, SWT.PUSH);
    wOk.setText(BaseMessages.getString(PKG, "ActionDbtDialog.Ok.Button"));
    wOk.addListener(SWT.Selection, e -> ok());
    Button wCancel = new Button(shell, SWT.PUSH);
    wCancel.setText(BaseMessages.getString(PKG, "ActionDbtDialog.Cancel.Button"));
    wCancel.addListener(SWT.Selection, e -> cancel());
    BaseTransformDialog.positionBottomButtons(shell, new Button[] {wOk, wCancel}, margin, null);

    // Vars table
    Label wlVars = new Label(shell, SWT.LEFT);
    wlVars.setText(BaseMessages.getString(PKG, "ActionDbtDialog.Vars.Label"));
    PropsUi.setLook(wlVars);
    FormData fdlVars = new FormData();
    fdlVars.left = new FormAttachment(0, 0);
    fdlVars.top = new FormAttachment(wEmitOpenLineage, margin);
    wlVars.setLayoutData(fdlVars);
    wVars = pairTable(wlVars, action.getVars().size(), margin);
    FormData fdVars = (FormData) wVars.getLayoutData();
    fdVars.bottom = new FormAttachment(wEmitOpenLineage, 120 + margin);
    wVars.setLayoutData(fdVars);

    // Env vars table
    Label wlEnv = new Label(shell, SWT.LEFT);
    wlEnv.setText(BaseMessages.getString(PKG, "ActionDbtDialog.EnvVars.Label"));
    PropsUi.setLook(wlEnv);
    FormData fdlEnv = new FormData();
    fdlEnv.left = new FormAttachment(0, 0);
    fdlEnv.top = new FormAttachment(wVars, margin);
    wlEnv.setLayoutData(fdlEnv);
    wEnvVars = pairTable(wlEnv, action.getEnvVars().size(), margin);
    FormData fdEnv = (FormData) wEnvVars.getLayoutData();
    fdEnv.bottom = new FormAttachment(wOk, -2 * margin);
    wEnvVars.setLayoutData(fdEnv);

    getData();

    BaseTransformDialog.setSize(shell);
    shell.open();
    Display display = parent.getDisplay();
    while (!shell.isDisposed()) {
      if (!display.readAndDispatch()) {
        display.sleep();
      }
    }
    return action;
  }

  private String[] operationCodes() {
    DbtOperation[] ops = DbtOperation.values();
    String[] codes = new String[ops.length];
    for (int i = 0; i < ops.length; i++) {
      codes[i] = ops[i].getCode();
    }
    return codes;
  }

  private Combo addCombo(
      String labelKey,
      org.eclipse.swt.widgets.Control top,
      int middle,
      int margin,
      String[] items) {
    Label label = new Label(shell, SWT.RIGHT);
    label.setText(BaseMessages.getString(PKG, labelKey));
    PropsUi.setLook(label);
    FormData fdl = new FormData();
    fdl.left = new FormAttachment(0, 0);
    fdl.right = new FormAttachment(middle, -margin);
    fdl.top = new FormAttachment(top, margin);
    label.setLayoutData(fdl);
    Combo combo = new Combo(shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER | SWT.READ_ONLY);
    combo.setItems(items);
    PropsUi.setLook(combo);
    FormData fd = new FormData();
    fd.left = new FormAttachment(middle, 0);
    fd.right = new FormAttachment(100, 0);
    fd.top = new FormAttachment(top, margin);
    combo.setLayoutData(fd);
    return combo;
  }

  private TextVar addTextVar(
      String labelKey, org.eclipse.swt.widgets.Control top, int middle, int margin) {
    Label label = new Label(shell, SWT.RIGHT);
    label.setText(BaseMessages.getString(PKG, labelKey));
    PropsUi.setLook(label);
    FormData fdl = new FormData();
    fdl.left = new FormAttachment(0, 0);
    fdl.right = new FormAttachment(middle, -margin);
    fdl.top = new FormAttachment(top, margin);
    label.setLayoutData(fdl);
    TextVar text = new TextVar(variables, shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    PropsUi.setLook(text);
    FormData fd = new FormData();
    fd.left = new FormAttachment(middle, 0);
    fd.right = new FormAttachment(100, 0);
    fd.top = new FormAttachment(top, margin);
    text.setLayoutData(fd);
    return text;
  }

  private Button addCheck(
      String labelKey, org.eclipse.swt.widgets.Control top, int middle, int margin) {
    Label label = new Label(shell, SWT.RIGHT);
    label.setText(BaseMessages.getString(PKG, labelKey));
    PropsUi.setLook(label);
    FormData fdl = new FormData();
    fdl.left = new FormAttachment(0, 0);
    fdl.right = new FormAttachment(middle, -margin);
    fdl.top = new FormAttachment(top, margin);
    label.setLayoutData(fdl);
    Button button = new Button(shell, SWT.CHECK);
    PropsUi.setLook(button);
    FormData fd = new FormData();
    fd.left = new FormAttachment(middle, 0);
    fd.right = new FormAttachment(100, 0);
    fd.top = new FormAttachment(top, margin);
    button.setLayoutData(fd);
    return button;
  }

  private TableView pairTable(org.eclipse.swt.widgets.Control top, int rows, int margin) {
    ColumnInfo[] columns =
        new ColumnInfo[] {
          new ColumnInfo(
              BaseMessages.getString(PKG, "ActionDbtDialog.Column.Name"),
              ColumnInfo.COLUMN_TYPE_TEXT,
              false),
          new ColumnInfo(
              BaseMessages.getString(PKG, "ActionDbtDialog.Column.Value"),
              ColumnInfo.COLUMN_TYPE_TEXT,
              false)
        };
    TableView table =
        new TableView(
            variables,
            shell,
            SWT.BORDER | SWT.FULL_SELECTION | SWT.MULTI,
            columns,
            rows,
            null,
            props);
    FormData fd = new FormData();
    fd.left = new FormAttachment(0, 0);
    fd.right = new FormAttachment(100, 0);
    fd.top = new FormAttachment(top, margin);
    table.setLayoutData(fd);
    return table;
  }

  private void getData() {
    wName.setText(Const.NVL(action.getName(), ""));
    wProject.setText(Const.NVL(action.getDbtProjectName(), ""));
    wOperation.setText(DbtOperation.fromCode(action.getOperation()).getCode());
    wTarget.setText(Const.NVL(action.getTarget(), ""));
    wSelect.setText(Const.NVL(action.getSelect(), ""));
    wExclude.setText(Const.NVL(action.getExclude(), ""));
    wThreads.setText(Const.NVL(action.getThreads(), ""));
    wTimeout.setText(Const.NVL(action.getTimeout(), ""));
    wFullRefresh.setSelection(action.isFullRefresh());
    wEmitOpenLineage.setSelection(action.isEmitOpenLineage());
    for (int i = 0; i < action.getVars().size(); i++) {
      DbtNameValue v = action.getVars().get(i);
      TableItem item = wVars.table.getItem(i);
      item.setText(1, Const.NVL(v.getName(), ""));
      item.setText(2, Const.NVL(v.getValue(), ""));
    }
    for (int i = 0; i < action.getEnvVars().size(); i++) {
      DbtNameValue v = action.getEnvVars().get(i);
      TableItem item = wEnvVars.table.getItem(i);
      item.setText(1, Const.NVL(v.getName(), ""));
      item.setText(2, Const.NVL(v.getValue(), ""));
    }
    wVars.optimizeTableView();
    wEnvVars.optimizeTableView();
  }

  private void cancel() {
    action.setChanged(changed);
    action = null;
    dispose();
  }

  private void ok() {
    if (Utils.isEmpty(wName.getText())) {
      return;
    }
    action.setName(wName.getText());
    action.setDbtProjectName(wProject.getText());
    action.setOperation(wOperation.getText());
    action.setTarget(wTarget.getText());
    action.setSelect(wSelect.getText());
    action.setExclude(wExclude.getText());
    action.setThreads(wThreads.getText());
    action.setTimeout(wTimeout.getText());
    action.setFullRefresh(wFullRefresh.getSelection());
    action.setEmitOpenLineage(wEmitOpenLineage.getSelection());

    action.getVars().clear();
    for (TableItem item : wVars.getNonEmptyItems()) {
      action.getVars().add(new DbtNameValue(item.getText(1), item.getText(2)));
    }
    action.getEnvVars().clear();
    for (TableItem item : wEnvVars.getNonEmptyItems()) {
      action.getEnvVars().add(new DbtNameValue(item.getText(1), item.getText(2)));
    }
    action.setChanged();
    dispose();
  }
}
