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

package org.apache.hop.workflow.actions.databricks;

import org.apache.hop.core.Const;
import org.apache.hop.core.util.Utils;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.databricks.metadata.DatabricksConnection;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.ui.core.PropsUi;
import org.apache.hop.ui.core.dialog.BaseDialog;
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
import org.eclipse.swt.custom.CTabFolder;
import org.eclipse.swt.custom.CTabItem;
import org.eclipse.swt.custom.StyledText;
import org.eclipse.swt.events.ModifyListener;
import org.eclipse.swt.layout.FormAttachment;
import org.eclipse.swt.layout.FormData;
import org.eclipse.swt.layout.FormLayout;
import org.eclipse.swt.widgets.Button;
import org.eclipse.swt.widgets.Composite;
import org.eclipse.swt.widgets.Control;
import org.eclipse.swt.widgets.Label;
import org.eclipse.swt.widgets.Shell;
import org.eclipse.swt.widgets.Text;

/** Dialog for {@link ActionDatabricksJobRun}. */
public class ActionDatabricksJobRunDialog extends ActionDialog {
  private static final Class<?> PKG = ActionDatabricksJobRun.class;

  private ActionDatabricksJobRun action;
  private boolean changed;

  private Text wName;
  private MetaSelectionLine<DatabricksConnection> wConnection;
  private CCombo wRunMode;
  private TextVar wJobId;
  private StyledText wSubmitJson;
  private CCombo wWaitMode;
  private TextVar wTimeout;
  private TextVar wPoll;
  private TextVar wVarJobId;
  private TextVar wVarRunId;
  private TextVar wVarStatus;
  private TextVar wVarPageUrl;
  private TextVar wVarError;

  private Label wlJobId;
  private Label wlSubmitJson;

  public ActionDatabricksJobRunDialog(
      Shell parent,
      ActionDatabricksJobRun action,
      WorkflowMeta workflowMeta,
      IVariables variables) {
    super(parent, workflowMeta, variables);
    this.action = action;
    if (this.action.getName() == null) {
      this.action.setName(BaseMessages.getString(PKG, "ActionDatabricksJobRun.Name.Default"));
    }
  }

  @Override
  public IAction open() {
    shell = new Shell(getParent(), SWT.DIALOG_TRIM | SWT.MIN | SWT.MAX | SWT.RESIZE);
    PropsUi.setLook(shell);
    WorkflowDialog.setShellImage(shell, action);

    FormLayout formLayout = new FormLayout();
    formLayout.marginWidth = PropsUi.getFormMargin();
    formLayout.marginHeight = PropsUi.getFormMargin();
    shell.setLayout(formLayout);
    shell.setText(BaseMessages.getString(PKG, "ActionDatabricksJobRun.Title"));

    int middle = props.getMiddlePct();
    int margin = PropsUi.getMargin();
    changed = action.hasChanged();
    ModifyListener lsMod = e -> action.setChanged();

    Button wOk = new Button(shell, SWT.PUSH);
    wOk.setText(BaseMessages.getString(PKG, "System.Button.OK"));
    wOk.addListener(SWT.Selection, e -> ok());
    Button wCancel = new Button(shell, SWT.PUSH);
    wCancel.setText(BaseMessages.getString(PKG, "System.Button.Cancel"));
    wCancel.addListener(SWT.Selection, e -> cancel());
    BaseTransformDialog.positionBottomButtons(shell, new Button[] {wOk, wCancel}, margin, null);

    Label wlName = new Label(shell, SWT.RIGHT);
    wlName.setText(BaseMessages.getString(PKG, "ActionDatabricksJobRun.Name.Label"));
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
    fdName.top = new FormAttachment(wlName, 0, SWT.CENTER);
    fdName.right = new FormAttachment(100, 0);
    wName.setLayoutData(fdName);

    CTabFolder wTabFolder = new CTabFolder(shell, SWT.BORDER);
    PropsUi.setLook(wTabFolder);
    FormData fdTab = new FormData();
    fdTab.left = new FormAttachment(0, 0);
    fdTab.top = new FormAttachment(wName, margin);
    fdTab.right = new FormAttachment(100, 0);
    fdTab.bottom = new FormAttachment(wOk, -margin);
    wTabFolder.setLayoutData(fdTab);

    // --- Job tab ---
    CTabItem tabJob = new CTabItem(wTabFolder, SWT.NONE);
    tabJob.setText(BaseMessages.getString(PKG, "ActionDatabricksJobRun.Tab.Job"));
    Composite wJobComp = new Composite(wTabFolder, SWT.NONE);
    PropsUi.setLook(wJobComp);
    wJobComp.setLayout(new FormLayout());
    tabJob.setControl(wJobComp);

    wConnection =
        new MetaSelectionLine<>(
            variables,
            metadataProvider,
            DatabricksConnection.class,
            wJobComp,
            SWT.NONE,
            BaseMessages.getString(PKG, "ActionDatabricksJobRun.Connection.Label"),
            BaseMessages.getString(PKG, "ActionDatabricksJobRun.Connection.Tooltip"));
    PropsUi.setLook(wConnection);
    FormData fdConn = new FormData();
    fdConn.left = new FormAttachment(0, 0);
    fdConn.top = new FormAttachment(0, margin);
    fdConn.right = new FormAttachment(100, 0);
    wConnection.setLayoutData(fdConn);
    try {
      wConnection.fillItems();
    } catch (Exception e) {
      // ignore empty metadata
    }
    wConnection.addModifyListener(lsMod);

    Label wlRunMode = new Label(wJobComp, SWT.RIGHT);
    wlRunMode.setText(BaseMessages.getString(PKG, "ActionDatabricksJobRun.RunMode.Label"));
    PropsUi.setLook(wlRunMode);
    FormData fdlRunMode = new FormData();
    fdlRunMode.left = new FormAttachment(0, 0);
    fdlRunMode.right = new FormAttachment(middle, -margin);
    fdlRunMode.top = new FormAttachment(wConnection, margin);
    wlRunMode.setLayoutData(fdlRunMode);
    wRunMode = new CCombo(wJobComp, SWT.BORDER | SWT.READ_ONLY);
    wRunMode.setItems(
        new String[] {
          BaseMessages.getString(PKG, "ActionDatabricksJobRun.RunMode.RunExisting"),
          BaseMessages.getString(PKG, "ActionDatabricksJobRun.RunMode.SubmitOnce")
        });
    PropsUi.setLook(wRunMode);
    FormData fdRunMode = new FormData();
    fdRunMode.left = new FormAttachment(middle, 0);
    fdRunMode.top = new FormAttachment(wConnection, margin);
    fdRunMode.right = new FormAttachment(100, 0);
    wRunMode.setLayoutData(fdRunMode);
    wRunMode.addModifyListener(lsMod);
    wRunMode.addListener(SWT.Selection, e -> enableModeFields());

    wlJobId = new Label(wJobComp, SWT.RIGHT);
    wlJobId.setText(BaseMessages.getString(PKG, "ActionDatabricksJobRun.JobId.Label"));
    PropsUi.setLook(wlJobId);
    FormData fdlJobId = new FormData();
    fdlJobId.left = new FormAttachment(0, 0);
    fdlJobId.right = new FormAttachment(middle, -margin);
    fdlJobId.top = new FormAttachment(wRunMode, margin);
    wlJobId.setLayoutData(fdlJobId);
    wJobId = new TextVar(variables, wJobComp, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    PropsUi.setLook(wJobId);
    wJobId.addModifyListener(lsMod);
    FormData fdJobId = new FormData();
    fdJobId.left = new FormAttachment(middle, 0);
    fdJobId.top = new FormAttachment(wRunMode, margin);
    fdJobId.right = new FormAttachment(100, 0);
    wJobId.setLayoutData(fdJobId);

    wlSubmitJson = new Label(wJobComp, SWT.RIGHT);
    wlSubmitJson.setText(BaseMessages.getString(PKG, "ActionDatabricksJobRun.SubmitJson.Label"));
    PropsUi.setLook(wlSubmitJson);
    FormData fdlSubmit = new FormData();
    fdlSubmit.left = new FormAttachment(0, 0);
    fdlSubmit.right = new FormAttachment(middle, -margin);
    fdlSubmit.top = new FormAttachment(wJobId, margin);
    wlSubmitJson.setLayoutData(fdlSubmit);
    wSubmitJson =
        new StyledText(wJobComp, SWT.MULTI | SWT.LEFT | SWT.BORDER | SWT.V_SCROLL | SWT.H_SCROLL);
    PropsUi.setLook(wSubmitJson);
    wSubmitJson.addModifyListener(lsMod);
    FormData fdSubmit = new FormData();
    fdSubmit.left = new FormAttachment(middle, 0);
    fdSubmit.top = new FormAttachment(wJobId, margin);
    fdSubmit.right = new FormAttachment(100, 0);
    fdSubmit.bottom = new FormAttachment(100, -margin);
    fdSubmit.height = 120;
    wSubmitJson.setLayoutData(fdSubmit);

    // --- Run tab ---
    CTabItem tabRun = new CTabItem(wTabFolder, SWT.NONE);
    tabRun.setText(BaseMessages.getString(PKG, "ActionDatabricksJobRun.Tab.Run"));
    Composite wRunComp = new Composite(wTabFolder, SWT.NONE);
    PropsUi.setLook(wRunComp);
    wRunComp.setLayout(new FormLayout());
    tabRun.setControl(wRunComp);

    Label wlWait = new Label(wRunComp, SWT.RIGHT);
    wlWait.setText(BaseMessages.getString(PKG, "ActionDatabricksJobRun.WaitMode.Label"));
    PropsUi.setLook(wlWait);
    FormData fdlWait = new FormData();
    fdlWait.left = new FormAttachment(0, 0);
    fdlWait.right = new FormAttachment(middle, -margin);
    fdlWait.top = new FormAttachment(0, margin);
    wlWait.setLayoutData(fdlWait);
    wWaitMode = new CCombo(wRunComp, SWT.BORDER | SWT.READ_ONLY);
    wWaitMode.setItems(
        new String[] {
          BaseMessages.getString(PKG, "ActionDatabricksJobRun.WaitMode.Wait"),
          BaseMessages.getString(PKG, "ActionDatabricksJobRun.WaitMode.FireAndForget")
        });
    PropsUi.setLook(wWaitMode);
    FormData fdWait = new FormData();
    fdWait.left = new FormAttachment(middle, 0);
    fdWait.top = new FormAttachment(0, margin);
    fdWait.right = new FormAttachment(100, 0);
    wWaitMode.setLayoutData(fdWait);
    wWaitMode.addModifyListener(lsMod);

    Control last = addTextVarRow(wRunComp, wWaitMode, middle, margin, lsMod, "Timeout", true);
    wTimeout = (TextVar) last;
    last = addTextVarRow(wRunComp, wTimeout, middle, margin, lsMod, "Poll", true);
    wPoll = (TextVar) last;
    last = addTextVarRow(wRunComp, wPoll, middle, margin, lsMod, "VarJobId", false);
    wVarJobId = (TextVar) last;
    last = addTextVarRow(wRunComp, wVarJobId, middle, margin, lsMod, "VarRunId", false);
    wVarRunId = (TextVar) last;
    last = addTextVarRow(wRunComp, wVarRunId, middle, margin, lsMod, "VarStatus", false);
    wVarStatus = (TextVar) last;
    last = addTextVarRow(wRunComp, wVarStatus, middle, margin, lsMod, "VarPageUrl", false);
    wVarPageUrl = (TextVar) last;
    last = addTextVarRow(wRunComp, wVarPageUrl, middle, margin, lsMod, "VarError", false);
    wVarError = (TextVar) last;

    wTabFolder.setSelection(0);
    getData();
    enableModeFields();
    wName.setFocus();

    BaseDialog.defaultShellHandling(shell, c -> ok(), c -> cancel());
    return action;
  }

  private Control addTextVarRow(
      Composite parent,
      Control above,
      int middle,
      int margin,
      ModifyListener lsMod,
      String key,
      boolean ignored) {
    Label wl = new Label(parent, SWT.RIGHT);
    wl.setText(BaseMessages.getString(PKG, "ActionDatabricksJobRun." + key + ".Label"));
    PropsUi.setLook(wl);
    FormData fdl = new FormData();
    fdl.left = new FormAttachment(0, 0);
    fdl.right = new FormAttachment(middle, -margin);
    fdl.top = new FormAttachment(above, margin);
    wl.setLayoutData(fdl);
    TextVar w = new TextVar(variables, parent, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    PropsUi.setLook(w);
    w.addModifyListener(lsMod);
    FormData fd = new FormData();
    fd.left = new FormAttachment(middle, 0);
    fd.top = new FormAttachment(above, margin);
    fd.right = new FormAttachment(100, 0);
    w.setLayoutData(fd);
    // stash for getData via field assignment in open()
    return w;
  }

  private void enableModeFields() {
    boolean existing = wRunMode.getSelectionIndex() <= 0;
    wlJobId.setEnabled(existing);
    wJobId.setEnabled(existing);
    wlSubmitJson.setEnabled(!existing);
    wSubmitJson.setEnabled(!existing);
  }

  public void getData() {
    wName.setText(Const.NVL(action.getName(), ""));
    wConnection.setText(Const.NVL(action.getConnectionName(), ""));
    if (ActionDatabricksJobRun.MODE_SUBMIT_ONCE.equalsIgnoreCase(action.getRunMode())) {
      wRunMode.select(1);
    } else {
      wRunMode.select(0);
    }
    wJobId.setText(Const.NVL(action.getJobId(), ""));
    wSubmitJson.setText(Const.NVL(action.getSubmitRunJson(), ""));
    if (ActionDatabricksJobRun.WAIT_FIRE_AND_FORGET.equalsIgnoreCase(action.getWaitMode())) {
      wWaitMode.select(1);
    } else {
      wWaitMode.select(0);
    }
    wTimeout.setText(Const.NVL(action.getTimeoutSeconds(), "3600"));
    wPoll.setText(Const.NVL(action.getPollIntervalSeconds(), "15"));
    wVarJobId.setText(Const.NVL(action.getResultVariableJobId(), "DatabricksJobId"));
    wVarRunId.setText(Const.NVL(action.getResultVariableRunId(), "DatabricksRunId"));
    wVarStatus.setText(Const.NVL(action.getResultVariableStatus(), "DatabricksStatus"));
    wVarPageUrl.setText(Const.NVL(action.getResultVariablePageUrl(), "DatabricksRunPageUrl"));
    wVarError.setText(Const.NVL(action.getResultVariableError(), "DatabricksError"));
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
    action.setConnectionName(wConnection.getText());
    action.setRunMode(
        wRunMode.getSelectionIndex() == 1
            ? ActionDatabricksJobRun.MODE_SUBMIT_ONCE
            : ActionDatabricksJobRun.MODE_RUN_EXISTING);
    action.setJobId(wJobId.getText());
    action.setSubmitRunJson(wSubmitJson.getText());
    action.setWaitMode(
        wWaitMode.getSelectionIndex() == 1
            ? ActionDatabricksJobRun.WAIT_FIRE_AND_FORGET
            : ActionDatabricksJobRun.WAIT_WAIT);
    action.setTimeoutSeconds(wTimeout.getText());
    action.setPollIntervalSeconds(wPoll.getText());
    action.setResultVariableJobId(wVarJobId.getText());
    action.setResultVariableRunId(wVarRunId.getText());
    action.setResultVariableStatus(wVarStatus.getText());
    action.setResultVariablePageUrl(wVarPageUrl.getText());
    action.setResultVariableError(wVarError.getText());
    dispose();
  }
}
