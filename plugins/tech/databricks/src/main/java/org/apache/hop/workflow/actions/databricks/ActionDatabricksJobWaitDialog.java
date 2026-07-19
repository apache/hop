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
import org.eclipse.swt.events.ModifyListener;
import org.eclipse.swt.layout.FormAttachment;
import org.eclipse.swt.layout.FormData;
import org.eclipse.swt.layout.FormLayout;
import org.eclipse.swt.widgets.Button;
import org.eclipse.swt.widgets.Control;
import org.eclipse.swt.widgets.Label;
import org.eclipse.swt.widgets.Shell;
import org.eclipse.swt.widgets.Text;

/** Dialog for {@link ActionDatabricksJobWait}. */
public class ActionDatabricksJobWaitDialog extends ActionDialog {
  private static final Class<?> PKG = ActionDatabricksJobWait.class;

  private ActionDatabricksJobWait action;
  private boolean changed;

  private Text wName;
  private MetaSelectionLine<DatabricksConnection> wConnection;
  private TextVar wRunId;
  private TextVar wTimeout;
  private TextVar wPoll;
  private Button wCancelOnStop;
  private TextVar wVarJobId;
  private TextVar wVarRunId;
  private TextVar wVarStatus;
  private TextVar wVarPageUrl;
  private TextVar wVarError;

  public ActionDatabricksJobWaitDialog(
      Shell parent,
      ActionDatabricksJobWait action,
      WorkflowMeta workflowMeta,
      IVariables variables) {
    super(parent, workflowMeta, variables);
    this.action = action;
    if (this.action.getName() == null) {
      this.action.setName(BaseMessages.getString(PKG, "ActionDatabricksJobWait.Name.Default"));
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
    shell.setText(BaseMessages.getString(PKG, "ActionDatabricksJobWait.Title"));

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
    wlName.setText(BaseMessages.getString(PKG, "ActionDatabricksJobWait.Name.Label"));
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

    wConnection =
        new MetaSelectionLine<>(
            variables,
            metadataProvider,
            DatabricksConnection.class,
            shell,
            SWT.NONE,
            BaseMessages.getString(PKG, "ActionDatabricksJobWait.Connection.Label"),
            BaseMessages.getString(PKG, "ActionDatabricksJobWait.Connection.Tooltip"));
    FormData fdConn = new FormData();
    fdConn.left = new FormAttachment(0, 0);
    fdConn.top = new FormAttachment(wName, margin);
    fdConn.right = new FormAttachment(100, 0);
    wConnection.setLayoutData(fdConn);
    try {
      wConnection.fillItems();
    } catch (Exception ignored) {
      // empty metadata store
    }
    wConnection.addModifyListener(lsMod);

    Control last = wConnection;
    wRunId = addField(shell, last, middle, margin, lsMod, "RunId");
    last = wRunId;
    wTimeout = addField(shell, last, middle, margin, lsMod, "Timeout");
    last = wTimeout;
    wPoll = addField(shell, last, middle, margin, lsMod, "Poll");
    last = wPoll;

    wCancelOnStop = new Button(shell, SWT.CHECK);
    PropsUi.setLook(wCancelOnStop);
    wCancelOnStop.setText(
        BaseMessages.getString(PKG, "ActionDatabricksJobWait.CancelOnStop.Label"));
    FormData fdCancel = new FormData();
    fdCancel.left = new FormAttachment(middle, 0);
    fdCancel.top = new FormAttachment(last, margin);
    wCancelOnStop.setLayoutData(fdCancel);
    wCancelOnStop.addListener(SWT.Selection, e -> action.setChanged());
    last = wCancelOnStop;

    wVarJobId = addField(shell, last, middle, margin, lsMod, "VarJobId");
    last = wVarJobId;
    wVarRunId = addField(shell, last, middle, margin, lsMod, "VarRunId");
    last = wVarRunId;
    wVarStatus = addField(shell, last, middle, margin, lsMod, "VarStatus");
    last = wVarStatus;
    wVarPageUrl = addField(shell, last, middle, margin, lsMod, "VarPageUrl");
    last = wVarPageUrl;
    wVarError = addField(shell, last, middle, margin, lsMod, "VarError");

    getData();
    wName.setFocus();
    BaseDialog.defaultShellHandling(shell, c -> ok(), c -> cancel());
    return action;
  }

  private TextVar addField(
      org.eclipse.swt.widgets.Composite parent,
      Control above,
      int middle,
      int margin,
      ModifyListener lsMod,
      String key) {
    Label wl = new Label(parent, SWT.RIGHT);
    wl.setText(BaseMessages.getString(PKG, "ActionDatabricksJobWait." + key + ".Label"));
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
    return w;
  }

  private void getData() {
    wName.setText(Const.NVL(action.getName(), ""));
    wConnection.setText(Const.NVL(action.getConnectionName(), ""));
    wRunId.setText(Const.NVL(action.getRunId(), "${DatabricksRunId}"));
    wTimeout.setText(Const.NVL(action.getTimeoutSeconds(), "3600"));
    wPoll.setText(Const.NVL(action.getPollIntervalSeconds(), "15"));
    wCancelOnStop.setSelection(action.isCancelOnWorkflowStop());
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
    action.setRunId(wRunId.getText());
    action.setTimeoutSeconds(wTimeout.getText());
    action.setPollIntervalSeconds(wPoll.getText());
    action.setCancelOnWorkflowStop(wCancelOnStop.getSelection());
    action.setResultVariableJobId(wVarJobId.getText());
    action.setResultVariableRunId(wVarRunId.getText());
    action.setResultVariableStatus(wVarStatus.getText());
    action.setResultVariablePageUrl(wVarPageUrl.getText());
    action.setResultVariableError(wVarError.getText());
    dispose();
  }
}
