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

package org.apache.hop.ui.workflow.actions.dummy;

import org.apache.hop.core.Const;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.ui.core.dialog.BaseDialog;
import org.apache.hop.ui.workflow.action.ActionDialog;
import org.apache.hop.workflow.WorkflowMeta;
import org.apache.hop.workflow.action.IAction;
import org.apache.hop.workflow.actions.dummy.ActionDummy;
import org.eclipse.swt.widgets.Shell;

public class ActionDummyDialog extends ActionDialog {
  private static final Class<?> PKG = ActionDummy.class;

  private ActionDummy action;

  private boolean backupChanged;

  public ActionDummyDialog(
      Shell parent, ActionDummy action, WorkflowMeta workflowMeta, IVariables variables) {
    super(parent, workflowMeta, variables);
    this.action = action;
  }

  @Override
  public IAction open() {
    createShell(BaseMessages.getString(PKG, "ActionDummyDialog.Title"), action);
    buildButtonBar().ok(e -> ok()).cancel(e -> cancel()).build();

    backupChanged = action.hasChanged();

    getData();
    focusActionName();
    BaseDialog.defaultShellHandling(shell, c -> ok(), c -> cancel());

    return action;
  }

  @Override
  protected void onActionNameModified() {
    action.setChanged();
  }

  public void getData() {
    wName.setText(Const.NVL(action.getName(), ""));
  }

  private void cancel() {
    action.setChanged(backupChanged);
    action = null;
    dispose();
  }

  private void ok() {
    action.setName(wName.getText());
    dispose();
  }
}
