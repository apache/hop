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

package org.apache.hop.replay;

import org.apache.hop.core.Const;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.ui.core.dialog.BaseDialog;
import org.apache.hop.ui.core.gui.GuiCompositeWidgets;
import org.apache.hop.ui.workflow.action.ActionDialog;
import org.apache.hop.workflow.WorkflowMeta;
import org.apache.hop.workflow.action.ActionMeta;
import org.apache.hop.workflow.action.IAction;
import org.eclipse.swt.widgets.Shell;

public class ReplayGateDialog extends ActionDialog {
  private static final Class<?> PKG = ReplayGateDialog.class;

  private final ActionMeta actionMeta;
  private final ReplayGate input;
  private GuiCompositeWidgets widgets;
  private boolean accepted;

  public ReplayGateDialog(
      Shell parent,
      IVariables variables,
      WorkflowMeta workflowMeta,
      ActionMeta actionMeta,
      ReplayGate input) {
    super(parent, workflowMeta, variables);
    this.actionMeta = actionMeta;
    this.input = input;
  }

  public boolean openGate() {
    String actionName = actionMeta == null ? "" : actionMeta.getName();
    createShell(
        BaseMessages.getString(PKG, "ReplayGateDialog.Title", actionName),
        actionMeta == null ? null : actionMeta.getAction());
    wName.setText(Const.NVL(actionName, ""));
    wName.setEditable(false);

    buildButtonBar().ok(e -> ok()).cancel(e -> cancel()).build();

    widgets =
        GuiCompositeWidgets.addScrolledComposite(
            shell, variables, wName, wOk, ReplayGate.GUI_PLUGIN_ELEMENT_PARENT_ID, input);

    loading = false;
    BaseDialog.defaultShellHandling(shell, c -> ok(), c -> cancel());
    return accepted;
  }

  @Override
  public IAction open() {
    openGate();
    return null;
  }

  private void ok() {
    if (shell == null || shell.isDisposed()) {
      return;
    }
    if (widgets != null) {
      widgets.getWidgetsContents(input, ReplayGate.GUI_PLUGIN_ELEMENT_PARENT_ID);
    }
    accepted = true;
    dispose();
  }

  private void cancel() {
    if (shell == null || shell.isDisposed()) {
      return;
    }
    accepted = false;
    dispose();
  }
}
