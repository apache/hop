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
package org.apache.hop.lint;

import org.apache.hop.core.Const;
import org.apache.hop.core.util.Utils;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.ui.core.dialog.BaseDialog;
import org.apache.hop.ui.core.dialog.MessageBox;
import org.apache.hop.ui.core.gui.GuiCompositeWidgets;
import org.apache.hop.ui.core.gui.GuiCompositeWidgetsAdapter;
import org.apache.hop.ui.workflow.action.ActionDialog;
import org.apache.hop.workflow.WorkflowMeta;
import org.apache.hop.workflow.action.IAction;
import org.eclipse.swt.SWT;
import org.eclipse.swt.widgets.Control;
import org.eclipse.swt.widgets.Shell;

/** The options of the Run Linter action, built from its annotated fields. */
public class ActionRunLinterDialog extends ActionDialog {
  private static final Class<?> PKG = ActionRunLinter.class;

  private ActionRunLinter action;
  private GuiCompositeWidgets widgets;

  public ActionRunLinterDialog(
      Shell parent, IAction action, WorkflowMeta workflowMeta, IVariables variables) {
    super(parent, workflowMeta, variables);
    this.action = (ActionRunLinter) action;
    if (this.action.getName() == null) {
      this.action.setName(BaseMessages.getString(PKG, "ActionRunLinter.Name"));
    }
  }

  @Override
  public IAction open() {
    createShell(BaseMessages.getString(PKG, "ActionRunLinterDialog.Title"), action);
    boolean changed = action.hasChanged();

    buildButtonBar().ok(e -> ok()).cancel(e -> cancel()).build();

    widgets =
        GuiCompositeWidgets.addScrolledComposite(
            shell, variables, wSpacer, wOk, ActionRunLinter.GUI_PLUGIN_ELEMENT_PARENT_ID, action);
    widgets.setWidgetsListener(
        new GuiCompositeWidgetsAdapter() {
          @Override
          public void widgetModified(
              GuiCompositeWidgets compositeWidgets, Control changedWidget, String widgetId) {
            action.setChanged();
          }
        });

    wName.setText(Const.NVL(action.getName(), ""));
    action.setChanged(changed);
    focusActionName();
    BaseDialog.defaultShellHandling(shell, c -> ok(), c -> cancel());

    return action;
  }

  @Override
  protected void onActionNameModified() {
    action.setChanged();
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
    widgets.getWidgetsContents(action, ActionRunLinter.GUI_PLUGIN_ELEMENT_PARENT_ID);
    action.setChanged();
    dispose();
  }
}
