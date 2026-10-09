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

package org.apache.hop.workflow.actions.deleteexecutioninfo;

import org.apache.commons.lang3.StringUtils;
import org.apache.hop.core.Const;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.ui.core.dialog.BaseDialog;
import org.apache.hop.ui.core.gui.GuiCompositeWidgets;
import org.apache.hop.ui.workflow.action.ActionDialog;
import org.apache.hop.workflow.WorkflowMeta;
import org.apache.hop.workflow.action.IAction;
import org.eclipse.swt.widgets.Shell;

/** Dialog for {@link ActionDeleteExecutionInfo}. Fields come from the action annotations. */
public class ActionDeleteExecutionInfoDialog extends ActionDialog {
  private static final Class<?> PKG = ActionDeleteExecutionInfo.class;

  private ActionDeleteExecutionInfo action;
  private boolean changed;
  private GuiCompositeWidgets widgets;

  public ActionDeleteExecutionInfoDialog(
      Shell parent,
      ActionDeleteExecutionInfo action,
      WorkflowMeta workflowMeta,
      IVariables variables) {
    super(parent, workflowMeta, variables);
    this.action = action;
    if (this.action.getName() == null) {
      this.action.setName(BaseMessages.getString(PKG, "ActionDeleteExecutionInfo.Name"));
    }
  }

  @Override
  public IAction open() {
    createShell(BaseMessages.getString(PKG, "ActionDeleteExecutionInfo.Name"), action);
    changed = action.hasChanged();
    buildButtonBar().ok(e -> ok()).cancel(e -> cancel()).build();
    widgets =
        GuiCompositeWidgets.addScrolledComposite(
            shell,
            variables,
            wName,
            wOk,
            ActionDeleteExecutionInfo.GUI_PLUGIN_ELEMENT_PARENT_ID,
            action);
    // The name is not an annotated widget. createShell leaves the field empty.
    wName.setText(Const.NVL(action.getName(), ""));
    action.setChanged(changed);
    focusActionName();
    BaseDialog.defaultShellHandling(shell, c -> ok(), c -> cancel());
    return action;
  }

  private void ok() {
    if (StringUtils.isEmpty(wName.getText())) {
      return;
    }
    action.setName(wName.getText());
    widgets.getWidgetsContents(action, ActionDeleteExecutionInfo.GUI_PLUGIN_ELEMENT_PARENT_ID);
    action.setChanged();
    dispose();
  }

  private void cancel() {
    action.setChanged(changed);
    action = null;
    dispose();
  }
}
