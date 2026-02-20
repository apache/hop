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
 *
 */

package org.apache.hop.documentation;

import org.apache.hop.core.Const;
import org.apache.hop.core.util.Utils;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.ui.core.PropsUi;
import org.apache.hop.ui.core.dialog.BaseDialog;
import org.apache.hop.ui.core.dialog.MessageBox;
import org.apache.hop.ui.core.gui.GuiCompositeWidgets;
import org.apache.hop.ui.workflow.action.ActionDialog;
import org.apache.hop.workflow.WorkflowMeta;
import org.apache.hop.workflow.action.IAction;
import org.eclipse.swt.SWT;
import org.eclipse.swt.custom.ScrolledComposite;
import org.eclipse.swt.layout.FillLayout;
import org.eclipse.swt.layout.FormAttachment;
import org.eclipse.swt.layout.FormData;
import org.eclipse.swt.layout.FormLayout;
import org.eclipse.swt.widgets.Composite;
import org.eclipse.swt.widgets.Shell;

/** This dialog allows you to edit a ActionDoc object. */
public class ActionDocDialog extends ActionDialog {
  private static final Class<?> PKG = ActionDoc.class;

  private GuiCompositeWidgets widgets;
  private Composite wWidgets;
  private ActionDoc action;

  public ActionDocDialog(
      Shell parent, ActionDoc action, WorkflowMeta workflowMeta, IVariables variables) {
    super(parent, workflowMeta, variables);
    this.action = action;
    if (this.action.getName() == null) {
      this.action.setName(BaseMessages.getString(PKG, "ActionDoc.Name.Default"));
    }
  }

  @Override
  public IAction open() {
    createShell(BaseMessages.getString(PKG, "ActionDoc.Title"), action);
    int margin = this.margin;
    int middle = this.middle;

    buildButtonBar().ok(e -> ok()).cancel(e -> cancel()).build();

    ScrolledComposite sc = new ScrolledComposite(shell, SWT.V_SCROLL | SWT.H_SCROLL);
    PropsUi.setLook(sc);
    FormData fdSc = new FormData();
    fdSc.left = new FormAttachment(0, margin);
    fdSc.top = new FormAttachment(wSpacer, margin);
    fdSc.right = new FormAttachment(100, -margin);
    fdSc.bottom = new FormAttachment(wCancel, -margin);
    sc.setLayoutData(fdSc);
    sc.setLayout(new FillLayout());
    sc.setExpandHorizontal(true);
    sc.setExpandVertical(true);

    Composite wContent = new Composite(sc, SWT.NONE);
    PropsUi.setLook(wContent);
    FormLayout contentLayout = new FormLayout();
    contentLayout.marginWidth = PropsUi.getFormMargin();
    contentLayout.marginHeight = PropsUi.getFormMargin();
    wContent.setLayout(contentLayout);

    wWidgets = new Composite(wContent, SWT.BACKGROUND);
    wWidgets.setLayout(new FormLayout());
    PropsUi.setLook(wWidgets);
    FormData fdWidgets = new FormData();
    fdWidgets.left = new FormAttachment(0, 0);
    fdWidgets.right = new FormAttachment(100, 0);
    fdWidgets.top = new FormAttachment(0, margin);
    fdWidgets.bottom = new FormAttachment(100, 0);
    wWidgets.setLayoutData(fdWidgets);

    widgets = new GuiCompositeWidgets(variables);
    widgets.createCompositeWidgets(action, null, wWidgets, ActionDoc.GUI_WIDGETS_PARENT_ID, null);

    sc.setContent(wContent);
    wContent.pack();
    sc.setMinSize(wContent.computeSize(SWT.DEFAULT, SWT.DEFAULT));

    getData();
    focusActionName();
    BaseDialog.defaultShellHandling(shell, c -> ok(), c -> cancel());

    return action;
  }

  /** Copy information from the meta-data input to the dialog fields. */
  public void getData() {
    wName.setText(Const.NVL(action.getName(), ""));

    widgets.setWidgetsContents(action, wWidgets, ActionDoc.GUI_WIDGETS_PARENT_ID);
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

    widgets.getWidgetsContents(action, ActionDoc.GUI_WIDGETS_PARENT_ID);

    action.setChanged();
    dispose();
  }
}
