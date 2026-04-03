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

package org.apache.hop.ui.workflow.actions.missing;

import java.util.List;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.ui.core.PropsUi;
import org.apache.hop.ui.core.dialog.BaseDialog;
import org.apache.hop.ui.core.gui.GuiResource;
import org.apache.hop.ui.hopgui.HopGui;
import org.apache.hop.ui.workflow.action.ActionDialog;
import org.apache.hop.workflow.WorkflowMeta;
import org.apache.hop.workflow.action.IAction;
import org.apache.hop.workflow.actions.missing.MissingAction;
import org.eclipse.swt.SWT;
import org.eclipse.swt.events.SelectionAdapter;
import org.eclipse.swt.events.SelectionEvent;
import org.eclipse.swt.graphics.Image;
import org.eclipse.swt.layout.FormAttachment;
import org.eclipse.swt.layout.FormData;
import org.eclipse.swt.layout.FormLayout;
import org.eclipse.swt.widgets.Button;
import org.eclipse.swt.widgets.Display;
import org.eclipse.swt.widgets.Label;
import org.eclipse.swt.widgets.Shell;

public class MissingActionDialog extends ActionDialog {

  private static final Class<?> PKG = MissingActionDialog.class;

  private List<MissingAction> missingActions;
  private int mode;

  /** A reference to the action interface */
  protected IAction action;

  public static final int MISSING_ACTIONS = 1;
  public static final int MISSING_ACTION_ID = 2;

  public MissingActionDialog(Shell parent, List<MissingAction> missingActions) {
    super(parent, null, HopGui.getInstance().getVariables());

    this.missingActions = missingActions;
    this.mode = MISSING_ACTIONS;
  }

  public MissingActionDialog(
      Shell parent, IAction action, WorkflowMeta workflowMeta, IVariables variables) {
    super(parent, workflowMeta, variables);
    this.action = action;
    this.mode = MISSING_ACTION_ID;
  }

  private static String formatMissingEntries(List<MissingAction> items) {
    StringBuilder entries = new StringBuilder();
    for (int i = 0; i < items.size(); i++) {
      MissingAction item = items.get(i);
      entries.append("- ").append(item.getName()).append(" - ").append(item.getMissingPluginId());
      if (i < items.size() - 1) {
        entries.append("\n");
      } else {
        entries.append("\n\n");
      }
    }
    return entries.toString();
  }

  private String buildMessage() {
    if (mode == MISSING_ACTIONS) {
      return BaseMessages.getString(
          PKG, "MissingActionDialog.MissingActions", formatMissingEntries(missingActions));
    }
    return BaseMessages.getString(
        PKG, "MissingActionDialog.MissingActionId", ((MissingAction) action).getMissingPluginId());
  }

  @Override
  public IAction open() {
    Shell parent = getParent();
    String message = buildMessage();
    boolean showOpenFile = mode == MISSING_ACTIONS;

    Display display = parent.getDisplay();
    Shell dialogShell =
        new Shell(parent, SWT.DIALOG_TRIM | SWT.CLOSE | SWT.ICON | SWT.APPLICATION_MODAL);
    this.shell = dialogShell;

    PropsUi.setLook(dialogShell);
    dialogShell.setImage(GuiResource.getInstance().getImageHopUi());

    FormLayout formLayout = new FormLayout();
    formLayout.marginWidth = PropsUi.getFormMargin();
    formLayout.marginLeft = PropsUi.getFormMargin();
    formLayout.marginHeight = PropsUi.getFormMargin();

    dialogShell.setText(BaseMessages.getString(PKG, "MissingActionDialog.MissingPlugins"));
    dialogShell.setLayout(formLayout);

    Label image = new Label(dialogShell, SWT.NONE);
    PropsUi.setLook(image);
    Image icon = display.getSystemImage(SWT.ICON_QUESTION);
    image.setImage(icon);
    FormData imageData = new FormData();
    imageData.left = new FormAttachment(0, 5);
    imageData.right = new FormAttachment(11, 0);
    imageData.top = new FormAttachment(0, 10);
    image.setLayoutData(imageData);

    Label error = new Label(dialogShell, SWT.WRAP);
    PropsUi.setLook(error);
    error.setText(message);
    FormData errorData = new FormData();
    errorData.left = new FormAttachment(image, 5);
    errorData.right = new FormAttachment(100, -5);
    errorData.top = new FormAttachment(0, 10);
    error.setLayoutData(errorData);

    Label separator = new Label(dialogShell, SWT.WRAP);
    PropsUi.setLook(separator);
    FormData separatorData = new FormData();
    separatorData.top = new FormAttachment(error, 10);
    separator.setLayoutData(separatorData);

    Runnable confirm =
        () -> {
          dialogShell.dispose();
          action = new MissingAction();
        };
    Runnable cancel =
        () -> {
          dialogShell.dispose();
          action = null;
        };

    Button closeButton = new Button(dialogShell, SWT.PUSH);
    PropsUi.setLook(closeButton);
    FormData fdClose = new FormData();
    fdClose.right = new FormAttachment(98);
    fdClose.top = new FormAttachment(separator);
    closeButton.setLayoutData(fdClose);
    closeButton.setText(BaseMessages.getString(PKG, "MissingActionDialog.Close"));
    closeButton.addSelectionListener(
        new SelectionAdapter() {
          @Override
          public void widgetSelected(SelectionEvent e) {
            cancel.run();
          }
        });

    if (showOpenFile) {
      Button openButton = new Button(dialogShell, SWT.PUSH);
      PropsUi.setLook(openButton);
      FormData fdOpen = new FormData();
      fdOpen.right = new FormAttachment(closeButton, -5);
      fdOpen.bottom = new FormAttachment(closeButton, 0, SWT.BOTTOM);
      openButton.setLayoutData(fdOpen);
      openButton.setText(BaseMessages.getString(PKG, "MissingActionDialog.OpenFile"));
      openButton.addSelectionListener(
          new SelectionAdapter() {
            @Override
            public void widgetSelected(SelectionEvent e) {
              confirm.run();
            }
          });
    }

    BaseDialog.defaultShellHandling(dialogShell, v -> confirm.run(), v -> cancel.run());
    return action;
  }
}
