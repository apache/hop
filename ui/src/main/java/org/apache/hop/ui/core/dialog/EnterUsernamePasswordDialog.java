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

package org.apache.hop.ui.core.dialog;

import lombok.Getter;
import lombok.Setter;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.ui.core.PropsUi;
import org.apache.hop.ui.core.gui.GuiResource;
import org.apache.hop.ui.core.gui.WindowProperty;
import org.apache.hop.ui.pipeline.transform.BaseTransformDialog;
import org.eclipse.swt.SWT;
import org.eclipse.swt.layout.FormAttachment;
import org.eclipse.swt.layout.FormData;
import org.eclipse.swt.layout.FormLayout;
import org.eclipse.swt.widgets.Button;
import org.eclipse.swt.widgets.Dialog;
import org.eclipse.swt.widgets.Label;
import org.eclipse.swt.widgets.Shell;
import org.eclipse.swt.widgets.Text;

/**
 * Asks for a username and a password, for example to authenticate to a remote service. The password
 * is masked while it is typed. {@link #open()} returns both, or null if the dialog is canceled.
 */
public class EnterUsernamePasswordDialog extends Dialog {
  private static final Class<?> PKG = EnterUsernamePasswordDialog.class;

  private Text wUsername;
  private Text wPassword;

  private Shell shell;

  private final PropsUi props;

  @Getter @Setter private boolean mandatory;

  String[] returnValues;

  /**
   * Creates the dialog. It opens with {@link #open()}.
   *
   * @param parent the parent shell
   */
  public EnterUsernamePasswordDialog(Shell parent) {
    super(parent, SWT.NONE);
    this.props = PropsUi.getInstance();
  }

  public String[] open() {
    Shell parent = getParent();

    shell = new Shell(parent, SWT.DIALOG_TRIM | SWT.RESIZE | SWT.APPLICATION_MODAL | SWT.SHEET);
    PropsUi.setLook(shell);

    FormLayout formLayout = new FormLayout();
    formLayout.marginWidth = PropsUi.getFormMargin();
    formLayout.marginHeight = PropsUi.getFormMargin();

    shell.setLayout(formLayout);
    shell.setImage(GuiResource.getInstance().getImageHopUi());
    shell.setText(BaseMessages.getString(PKG, "EnterUsernamePasswordDialog.Title"));

    int middle = props.getMiddlePct();
    int margin = PropsUi.getMargin();

    // The Username line...
    Label wlUsername = new Label(shell, SWT.RIGHT);
    wlUsername.setText(BaseMessages.getString(PKG, "EnterUsernamePasswordDialog.Username.Label"));
    PropsUi.setLook(wlUsername);
    FormData fdlUsername = new FormData();
    fdlUsername.left = new FormAttachment(0, 0);
    fdlUsername.top = new FormAttachment(0, 0);
    fdlUsername.right = new FormAttachment(middle, -margin);
    wlUsername.setLayoutData(fdlUsername);
    wUsername = new Text(shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    PropsUi.setLook(wUsername);

    FormData fdUsername = new FormData();
    fdUsername.left = new FormAttachment(wlUsername, margin);
    fdUsername.top = new FormAttachment(0, 0);
    fdUsername.right = new FormAttachment(100, 0);
    wUsername.setLayoutData(fdUsername);

    // The Password line...
    Label wlPassword = new Label(shell, SWT.RIGHT);
    wlPassword.setText(BaseMessages.getString(PKG, "EnterUsernamePasswordDialog.Password.Label"));
    PropsUi.setLook(wlPassword);
    FormData fdlPassword = new FormData();
    fdlPassword.left = new FormAttachment(0, 0);
    fdlPassword.top = new FormAttachment(wlUsername, margin * 2);
    fdlPassword.right = new FormAttachment(middle, -margin);
    wlPassword.setLayoutData(fdlPassword);
    wPassword = new Text(shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER | SWT.PASSWORD);
    PropsUi.setLook(wPassword);

    FormData fdPassword = new FormData();
    fdPassword.left = new FormAttachment(wlPassword, margin);
    fdPassword.top = new FormAttachment(wlUsername, margin * 2);
    fdPassword.right = new FormAttachment(100, 0);
    wPassword.setLayoutData(fdPassword);

    // Some buttons
    Button wOk = new Button(shell, SWT.PUSH);
    wOk.setText(BaseMessages.getString(PKG, "System.Button.OK"));
    Button wCancel = new Button(shell, SWT.PUSH);
    wCancel.setText(BaseMessages.getString(PKG, "System.Button.Cancel"));

    BaseTransformDialog.positionBottomButtons(shell, new Button[] {wOk, wCancel}, margin, null);

    // Add listeners
    wOk.addListener(SWT.Selection, e -> ok());
    wCancel.addListener(SWT.Selection, e -> cancel());

    BaseDialog.defaultShellHandling(shell, c -> ok(), c -> cancel());

    return returnValues;
  }

  public void dispose() {
    props.setScreen(new WindowProperty(shell));
    shell.dispose();
  }

  private void cancel() {
    dispose();
  }

  private void ok() {
    returnValues = new String[] {wUsername.getText(), wPassword.getText()};
    dispose();
  }
}
