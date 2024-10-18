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
import org.eclipse.swt.widgets.Control;
import org.eclipse.swt.widgets.Dialog;
import org.eclipse.swt.widgets.Label;
import org.eclipse.swt.widgets.Shell;
import org.eclipse.swt.widgets.Text;

/** This dialog allows you to enter a (single line) String. */
public class EnterUsernamePasswordDialog extends Dialog {
  private static final Class<?> PKG = EnterUsernamePasswordDialog.class;

  private Text wUsername;
  private Text wPassword;

  private Button wOk;

  private Shell shell;

  private PropsUi props;

  private boolean mandatory;

  String[] returnValues;

  /**
   * Constructs with the ability to use environmental variable substitution.
   *
   * @param parent Parent gui object
   */
  public EnterUsernamePasswordDialog(Shell parent) {
    super(parent, SWT.NONE);
    this.props = PropsUi.getInstance();
  }

  public String[] open() {
    Shell parent = getParent();
    Control lastControl;

    shell = new Shell(parent, SWT.DIALOG_TRIM | SWT.RESIZE | SWT.APPLICATION_MODAL | SWT.SHEET);
    PropsUi.setLook(shell);

    FormLayout formLayout = new FormLayout();
    formLayout.marginWidth = PropsUi.getFormMargin();
    formLayout.marginHeight = PropsUi.getFormMargin();

    shell.setLayout(formLayout);
    shell.setImage(GuiResource.getInstance().getImageHopUi());
    shell.setText("Username/Password");

    int middle = props.getMiddlePct();
    int margin = PropsUi.getMargin();

    // The Username line...
    Label wlUsername = new Label(shell, SWT.RIGHT);
    wlUsername.setText("Username:");
    PropsUi.setLook(wlUsername);
    FormData fdlUsername = new FormData();
    fdlUsername.left = new FormAttachment(0, 0);
    fdlUsername.top = new FormAttachment(0, 0);
    fdlUsername.right = new FormAttachment(middle, -margin);
    wlUsername.setLayoutData(fdlUsername);
    wUsername = new Text(shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    PropsUi.setLook(wUsername);

    FormData fdUsername = new FormData();
    fdUsername.left = new FormAttachment(wlUsername, 0);
    fdUsername.top = new FormAttachment(0, 0);
    fdUsername.right = new FormAttachment(100, -margin);
    wUsername.setLayoutData(fdUsername);

    // The Password line...
    Label wlPassword = new Label(shell, SWT.RIGHT);
    wlPassword.setText("Password:");
    PropsUi.setLook(wlPassword);
    FormData fdlPassword = new FormData();
    fdlPassword.left = new FormAttachment(0, 0);
    fdlPassword.top = new FormAttachment(wlUsername, margin * 2);
    fdlPassword.right = new FormAttachment(middle, -margin);
    wlPassword.setLayoutData(fdlPassword);
    wPassword = new Text(shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER | SWT.PASSWORD);
    PropsUi.setLook(wPassword);
    lastControl = wPassword;

    FormData fdPassword = new FormData();
    fdPassword.left = new FormAttachment(wlPassword, 0);
    fdPassword.top = new FormAttachment(wlUsername, margin * 2);
    fdPassword.right = new FormAttachment(100, -margin);
    wPassword.setLayoutData(fdPassword);

    // Some buttons
    wOk = new Button(shell, SWT.PUSH);
    wOk.setText(BaseMessages.getString(PKG, "System.Button.OK"));
    Button wCancel = new Button(shell, SWT.PUSH);
    wCancel.setText(BaseMessages.getString(PKG, "System.Button.Cancel"));

    BaseTransformDialog.positionBottomButtons(
        shell, new Button[] {wOk, wCancel}, margin, lastControl);

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

  /**
   * @return the mandatory
   */
  public boolean isMandatory() {
    return mandatory;
  }

  /**
   * @param mandatory the manditory to set
   */
  public void setMandatory(boolean mandatory) {
    this.mandatory = mandatory;
  }
}
