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

package org.apache.hop.ui.core.dialog;

import org.apache.hop.core.Const;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.ui.core.PropsUi;
import org.apache.hop.ui.core.gui.GuiResource;
import org.apache.hop.ui.core.gui.WindowProperty;
import org.apache.hop.ui.pipeline.transform.BaseTransformDialog;
import org.eclipse.swt.SWT;
import org.eclipse.swt.events.SelectionAdapter;
import org.eclipse.swt.events.SelectionEvent;
import org.eclipse.swt.events.ShellAdapter;
import org.eclipse.swt.events.ShellEvent;
import org.eclipse.swt.layout.FormAttachment;
import org.eclipse.swt.layout.FormData;
import org.eclipse.swt.layout.FormLayout;
import org.eclipse.swt.widgets.Button;
import org.eclipse.swt.widgets.Dialog;
import org.eclipse.swt.widgets.Display;
import org.eclipse.swt.widgets.Label;
import org.eclipse.swt.widgets.Listener;
import org.eclipse.swt.widgets.Shell;
import org.eclipse.swt.widgets.Text;

/**
 * A dialog that asks for a password.
 *
 * @author Matt
 * @since 19-06-2003
 */
public class EnterPasswordDialog extends Dialog {
  private static final Class<?> PKG = EnterPasswordDialog.class; // For Translator

  private String title, message;

  private Label wlDesc;
  private Text wDesc;
  private FormData fdlDesc, fdDesc;
  private Button wOk, wCancel;
  private FormData fdOk, fdCancel;
  private Listener lsOk, lsCancel;

  private Shell shell;
  private SelectionAdapter lsDef;
  private PropsUi props;

  private String description;
  private boolean readonly, modal;

  /** @deprecated Use CT without the <i>props</i> parameter (at 2nd position) */
  @Deprecated
  public EnterPasswordDialog(
      Shell parent, PropsUi props, String title, String message, String description) {
    super(parent, SWT.NONE);
    this.props = props;
    this.title = title;
    this.message = message;
    this.description = description;
    this.readonly = false;
  }

  public EnterPasswordDialog(Shell parent, String title, String message, String description) {
    super(parent, SWT.NONE);
    this.props = PropsUi.getInstance();
    this.title = title;
    this.message = message;
    this.description = description;
    this.readonly = false;
  }

  public void setReadOnly() {
    readonly = true;
  }

  public void setModal() {
    modal = true;
  }

  public String open() {
    Shell parent = getParent();
    Display display = parent.getDisplay();

    shell =
        new Shell(
            parent,
            SWT.DIALOG_TRIM
                | SWT.RESIZE
                | SWT.MAX
                | SWT.MIN
                | (modal ? SWT.APPLICATION_MODAL | SWT.SHEET : SWT.NONE));
    props.setLook(shell);
    shell.setImage(GuiResource.getInstance().getImageHopUi());

    FormLayout formLayout = new FormLayout();
    formLayout.marginWidth = Const.FORM_MARGIN;
    formLayout.marginHeight = Const.FORM_MARGIN;

    shell.setLayout(formLayout);
    shell.setText(title);

    int margin = props.getMargin();

    // From transform line
    wlDesc = new Label(shell, SWT.NONE);
    wlDesc.setText(message);
    props.setLook(wlDesc);
    fdlDesc = new FormData();
    fdlDesc.left = new FormAttachment(0, 0);
    fdlDesc.top = new FormAttachment(0, margin);
    wlDesc.setLayoutData(fdlDesc);
    wDesc = new Text(shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER | SWT.PASSWORD);
    wDesc.setText("");
    props.setLook(wDesc);
    fdDesc = new FormData();
    fdDesc.left = new FormAttachment(0, 0);
    fdDesc.top = new FormAttachment(wlDesc, margin);
    fdDesc.right = new FormAttachment(100, 0);
    fdDesc.bottom = new FormAttachment(100, -50);
    wDesc.setLayoutData(fdDesc);
    wDesc.setEditable(!readonly);

    // Some buttons
    if (!readonly) {
      wOk = new Button(shell, SWT.PUSH);
      wOk.setText(BaseMessages.getString(PKG, "System.Button.OK"));
      wCancel = new Button(shell, SWT.PUSH);
      wCancel.setText(BaseMessages.getString(PKG, "System.Button.Cancel"));
      fdOk = new FormData();
      fdOk.left = new FormAttachment(33, 0);
      fdOk.bottom = new FormAttachment(100, 0);
      wOk.setLayoutData(fdOk);
      fdCancel = new FormData();
      fdCancel.left = new FormAttachment(66, 0);
      fdCancel.bottom = new FormAttachment(100, 0);
      wCancel.setLayoutData(fdCancel);

      // Add listeners
      lsCancel = e -> cancel();
      lsOk = e -> ok();

      wOk.addListener(SWT.Selection, lsOk);
      wCancel.addListener(SWT.Selection, lsCancel);
    } else {
      wOk = new Button(shell, SWT.PUSH);
      wOk.setText(BaseMessages.getString(PKG, "System.Button.Close"));
      fdOk = new FormData();
      fdOk.left = new FormAttachment(50, 0);
      fdOk.bottom = new FormAttachment(100, 0);
      wOk.setLayoutData(fdOk);

      // Add listeners
      lsOk = e -> ok();
      wOk.addListener(SWT.Selection, lsOk);
    }

    lsDef =
        new SelectionAdapter() {
          public void widgetDefaultSelected(SelectionEvent e) {
            ok();
          }
        };
    wDesc.addSelectionListener(lsDef);

    // Detect [X] or ALT-F4 or something that kills this window...
    shell.addShellListener(
        new ShellAdapter() {
          public void shellClosed(ShellEvent e) {
            cancel();
          }
        });

    getData();

    BaseTransformDialog.setSize(shell);

    shell.open();
    while (!shell.isDisposed()) {
      if (!display.readAndDispatch()) {
        display.sleep();
      }
    }
    return description;
  }

  public void dispose() {
    props.setScreen(new WindowProperty(shell));
    shell.dispose();
  }

  public void getData() {
    if (description != null) {
      wDesc.setText(description);
    }
  }

  private void cancel() {
    description = null;
    dispose();
  }

  private void ok() {
    description = wDesc.getText();
    dispose();
  }
}
