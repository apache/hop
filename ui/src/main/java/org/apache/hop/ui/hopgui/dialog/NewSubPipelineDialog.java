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

package org.apache.hop.ui.hopgui.dialog;

import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.ui.core.FormDataBuilder;
import org.apache.hop.ui.core.PropsUi;
import org.apache.hop.ui.core.dialog.BaseDialog;
import org.apache.hop.ui.core.gui.GuiResource;
import org.apache.hop.ui.core.gui.WindowProperty;
import org.eclipse.swt.SWT;
import org.eclipse.swt.layout.FormAttachment;
import org.eclipse.swt.layout.FormData;
import org.eclipse.swt.layout.FormLayout;
import org.eclipse.swt.widgets.*;

/**
 * Shows a pop-up message dialog after the sub- pipeline creation.
 *
 * @author Aliaksandr Kastenka
 */
public class NewSubPipelineDialog extends Dialog {
  private static final Class<?> PKG = NewSubPipelineDialog.class; // For Translator

  private Button wShow;

  private Shell shell;
  private PropsUi props;
  boolean doNotShow = false;

  public NewSubPipelineDialog(Shell parent, int style) {
    super(parent, style);
    props = PropsUi.getInstance();
  }

  public boolean open() {
    Shell parent = getParent();
    Display display = parent.getDisplay();

    shell = new Shell(parent, SWT.DIALOG_TRIM | SWT.MIN | SWT.MAX);
    props.setLook(shell);

    FormLayout formLayout = new FormLayout();
    formLayout.marginWidth = 15;
    formLayout.marginHeight = 15;

    shell.setLayout(formLayout);
    shell.setText(BaseMessages.getString(PKG, "NewSubPipelineDialog.Title"));
    shell.setImage(GuiResource.getInstance().getImageHopUi());

    Label wiInfo = new Label(shell, SWT.NONE);
    wiInfo.setImage(display.getSystemImage(SWT.ICON_INFORMATION));

    props.setLook(wiInfo);
    FormData fdiInfo = new FormData();
    fdiInfo.left = new FormAttachment(0, 0);
    fdiInfo.top = new FormAttachment(0, 0);
    wiInfo.setLayoutData(fdiInfo);

    Label wlInfo = new Label(shell, SWT.WRAP);
    wlInfo.setText(BaseMessages.getString(PKG, "NewSubPipelineDialog.PipelineCreated"));

    props.setLook(wlInfo);
    FormData fdlInfo = new FormData();
    fdlInfo.left = new FormAttachment(wiInfo, 15);
    fdlInfo.right = new FormAttachment(100, 0);
    fdlInfo.width = 320;
    fdlInfo.top = new FormAttachment(wiInfo, 0, SWT.TOP);
    fdlInfo.bottom = new FormAttachment(wiInfo, 0, SWT.BOTTOM);
    wlInfo.setLayoutData(fdlInfo);

    wShow = new Button(shell, SWT.CHECK);
    wShow.setText(BaseMessages.getString(PKG, "NewSubPipelineDialog.DoNotShowAgain"));
    wShow.setLayoutData(new FormDataBuilder().left().result());
    props.setLook(wShow);
    FormData fdShowButton = new FormData();
    fdShowButton.left = new FormAttachment(wlInfo, 0, SWT.LEFT);
    fdShowButton.right = new FormAttachment(wlInfo, 0, SWT.RIGHT);
    fdShowButton.top = new FormAttachment(wlInfo, 15, SWT.BOTTOM);
    wShow.setLayoutData(fdShowButton);

    Button wOk = new Button(shell, SWT.PUSH);
    wOk.setText(BaseMessages.getString(PKG, "System.Button.OK"));

    FormData fdOk = new FormData();
    fdOk.right = new FormAttachment(100, 0);
    fdOk.top = new FormAttachment(wShow, 30, SWT.BOTTOM);
    wOk.setLayoutData(fdOk);
    wOk.addListener(SWT.Selection, e -> close());

    BaseDialog.defaultShellHandling(shell, c -> close(), c -> close());

    return doNotShow;
  }

  public void dispose() {
    props.setScreen(new WindowProperty(shell));
    shell.dispose();
  }

  private void close() {
    doNotShow = wShow.getSelection();
    dispose();
  }
}
