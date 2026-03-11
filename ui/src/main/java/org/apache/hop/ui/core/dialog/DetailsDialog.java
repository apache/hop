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

import org.apache.hop.core.Const;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.ui.core.PropsUi;
import org.apache.hop.ui.core.gui.WindowProperty;
import org.apache.hop.ui.pipeline.transform.BaseTransformDialog;
import org.eclipse.swt.SWT;
import org.eclipse.swt.graphics.Image;
import org.eclipse.swt.layout.FormAttachment;
import org.eclipse.swt.layout.FormData;
import org.eclipse.swt.layout.FormLayout;
import org.eclipse.swt.widgets.Button;
import org.eclipse.swt.widgets.Label;
import org.eclipse.swt.widgets.Shell;
import org.eclipse.swt.widgets.Text;

public class DetailsDialog {
  private Shell shell;

  private final String title;
  private final String message;
  private final Shell parent;
  private final Image titleImage;
  private final String details;

  public DetailsDialog(
      Shell parentShell,
      String dialogTitle,
      Image dialogTitleImage,
      String dialogMessage,
      String details) {
    this.title = dialogTitle;
    this.message = dialogMessage;
    this.parent = parentShell;
    this.titleImage = dialogTitleImage;
    this.details = details;
  }

  public void open() {
    shell = new Shell(parent, SWT.DIALOG_TRIM | SWT.RESIZE);
    shell.setImage(titleImage);
    shell.setText(Const.NVL(title, ""));
    PropsUi.setLook(shell);

    FormLayout layout = new FormLayout();
    layout.marginLeft = PropsUi.getFormMargin();
    layout.marginRight = PropsUi.getFormMargin();
    layout.marginTop = PropsUi.getFormMargin();
    layout.marginBottom = PropsUi.getFormMargin();
    shell.setLayout(layout);
    int margin = PropsUi.getMargin();

    // Buttons first so content can reference their position
    Button wClose = new Button(shell, SWT.PUSH);
    PropsUi.setLook(wClose);
    wClose.setText(BaseMessages.getString("System.Button.Close"));
    wClose.addListener(SWT.Selection, e -> close());
    BaseTransformDialog.positionBottomButtons(shell, new Button[] {wClose}, margin, null);

    Label wLabel = new Label(shell, SWT.LEFT | SWT.WRAP);
    PropsUi.setLook(wLabel);
    wLabel.setText(Const.NVL(message, ""));
    FormData fdLabel = new FormData();
    fdLabel.left = new FormAttachment(0, 0);
    fdLabel.top = new FormAttachment(0, 0);
    fdLabel.right = new FormAttachment(100, 0);
    wLabel.setLayoutData(fdLabel);

    Text wDetailsText = new Text(shell, SWT.MULTI | SWT.H_SCROLL | SWT.V_SCROLL);
    PropsUi.setLook(wDetailsText);
    String safeDetails = Const.NVL(details, "");
    wDetailsText.setText(safeDetails);
    wDetailsText.setSelection(safeDetails.length());
    FormData fdDetails = new FormData();
    fdDetails.left = new FormAttachment(0, 0);
    fdDetails.right = new FormAttachment(100, 0);
    fdDetails.top = new FormAttachment(wLabel, margin);
    fdDetails.bottom = new FormAttachment(wClose, -margin);
    wDetailsText.setLayoutData(fdDetails);

    shell.setDefaultButton(wClose);
    BaseDialog.defaultShellHandling(shell, c -> close(), c -> close());
  }

  private void close() {
    PropsUi.getInstance().setScreen(new WindowProperty(shell));
    shell.dispose();
  }
}
