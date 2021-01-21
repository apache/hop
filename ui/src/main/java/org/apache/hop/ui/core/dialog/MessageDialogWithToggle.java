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

package org.apache.hop.ui.core.dialog;

import org.apache.hop.core.Const;
import org.apache.hop.ui.core.PropsUi;
import org.apache.hop.ui.core.gui.GuiResource;
import org.apache.hop.ui.core.gui.WindowProperty;
import org.apache.hop.ui.pipeline.transform.BaseTransformDialog;
import org.eclipse.swt.SWT;
import org.eclipse.swt.graphics.Image;
import org.eclipse.swt.layout.FormAttachment;
import org.eclipse.swt.layout.FormData;
import org.eclipse.swt.layout.FormLayout;
import org.eclipse.swt.widgets.Button;
import org.eclipse.swt.widgets.Display;
import org.eclipse.swt.widgets.Label;
import org.eclipse.swt.widgets.Shell;

public class MessageDialogWithToggle {

  private static final int OK = 0;
  private static final int CANCEL = 1;

  private Shell parent;

  private String title;
  private String message;

  private String[] buttonLabels;

  private int dialogImageType;

  private String toggleLabel;

  private boolean toggleState;

  private Shell shell;
  private final PropsUi props;
  private int returnCode = OK;

  @Deprecated
  public MessageDialogWithToggle(
      Shell parent,
      String title,
      Image image,
      String message,
      int dialogImageType,
      String[] buttonLabels,
      int defaultButton,
      String toggleLabel,
      boolean toggleState) {
    this(parent, title, message, dialogImageType, buttonLabels, toggleLabel, toggleState);
  }

  public MessageDialogWithToggle(
      Shell parent,
      String title,
      String message,
      int dialogImageType,
      String[] buttonLabels,
      String toggleLabel,
      boolean toggleState) {
    this.parent = parent;
    this.title = title;
    this.message = message;
    this.dialogImageType = dialogImageType;
    this.buttonLabels = buttonLabels;
    this.toggleLabel = toggleLabel;
    this.toggleState = toggleState;

    props = PropsUi.getInstance();
  }

  public int open() {
    int zoomedMargin = (int) (Const.MARGIN * props.getZoomFactor());

    // Create the shell with the appropriate look and title...
    //
    shell = new Shell(parent, SWT.APPLICATION_MODAL | SWT.CLOSE);
    FormLayout shellLayout = new FormLayout();
    shellLayout.marginTop = zoomedMargin;
    shellLayout.marginLeft = zoomedMargin;
    shellLayout.marginRight = zoomedMargin;
    shellLayout.marginBottom = zoomedMargin;
    shell.setLayout(shellLayout);
    shell.setImage(GuiResource.getInstance().getImageHopUi());
    shell.setText(Const.NVL(title, ""));

    // An image at the right hand side...
    //
    final Image[] image = new Image[1];
    Display display = parent.getDisplay();
    display.syncExec(() -> image[0] = display.getSystemImage(dialogImageType));

    Label wlImage = new Label(shell, SWT.NONE);
    wlImage.setImage(image[0]);
    FormData fdImage = new FormData();
    fdImage.right = new FormAttachment(100, 0);
    fdImage.top = new FormAttachment(0, 0);
    wlImage.setLayoutData(fdImage);

    // Add the message label...
    //
    Label wlMessage = new Label(shell, SWT.WRAP);
    wlMessage.setText(Const.NVL(message, ""));
    FormData fdMessage = new FormData();
    fdMessage.left = new FormAttachment(0, 0);
    fdMessage.top = new FormAttachment(0, 0);
    fdMessage.right = new FormAttachment(wlImage, -zoomedMargin);
    wlMessage.setLayoutData(fdMessage);

    // Below these 2 we put the checkbox with a label and a default state...
    //
    Button wToggle = new Button(shell, SWT.CHECK | SWT.LEFT);
    wToggle.setText(toggleLabel);
    wToggle.setSelection(toggleState);
    FormData fdToggle = new FormData();
    fdToggle.left = new FormAttachment(0, 0);
    fdToggle.right = new FormAttachment(100, 0);
    fdToggle.top = new FormAttachment(wlMessage, 2 * zoomedMargin);
    wToggle.setLayoutData(fdToggle);
    // Keep the state sync'ed up...
    wToggle.addListener(SWT.Selection, e -> toggleState = wToggle.getSelection());

    // Finally we need to add a few buttons below the checkbox...
    //
    Button[] buttons = new Button[buttonLabels.length];
    for (int i = 0; i < buttons.length; i++) {
      final int index = i;
      buttons[i] = new Button(shell, SWT.PUSH);
      buttons[i].setText(buttonLabels[i]);
      buttons[i].addListener(
          SWT.Selection,
          e -> {
            returnCode = index;
            dispose();
          });
    }
    BaseTransformDialog.positionBottomButtons(shell, buttons, zoomedMargin, wToggle);

    BaseTransformDialog.setSize(shell);
    shell.pack();

    shell.open();
    while (!shell.isDisposed()) {
      if (!display.readAndDispatch()) {
        display.sleep();
      }
    }

    return returnCode;
  }

  private void cancel() {
    returnCode = CANCEL;
    dispose();
  }

  private void dispose() {
    props.setScreen(new WindowProperty(shell));
    shell.dispose();
  }

  /**
   * Gets toggleState
   *
   * @return value of toggleState
   */
  public boolean getToggleState() {
    return toggleState;
  }
}
