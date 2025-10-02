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

import java.util.ArrayList;
import java.util.List;
import org.apache.hop.core.Const;
import org.apache.hop.i18n.BaseMessages;
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
import org.eclipse.swt.widgets.Dialog;
import org.eclipse.swt.widgets.Label;
import org.eclipse.swt.widgets.Shell;

/**
 * A replacement of the system message box dialog to make sure the correct font and colors are used.
 */
public class MessageBox extends Dialog {
  private static final Class<?> PKG = MessageBox.class;

  private final PropsUi props;

  private final int style;
  private String text;
  private String message;

  private int returnValue;

  private Shell shell;

  public MessageBox(Shell parent) {
    this(parent, SWT.ICON_INFORMATION | SWT.APPLICATION_MODAL);
  }

  public MessageBox(Shell parent, int style) {
    super(parent, style);
    this.style = style;
    this.props = PropsUi.getInstance();
    this.returnValue = 0;
  }

  public int open() {
    Shell parent = getParent();
    shell = new Shell(parent, SWT.DIALOG_TRIM | SWT.RESIZE | SWT.MAX | SWT.MIN | style);
    PropsUi.setLook(shell);
    shell.setImage(GuiResource.getInstance().getImageHop());
    shell.setText(Const.NVL(text, ""));

    FormLayout layout = new FormLayout();
    layout.marginLeft = PropsUi.getFormMargin();
    layout.marginRight = PropsUi.getFormMargin();
    layout.marginTop = PropsUi.getFormMargin();
    layout.marginBottom = PropsUi.getFormMargin();
    shell.setLayout(layout);

    int margin = PropsUi.getMargin();

    // Optional to the right
    //
    Image iconImage = null;
    if ((style & SWT.ICON_INFORMATION) != 0) {
      iconImage = shell.getDisplay().getSystemImage(SWT.ICON_INFORMATION);
    } else if ((style & SWT.ICON_ERROR) != 0) {
      iconImage = shell.getDisplay().getSystemImage(SWT.ICON_ERROR);
    } else if ((style & SWT.ICON_QUESTION) != 0) {
      iconImage = shell.getDisplay().getSystemImage(SWT.ICON_QUESTION);
    } else if ((style & SWT.ICON_WARNING) != 0) {
      iconImage = shell.getDisplay().getSystemImage(SWT.ICON_WARNING);
    } else if ((style & SWT.ICON_CANCEL) != 0) {
      iconImage = shell.getDisplay().getSystemImage(SWT.ICON_CANCEL);
    } else if ((style & SWT.ICON_SEARCH) != 0) {
      iconImage = shell.getDisplay().getSystemImage(SWT.ICON_SEARCH);
    } else if ((style & SWT.ICON_WORKING) != 0) {
      iconImage = shell.getDisplay().getSystemImage(SWT.ICON_WORKING);
    }

    Label wImage = null;
    if (iconImage != null) {
      wImage = new Label(shell, SWT.NONE);
      wImage.setImage(iconImage);

      FormData fdImage = new FormData();
      fdImage.right = new FormAttachment(100, -margin);
      fdImage.top = new FormAttachment(100, margin);
      wImage.setLayoutData(fdImage);
    }

    // The message...
    //
    Label wMessage = new Label(shell, SWT.LEFT | SWT.WRAP);
    PropsUi.setLook(wMessage);
    wMessage.setText(message);
    FormData fdMessage = new FormData();
    fdMessage.left = new FormAttachment(0, 0);
    if (wImage != null) {
      fdMessage.right = new FormAttachment(wImage, -margin);
    } else {
      fdMessage.right = new FormAttachment(100, 0);
    }
    fdMessage.top = new FormAttachment(0, 0);
    wMessage.setLayoutData(fdMessage);

    // Buttons at the bottom
    //
    List<Button> buttons = new ArrayList<>();
    if ((style & SWT.YES) != 0) {
      Button wYes = new Button(shell, SWT.PUSH);
      wYes.setText(BaseMessages.getString(PKG, "System.Button.Yes"));
      wYes.addListener(SWT.Selection, e -> yes());
      buttons.add(wYes);
    }
    if ((style & SWT.NO) != 0) {
      Button wNo = new Button(shell, SWT.PUSH);
      wNo.setText(BaseMessages.getString(PKG, "System.Button.No"));
      wNo.addListener(SWT.Selection, e -> no());
      buttons.add(wNo);
    }
    if ((style & SWT.CANCEL) != 0) {
      Button wCancel = new Button(shell, SWT.PUSH);
      wCancel.setText(BaseMessages.getString(PKG, "System.Button.Cancel"));
      wCancel.addListener(SWT.Selection, e -> cancel());
      buttons.add(wCancel);
    }
    if ((style & SWT.OK) != 0 || buttons.isEmpty()) {
      Button wOk = new Button(shell, SWT.PUSH);
      wOk.setText(BaseMessages.getString(PKG, "System.Button.OK"));
      wOk.addListener(SWT.Selection, e -> ok());
      buttons.add(0, wOk);
    }

    BaseTransformDialog.positionBottomButtons(
        shell, buttons.toArray(new Button[0]), margin, wMessage);

    shell.addListener(SWT.Close, e -> cancel());

    BaseTransformDialog.setSize(shell);

    shell.pack();
    shell.open();
    while (!shell.isDisposed()) {
      if (!shell.getDisplay().readAndDispatch()) {
        shell.getDisplay().sleep();
      }
    }

    return returnValue;
  }

  public void dispose() {
    props.setScreen(new WindowProperty(shell));
    shell.dispose();
  }

  public void ok() {
    returnValue = SWT.OK;
    dispose();
  }

  public void cancel() {
    returnValue = SWT.CANCEL;
    dispose();
  }

  public void yes() {
    returnValue = SWT.YES;
    dispose();
  }

  public void no() {
    returnValue = SWT.NO;
    dispose();
  }

  /**
   * Gets style
   *
   * @return value of style
   */
  @Override
  public int getStyle() {
    return style;
  }

  /**
   * Gets text
   *
   * @return value of text
   */
  @Override
  public String getText() {
    return text;
  }

  /**
   * Sets text
   *
   * @param text value of text
   */
  @Override
  public void setText(String text) {
    this.text = text;
  }

  /**
   * Gets message
   *
   * @return value of message
   */
  public String getMessage() {
    return message;
  }

  /**
   * Sets message
   *
   * @param message value of message
   */
  public void setMessage(String message) {
    this.message = message;
  }

  /**
   * Gets returnValue
   *
   * @return value of returnValue
   */
  public int getReturnValue() {
    return returnValue;
  }

  /**
   * Sets returnValue
   *
   * @param returnValue value of returnValue
   */
  public void setReturnValue(int returnValue) {
    this.returnValue = returnValue;
  }
}
