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
import lombok.Getter;
import lombok.Setter;
import org.apache.hop.core.Const;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.ui.core.FormDataBuilder;
import org.apache.hop.ui.core.PropsUi;
import org.apache.hop.ui.core.gui.GuiResource;
import org.apache.hop.ui.core.gui.WindowProperty;
import org.apache.hop.ui.pipeline.transform.BaseTransformDialog;
import org.eclipse.swt.SWT;
import org.eclipse.swt.graphics.Image;
import org.eclipse.swt.layout.FormAttachment;
import org.eclipse.swt.layout.FormData;
import org.eclipse.swt.layout.FormLayout;
import org.eclipse.swt.layout.GridData;
import org.eclipse.swt.layout.GridLayout;
import org.eclipse.swt.widgets.Button;
import org.eclipse.swt.widgets.Composite;
import org.eclipse.swt.widgets.Dialog;
import org.eclipse.swt.widgets.Label;
import org.eclipse.swt.widgets.Shell;

/**
 * A replacement of the system message box dialog to make sure the correct font and colors are used.
 */
@Getter
@Setter
public class MessageBox extends Dialog {
  private static final Class<?> PKG = MessageBox.class;

  private final PropsUi props;

  private final int style;
  private String text;
  private String message;

  private int returnValue;

  private Shell shell;

  private int minimumWidth = -1;
  private int minimumHeight = -1;

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

    int shellStyle = style & (SWT.APPLICATION_MODAL | SWT.SYSTEM_MODAL | SWT.PRIMARY_MODAL);
    shell = new Shell(parent, SWT.DIALOG_TRIM | SWT.RESIZE | SWT.MAX | SWT.MIN | shellStyle);
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

    Composite composite = new Composite(shell, SWT.NONE);
    PropsUi.setLook(composite);
    composite.setLayout(new GridLayout());
    GridLayout gridLayout = new GridLayout();
    gridLayout.numColumns = 2;
    gridLayout.horizontalSpacing = 15;
    composite.setLayout(gridLayout);

    // The message...
    //
    Label wMessage = new Label(composite, SWT.LEFT | SWT.WRAP);
    PropsUi.setLook(wMessage);
    wMessage.setText(message);
    wMessage.setLayoutData(new GridData(GridData.FILL_BOTH));

    // Optional image to the right
    //
    Image iconImage = getIconImage(style);
    if (iconImage != null) {
      Label wImage = new Label(composite, SWT.NONE);
      wImage.setImage(iconImage);
      wImage.setLayoutData(new GridData());
    }

    // Buttons at the bottom
    //
    List<Button> buttons = new ArrayList<>();
    if ((style & SWT.YES) != 0) {
      Button wYes = new Button(shell, SWT.PUSH);
      wYes.setText(BaseMessages.getString(PKG, "System.Button.Yes"));
      wYes.addListener(SWT.Selection, e -> yes());
      shell.setDefaultButton(wYes);
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
      shell.setDefaultButton(wOk);
      buttons.add(0, wOk);
    }

    // Set the composite to fill from top to bottom with margin
    composite.setLayoutData(
        new FormDataBuilder().top().left().right(100, 0).bottom(100, -50).result());

    BaseTransformDialog.positionBottomButtons(
        shell, buttons.toArray(new Button[0]), margin, composite);

    // Reposition buttons to be attached to the bottom of the shell instead of the composite
    // This ensures they stay at the bottom when the shell is resized
    if (!buttons.isEmpty()) {
      for (Button button : buttons) {
        FormData fd = (FormData) button.getLayoutData();
        // Keep horizontal positioning but attach to bottom of shell
        fd.top = null;
        fd.bottom = new FormAttachment(100, -margin);
      }
      // Update composite to fill space above buttons
      composite.setLayoutData(
          new FormDataBuilder()
              .top()
              .left()
              .right(100, 0)
              .bottom(buttons.get(0), -margin)
              .result());
    }

    shell.addListener(SWT.Close, e -> cancel());

    BaseTransformDialog.setSize(shell);

    // If minimum size is set, use it directly instead of packing
    if (minimumWidth > 0 || minimumHeight > 0) {
      shell.layout();
      shell.pack();
      int width = Math.max(shell.getSize().x, minimumWidth > 0 ? minimumWidth : 0);
      int height = Math.max(shell.getSize().y, minimumHeight > 0 ? minimumHeight : 0);
      shell.setSize(width, height);
      shell.setMinimumSize(
          minimumWidth > 0 ? minimumWidth : 0, minimumHeight > 0 ? minimumHeight : 0);
    } else {
      shell.pack();
    }

    shell.open();
    while (!shell.isDisposed()) {
      if (!shell.getDisplay().readAndDispatch()) {
        shell.getDisplay().sleep();
      }
    }

    return returnValue;
  }

  private Image getIconImage(int style) {
    if ((style & SWT.ICON_INFORMATION) != 0) {
      return shell.getDisplay().getSystemImage(SWT.ICON_INFORMATION);
    } else if ((style & SWT.ICON_ERROR) != 0) {
      return shell.getDisplay().getSystemImage(SWT.ICON_ERROR);
    } else if ((style & SWT.ICON_QUESTION) != 0) {
      return shell.getDisplay().getSystemImage(SWT.ICON_QUESTION);
    } else if ((style & SWT.ICON_WARNING) != 0) {
      return shell.getDisplay().getSystemImage(SWT.ICON_WARNING);
    } else if ((style & SWT.ICON_CANCEL) != 0) {
      return shell.getDisplay().getSystemImage(SWT.ICON_CANCEL);
    } else if ((style & SWT.ICON_SEARCH) != 0) {
      return shell.getDisplay().getSystemImage(SWT.ICON_SEARCH);
    } else if ((style & SWT.ICON_WORKING) != 0) {
      return shell.getDisplay().getSystemImage(SWT.ICON_WORKING);
    }
    return null;
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
   * Sets the minimum size for the dialog
   *
   * @param width minimum width in pixels
   * @param height minimum height in pixels
   */
  public void setMinimumSize(int width, int height) {
    this.minimumWidth = width;
    this.minimumHeight = height;
  }
}
