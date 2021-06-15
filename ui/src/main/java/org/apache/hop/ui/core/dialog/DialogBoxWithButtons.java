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
import org.apache.hop.ui.core.PropsUi;
import org.apache.hop.ui.core.gui.GuiResource;
import org.apache.hop.ui.core.gui.WindowProperty;
import org.apache.hop.ui.pipeline.transform.BaseTransformDialog;
import org.eclipse.swt.SWT;
import org.eclipse.swt.layout.FormAttachment;
import org.eclipse.swt.layout.FormData;
import org.eclipse.swt.layout.FormLayout;
import org.eclipse.swt.widgets.Button;
import org.eclipse.swt.widgets.Display;
import org.eclipse.swt.widgets.Label;
import org.eclipse.swt.widgets.Shell;

import java.util.ArrayList;
import java.util.concurrent.atomic.AtomicInteger;

public class DialogBoxWithButtons {

  private PropsUi props;
  private Shell shell;

  private final Shell parent;
  private final String title;
  private final String message;
  private final String[] buttonLabels;
  private final AtomicInteger choice;

  public DialogBoxWithButtons(Shell parent, String title, String message, String[] buttonLabels) {
    this.parent = parent;
    this.title = title;
    this.message = message;
    this.buttonLabels = buttonLabels;

    choice = new AtomicInteger();
  }

  public int open() {
    props = PropsUi.getInstance();
    shell = new Shell(parent, SWT.DIALOG_TRIM | SWT.CLOSE | SWT.RESIZE);
    shell.setText(Const.NVL(title, ""));
    shell.setImage(GuiResource.getInstance().getImageHopUi());
    FormLayout formLayout = new FormLayout();
    int formMargin = (int) (Const.FORM_MARGIN * props.getZoomFactor());
    formLayout.marginTop = formMargin;
    formLayout.marginLeft = formMargin;
    formLayout.marginBottom = formMargin;
    formLayout.marginRight = formMargin;
    shell.setLayout(formLayout);

    Label wLabel = new Label(shell, SWT.CENTER | SWT.WRAP);
    props.setLook(wLabel);
    wLabel.setText(Const.NVL(message, ""));
    FormData fdLabel = new FormData();
    fdLabel.left = new FormAttachment(0, 0);
    fdLabel.top = new FormAttachment(0, 0);
    fdLabel.right = new FormAttachment(100, 0);
    wLabel.setLayoutData(fdLabel);

    java.util.List<Button> buttons = new ArrayList<>();
    for (int i = 0; i < buttonLabels.length; i++) {
      String buttonLabel = buttonLabels[i];
      Button button = new Button(shell, SWT.PUSH);
      props.setLook(button);
      button.setText(Const.NVL(buttonLabel, ""));
      buttons.add(button);
      final int index = i;
      button.addListener(
          SWT.Selection,
          e -> {
            choice.set(index);
            dispose();
          });
    }
    BaseTransformDialog.positionBottomButtons(
        shell, buttons.toArray(new Button[0]), formMargin, wLabel);

    BaseTransformDialog.setSize(shell);
    shell.addListener(SWT.Close, e -> dispose());
    BaseTransformDialog.setSize(shell);

    shell.open();

    Display display = shell.getDisplay();
    while (!shell.isDisposed()) {
      if (!display.readAndDispatch()) {
        display.sleep();
      }
    }

    return choice.get();
  }

  private void close() {
    choice.set(0xFF);
  }

  private void dispose() {
    props.setScreen(new WindowProperty(shell));
    shell.dispose();
  }
}
