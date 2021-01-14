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

import org.apache.hop.core.Condition;
import org.apache.hop.core.Const;
import org.apache.hop.core.Props;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.ui.core.PropsUi;
import org.apache.hop.ui.core.gui.GuiResource;
import org.apache.hop.ui.core.gui.WindowProperty;
import org.apache.hop.ui.core.widget.ConditionEditor;
import org.apache.hop.ui.pipeline.transform.BaseTransformDialog;
import org.eclipse.swt.SWT;
import org.eclipse.swt.layout.FormAttachment;
import org.eclipse.swt.layout.FormData;
import org.eclipse.swt.layout.FormLayout;
import org.eclipse.swt.widgets.Button;
import org.eclipse.swt.widgets.Dialog;
import org.eclipse.swt.widgets.Display;
import org.eclipse.swt.widgets.Shell;

/**
 * This dialog allows you to enter a condition in a graphical way.
 *
 * @author Matt
 * @since 29-07-2004
 */
public class EnterConditionDialog extends Dialog {
  private static final Class<?> PKG = EnterConditionDialog.class; // For Translator

  private PropsUi props;

  private Shell shell;
  private ConditionEditor wCond;

  private Button wOk;
  private Button wCancel;

  private Condition condition;
  private IRowMeta fields;

  public EnterConditionDialog(Shell parent, int style, IRowMeta fields, Condition condition) {
    super(parent, style);
    this.props = PropsUi.getInstance();
    this.fields = fields;
    this.condition = condition;
  }

  public Condition open() {
    Shell parent = getParent();
    shell = new Shell(parent, SWT.DIALOG_TRIM | SWT.RESIZE | SWT.MAX | SWT.MIN);
    props.setLook(shell);
    shell.setText(BaseMessages.getString(PKG, "EnterConditionDialog.Title"));
    shell.setImage(GuiResource.getInstance().getImageHopUi());

    FormLayout formLayout = new FormLayout();
    formLayout.marginWidth = Const.FORM_MARGIN;
    formLayout.marginHeight = Const.FORM_MARGIN;

    shell.setLayout(formLayout);

    // Condition widget
    wCond = new ConditionEditor(shell, SWT.NONE, condition, fields);
    props.setLook(wCond, Props.WIDGET_STYLE_FIXED);

    if (!getData()) {
      return null;
    }

    // Buttons
    wOk = new Button(shell, SWT.PUSH);
    wOk.setText(BaseMessages.getString(PKG, "System.Button.OK"));

    wCancel = new Button(shell, SWT.PUSH);
    wCancel.setText(BaseMessages.getString(PKG, "System.Button.Cancel"));

    FormData fdCond = new FormData();

    int margin = props.getMargin() * 2;

    fdCond.left = new FormAttachment(0, 0); // To the right of the label
    fdCond.top = new FormAttachment(0, 0);
    fdCond.right = new FormAttachment(100, 0);
    fdCond.bottom = new FormAttachment(100, -50);
    wCond.setLayoutData(fdCond);

    BaseTransformDialog.positionBottomButtons(shell, new Button[] {wOk, wCancel}, margin, null);

    // Add listeners
    wCancel.addListener(
        SWT.Selection,
        e -> {
          condition = null;
          dispose();
        });

    wOk.addListener(SWT.Selection, e -> handleOK());

    BaseTransformDialog.setSize(shell);

    shell.open();
    Display display = parent.getDisplay();
    while (!shell.isDisposed()) {
      if (!display.readAndDispatch()) {
        display.sleep();
      }
    }
    return condition;
  }

  private boolean getData() {
    return true;
  }

  public void dispose() {
    props.setScreen(new WindowProperty(shell));
    shell.dispose();
  }

  public void handleOK() {
    if (wCond.getLevel() > 0) {
      wCond.goUp();
    } else {
      dispose();
    }
  }
}
