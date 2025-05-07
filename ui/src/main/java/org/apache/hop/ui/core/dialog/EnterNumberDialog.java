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

import org.apache.commons.lang.StringUtils;
import org.apache.hop.core.Const;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.ui.core.ConstUi;
import org.apache.hop.ui.core.FormDataBuilder;
import org.apache.hop.ui.core.PropsUi;
import org.apache.hop.ui.core.gui.GuiResource;
import org.apache.hop.ui.core.gui.WindowProperty;
import org.eclipse.swt.SWT;
import org.eclipse.swt.events.MouseAdapter;
import org.eclipse.swt.events.MouseEvent;
import org.eclipse.swt.layout.FormAttachment;
import org.eclipse.swt.layout.FormData;
import org.eclipse.swt.layout.FormLayout;
import org.eclipse.swt.widgets.Button;
import org.eclipse.swt.widgets.Control;
import org.eclipse.swt.widgets.Dialog;
import org.eclipse.swt.widgets.Label;
import org.eclipse.swt.widgets.Shell;
import org.eclipse.swt.widgets.Text;

/** This dialog allows you to enter a number. */
public class EnterNumberDialog extends Dialog {
  private static final Class<?> PKG = EnterNumberDialog.class;

  protected Text wNumber;
  protected Text wFrom;
  protected Text wTo;
  protected Button wOk;
  protected Button wCancel;
  protected Button wCheckbox;
  private boolean hideCancelButton;

  protected Shell shell;

  protected int samples;
  private String shellText;
  private String lineText;
  private String checkboxLabel;
  private PropsUi props;

  private int width;
  EnterNumberDialogResult result = new EnterNumberDialogResult();

  public EnterNumberDialog(Shell parent, int samples, String shellText, String lineText) {
    this(parent, samples, shellText, lineText, null);
  }

  public EnterNumberDialog(
      Shell parent,
      int samples,
      String shellText,
      String lineText,
      final String checkboxLabel,
      final int width) {
    this(parent, samples, shellText, lineText, checkboxLabel);
    this.width = width;
  }

  public EnterNumberDialog(
      Shell parent, int samples, String shellText, String lineText, final String checkboxLabel) {
    super(parent, SWT.NONE);
    this.props = PropsUi.getInstance();
    this.samples = samples;
    this.shellText = shellText;
    this.lineText = lineText;
    this.checkboxLabel = checkboxLabel;
  }

  public EnterNumberDialogResult openWithFromTo() {
    createDialog(true);
    return result;
  }

  public int open() {
    createDialog(false);
    return samples;
  }

  private void createDialog(boolean withFromTo) {
    Shell parent = getParent();

    shell = new Shell(parent, SWT.DIALOG_TRIM | SWT.APPLICATION_MODAL | SWT.SHEET);
    PropsUi.setLook(shell);
    shell.setImage(GuiResource.getInstance().getImageHopUi());

    FormLayout formLayout = new FormLayout();
    formLayout.marginWidth = BaseDialog.MARGIN_SIZE;
    formLayout.marginHeight = BaseDialog.MARGIN_SIZE;

    shell.setLayout(formLayout);
    shell.setText(shellText);

    // Number of lines
    Label wlNumber = new Label(shell, SWT.NONE);
    wlNumber.setText(lineText);
    PropsUi.setLook(wlNumber);
    FormData fdlNumber = new FormData();
    fdlNumber.left = new FormAttachment(0, 0);
    fdlNumber.top = new FormAttachment(0, 0);
    wlNumber.setLayoutData(fdlNumber);
    wNumber = new Text(shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    wNumber.setText("100");
    PropsUi.setLook(wNumber);
    FormData fdNumber = new FormData();
    fdNumber.left = new FormAttachment(0, 0);
    fdNumber.top = new FormAttachment(wlNumber, BaseDialog.LABEL_SPACING);
    fdNumber.right = new FormAttachment(100, 0);
    wNumber.setLayoutData(fdNumber);

    Control lastControl = wNumber;
    if (StringUtils.isNotBlank(checkboxLabel)) {
      wCheckbox = new Button(shell, SWT.CHECK);
      PropsUi.setLook(wCheckbox);
      FormData fdCheckbox = new FormData();
      fdCheckbox.left = new FormAttachment(0, 0);
      fdCheckbox.top = new FormAttachment(wNumber, BaseDialog.ELEMENT_SPACING);
      fdCheckbox.width = ConstUi.CHECKBOX_WIDTH;
      wCheckbox.setLayoutData(fdCheckbox);

      Label wlCheckbox = new Label(shell, SWT.LEFT);
      wlCheckbox.addMouseListener(
          new MouseAdapter() {
            @Override
            public void mouseDown(MouseEvent mouseEvent) {
              // toggle the checkbox when the label is clicked
              wCheckbox.setSelection(!wCheckbox.getSelection());
            }
          });
      wlCheckbox.setText(checkboxLabel);
      PropsUi.setLook(wlCheckbox);
      FormData fdlCheckbox = new FormData();
      fdlCheckbox.left = new FormAttachment(wCheckbox, 0);
      fdlCheckbox.top = new FormAttachment(wCheckbox, 0, SWT.CENTER);
      wlCheckbox.setLayoutData(fdlCheckbox);
      lastControl = wlCheckbox;
    }

    if (withFromTo) {
      // Add from-to
      Label wlFrom = new Label(shell, SWT.RIGHT);
      wlFrom.setText("From:");
      PropsUi.setLook(wlFrom);
      FormData fdlFrom = new FormData();
      fdlFrom.left = new FormAttachment(0, 0);
      fdlFrom.top = new FormAttachment(lastControl, BaseDialog.ELEMENT_SPACING);
      wlFrom.setLayoutData(fdlFrom);
      wFrom = new Text(shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
      PropsUi.setLook(wFrom);
      FormData fdFrom = new FormData();
      fdFrom.left = new FormAttachment(wlFrom, BaseDialog.ELEMENT_SPACING);
      fdFrom.top = new FormAttachment(lastControl, BaseDialog.ELEMENT_SPACING);
      wFrom.setLayoutData(fdFrom);
      wFrom.addModifyListener(e -> wNumber.setEnabled(wFrom.getText().isEmpty()));
      Label wlTo = new Label(shell, SWT.RIGHT);
      wlTo.setText("To:");
      PropsUi.setLook(wlTo);
      FormData fdlTo = new FormData();
      fdlTo.left = new FormAttachment(wFrom, BaseDialog.ELEMENT_SPACING);
      fdlTo.top = new FormAttachment(lastControl, BaseDialog.ELEMENT_SPACING);
      wlTo.setLayoutData(fdlTo);
      wTo = new Text(shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
      PropsUi.setLook(wTo);
      FormData fdTo = new FormData();
      fdTo.left = new FormAttachment(wlTo, BaseDialog.ELEMENT_SPACING);
      fdTo.top = new FormAttachment(lastControl, BaseDialog.ELEMENT_SPACING);
      wTo.setLayoutData(fdTo);
      lastControl = wlFrom;
    }

    // Some buttons
    wOk = new Button(shell, SWT.PUSH);
    wOk.setText(BaseMessages.getString(PKG, "System.Button.OK"));
    if (!hideCancelButton) {
      wCancel = new Button(shell, SWT.PUSH);
      wCancel.setText(BaseMessages.getString(PKG, "System.Button.Cancel"));
    }

    wCancel.setLayoutData(
        new FormDataBuilder()
            .top(lastControl, BaseDialog.ELEMENT_SPACING * 2)
            .right(100, 0)
            .result());
    wOk.setLayoutData(
        new FormDataBuilder()
            .top(lastControl, BaseDialog.ELEMENT_SPACING * 2)
            .right(wCancel, Const.isOSX() ? 0 : -BaseDialog.LABEL_SPACING)
            .result());

    // Add listeners
    wOk.addListener(SWT.Selection, e -> ok());
    wCancel.addListener(SWT.Selection, e -> cancel());

    getData();

    BaseDialog.defaultShellHandling(shell, c -> ok(), c -> cancel());
  }

  public void dispose() {
    props.setScreen(new WindowProperty(shell));
    shell.dispose();
  }

  public void getData() {
    wNumber.setText(Integer.toString(samples));
    wNumber.selectAll();
  }

  private void cancel() {
    samples = -1;
    result.setNumberOfLines(-1);
    result.setFromLine(-1);
    result.setToLine(-1);
    dispose();
  }

  protected void ok() {
    try {
      if (wFrom != null && !wFrom.getText().isEmpty()) {
        result.setNumberOfLines(-1);
        result.setFromLine(Integer.parseInt(wFrom.getText()));
        result.setToLine(Integer.parseInt(wTo.getText()));
      } else {
        samples = Integer.parseInt(wNumber.getText());
        result.setNumberOfLines(Integer.parseInt(wNumber.getText()));
      }
      dispose();
    } catch (Exception e) {
      MessageBox mb = new MessageBox(shell, SWT.OK | SWT.ICON_ERROR);
      mb.setMessage(BaseMessages.getString(PKG, "Dialog.Error.EnterInteger"));
      mb.setText(BaseMessages.getString(PKG, "Dialog.Error.Header"));
      mb.open();
      wNumber.selectAll();
    }
  }

  public void setHideCancel(boolean hideCancel) {
    hideCancelButton = hideCancel;
  }
}
