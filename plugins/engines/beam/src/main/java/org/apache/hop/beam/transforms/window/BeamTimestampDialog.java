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

package org.apache.hop.beam.transforms.window;

import org.apache.hop.core.Const;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.util.Utils;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.ui.core.PropsUi;
import org.apache.hop.ui.core.dialog.BaseDialog;
import org.apache.hop.ui.pipeline.transform.BaseTransformDialog;
import org.eclipse.swt.SWT;
import org.eclipse.swt.custom.ScrolledComposite;
import org.eclipse.swt.graphics.Rectangle;
import org.eclipse.swt.layout.FillLayout;
import org.eclipse.swt.layout.FormAttachment;
import org.eclipse.swt.layout.FormData;
import org.eclipse.swt.layout.FormLayout;
import org.eclipse.swt.widgets.Button;
import org.eclipse.swt.widgets.Combo;
import org.eclipse.swt.widgets.Composite;
import org.eclipse.swt.widgets.Control;
import org.eclipse.swt.widgets.Label;
import org.eclipse.swt.widgets.Shell;

public class BeamTimestampDialog extends BaseTransformDialog {
  private static final Class<?> PKG = BeamTimestampDialog.class;
  private final BeamTimestampMeta input;

  private Combo wFieldName;
  private Button wReading;

  public BeamTimestampDialog(
      Shell parent,
      IVariables variables,
      BeamTimestampMeta transformMeta,
      PipelineMeta pipelineMeta) {
    super(parent, variables, transformMeta, pipelineMeta);
    input = transformMeta;
  }

  @Override
  public String open() {
    createShell(BaseMessages.getString(PKG, "BeamTimestampDialog.DialogTitle"));
    buildButtonBar().ok(e -> ok()).cancel(e -> cancel()).build();

    ScrolledComposite scrolledComposite = new ScrolledComposite(shell, SWT.V_SCROLL | SWT.H_SCROLL);
    PropsUi.setLook(scrolledComposite);
    FormData fdScrolledComposite = new FormData();
    fdScrolledComposite.left = new FormAttachment(0, 0);
    fdScrolledComposite.top = new FormAttachment(wSpacer, 0);
    fdScrolledComposite.right = new FormAttachment(100, 0);
    fdScrolledComposite.bottom = new FormAttachment(wOk, -margin);
    scrolledComposite.setLayoutData(fdScrolledComposite);
    scrolledComposite.setLayout(new FillLayout());

    Composite wContent = new Composite(scrolledComposite, SWT.NONE);
    PropsUi.setLook(wContent);
    FormLayout contentLayout = new FormLayout();
    contentLayout.marginWidth = PropsUi.getFormMargin();
    contentLayout.marginHeight = PropsUi.getFormMargin();
    wContent.setLayout(contentLayout);

    changed = input.hasChanged();

    Control lastControl = null;

    String[] fieldNames;
    try {
      fieldNames = pipelineMeta.getPrevTransformFields(variables, transformMeta).getFieldNames();
    } catch (HopException e) {
      log.logError("Error getting fields from previous transforms", e);
      fieldNames = new String[] {};
    }

    Label wlFieldName = new Label(wContent, SWT.RIGHT);
    wlFieldName.setText(BaseMessages.getString(PKG, "BeamTimestampDialog.FieldName"));
    PropsUi.setLook(wlFieldName);
    FormData fdlFieldName = new FormData();
    fdlFieldName.left = new FormAttachment(0, 0);
    fdlFieldName.top = new FormAttachment(0, margin);
    fdlFieldName.right = new FormAttachment(middle, -margin);
    wlFieldName.setLayoutData(fdlFieldName);
    wFieldName = new Combo(wContent, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    PropsUi.setLook(wFieldName);
    wFieldName.setItems(fieldNames);
    FormData fdFieldName = new FormData();
    fdFieldName.left = new FormAttachment(middle, 0);
    fdFieldName.top = new FormAttachment(wlFieldName, 0, SWT.CENTER);
    fdFieldName.right = new FormAttachment(100, 0);
    wFieldName.setLayoutData(fdFieldName);
    lastControl = wFieldName;

    Label wlReading = new Label(wContent, SWT.RIGHT);
    wlReading.setText(BaseMessages.getString(PKG, "BeamTimestampDialog.Reading"));
    PropsUi.setLook(wlReading);
    FormData fdlReading = new FormData();
    fdlReading.left = new FormAttachment(0, 0);
    fdlReading.top = new FormAttachment(lastControl, margin);
    fdlReading.right = new FormAttachment(middle, -margin);
    wlReading.setLayoutData(fdlReading);
    wReading = new Button(wContent, SWT.CHECK | SWT.LEFT);
    PropsUi.setLook(wReading);
    FormData fdReading = new FormData();
    fdReading.left = new FormAttachment(middle, 0);
    fdReading.top = new FormAttachment(wlReading, 0, SWT.CENTER);
    fdReading.right = new FormAttachment(100, 0);
    wReading.setLayoutData(fdReading);

    wContent.pack();
    Rectangle bounds = wContent.getBounds();
    scrolledComposite.setContent(wContent);
    scrolledComposite.setExpandHorizontal(true);
    scrolledComposite.setExpandVertical(true);
    scrolledComposite.setMinWidth(bounds.width);
    scrolledComposite.setMinHeight(bounds.height);

    getData();
    focusTransformName();
    BaseDialog.defaultShellHandling(shell, c -> ok(), c -> cancel());

    return transformName;
  }

  /** Populate the widgets. */
  public void getData() {
    wFieldName.setText(Const.NVL(input.getFieldName(), ""));
    wReading.setSelection(input.isReadingTimestamp());
  }

  private void cancel() {
    transformName = null;
    input.setChanged(changed);
    dispose();
  }

  private void ok() {
    if (Utils.isEmpty(wTransformName.getText())) {
      return;
    }

    getInfo(input);

    dispose();
  }

  private void getInfo(BeamTimestampMeta in) {
    transformName = wTransformName.getText(); // return value

    in.setFieldName(wFieldName.getText());
    in.setReadingTimestamp(wReading.getSelection());

    input.setChanged();
  }
}
