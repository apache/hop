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

package org.apache.hop.reflection.pipeline.transform;

import org.apache.hop.core.util.Utils;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.ui.core.PropsUi;
import org.apache.hop.ui.core.dialog.BaseDialog;
import org.apache.hop.ui.pipeline.transform.BaseTransformDialog;
import org.eclipse.swt.SWT;
import org.eclipse.swt.events.ModifyListener;
import org.eclipse.swt.layout.FormAttachment;
import org.eclipse.swt.layout.FormData;
import org.eclipse.swt.widgets.Button;
import org.eclipse.swt.widgets.Control;
import org.eclipse.swt.widgets.Label;
import org.eclipse.swt.widgets.Shell;

public class PipelineLoggingDialog extends BaseTransformDialog {

  private static final Class<?> PKG = PipelineLoggingDialog.class;

  private final PipelineLoggingMeta input;
  private Button wLoggingTransforms;

  public PipelineLoggingDialog(
      Shell parent,
      IVariables variables,
      PipelineLoggingMeta transformMeta,
      PipelineMeta pipelineMeta) {
    super(parent, variables, transformMeta, pipelineMeta);
    this.input = transformMeta;
  }

  @Override
  public String open() {
    createShell(BaseMessages.getString(PKG, "PipelineLoggingDialog.Shell.Title"));

    buildButtonBar().ok(e -> ok()).cancel(e -> cancel()).build();

    ModifyListener lsMod = e -> input.setChanged();
    changed = input.hasChanged();

    Control lastControl = wSpacer;

    // LoggingTransforms line
    Label wlLoggingTransforms = new Label(shell, SWT.RIGHT);
    wlLoggingTransforms.setText(
        BaseMessages.getString(PKG, "PipelineLoggingDialog.LoggingTransforms.Label"));
    PropsUi.setLook(wlLoggingTransforms);
    FormData fdlLoggingTransforms = new FormData();
    fdlLoggingTransforms.left = new FormAttachment(0, 0);
    fdlLoggingTransforms.right = new FormAttachment(middle, -margin);
    fdlLoggingTransforms.top = new FormAttachment(lastControl, margin);
    wlLoggingTransforms.setLayoutData(fdlLoggingTransforms);
    wLoggingTransforms = new Button(shell, SWT.CHECK | SWT.LEFT);
    PropsUi.setLook(wLoggingTransforms);
    FormData fdLoggingTransforms = new FormData();
    fdLoggingTransforms.left = new FormAttachment(middle, 0);
    fdLoggingTransforms.top = new FormAttachment(wlLoggingTransforms, 0, SWT.CENTER);
    fdLoggingTransforms.right = new FormAttachment(100, 0);
    wLoggingTransforms.setLayoutData(fdLoggingTransforms);

    getData();
    focusTransformName();
    BaseDialog.defaultShellHandling(shell, c -> ok(), c -> cancel());

    return transformName;
  }

  /** Copy information from the meta-data input to the dialog fields. */
  public void getData() {
    wLoggingTransforms.setSelection(input.isLoggingTransforms());
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

    transformName = wTransformName.getText(); // return value

    input.setLoggingTransforms(wLoggingTransforms.getSelection());

    dispose();
  }
}
