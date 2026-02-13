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

package org.apache.hop.reflection.workflow.transform;

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

public class WorkflowLoggingDialog extends BaseTransformDialog {

  private static final Class<?> PKG = WorkflowLoggingDialog.class;

  private final WorkflowLoggingMeta input;
  private Button wLoggingActionResults;

  public WorkflowLoggingDialog(
      Shell parent,
      IVariables variables,
      WorkflowLoggingMeta transformMeta,
      PipelineMeta pipelineMeta) {
    super(parent, variables, transformMeta, pipelineMeta);
    this.input = transformMeta;
  }

  @Override
  public String open() {
    createShell(BaseMessages.getString(PKG, "WorkflowLoggingDialog.Shell.Title"));

    buildButtonBar().ok(e -> ok()).cancel(e -> cancel()).build();

    ModifyListener lsMod = e -> input.setChanged();
    changed = input.hasChanged();

    Control lastControl = wSpacer;

    // Logging action results line
    Label wlLoggingActionResults = new Label(shell, SWT.RIGHT);
    wlLoggingActionResults.setText(
        BaseMessages.getString(PKG, "WorkflowLoggingDialog.LoggingActions.Label"));
    PropsUi.setLook(wlLoggingActionResults);
    FormData fdlLoggingActionResults = new FormData();
    fdlLoggingActionResults.left = new FormAttachment(0, 0);
    fdlLoggingActionResults.right = new FormAttachment(middle, -margin);
    fdlLoggingActionResults.top = new FormAttachment(lastControl, margin);
    wlLoggingActionResults.setLayoutData(fdlLoggingActionResults);
    wLoggingActionResults = new Button(shell, SWT.CHECK | SWT.LEFT);
    PropsUi.setLook(wLoggingActionResults);
    FormData fdLoggingActionResults = new FormData();
    fdLoggingActionResults.left = new FormAttachment(middle, 0);
    fdLoggingActionResults.top = new FormAttachment(wlLoggingActionResults, 0, SWT.CENTER);
    fdLoggingActionResults.right = new FormAttachment(100, 0);
    wLoggingActionResults.setLayoutData(fdLoggingActionResults);

    getData();
    focusTransformName();
    BaseDialog.defaultShellHandling(shell, c -> ok(), c -> cancel());

    return transformName;
  }

  /** Copy information from the meta-data input to the dialog fields. */
  public void getData() {
    wLoggingActionResults.setSelection(input.isLoggingActionResults());
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

    input.setLoggingActionResults(wLoggingActionResults.getSelection());

    dispose();
  }
}
