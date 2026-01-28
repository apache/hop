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

package org.apache.hop.pipeline.transforms.samplerows;

import org.apache.hop.core.Const;
import org.apache.hop.core.util.Utils;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.ui.core.PropsUi;
import org.apache.hop.ui.core.dialog.BaseDialog;
import org.apache.hop.ui.core.widget.LabelTextVar;
import org.apache.hop.ui.pipeline.transform.BaseTransformDialog;
import org.eclipse.swt.events.ModifyListener;
import org.eclipse.swt.layout.FormAttachment;
import org.eclipse.swt.layout.FormData;
import org.eclipse.swt.widgets.Shell;

public class SampleRowsDialog extends BaseTransformDialog {
  private static final Class<?> PKG = SampleRowsMeta.class;

  private LabelTextVar wLinesRange;
  private LabelTextVar wLineNumberField;
  private SampleRowsMeta input;

  public SampleRowsDialog(
      Shell parent, IVariables variables, SampleRowsMeta transformMeta, PipelineMeta pipelineMeta) {
    super(parent, variables, transformMeta, pipelineMeta);
    input = transformMeta;
  }

  @Override
  public String open() {
    createShell(BaseMessages.getString(PKG, "SampleRowsDialog.Shell.Title"));

    buildButtonBar().ok(e -> ok()).cancel(e -> cancel()).build();

    ModifyListener lsMod = e -> input.setChanged();
    changed = input.hasChanged();

    // Lines ragne
    wLinesRange =
        new LabelTextVar(
            variables,
            shell,
            BaseMessages.getString(PKG, "SampleRowsDialog.LinesRange.Label"),
            BaseMessages.getString(PKG, "SampleRowsDialog.LinesRange.Tooltip"));
    PropsUi.setLook(wLinesRange);
    wLinesRange.addModifyListener(lsMod);
    FormData fdLinesRange = new FormData();
    fdLinesRange.left = new FormAttachment(0, -margin);
    fdLinesRange.top = new FormAttachment(wSpacer, margin);
    fdLinesRange.right = new FormAttachment(100, -margin);
    wLinesRange.setLayoutData(fdLinesRange);

    // Add line number to output?
    wLineNumberField =
        new LabelTextVar(
            variables,
            shell,
            BaseMessages.getString(PKG, "SampleRowsDialog.LineNumberField.Label"),
            BaseMessages.getString(PKG, "SampleRowsDialog.LineNumberField.Tooltip"));
    PropsUi.setLook(wLinesRange);
    wLineNumberField.addModifyListener(lsMod);
    FormData fdLineNumberField = new FormData();
    fdLineNumberField.left = new FormAttachment(0, -margin);
    fdLineNumberField.top = new FormAttachment(wLinesRange, margin);
    fdLineNumberField.right = new FormAttachment(100, -margin);
    wLineNumberField.setLayoutData(fdLineNumberField);

    getData();
    input.setChanged(changed);
    focusTransformName();
    BaseDialog.defaultShellHandling(shell, c -> ok(), c -> cancel());

    return transformName;
  }

  /** Copy information from the meta-data input to the dialog fields. */
  public void getData() {
    if (input.getLinesRange() != null) {
      wLinesRange.setText(Const.NVL(input.getLinesRange(), SampleRowsMeta.DEFAULT_RANGE));
    }
    if (input.getLineNumberField() != null) {
      wLineNumberField.setText(input.getLineNumberField());
    }
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

    // Get the information for the dialog into the input structure.
    getInfo(input);

    dispose();
  }

  private void getInfo(SampleRowsMeta inf) {
    inf.setLinesRange(Const.NVL(wLinesRange.getText(), SampleRowsMeta.DEFAULT_RANGE));
    inf.setLineNumberField(wLineNumberField.getText());
    transformName = wTransformName.getText(); // return value
  }
}
