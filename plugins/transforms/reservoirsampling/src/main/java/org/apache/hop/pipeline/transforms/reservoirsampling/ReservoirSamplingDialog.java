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

package org.apache.hop.pipeline.transforms.reservoirsampling;

import org.apache.hop.core.Const;
import org.apache.hop.core.util.Utils;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.ui.core.PropsUi;
import org.apache.hop.ui.core.dialog.BaseDialog;
import org.apache.hop.ui.core.widget.TextVar;
import org.apache.hop.ui.pipeline.transform.BaseTransformDialog;
import org.eclipse.swt.SWT;
import org.eclipse.swt.events.ModifyListener;
import org.eclipse.swt.layout.FormAttachment;
import org.eclipse.swt.layout.FormData;
import org.eclipse.swt.widgets.Label;
import org.eclipse.swt.widgets.Shell;

public class ReservoirSamplingDialog extends BaseTransformDialog {

  private static final Class<?> PKG = ReservoirSamplingMeta.class;

  private TextVar wSampleSize;

  private TextVar wSeed;

  /**
   * meta data for the transform. A copy is made so that changes, in terms of choices made by the
   * user, can be detected.
   */
  private final ReservoirSamplingMeta input;

  public ReservoirSamplingDialog(
      Shell parent,
      IVariables variables,
      ReservoirSamplingMeta transformMeta,
      PipelineMeta pipelineMeta) {

    super(parent, variables, transformMeta, pipelineMeta);

    // The order here is important...
    // currentMeta is looked at for changes
    //
    input = transformMeta;
  }

  /**
   * Open the dialog
   *
   * @return the transform name
   */
  @Override
  public String open() {
    createShell(BaseMessages.getString(PKG, "ReservoirSamplingDialog.Shell.Title"));

    buildButtonBar().ok(e -> ok()).cancel(e -> cancel()).build();

    // used to listen to a text field (m_wTransformName)
    ModifyListener lsMod = e -> input.setChanged();

    // various UI bits and pieces

    // Sample size text field
    Label mWlSampleSize = new Label(shell, SWT.RIGHT);
    mWlSampleSize.setText(BaseMessages.getString(PKG, "ReservoirSamplingDialog.SampleSize.Label"));
    PropsUi.setLook(mWlSampleSize);

    FormData mFdlSampleSize = new FormData();
    mFdlSampleSize.left = new FormAttachment(0, 0);
    mFdlSampleSize.right = new FormAttachment(middle, -margin);
    mFdlSampleSize.top = new FormAttachment(wSpacer, margin);
    mWlSampleSize.setLayoutData(mFdlSampleSize);

    wSampleSize = new TextVar(variables, shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    PropsUi.setLook(wSampleSize);
    wSampleSize.addModifyListener(lsMod);
    FormData mFdSampleSize = new FormData();
    mFdSampleSize.left = new FormAttachment(mWlSampleSize, margin);
    mFdSampleSize.right = new FormAttachment(100, -margin);
    mFdSampleSize.top = new FormAttachment(wSpacer, margin);
    wSampleSize.setLayoutData(mFdSampleSize);

    // Seed text field
    Label mWlSeed = new Label(shell, SWT.RIGHT);
    mWlSeed.setText(BaseMessages.getString(PKG, "ReservoirSamplingDialog.Seed.Label"));
    PropsUi.setLook(mWlSeed);

    FormData mFdlSeed = new FormData();
    mFdlSeed.left = new FormAttachment(0, 0);
    mFdlSeed.right = new FormAttachment(middle, -margin);
    mFdlSeed.top = new FormAttachment(wSampleSize, margin);
    mWlSeed.setLayoutData(mFdlSeed);

    wSeed = new TextVar(variables, shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    PropsUi.setLook(wSeed);
    wSeed.addModifyListener(lsMod);
    FormData mFdSeed = new FormData();
    mFdSeed.left = new FormAttachment(mWlSeed, margin);
    mFdSeed.right = new FormAttachment(100, -margin);
    mFdSeed.top = new FormAttachment(wSampleSize, margin);
    wSeed.setLayoutData(mFdSeed);

    // Whenever something changes, set the tooltip to the expanded version:
    wSampleSize.addModifyListener(
        e -> wSampleSize.setToolTipText(variables.resolve(wSampleSize.getText())));

    // Whenever something changes, set the tooltip to the expanded version:
    wSeed.addModifyListener(e -> wSeed.setToolTipText(variables.resolve(wSeed.getText())));

    getData();
    focusTransformName();
    BaseDialog.defaultShellHandling(shell, c -> ok(), c -> cancel());

    return transformName;
  }

  private void cancel() {
    transformName = null;
    input.setChanged(changed);
    dispose();
  }

  private void getData() {
    wSampleSize.setText(Const.NVL(input.getSampleSize(), ""));
    wSeed.setText(Const.NVL(input.getSeed(), ""));
  }

  private void ok() {
    if (Utils.isEmpty(wTransformName.getText())) {
      return;
    }

    transformName = wTransformName.getText(); // return value

    input.setSampleSize(wSampleSize.getText());
    input.setSeed(wSeed.getText());

    input.setChanged();
    dispose();
  }
}
