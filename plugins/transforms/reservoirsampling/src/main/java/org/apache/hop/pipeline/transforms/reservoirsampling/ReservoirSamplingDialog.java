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
import org.eclipse.swt.layout.FormLayout;
import org.eclipse.swt.widgets.Button;
import org.eclipse.swt.widgets.Label;
import org.eclipse.swt.widgets.Shell;
import org.eclipse.swt.widgets.Text;

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
    Shell parent = getParent();

    shell = new Shell(parent, SWT.DIALOG_TRIM | SWT.RESIZE | SWT.MIN | SWT.MAX);

    PropsUi.setLook(shell);
    setShellImage(shell, input);

    // used to listen to a text field (m_wTransformName)
    ModifyListener lsMod = e -> input.setChanged();

    FormLayout formLayout = new FormLayout();
    formLayout.marginWidth = PropsUi.getFormMargin();
    formLayout.marginHeight = PropsUi.getFormMargin();

    shell.setLayout(formLayout);
    shell.setText(BaseMessages.getString(PKG, "ReservoirSamplingDialog.Shell.Title"));

    int middle = props.getMiddlePct();
    int margin = PropsUi.getMargin();

    // TransformName line
    // various UI bits and pieces
    Label mWlTransformName = new Label(shell, SWT.RIGHT);
    mWlTransformName.setText(
        BaseMessages.getString(PKG, "ReservoirSamplingDialog.TransformName.Label"));
    PropsUi.setLook(mWlTransformName);

    FormData mFdlTransformName = new FormData();
    mFdlTransformName.left = new FormAttachment(0, 0);
    mFdlTransformName.right = new FormAttachment(middle, -margin);
    mFdlTransformName.top = new FormAttachment(0, margin);
    mWlTransformName.setLayoutData(mFdlTransformName);
    wTransformName = new Text(shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    wTransformName.setText(transformName);
    PropsUi.setLook(wTransformName);
    wTransformName.addModifyListener(lsMod);

    // format the text field
    FormData mFdTransformName = new FormData();
    mFdTransformName.left = new FormAttachment(middle, 0);
    mFdTransformName.top = new FormAttachment(0, margin);
    mFdTransformName.right = new FormAttachment(100, 0);
    wTransformName.setLayoutData(mFdTransformName);

    // Sample size text field
    Label mWlSampleSize = new Label(shell, SWT.RIGHT);
    mWlSampleSize.setText(BaseMessages.getString(PKG, "ReservoirSamplingDialog.SampleSize.Label"));
    PropsUi.setLook(mWlSampleSize);

    FormData mFdlSampleSize = new FormData();
    mFdlSampleSize.left = new FormAttachment(0, 0);
    mFdlSampleSize.right = new FormAttachment(middle, -margin);
    mFdlSampleSize.top = new FormAttachment(wTransformName, margin);
    mWlSampleSize.setLayoutData(mFdlSampleSize);

    wSampleSize = new TextVar(variables, shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    PropsUi.setLook(wSampleSize);
    wSampleSize.addModifyListener(lsMod);
    FormData mFdSampleSize = new FormData();
    mFdSampleSize.left = new FormAttachment(mWlSampleSize, margin);
    mFdSampleSize.right = new FormAttachment(100, -margin);
    mFdSampleSize.top = new FormAttachment(wTransformName, margin);
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

    // Some buttons
    wOk = new Button(shell, SWT.PUSH);
    wOk.setText(BaseMessages.getString(PKG, "System.Button.OK"));
    wCancel = new Button(shell, SWT.PUSH);
    wCancel.setText(BaseMessages.getString(PKG, "System.Button.Cancel"));

    setButtonPositions(new Button[] {wOk, wCancel}, margin, wSeed);

    // Add listeners
    wCancel.addListener(SWT.Selection, e -> cancel());
    wOk.addListener(SWT.Selection, e -> ok());

    // Whenever something changes, set the tooltip to the expanded version:
    wSampleSize.addModifyListener(
        e -> wSampleSize.setToolTipText(variables.resolve(wSampleSize.getText())));

    // Whenever something changes, set the tooltip to the expanded version:
    wSeed.addModifyListener(e -> wSeed.setToolTipText(variables.resolve(wSeed.getText())));

    getData();

    BaseDialog.defaultShellHandling(shell, c -> ok(), c -> cancel());

    return transformName;
  }

  private void cancel() {
    transformName = null;
    input.setChanged(changed);
    dispose();
  }

  private void getData() {
    wTransformName.setText(Const.NVL(transformName, ""));
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
