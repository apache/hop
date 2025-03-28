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

package org.apache.hop.beam.transforms.io;

import org.apache.hop.beam.metadata.FileDefinition;
import org.apache.hop.core.Const;
import org.apache.hop.core.util.Utils;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.ui.core.PropsUi;
import org.apache.hop.ui.core.dialog.BaseDialog;
import org.apache.hop.ui.core.widget.MetaSelectionLine;
import org.apache.hop.ui.core.widget.TextVar;
import org.apache.hop.ui.pipeline.transform.BaseTransformDialog;
import org.eclipse.swt.SWT;
import org.eclipse.swt.layout.FormAttachment;
import org.eclipse.swt.layout.FormData;
import org.eclipse.swt.layout.FormLayout;
import org.eclipse.swt.widgets.Button;
import org.eclipse.swt.widgets.Control;
import org.eclipse.swt.widgets.Label;
import org.eclipse.swt.widgets.Shell;
import org.eclipse.swt.widgets.Text;

public class BeamOutputDialog extends BaseTransformDialog {
  private static final Class<?> PKG = BeamOutput.class;
  private final BeamOutputMeta input;

  int middle;
  int margin;

  private boolean getpreviousFields = false;

  private TextVar wOutputLocation;
  private MetaSelectionLine<FileDefinition> wFileDefinition;
  private TextVar wFilePrefix;
  private TextVar wFileSuffix;
  private Button wWindowed;

  public BeamOutputDialog(
      Shell parent, IVariables variables, BeamOutputMeta transformMeta, PipelineMeta pipelineMeta) {
    super(parent, variables, transformMeta, pipelineMeta);
    input = transformMeta;
  }

  @Override
  public String open() {
    Shell parent = getParent();

    shell = new Shell(parent, SWT.DIALOG_TRIM | SWT.RESIZE | SWT.MAX | SWT.MIN);
    PropsUi.setLook(shell);
    setShellImage(shell, input);

    changed = input.hasChanged();

    FormLayout formLayout = new FormLayout();
    formLayout.marginWidth = PropsUi.getFormMargin();
    formLayout.marginHeight = PropsUi.getFormMargin();

    shell.setLayout(formLayout);
    shell.setText(BaseMessages.getString(PKG, "BeamOutputDialog.DialogTitle"));

    middle = props.getMiddlePct();
    margin = PropsUi.getMargin();

    // Transform name line
    wlTransformName = new Label(shell, SWT.RIGHT);
    wlTransformName.setText(BaseMessages.getString(PKG, "System.TransformName.Label"));
    wlTransformName.setToolTipText(BaseMessages.getString(PKG, "System.TransformName.Tooltip"));
    PropsUi.setLook(wlTransformName);
    fdlTransformName = new FormData();
    fdlTransformName.left = new FormAttachment(0, 0);
    fdlTransformName.top = new FormAttachment(0, margin);
    fdlTransformName.right = new FormAttachment(middle, -margin);
    wlTransformName.setLayoutData(fdlTransformName);
    wTransformName = new Text(shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    wTransformName.setText(transformName);
    PropsUi.setLook(wTransformName);
    fdTransformName = new FormData();
    fdTransformName.left = new FormAttachment(middle, 0);
    fdTransformName.top = new FormAttachment(wlTransformName, 0, SWT.CENTER);
    fdTransformName.right = new FormAttachment(100, 0);
    wTransformName.setLayoutData(fdTransformName);
    Control lastControl = wTransformName;

    Label wlOutputLocation = new Label(shell, SWT.RIGHT);
    wlOutputLocation.setText(BaseMessages.getString(PKG, "BeamOutputDialog.OutputLocation"));
    PropsUi.setLook(wlOutputLocation);
    FormData fdlOutputLocation = new FormData();
    fdlOutputLocation.left = new FormAttachment(0, 0);
    fdlOutputLocation.top = new FormAttachment(lastControl, margin);
    fdlOutputLocation.right = new FormAttachment(middle, -margin);
    wlOutputLocation.setLayoutData(fdlOutputLocation);
    wOutputLocation = new TextVar(variables, shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    PropsUi.setLook(wOutputLocation);
    FormData fdOutputLocation = new FormData();
    fdOutputLocation.left = new FormAttachment(middle, 0);
    fdOutputLocation.top = new FormAttachment(wlOutputLocation, 0, SWT.CENTER);
    fdOutputLocation.right = new FormAttachment(100, 0);
    wOutputLocation.setLayoutData(fdOutputLocation);
    lastControl = wOutputLocation;

    Label wlFilePrefix = new Label(shell, SWT.RIGHT);
    wlFilePrefix.setText(BaseMessages.getString(PKG, "BeamOutputDialog.FilePrefix"));
    PropsUi.setLook(wlFilePrefix);
    FormData fdlFilePrefix = new FormData();
    fdlFilePrefix.left = new FormAttachment(0, 0);
    fdlFilePrefix.top = new FormAttachment(lastControl, margin);
    fdlFilePrefix.right = new FormAttachment(middle, -margin);
    wlFilePrefix.setLayoutData(fdlFilePrefix);
    wFilePrefix = new TextVar(variables, shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    PropsUi.setLook(wFilePrefix);
    FormData fdFilePrefix = new FormData();
    fdFilePrefix.left = new FormAttachment(middle, 0);
    fdFilePrefix.top = new FormAttachment(wlFilePrefix, 0, SWT.CENTER);
    fdFilePrefix.right = new FormAttachment(100, 0);
    wFilePrefix.setLayoutData(fdFilePrefix);
    lastControl = wFilePrefix;

    Label wlFileSuffix = new Label(shell, SWT.RIGHT);
    wlFileSuffix.setText(BaseMessages.getString(PKG, "BeamOutputDialog.FileSuffix"));
    PropsUi.setLook(wlFileSuffix);
    FormData fdlFileSuffix = new FormData();
    fdlFileSuffix.left = new FormAttachment(0, 0);
    fdlFileSuffix.top = new FormAttachment(lastControl, margin);
    fdlFileSuffix.right = new FormAttachment(middle, -margin);
    wlFileSuffix.setLayoutData(fdlFileSuffix);
    wFileSuffix = new TextVar(variables, shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    PropsUi.setLook(wFileSuffix);
    FormData fdFileSuffix = new FormData();
    fdFileSuffix.left = new FormAttachment(middle, 0);
    fdFileSuffix.top = new FormAttachment(wlFileSuffix, 0, SWT.CENTER);
    fdFileSuffix.right = new FormAttachment(100, 0);
    wFileSuffix.setLayoutData(fdFileSuffix);
    lastControl = wFileSuffix;

    Label wlWindowed = new Label(shell, SWT.RIGHT);
    wlWindowed.setText(BaseMessages.getString(PKG, "BeamOutputDialog.Windowed"));
    PropsUi.setLook(wlWindowed);
    FormData fdlWindowed = new FormData();
    fdlWindowed.left = new FormAttachment(0, 0);
    fdlWindowed.top = new FormAttachment(lastControl, margin);
    fdlWindowed.right = new FormAttachment(middle, -margin);
    wlWindowed.setLayoutData(fdlWindowed);
    wWindowed = new Button(shell, SWT.CHECK);
    PropsUi.setLook(wWindowed);
    FormData fdWindowed = new FormData();
    fdWindowed.left = new FormAttachment(middle, 0);
    fdWindowed.top = new FormAttachment(wlWindowed, 0, SWT.CENTER);
    fdWindowed.right = new FormAttachment(100, 0);
    wWindowed.setLayoutData(fdWindowed);
    lastControl = wWindowed;

    wFileDefinition =
        new MetaSelectionLine<>(
            variables,
            metadataProvider,
            FileDefinition.class,
            shell,
            SWT.NONE,
            BaseMessages.getString(PKG, "BeamOutputDialog.FileDefinition"),
            BaseMessages.getString(PKG, "BeamOutputDialog.FileDefinition"));
    PropsUi.setLook(wFileDefinition);
    FormData fdFileDefinition = new FormData();
    fdFileDefinition.left = new FormAttachment(0, 0);
    fdFileDefinition.top = new FormAttachment(lastControl, margin);
    fdFileDefinition.right = new FormAttachment(100, 0);
    wFileDefinition.setLayoutData(fdFileDefinition);
    lastControl = wFileDefinition;

    try {
      wFileDefinition.fillItems();
    } catch (Exception e) {
      log.logError("Error getting file definition items", e);
    }

    // Buttons go at the very bottom
    //
    wOk = new Button(shell, SWT.PUSH);
    wOk.setText(BaseMessages.getString(PKG, "System.Button.OK"));
    wOk.addListener(SWT.Selection, e -> ok());
    wCancel = new Button(shell, SWT.PUSH);
    wCancel.setText(BaseMessages.getString(PKG, "System.Button.Cancel"));
    wCancel.addListener(SWT.Selection, e -> cancel());
    BaseTransformDialog.positionBottomButtons(
        shell, new Button[] {wOk, wCancel}, margin, lastControl);

    getData();

    BaseDialog.defaultShellHandling(shell, c -> ok(), c -> cancel());

    return transformName;
  }

  /** Populate the widgets. */
  public void getData() {
    wTransformName.setText(transformName);
    wFileDefinition.setText(Const.NVL(input.getFileDefinitionName(), ""));
    wOutputLocation.setText(Const.NVL(input.getOutputLocation(), ""));
    wFilePrefix.setText(Const.NVL(input.getFilePrefix(), ""));
    wFileSuffix.setText(Const.NVL(input.getFileSuffix(), ""));
    wWindowed.setSelection(input.isWindowed());

    wTransformName.selectAll();
    wTransformName.setFocus();
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

  private void getInfo(BeamOutputMeta in) {
    transformName = wTransformName.getText(); // return value

    in.setFileDefinitionName(wFileDefinition.getText());
    in.setOutputLocation(wOutputLocation.getText());
    in.setFilePrefix(wFilePrefix.getText());
    in.setFileSuffix(wFileSuffix.getText());
    in.setWindowed(wWindowed.getSelection());

    input.setChanged();
  }
}
