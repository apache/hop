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

package org.apache.hop.beam.transforms.bigtable;

import org.apache.hop.core.Const;
import org.apache.hop.core.util.Utils;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.BaseTransformMeta;
import org.apache.hop.pipeline.transform.ITransformDialog;
import org.apache.hop.ui.core.dialog.BaseDialog;
import org.apache.hop.ui.core.widget.TextVar;
import org.apache.hop.ui.pipeline.transform.BaseTransformDialog;
import org.eclipse.swt.SWT;
import org.eclipse.swt.layout.FormAttachment;
import org.eclipse.swt.layout.FormData;
import org.eclipse.swt.layout.FormLayout;
import org.eclipse.swt.widgets.*;

public class BeamBigtableInputDialog extends BaseTransformDialog implements ITransformDialog {
  private static final Class<?> PKG = BeamBigtableInputDialog.class; // For Translator
  private final BeamBigtableInputMeta input;

  private TextVar wProjectId;
  private TextVar wInstanceId;
  private TextVar wTableId;

  public BeamBigtableInputDialog(
      Shell parent, IVariables variables, Object in, PipelineMeta pipelineMeta, String sname) {
    super(parent, variables, (BaseTransformMeta) in, pipelineMeta, sname);
    input = (BeamBigtableInputMeta) in;
  }

  public String open() {
    Shell parent = getParent();

    shell = new Shell(parent, SWT.DIALOG_TRIM | SWT.RESIZE | SWT.MAX | SWT.MIN);
    props.setLook(shell);
    setShellImage(shell, input);

    FormLayout formLayout = new FormLayout();
    formLayout.marginWidth = Const.FORM_MARGIN;
    formLayout.marginHeight = Const.FORM_MARGIN;

    shell.setLayout(formLayout);
    shell.setText(BaseMessages.getString(PKG, "BeamBigtableInputDialog.DialogTitle"));

    int middle = props.getMiddlePct();
    int margin = Const.MARGIN;

    // TransformName line
    wlTransformName = new Label(shell, SWT.RIGHT);
    wlTransformName.setText(BaseMessages.getString(PKG, "System.Label.TransformName"));
    props.setLook(wlTransformName);
    fdlTransformName = new FormData();
    fdlTransformName.left = new FormAttachment(0, 0);
    fdlTransformName.top = new FormAttachment(0, margin);
    fdlTransformName.right = new FormAttachment(middle, -margin);
    wlTransformName.setLayoutData(fdlTransformName);
    wTransformName = new Text(shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    wTransformName.setText(transformName);
    props.setLook(wTransformName);
    fdTransformName = new FormData();
    fdTransformName.left = new FormAttachment(middle, 0);
    fdTransformName.top = new FormAttachment(wlTransformName, 0, SWT.CENTER);
    fdTransformName.right = new FormAttachment(100, 0);
    wTransformName.setLayoutData(fdTransformName);
    Control lastControl = wTransformName;

    Label wlProjectId = new Label(shell, SWT.RIGHT);
    wlProjectId.setText(BaseMessages.getString(PKG, "BeamBigtableInputDialog.ProjectId"));
    props.setLook(wlProjectId);
    FormData fdlProjectId = new FormData();
    fdlProjectId.left = new FormAttachment(0, 0);
    fdlProjectId.top = new FormAttachment(lastControl, margin);
    fdlProjectId.right = new FormAttachment(middle, -margin);
    wlProjectId.setLayoutData(fdlProjectId);
    wProjectId = new TextVar(variables, shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    props.setLook(wProjectId);
    FormData fdProjectId = new FormData();
    fdProjectId.left = new FormAttachment(middle, 0);
    fdProjectId.top = new FormAttachment(wlProjectId, 0, SWT.CENTER);
    fdProjectId.right = new FormAttachment(100, 0);
    wProjectId.setLayoutData(fdProjectId);
    lastControl = wProjectId;

    Label wlInstanceId = new Label(shell, SWT.RIGHT);
    wlInstanceId.setText(BaseMessages.getString(PKG, "BeamBigtableInputDialog.InstanceId"));
    props.setLook(wlInstanceId);
    FormData fdlInstanceId = new FormData();
    fdlInstanceId.left = new FormAttachment(0, 0);
    fdlInstanceId.top = new FormAttachment(lastControl, margin);
    fdlInstanceId.right = new FormAttachment(middle, -margin);
    wlInstanceId.setLayoutData(fdlInstanceId);
    wInstanceId = new TextVar(variables, shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    props.setLook(wInstanceId);
    FormData fdInstanceId = new FormData();
    fdInstanceId.left = new FormAttachment(middle, 0);
    fdInstanceId.top = new FormAttachment(wlInstanceId, 0, SWT.CENTER);
    fdInstanceId.right = new FormAttachment(100, 0);
    wInstanceId.setLayoutData(fdInstanceId);
    lastControl = wInstanceId;

    Label wlTableId = new Label(shell, SWT.RIGHT);
    wlTableId.setText(BaseMessages.getString(PKG, "BeamBigtableInputDialog.TableId"));
    props.setLook(wlTableId);
    FormData fdlTableId = new FormData();
    fdlTableId.left = new FormAttachment(0, 0);
    fdlTableId.top = new FormAttachment(lastControl, margin);
    fdlTableId.right = new FormAttachment(middle, -margin);
    wlTableId.setLayoutData(fdlTableId);
    wTableId = new TextVar(variables, shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    props.setLook(wTableId);
    FormData fdTableId = new FormData();
    fdTableId.left = new FormAttachment(middle, 0);
    fdTableId.top = new FormAttachment(wlTableId, 0, SWT.CENTER);
    fdTableId.right = new FormAttachment(100, 0);
    wTableId.setLayoutData(fdTableId);
    lastControl = wTableId;

    wOk = new Button(shell, SWT.PUSH);
    wOk.setText(BaseMessages.getString(PKG, "System.Button.OK"));
    wOk.addListener(SWT.Selection, e -> ok());
    wCancel = new Button(shell, SWT.PUSH);
    wCancel.setText(BaseMessages.getString(PKG, "System.Button.Cancel"));
    wCancel.addListener(SWT.Selection, e -> cancel());
    setButtonPositions(new Button[] {wOk, wCancel}, margin, lastControl);

    getData();

    BaseDialog.defaultShellHandling(shell, c -> ok(), c -> cancel());

    return transformName;
  }

  /** Populate the widgets. */
  public void getData() {
    wTransformName.setText(transformName);
    wProjectId.setText(Const.NVL(input.getProjectId(), ""));
    wInstanceId.setText(Const.NVL(input.getInstanceId(), ""));
    wTableId.setText(Const.NVL(input.getTableId(), ""));

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

  private void getInfo(BeamBigtableInputMeta in) {
    transformName = wTransformName.getText(); // return value

    in.setProjectId(wProjectId.getText());
    in.setInstanceId(wInstanceId.getText());
    in.setTableId(wTableId.getText());

    input.setChanged();
  }
}
