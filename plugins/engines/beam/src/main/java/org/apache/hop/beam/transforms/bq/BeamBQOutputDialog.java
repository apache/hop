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

package org.apache.hop.beam.transforms.bq;

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
import org.eclipse.swt.layout.FormAttachment;
import org.eclipse.swt.layout.FormData;
import org.eclipse.swt.layout.FormLayout;
import org.eclipse.swt.widgets.Button;
import org.eclipse.swt.widgets.Control;
import org.eclipse.swt.widgets.Label;
import org.eclipse.swt.widgets.Shell;
import org.eclipse.swt.widgets.Text;

public class BeamBQOutputDialog extends BaseTransformDialog {
  private static final Class<?> PKG = BeamBQOutputDialog.class;
  private final BeamBQOutputMeta input;

  int middle;
  int margin;

  private TextVar wProjectId;
  private TextVar wDatasetId;
  private TextVar wTableId;
  private Button wCreateIfNeeded;
  private Button wTruncateTable;
  private Button wFailIfNotEmpty;

  public BeamBQOutputDialog(
      Shell parent,
      IVariables variables,
      BeamBQOutputMeta transformMeta,
      PipelineMeta pipelineMeta) {
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
    shell.setText(BaseMessages.getString(PKG, "BeamBQOutputDialog.DialogTitle"));

    middle = props.getMiddlePct();
    margin = PropsUi.getMargin();

    // TransformName line
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

    Label wlProjectId = new Label(shell, SWT.RIGHT);
    wlProjectId.setText(BaseMessages.getString(PKG, "BeamBQOutputDialog.ProjectId"));
    PropsUi.setLook(wlProjectId);
    FormData fdlProjectId = new FormData();
    fdlProjectId.left = new FormAttachment(0, 0);
    fdlProjectId.top = new FormAttachment(lastControl, margin);
    fdlProjectId.right = new FormAttachment(middle, -margin);
    wlProjectId.setLayoutData(fdlProjectId);
    wProjectId = new TextVar(variables, shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    PropsUi.setLook(wProjectId);
    FormData fdProjectId = new FormData();
    fdProjectId.left = new FormAttachment(middle, 0);
    fdProjectId.top = new FormAttachment(wlProjectId, 0, SWT.CENTER);
    fdProjectId.right = new FormAttachment(100, 0);
    wProjectId.setLayoutData(fdProjectId);
    lastControl = wProjectId;

    Label wlDatasetId = new Label(shell, SWT.RIGHT);
    wlDatasetId.setText(BaseMessages.getString(PKG, "BeamBQOutputDialog.DatasetId"));
    PropsUi.setLook(wlDatasetId);
    FormData fdlDatasetId = new FormData();
    fdlDatasetId.left = new FormAttachment(0, 0);
    fdlDatasetId.top = new FormAttachment(lastControl, margin);
    fdlDatasetId.right = new FormAttachment(middle, -margin);
    wlDatasetId.setLayoutData(fdlDatasetId);
    wDatasetId = new TextVar(variables, shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    PropsUi.setLook(wDatasetId);
    FormData fdDatasetId = new FormData();
    fdDatasetId.left = new FormAttachment(middle, 0);
    fdDatasetId.top = new FormAttachment(wlDatasetId, 0, SWT.CENTER);
    fdDatasetId.right = new FormAttachment(100, 0);
    wDatasetId.setLayoutData(fdDatasetId);
    lastControl = wDatasetId;

    Label wlTableId = new Label(shell, SWT.RIGHT);
    wlTableId.setText(BaseMessages.getString(PKG, "BeamBQOutputDialog.TableId"));
    PropsUi.setLook(wlTableId);
    FormData fdlTableId = new FormData();
    fdlTableId.left = new FormAttachment(0, 0);
    fdlTableId.top = new FormAttachment(lastControl, margin);
    fdlTableId.right = new FormAttachment(middle, -margin);
    wlTableId.setLayoutData(fdlTableId);
    wTableId = new TextVar(variables, shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    PropsUi.setLook(wTableId);
    FormData fdTableId = new FormData();
    fdTableId.left = new FormAttachment(middle, 0);
    fdTableId.top = new FormAttachment(wlTableId, 0, SWT.CENTER);
    fdTableId.right = new FormAttachment(100, 0);
    wTableId.setLayoutData(fdTableId);
    lastControl = wTableId;

    Label wlCreateIfNeeded = new Label(shell, SWT.RIGHT);
    wlCreateIfNeeded.setText(BaseMessages.getString(PKG, "BeamBQOutputDialog.CreateIfNeeded"));
    PropsUi.setLook(wlCreateIfNeeded);
    FormData fdlCreateIfNeeded = new FormData();
    fdlCreateIfNeeded.left = new FormAttachment(0, 0);
    fdlCreateIfNeeded.top = new FormAttachment(lastControl, margin);
    fdlCreateIfNeeded.right = new FormAttachment(middle, -margin);
    wlCreateIfNeeded.setLayoutData(fdlCreateIfNeeded);
    wCreateIfNeeded = new Button(shell, SWT.CHECK | SWT.LEFT);
    PropsUi.setLook(wCreateIfNeeded);
    FormData fdCreateIfNeeded = new FormData();
    fdCreateIfNeeded.left = new FormAttachment(middle, 0);
    fdCreateIfNeeded.top = new FormAttachment(wlCreateIfNeeded, 0, SWT.CENTER);
    fdCreateIfNeeded.right = new FormAttachment(100, 0);
    wCreateIfNeeded.setLayoutData(fdCreateIfNeeded);
    lastControl = wCreateIfNeeded;

    Label wlTruncateTable = new Label(shell, SWT.RIGHT);
    wlTruncateTable.setText(BaseMessages.getString(PKG, "BeamBQOutputDialog.TruncateTable"));
    PropsUi.setLook(wlTruncateTable);
    FormData fdlTruncateTable = new FormData();
    fdlTruncateTable.left = new FormAttachment(0, 0);
    fdlTruncateTable.top = new FormAttachment(lastControl, margin);
    fdlTruncateTable.right = new FormAttachment(middle, -margin);
    wlTruncateTable.setLayoutData(fdlTruncateTable);
    wTruncateTable = new Button(shell, SWT.CHECK | SWT.LEFT);
    PropsUi.setLook(wTruncateTable);
    FormData fdTruncateTable = new FormData();
    fdTruncateTable.left = new FormAttachment(middle, 0);
    fdTruncateTable.top = new FormAttachment(wlTruncateTable, 0, SWT.CENTER);
    fdTruncateTable.right = new FormAttachment(100, 0);
    wTruncateTable.setLayoutData(fdTruncateTable);
    lastControl = wTruncateTable;

    Label wlFailIfNotEmpty = new Label(shell, SWT.RIGHT);
    wlFailIfNotEmpty.setText(BaseMessages.getString(PKG, "BeamBQOutputDialog.FailIfNotEmpty"));
    PropsUi.setLook(wlFailIfNotEmpty);
    FormData fdlFailIfNotEmpty = new FormData();
    fdlFailIfNotEmpty.left = new FormAttachment(0, 0);
    fdlFailIfNotEmpty.top = new FormAttachment(lastControl, margin);
    fdlFailIfNotEmpty.right = new FormAttachment(middle, -margin);
    wlFailIfNotEmpty.setLayoutData(fdlFailIfNotEmpty);
    wFailIfNotEmpty = new Button(shell, SWT.CHECK | SWT.LEFT);
    PropsUi.setLook(wFailIfNotEmpty);
    FormData fdFailIfNotEmpty = new FormData();
    fdFailIfNotEmpty.left = new FormAttachment(middle, 0);
    fdFailIfNotEmpty.top = new FormAttachment(wlFailIfNotEmpty, 0, SWT.CENTER);
    fdFailIfNotEmpty.right = new FormAttachment(100, 0);
    wFailIfNotEmpty.setLayoutData(fdFailIfNotEmpty);
    lastControl = wFailIfNotEmpty;

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
    wProjectId.setText(Const.NVL(input.getProjectId(), ""));
    wDatasetId.setText(Const.NVL(input.getDatasetId(), ""));
    wTableId.setText(Const.NVL(input.getTableId(), ""));
    wCreateIfNeeded.setSelection(input.isCreatingIfNeeded());
    wTruncateTable.setSelection(input.isTruncatingTable());
    wFailIfNotEmpty.setSelection(input.isFailingIfNotEmpty());
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

  private void getInfo(BeamBQOutputMeta in) {
    transformName = wTransformName.getText(); // return value

    in.setProjectId(wProjectId.getText());
    in.setDatasetId(wDatasetId.getText());
    in.setTableId(wTableId.getText());
    in.setCreatingIfNeeded(wCreateIfNeeded.getSelection());
    in.setTruncatingTable(wTruncateTable.getSelection());
    in.setFailingIfNotEmpty(wFailIfNotEmpty.getSelection());
    input.setChanged();
  }
}
