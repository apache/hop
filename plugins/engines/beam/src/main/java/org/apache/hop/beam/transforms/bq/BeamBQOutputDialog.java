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
import org.eclipse.swt.custom.ScrolledComposite;
import org.eclipse.swt.graphics.Rectangle;
import org.eclipse.swt.layout.FillLayout;
import org.eclipse.swt.layout.FormAttachment;
import org.eclipse.swt.layout.FormData;
import org.eclipse.swt.layout.FormLayout;
import org.eclipse.swt.widgets.Button;
import org.eclipse.swt.widgets.Composite;
import org.eclipse.swt.widgets.Control;
import org.eclipse.swt.widgets.Label;
import org.eclipse.swt.widgets.Shell;

public class BeamBQOutputDialog extends BaseTransformDialog {
  private static final Class<?> PKG = BeamBQOutputDialog.class;
  private final BeamBQOutputMeta input;

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
    createShell(BaseMessages.getString(PKG, "BeamBQOutputDialog.DialogTitle"));
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

    Label wlProjectId = new Label(wContent, SWT.RIGHT);
    wlProjectId.setText(BaseMessages.getString(PKG, "BeamBQOutputDialog.ProjectId"));
    PropsUi.setLook(wlProjectId);
    FormData fdlProjectId = new FormData();
    fdlProjectId.left = new FormAttachment(0, 0);
    fdlProjectId.top = new FormAttachment(0, margin);
    fdlProjectId.right = new FormAttachment(middle, -margin);
    wlProjectId.setLayoutData(fdlProjectId);
    wProjectId = new TextVar(variables, wContent, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    PropsUi.setLook(wProjectId);
    FormData fdProjectId = new FormData();
    fdProjectId.left = new FormAttachment(middle, 0);
    fdProjectId.top = new FormAttachment(wlProjectId, 0, SWT.CENTER);
    fdProjectId.right = new FormAttachment(100, 0);
    wProjectId.setLayoutData(fdProjectId);
    lastControl = wProjectId;

    Label wlDatasetId = new Label(wContent, SWT.RIGHT);
    wlDatasetId.setText(BaseMessages.getString(PKG, "BeamBQOutputDialog.DatasetId"));
    PropsUi.setLook(wlDatasetId);
    FormData fdlDatasetId = new FormData();
    fdlDatasetId.left = new FormAttachment(0, 0);
    fdlDatasetId.top = new FormAttachment(lastControl, margin);
    fdlDatasetId.right = new FormAttachment(middle, -margin);
    wlDatasetId.setLayoutData(fdlDatasetId);
    wDatasetId = new TextVar(variables, wContent, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    PropsUi.setLook(wDatasetId);
    FormData fdDatasetId = new FormData();
    fdDatasetId.left = new FormAttachment(middle, 0);
    fdDatasetId.top = new FormAttachment(wlDatasetId, 0, SWT.CENTER);
    fdDatasetId.right = new FormAttachment(100, 0);
    wDatasetId.setLayoutData(fdDatasetId);
    lastControl = wDatasetId;

    Label wlTableId = new Label(wContent, SWT.RIGHT);
    wlTableId.setText(BaseMessages.getString(PKG, "BeamBQOutputDialog.TableId"));
    PropsUi.setLook(wlTableId);
    FormData fdlTableId = new FormData();
    fdlTableId.left = new FormAttachment(0, 0);
    fdlTableId.top = new FormAttachment(lastControl, margin);
    fdlTableId.right = new FormAttachment(middle, -margin);
    wlTableId.setLayoutData(fdlTableId);
    wTableId = new TextVar(variables, wContent, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    PropsUi.setLook(wTableId);
    FormData fdTableId = new FormData();
    fdTableId.left = new FormAttachment(middle, 0);
    fdTableId.top = new FormAttachment(wlTableId, 0, SWT.CENTER);
    fdTableId.right = new FormAttachment(100, 0);
    wTableId.setLayoutData(fdTableId);
    lastControl = wTableId;

    Label wlCreateIfNeeded = new Label(wContent, SWT.RIGHT);
    wlCreateIfNeeded.setText(BaseMessages.getString(PKG, "BeamBQOutputDialog.CreateIfNeeded"));
    PropsUi.setLook(wlCreateIfNeeded);
    FormData fdlCreateIfNeeded = new FormData();
    fdlCreateIfNeeded.left = new FormAttachment(0, 0);
    fdlCreateIfNeeded.top = new FormAttachment(lastControl, margin);
    fdlCreateIfNeeded.right = new FormAttachment(middle, -margin);
    wlCreateIfNeeded.setLayoutData(fdlCreateIfNeeded);
    wCreateIfNeeded = new Button(wContent, SWT.CHECK | SWT.LEFT);
    PropsUi.setLook(wCreateIfNeeded);
    FormData fdCreateIfNeeded = new FormData();
    fdCreateIfNeeded.left = new FormAttachment(middle, 0);
    fdCreateIfNeeded.top = new FormAttachment(wlCreateIfNeeded, 0, SWT.CENTER);
    fdCreateIfNeeded.right = new FormAttachment(100, 0);
    wCreateIfNeeded.setLayoutData(fdCreateIfNeeded);
    lastControl = wCreateIfNeeded;

    Label wlTruncateTable = new Label(wContent, SWT.RIGHT);
    wlTruncateTable.setText(BaseMessages.getString(PKG, "BeamBQOutputDialog.TruncateTable"));
    PropsUi.setLook(wlTruncateTable);
    FormData fdlTruncateTable = new FormData();
    fdlTruncateTable.left = new FormAttachment(0, 0);
    fdlTruncateTable.top = new FormAttachment(lastControl, margin);
    fdlTruncateTable.right = new FormAttachment(middle, -margin);
    wlTruncateTable.setLayoutData(fdlTruncateTable);
    wTruncateTable = new Button(wContent, SWT.CHECK | SWT.LEFT);
    PropsUi.setLook(wTruncateTable);
    FormData fdTruncateTable = new FormData();
    fdTruncateTable.left = new FormAttachment(middle, 0);
    fdTruncateTable.top = new FormAttachment(wlTruncateTable, 0, SWT.CENTER);
    fdTruncateTable.right = new FormAttachment(100, 0);
    wTruncateTable.setLayoutData(fdTruncateTable);
    lastControl = wTruncateTable;

    Label wlFailIfNotEmpty = new Label(wContent, SWT.RIGHT);
    wlFailIfNotEmpty.setText(BaseMessages.getString(PKG, "BeamBQOutputDialog.FailIfNotEmpty"));
    PropsUi.setLook(wlFailIfNotEmpty);
    FormData fdlFailIfNotEmpty = new FormData();
    fdlFailIfNotEmpty.left = new FormAttachment(0, 0);
    fdlFailIfNotEmpty.top = new FormAttachment(lastControl, margin);
    fdlFailIfNotEmpty.right = new FormAttachment(middle, -margin);
    wlFailIfNotEmpty.setLayoutData(fdlFailIfNotEmpty);
    wFailIfNotEmpty = new Button(wContent, SWT.CHECK | SWT.LEFT);
    PropsUi.setLook(wFailIfNotEmpty);
    FormData fdFailIfNotEmpty = new FormData();
    fdFailIfNotEmpty.left = new FormAttachment(middle, 0);
    fdFailIfNotEmpty.top = new FormAttachment(wlFailIfNotEmpty, 0, SWT.CENTER);
    fdFailIfNotEmpty.right = new FormAttachment(100, 0);
    wFailIfNotEmpty.setLayoutData(fdFailIfNotEmpty);

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
    wProjectId.setText(Const.NVL(input.getProjectId(), ""));
    wDatasetId.setText(Const.NVL(input.getDatasetId(), ""));
    wTableId.setText(Const.NVL(input.getTableId(), ""));
    wCreateIfNeeded.setSelection(input.isCreatingIfNeeded());
    wTruncateTable.setSelection(input.isTruncatingTable());
    wFailIfNotEmpty.setSelection(input.isFailingIfNotEmpty());
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
