/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

package org.apache.hop.pipeline.transforms.execinfo;

import org.apache.hop.core.Const;
import org.apache.hop.core.util.Utils;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.execution.ExecutionInfoLocation;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.ui.core.PropsUi;
import org.apache.hop.ui.core.dialog.BaseDialog;
import org.apache.hop.ui.core.widget.LabelComboVar;
import org.apache.hop.ui.core.widget.MetaSelectionLine;
import org.apache.hop.ui.pipeline.transform.BaseTransformDialog;
import org.eclipse.swt.SWT;
import org.eclipse.swt.custom.CCombo;
import org.eclipse.swt.custom.ScrolledComposite;
import org.eclipse.swt.graphics.Rectangle;
import org.eclipse.swt.layout.FillLayout;
import org.eclipse.swt.layout.FormAttachment;
import org.eclipse.swt.layout.FormData;
import org.eclipse.swt.layout.FormLayout;
import org.eclipse.swt.widgets.Composite;
import org.eclipse.swt.widgets.Control;
import org.eclipse.swt.widgets.Label;
import org.eclipse.swt.widgets.Shell;

public class ExecInfoDialog extends BaseTransformDialog {
  private static final Class<?> PKG = ExecInfoMeta.class;

  private MetaSelectionLine<ExecutionInfoLocation> wLocation;
  private CCombo wOperationType;

  private LabelComboVar wFieldId;
  private LabelComboVar wFieldParentId;
  private LabelComboVar wFieldName;
  private LabelComboVar wFieldType;
  private LabelComboVar wFieldChildren;
  private LabelComboVar wFieldLimit;

  private final ExecInfoMeta input;

  public ExecInfoDialog(
      Shell parent, IVariables variables, ExecInfoMeta transformMeta, PipelineMeta pipelineMeta) {
    super(parent, variables, transformMeta, pipelineMeta);
    this.input = transformMeta;
  }

  @Override
  public String open() {
    createShell(BaseMessages.getString(PKG, "ExecInfoDialog.Shell.Title"));

    buildButtonBar().ok(e -> ok()).cancel(e -> cancel()).build();

    ScrolledComposite sc = new ScrolledComposite(shell, SWT.V_SCROLL | SWT.H_SCROLL);
    PropsUi.setLook(sc);
    FormData fdSc = new FormData();
    fdSc.left = new FormAttachment(0, 0);
    fdSc.top = new FormAttachment(wSpacer, 0);
    fdSc.right = new FormAttachment(100, 0);
    fdSc.bottom = new FormAttachment(wOk, -margin);
    sc.setLayoutData(fdSc);
    sc.setLayout(new FillLayout());

    Composite wContent = new Composite(sc, SWT.NONE);
    PropsUi.setLook(wContent);
    FormLayout contentLayout = new FormLayout();
    contentLayout.marginWidth = PropsUi.getFormMargin();
    contentLayout.marginHeight = PropsUi.getFormMargin();
    wContent.setLayout(contentLayout);

    Control lastControl = wContent;

    wLocation =
        new MetaSelectionLine<>(
            variables,
            metadataProvider,
            ExecutionInfoLocation.class,
            wContent,
            SWT.NONE,
            BaseMessages.getString(PKG, "ExecInfoDialog.Location.Label"),
            BaseMessages.getString(PKG, "ExecInfoDialog.Location.Tooltip"));
    PropsUi.setLook(wLocation);
    FormData fdLocation = new FormData();
    fdLocation.left = new FormAttachment(0, 0);
    fdLocation.top = new FormAttachment(lastControl, margin);
    fdLocation.right = new FormAttachment(100, 0);
    wLocation.setLayoutData(fdLocation);
    lastControl = wLocation;

    Label wlOperationType = new Label(wContent, SWT.RIGHT);
    wlOperationType.setText(BaseMessages.getString(PKG, "ExecInfoDialog.OperationType.Label"));
    wlOperationType.setToolTipText(
        BaseMessages.getString(PKG, "ExecInfoDialog.OperationType.Tooltip"));
    PropsUi.setLook(wlOperationType);
    FormData fdlOperationType = new FormData();
    fdlOperationType.left = new FormAttachment(0, 0);
    fdlOperationType.top = new FormAttachment(lastControl, margin);
    fdlOperationType.right = new FormAttachment(middle, -margin);
    wlOperationType.setLayoutData(fdlOperationType);
    wOperationType = new CCombo(wContent, SWT.LEFT | SWT.BORDER);
    wOperationType.setToolTipText(
        BaseMessages.getString(PKG, "ExecInfoDialog.OperationType.Tooltip"));
    PropsUi.setLook(wOperationType);
    FormData fdOperationType = new FormData();
    fdOperationType.left = new FormAttachment(middle, 0);
    fdOperationType.top = new FormAttachment(lastControl, margin);
    fdOperationType.right = new FormAttachment(100, 0);
    wOperationType.setLayoutData(fdOperationType);
    wOperationType.addListener(SWT.Selection, e -> enableFields());
    lastControl = wOperationType;

    wFieldId =
        new LabelComboVar(
            variables,
            wContent,
            BaseMessages.getString(PKG, "ExecInfoDialog.FieldId.Label"),
            BaseMessages.getString(PKG, "ExecInfoDialog.FieldId.Tooltip"));
    PropsUi.setLook(wFieldId);
    FormData fdFieldId = new FormData();
    fdFieldId.left = new FormAttachment(0, 0);
    fdFieldId.top = new FormAttachment(lastControl, margin);
    fdFieldId.right = new FormAttachment(100, 0);
    wFieldId.setLayoutData(fdFieldId);
    lastControl = wFieldId;

    wFieldParentId =
        new LabelComboVar(
            variables,
            wContent,
            BaseMessages.getString(PKG, "ExecInfoDialog.FieldParentId.Label"),
            BaseMessages.getString(PKG, "ExecInfoDialog.FieldParentId.Tooltip"));
    PropsUi.setLook(wFieldParentId);
    FormData fdFieldParentId = new FormData();
    fdFieldParentId.left = new FormAttachment(0, 0);
    fdFieldParentId.top = new FormAttachment(lastControl, margin);
    fdFieldParentId.right = new FormAttachment(100, 0);
    wFieldParentId.setLayoutData(fdFieldParentId);
    lastControl = wFieldParentId;

    wFieldName =
        new LabelComboVar(
            variables,
            wContent,
            BaseMessages.getString(PKG, "ExecInfoDialog.FieldName.Label"),
            BaseMessages.getString(PKG, "ExecInfoDialog.FieldName.Tooltip"));
    PropsUi.setLook(wFieldName);
    FormData fdFieldName = new FormData();
    fdFieldName.left = new FormAttachment(0, 0);
    fdFieldName.top = new FormAttachment(lastControl, margin);
    fdFieldName.right = new FormAttachment(100, 0);
    wFieldName.setLayoutData(fdFieldName);
    lastControl = wFieldName;

    wFieldType =
        new LabelComboVar(
            variables,
            wContent,
            BaseMessages.getString(PKG, "ExecInfoDialog.FieldType.Label"),
            BaseMessages.getString(PKG, "ExecInfoDialog.FieldType.Tooltip"));
    PropsUi.setLook(wFieldType);
    FormData fdFieldType = new FormData();
    fdFieldType.left = new FormAttachment(0, 0);
    fdFieldType.top = new FormAttachment(lastControl, margin);
    fdFieldType.right = new FormAttachment(100, 0);
    wFieldType.setLayoutData(fdFieldType);
    lastControl = wFieldType;

    wFieldChildren =
        new LabelComboVar(
            variables,
            wContent,
            BaseMessages.getString(PKG, "ExecInfoDialog.FieldChildren.Label"),
            BaseMessages.getString(PKG, "ExecInfoDialog.FieldChildren.Tooltip"));
    PropsUi.setLook(wFieldChildren);
    FormData fdFieldChildren = new FormData();
    fdFieldChildren.left = new FormAttachment(0, 0);
    fdFieldChildren.top = new FormAttachment(lastControl, margin);
    fdFieldChildren.right = new FormAttachment(100, 0);
    wFieldChildren.setLayoutData(fdFieldChildren);
    lastControl = wFieldChildren;

    wFieldLimit =
        new LabelComboVar(
            variables,
            wContent,
            BaseMessages.getString(PKG, "ExecInfoDialog.FieldLimit.Label"),
            BaseMessages.getString(PKG, "ExecInfoDialog.FieldLimit.Tooltip"));
    PropsUi.setLook(wFieldLimit);
    FormData fdFieldLimit = new FormData();
    fdFieldLimit.left = new FormAttachment(0, 0);
    fdFieldLimit.top = new FormAttachment(lastControl, margin);
    fdFieldLimit.right = new FormAttachment(100, 0);
    wFieldLimit.setLayoutData(fdFieldLimit);
    lastControl = wFieldLimit;

    wContent.pack();
    Rectangle bounds = wContent.getBounds();
    sc.setContent(wContent);
    sc.setExpandHorizontal(true);
    sc.setExpandVertical(true);
    sc.setMinWidth(bounds.width);
    sc.setMinHeight(bounds.height);

    getData();
    enableFields();
    input.setChanged(changed);
    focusTransformName();
    BaseDialog.defaultShellHandling(shell, c -> ok(), c -> cancel());

    return transformName;
  }

  /** Copy information from the meta-data input to the dialog fields. */
  public void getData() {

    // Location
    //
    try {
      wLocation.fillItems();
    } catch (Exception e) {
      log.logError("Error getting a list of execution information locations", e);
    }
    wLocation.setText(Const.NVL(input.getLocation(), ""));

    // Operation type
    //
    wOperationType.setItems(ExecInfoMeta.OperationType.getDescriptions());
    wOperationType.setText(
        input.getOperationType() == null ? "" : input.getOperationType().getDescription());

    String[] fieldNames;
    try {
      fieldNames = pipelineMeta.getPrevTransformFields(variables, transformName).getFieldNames();
    } catch (Exception e) {
      // Ignore
      fieldNames = new String[] {};
    }

    wFieldId.setItems(fieldNames);
    wFieldId.setText(Const.NVL(input.getIdFieldName(), ""));
    wFieldParentId.setItems(fieldNames);
    wFieldParentId.setText(Const.NVL(input.getParentIdFieldName(), ""));
    wFieldChildren.setItems(fieldNames);
    wFieldChildren.setText(Const.NVL(input.getIncludeChildrenFieldName(), ""));
    wFieldName.setItems(fieldNames);
    wFieldName.setText(Const.NVL(input.getNameFieldName(), ""));
    wFieldType.setItems(fieldNames);
    wFieldType.setText(Const.NVL(input.getTypeFieldName(), ""));
    wFieldLimit.setItems(fieldNames);
    wFieldLimit.setText(Const.NVL(input.getLimitFieldName(), ""));

    enableFields();
  }

  private void enableFields() {
    ExecInfoMeta.OperationType operationType =
        ExecInfoMeta.OperationType.getTypeByDescription(wOperationType.getText());

    wFieldId.setEnabled(operationType != null && operationType.isAcceptingExecutionId());
    wFieldParentId.setEnabled(
        operationType != null && operationType.isAcceptingParentExecutionId());
    wFieldName.setEnabled(operationType != null && operationType.isAcceptingName());
    wFieldType.setEnabled(operationType != null && operationType.isAcceptingExecutionType());
    wFieldChildren.setEnabled(operationType != null && operationType.isAcceptingIncludeChildren());
    wFieldLimit.setEnabled(operationType != null && operationType.isAcceptingLimit());
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
    input.setLocation(wLocation.getText());
    input.setOperationType(
        ExecInfoMeta.OperationType.getTypeByDescription(wOperationType.getText()));
    input.setIdFieldName(wFieldId.getText());
    input.setParentIdFieldName(wFieldParentId.getText());
    input.setNameFieldName(wFieldName.getText());
    input.setTypeFieldName(wFieldType.getText());
    input.setIncludeChildrenFieldName(wFieldChildren.getText());
    input.setLimitFieldName(wFieldLimit.getText());

    transformName = wTransformName.getText(); // return value
    dispose();
  }
}
