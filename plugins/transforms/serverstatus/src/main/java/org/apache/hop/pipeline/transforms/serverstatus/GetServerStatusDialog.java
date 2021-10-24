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

package org.apache.hop.pipeline.transforms.serverstatus;

import org.apache.commons.lang.StringUtils;
import org.apache.hop.core.Const;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.BaseTransformMeta;
import org.apache.hop.pipeline.transform.ITransformDialog;
import org.apache.hop.ui.core.dialog.BaseDialog;
import org.apache.hop.ui.pipeline.transform.BaseTransformDialog;
import org.eclipse.swt.SWT;
import org.eclipse.swt.layout.FormAttachment;
import org.eclipse.swt.layout.FormData;
import org.eclipse.swt.layout.FormLayout;
import org.eclipse.swt.widgets.*;

public class GetServerStatusDialog extends BaseTransformDialog implements ITransformDialog {
  private static Class<?> PKG = GetServerStatusDialog.class; // For Translator
  private final GetServerStatusMeta input;

  int middle;
  int margin;

  private boolean getPreviousFields = false;

  private Combo wServerField;
  private Text wErrorMessage;
  private Text wStatusDescription;
  private Text wServerLoad;
  private Text wMemoryFree;
  private Text wMemoryTotal;
  private Text wCpuCores;
  private Text wCpuProcessTime;
  private Text wOsName;
  private Text wOsVersion;
  private Text wOsArchitecture;
  private Text wActivePipelines;
  private Text wActiveWorkflows;
  private Text wAvailable;
  private Text wResponseNs;

  public GetServerStatusDialog(
      Shell parent, IVariables variables, Object in, PipelineMeta pipelineMeta, String sname) {
    super(parent, variables, (BaseTransformMeta) in, pipelineMeta, sname);
    input = (GetServerStatusMeta) in;
  }

  @Override
  public String open() {
    Shell parent = getParent();

    shell = new Shell(parent, SWT.DIALOG_TRIM | SWT.RESIZE | SWT.MAX | SWT.MIN);
    props.setLook(shell);
    setShellImage(shell, input);

    FormLayout formLayout = new FormLayout();
    formLayout.marginWidth = Const.FORM_MARGIN;
    formLayout.marginHeight = Const.FORM_MARGIN;

    shell.setLayout(formLayout);
    shell.setText(BaseMessages.getString(PKG, "GetServerStatus.Transform.Name"));

    middle = props.getMiddlePct();
    margin = Const.MARGIN;

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

    Label wlServerField = new Label(shell, SWT.RIGHT);
    wlServerField.setText(BaseMessages.getString(PKG, "GetServerStatusDialog.ServerField"));
    props.setLook(wlServerField);
    FormData fdlServerField = new FormData();
    fdlServerField.left = new FormAttachment(0, 0);
    fdlServerField.top = new FormAttachment(lastControl, margin);
    fdlServerField.right = new FormAttachment(middle, -margin);
    wlServerField.setLayoutData(fdlServerField);
    wServerField = new Combo(shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    props.setLook(wServerField);
    FormData fdServerField = new FormData();
    fdServerField.left = new FormAttachment(middle, 0);
    fdServerField.top = new FormAttachment(wlServerField, 0, SWT.CENTER);
    fdServerField.right = new FormAttachment(100, 0);
    wServerField.setLayoutData(fdServerField);
    lastControl = wServerField;

    Label wlErrorMessage = new Label(shell, SWT.RIGHT);
    wlErrorMessage.setText(BaseMessages.getString(PKG, "GetServerStatusDialog.ErrorMessage"));
    props.setLook(wlErrorMessage);
    FormData fdlErrorMessage = new FormData();
    fdlErrorMessage.left = new FormAttachment(0, 0);
    fdlErrorMessage.top = new FormAttachment(lastControl, margin);
    fdlErrorMessage.right = new FormAttachment(middle, -margin);
    wlErrorMessage.setLayoutData(fdlErrorMessage);
    wErrorMessage = new Text(shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    props.setLook(wErrorMessage);
    FormData fdErrorMessage = new FormData();
    fdErrorMessage.left = new FormAttachment(middle, 0);
    fdErrorMessage.top = new FormAttachment(wlErrorMessage, 0, SWT.CENTER);
    fdErrorMessage.right = new FormAttachment(100, 0);
    wErrorMessage.setLayoutData(fdErrorMessage);
    lastControl = wErrorMessage;

    Label wlStatusDescription = new Label(shell, SWT.RIGHT);
    wlStatusDescription.setText(
        BaseMessages.getString(PKG, "GetServerStatusDialog.StatusDescription"));
    props.setLook(wlStatusDescription);
    FormData fdlStatusDescription = new FormData();
    fdlStatusDescription.left = new FormAttachment(0, 0);
    fdlStatusDescription.top = new FormAttachment(lastControl, margin);
    fdlStatusDescription.right = new FormAttachment(middle, -margin);
    wlStatusDescription.setLayoutData(fdlStatusDescription);
    wStatusDescription = new Text(shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    props.setLook(wStatusDescription);
    FormData fdStatusDescription = new FormData();
    fdStatusDescription.left = new FormAttachment(middle, 0);
    fdStatusDescription.top = new FormAttachment(wlStatusDescription, 0, SWT.CENTER);
    fdStatusDescription.right = new FormAttachment(100, 0);
    wStatusDescription.setLayoutData(fdStatusDescription);
    lastControl = wStatusDescription;

    Label wlServerLoad = new Label(shell, SWT.RIGHT);
    wlServerLoad.setText(BaseMessages.getString(PKG, "GetServerStatusDialog.ServerLoad"));
    props.setLook(wlServerLoad);
    FormData fdlServerLoad = new FormData();
    fdlServerLoad.left = new FormAttachment(0, 0);
    fdlServerLoad.top = new FormAttachment(lastControl, margin);
    fdlServerLoad.right = new FormAttachment(middle, -margin);
    wlServerLoad.setLayoutData(fdlServerLoad);
    wServerLoad = new Text(shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    props.setLook(wServerLoad);
    FormData fdServerLoad = new FormData();
    fdServerLoad.left = new FormAttachment(middle, 0);
    fdServerLoad.top = new FormAttachment(wlServerLoad, 0, SWT.CENTER);
    fdServerLoad.right = new FormAttachment(100, 0);
    wServerLoad.setLayoutData(fdServerLoad);
    lastControl = wServerLoad;

    Label wlMemoryFree = new Label(shell, SWT.RIGHT);
    wlMemoryFree.setText(BaseMessages.getString(PKG, "GetServerStatusDialog.MemoryFree"));
    props.setLook(wlMemoryFree);
    FormData fdlMemoryFree = new FormData();
    fdlMemoryFree.left = new FormAttachment(0, 0);
    fdlMemoryFree.top = new FormAttachment(lastControl, margin);
    fdlMemoryFree.right = new FormAttachment(middle, -margin);
    wlMemoryFree.setLayoutData(fdlMemoryFree);
    wMemoryFree = new Text(shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    props.setLook(wMemoryFree);
    FormData fdMemoryFree = new FormData();
    fdMemoryFree.left = new FormAttachment(middle, 0);
    fdMemoryFree.top = new FormAttachment(wlMemoryFree, 0, SWT.CENTER);
    fdMemoryFree.right = new FormAttachment(100, 0);
    wMemoryFree.setLayoutData(fdMemoryFree);
    lastControl = wMemoryFree;

    Label wlMemoryTotal = new Label(shell, SWT.RIGHT);
    wlMemoryTotal.setText(BaseMessages.getString(PKG, "GetServerStatusDialog.MemoryTotal"));
    props.setLook(wlMemoryTotal);
    FormData fdlMemoryTotal = new FormData();
    fdlMemoryTotal.left = new FormAttachment(0, 0);
    fdlMemoryTotal.top = new FormAttachment(lastControl, margin);
    fdlMemoryTotal.right = new FormAttachment(middle, -margin);
    wlMemoryTotal.setLayoutData(fdlMemoryTotal);
    wMemoryTotal = new Text(shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    props.setLook(wMemoryTotal);
    FormData fdMemoryTotal = new FormData();
    fdMemoryTotal.left = new FormAttachment(middle, 0);
    fdMemoryTotal.top = new FormAttachment(wlMemoryTotal, 0, SWT.CENTER);
    fdMemoryTotal.right = new FormAttachment(100, 0);
    wMemoryTotal.setLayoutData(fdMemoryTotal);
    lastControl = wMemoryTotal;

    Label wlCpuCores = new Label(shell, SWT.RIGHT);
    wlCpuCores.setText(BaseMessages.getString(PKG, "GetServerStatusDialog.CpuCores"));
    props.setLook(wlCpuCores);
    FormData fdlCpuCores = new FormData();
    fdlCpuCores.left = new FormAttachment(0, 0);
    fdlCpuCores.top = new FormAttachment(lastControl, margin);
    fdlCpuCores.right = new FormAttachment(middle, -margin);
    wlCpuCores.setLayoutData(fdlCpuCores);
    wCpuCores = new Text(shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    props.setLook(wCpuCores);
    FormData fdCpuCores = new FormData();
    fdCpuCores.left = new FormAttachment(middle, 0);
    fdCpuCores.top = new FormAttachment(wlCpuCores, 0, SWT.CENTER);
    fdCpuCores.right = new FormAttachment(100, 0);
    wCpuCores.setLayoutData(fdCpuCores);
    lastControl = wCpuCores;

    Label wlCpuProcessTime = new Label(shell, SWT.RIGHT);
    wlCpuProcessTime.setText(BaseMessages.getString(PKG, "GetServerStatusDialog.CpuProcessTime"));
    props.setLook(wlCpuProcessTime);
    FormData fdlCpuProcessTime = new FormData();
    fdlCpuProcessTime.left = new FormAttachment(0, 0);
    fdlCpuProcessTime.top = new FormAttachment(lastControl, margin);
    fdlCpuProcessTime.right = new FormAttachment(middle, -margin);
    wlCpuProcessTime.setLayoutData(fdlCpuProcessTime);
    wCpuProcessTime = new Text(shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    props.setLook(wCpuProcessTime);
    FormData fdCpuProcessTime = new FormData();
    fdCpuProcessTime.left = new FormAttachment(middle, 0);
    fdCpuProcessTime.top = new FormAttachment(wlCpuProcessTime, 0, SWT.CENTER);
    fdCpuProcessTime.right = new FormAttachment(100, 0);
    wCpuProcessTime.setLayoutData(fdCpuProcessTime);
    lastControl = wCpuProcessTime;

    Label wlOsName = new Label(shell, SWT.RIGHT);
    wlOsName.setText(BaseMessages.getString(PKG, "GetServerStatusDialog.OsName"));
    props.setLook(wlOsName);
    FormData fdlOsName = new FormData();
    fdlOsName.left = new FormAttachment(0, 0);
    fdlOsName.top = new FormAttachment(lastControl, margin);
    fdlOsName.right = new FormAttachment(middle, -margin);
    wlOsName.setLayoutData(fdlOsName);
    wOsName = new Text(shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    props.setLook(wOsName);
    FormData fdOsName = new FormData();
    fdOsName.left = new FormAttachment(middle, 0);
    fdOsName.top = new FormAttachment(wlOsName, 0, SWT.CENTER);
    fdOsName.right = new FormAttachment(100, 0);
    wOsName.setLayoutData(fdOsName);
    lastControl = wOsName;

    Label wlOsVersion = new Label(shell, SWT.RIGHT);
    wlOsVersion.setText(BaseMessages.getString(PKG, "GetServerStatusDialog.OsVersion"));
    props.setLook(wlOsVersion);
    FormData fdlOsVersion = new FormData();
    fdlOsVersion.left = new FormAttachment(0, 0);
    fdlOsVersion.top = new FormAttachment(lastControl, margin);
    fdlOsVersion.right = new FormAttachment(middle, -margin);
    wlOsVersion.setLayoutData(fdlOsVersion);
    wOsVersion = new Text(shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    props.setLook(wOsVersion);
    FormData fdOsVersion = new FormData();
    fdOsVersion.left = new FormAttachment(middle, 0);
    fdOsVersion.top = new FormAttachment(wlOsVersion, 0, SWT.CENTER);
    fdOsVersion.right = new FormAttachment(100, 0);
    wOsVersion.setLayoutData(fdOsVersion);
    lastControl = wOsVersion;

    Label wlOsArchitecture = new Label(shell, SWT.RIGHT);
    wlOsArchitecture.setText(BaseMessages.getString(PKG, "GetServerStatusDialog.OsArchitecture"));
    props.setLook(wlOsArchitecture);
    FormData fdlOsArchitecture = new FormData();
    fdlOsArchitecture.left = new FormAttachment(0, 0);
    fdlOsArchitecture.top = new FormAttachment(lastControl, margin);
    fdlOsArchitecture.right = new FormAttachment(middle, -margin);
    wlOsArchitecture.setLayoutData(fdlOsArchitecture);
    wOsArchitecture = new Text(shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    props.setLook(wOsArchitecture);
    FormData fdOsArchitecture = new FormData();
    fdOsArchitecture.left = new FormAttachment(middle, 0);
    fdOsArchitecture.top = new FormAttachment(wlOsArchitecture, 0, SWT.CENTER);
    fdOsArchitecture.right = new FormAttachment(100, 0);
    wOsArchitecture.setLayoutData(fdOsArchitecture);
    lastControl = wOsArchitecture;

    Label wlActivePipelines = new Label(shell, SWT.RIGHT);
    wlActivePipelines.setText(BaseMessages.getString(PKG, "GetServerStatusDialog.ActivePipelines"));
    props.setLook(wlActivePipelines);
    FormData fdlActivePipelines = new FormData();
    fdlActivePipelines.left = new FormAttachment(0, 0);
    fdlActivePipelines.top = new FormAttachment(lastControl, margin);
    fdlActivePipelines.right = new FormAttachment(middle, -margin);
    wlActivePipelines.setLayoutData(fdlActivePipelines);
    wActivePipelines = new Text(shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    props.setLook(wActivePipelines);
    FormData fdActivePipelines = new FormData();
    fdActivePipelines.left = new FormAttachment(middle, 0);
    fdActivePipelines.top = new FormAttachment(wlActivePipelines, 0, SWT.CENTER);
    fdActivePipelines.right = new FormAttachment(100, 0);
    wActivePipelines.setLayoutData(fdActivePipelines);
    lastControl = wActivePipelines;

    Label wlActiveWorkflows = new Label(shell, SWT.RIGHT);
    wlActiveWorkflows.setText(BaseMessages.getString(PKG, "GetServerStatusDialog.ActiveWorkflows"));
    props.setLook(wlActiveWorkflows);
    FormData fdlActiveWorkflows = new FormData();
    fdlActiveWorkflows.left = new FormAttachment(0, 0);
    fdlActiveWorkflows.top = new FormAttachment(lastControl, margin);
    fdlActiveWorkflows.right = new FormAttachment(middle, -margin);
    wlActiveWorkflows.setLayoutData(fdlActiveWorkflows);
    wActiveWorkflows = new Text(shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    props.setLook(wActiveWorkflows);
    FormData fdActiveWorkflows = new FormData();
    fdActiveWorkflows.left = new FormAttachment(middle, 0);
    fdActiveWorkflows.top = new FormAttachment(wlActiveWorkflows, 0, SWT.CENTER);
    fdActiveWorkflows.right = new FormAttachment(100, 0);
    wActiveWorkflows.setLayoutData(fdActiveWorkflows);
    lastControl = wActiveWorkflows;

    Label wlAvailable = new Label(shell, SWT.RIGHT);
    wlAvailable.setText(BaseMessages.getString(PKG, "GetServerStatusDialog.Available"));
    props.setLook(wlAvailable);
    FormData fdlAvailable = new FormData();
    fdlAvailable.left = new FormAttachment(0, 0);
    fdlAvailable.top = new FormAttachment(lastControl, margin);
    fdlAvailable.right = new FormAttachment(middle, -margin);
    wlAvailable.setLayoutData(fdlAvailable);
    wAvailable = new Text(shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    props.setLook(wAvailable);
    FormData fdAvailable = new FormData();
    fdAvailable.left = new FormAttachment(middle, 0);
    fdAvailable.top = new FormAttachment(wlAvailable, 0, SWT.CENTER);
    fdAvailable.right = new FormAttachment(100, 0);
    wAvailable.setLayoutData(fdAvailable);
    lastControl = wAvailable;

    Label wlResponseNs = new Label(shell, SWT.RIGHT);
    wlResponseNs.setText(BaseMessages.getString(PKG, "GetServerStatusDialog.ResponseNs"));
    props.setLook(wlResponseNs);
    FormData fdlResponseNs = new FormData();
    fdlResponseNs.left = new FormAttachment(0, 0);
    fdlResponseNs.top = new FormAttachment(lastControl, margin);
    fdlResponseNs.right = new FormAttachment(middle, -margin);
    wlResponseNs.setLayoutData(fdlResponseNs);
    wResponseNs = new Text(shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    props.setLook(wResponseNs);
    FormData fdResponseNs = new FormData();
    fdResponseNs.left = new FormAttachment(middle, 0);
    fdResponseNs.top = new FormAttachment(wlResponseNs, 0, SWT.CENTER);
    fdResponseNs.right = new FormAttachment(100, 0);
    wResponseNs.setLayoutData(fdResponseNs);
    lastControl = wResponseNs;

    wOk = new Button(shell, SWT.PUSH);
    wOk.setText(BaseMessages.getString(PKG, "System.Button.OK"));
    wOk.addListener(SWT.Selection, e -> ok());
    wCancel = new Button(shell, SWT.PUSH);
    wCancel.setText(BaseMessages.getString(PKG, "System.Button.Cancel"));
    wCancel.addListener(SWT.Selection, e -> cancel());
    setButtonPositions(new Button[] {wOk, wCancel}, margin, lastControl);

    // Get field names...
    //
    try {
      wServerField.setItems(
          pipelineMeta.getPrevTransformFields(variables, transformName).getFieldNames());
    } catch (Exception e) {
      log.logError("Error getting field names", e);
    }

    getData();

    BaseDialog.defaultShellHandling(shell, c -> ok(), c -> cancel());

    return transformName;
  }

  /** Populate the widgets. */
  public void getData() {
    wTransformName.setText(transformName);

    wServerField.setText(Const.NVL(input.getServerField(), ""));
    wErrorMessage.setText(Const.NVL(input.getErrorMessageField(), ""));
    wStatusDescription.setText(Const.NVL(input.getStatusDescriptionField(), ""));
    wServerLoad.setText(Const.NVL(input.getServerLoadField(), ""));
    wMemoryFree.setText(Const.NVL(input.getMemoryFreeField(), ""));
    wMemoryTotal.setText(Const.NVL(input.getMemoryTotalField(), ""));
    wCpuCores.setText(Const.NVL(input.getCpuCoresField(), ""));
    wCpuProcessTime.setText(Const.NVL(input.getCpuProcessTimeField(), ""));
    wOsName.setText(Const.NVL(input.getOsNameField(), ""));
    wOsVersion.setText(Const.NVL(input.getOsVersionField(), ""));
    wOsArchitecture.setText(Const.NVL(input.getOsArchitectureField(), ""));
    wActivePipelines.setText(Const.NVL(input.getActivePipelinesField(), ""));
    wActiveWorkflows.setText(Const.NVL(input.getActiveWorkflowsField(), ""));
    wAvailable.setText(Const.NVL(input.getAvailableField(), ""));
    wResponseNs.setText(Const.NVL(input.getResponseNsField(), ""));

    wTransformName.selectAll();
    wTransformName.setFocus();
  }

  private void cancel() {
    transformName = null;
    dispose();
  }

  private void ok() {
    if (StringUtils.isEmpty(wTransformName.getText())) {
      return;
    }

    getInfo(input);
    dispose();
  }

  private void getInfo(GetServerStatusMeta in) {
    transformName = wTransformName.getText(); // return value

    in.setServerField(wServerField.getText());
    in.setErrorMessageField(wErrorMessage.getText());
    in.setStatusDescriptionField(wStatusDescription.getText());
    in.setServerLoadField(wServerLoad.getText());
    in.setMemoryFreeField(wMemoryFree.getText());
    in.setMemoryTotalField(wMemoryTotal.getText());
    in.setCpuCoresField(wCpuCores.getText());
    in.setCpuProcessTimeField(wCpuProcessTime.getText());
    in.setOsNameField(wOsName.getText());
    in.setOsVersionField(wOsVersion.getText());
    in.setOsArchitectureField(wOsArchitecture.getText());
    in.setActivePipelinesField(wActivePipelines.getText());
    in.setActiveWorkflowsField(wActiveWorkflows.getText());
    in.setAvailableField(wAvailable.getText());
    in.setResponseNsField(wResponseNs.getText());

    in.setChanged();
  }
}
