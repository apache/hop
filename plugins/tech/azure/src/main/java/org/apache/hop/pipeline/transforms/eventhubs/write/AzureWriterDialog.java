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

package org.apache.hop.pipeline.transforms.eventhubs.write;

import org.apache.commons.lang.StringUtils;
import org.apache.hop.core.Const;
import org.apache.hop.core.exception.HopTransformException;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.BaseTransformMeta;
import org.apache.hop.pipeline.transform.ITransformDialog;
import org.apache.hop.ui.core.dialog.BaseDialog;
import org.apache.hop.ui.core.widget.ComboVar;
import org.apache.hop.ui.core.widget.TextVar;
import org.apache.hop.ui.pipeline.transform.BaseTransformDialog;
import org.eclipse.swt.SWT;
import org.eclipse.swt.events.ModifyListener;
import org.eclipse.swt.layout.FormAttachment;
import org.eclipse.swt.layout.FormData;
import org.eclipse.swt.layout.FormLayout;
import org.eclipse.swt.widgets.*;

public class AzureWriterDialog extends BaseTransformDialog implements ITransformDialog {

  private static Class<?> PKG = AzureWriterMeta.class; // for i18n purposes, needed by Translator2!!

  private Text wTransformName;
  private TextVar wNamespace;
  private TextVar wEventHub;
  private TextVar wSasKeyName;
  private TextVar wSasKey;
  private TextVar wBatchSize;
  private ComboVar wMessageField;

  private AzureWriterMeta input;

  public AzureWriterDialog(
      Shell parent,
      IVariables variables,
      Object inputMetadata,
      PipelineMeta transMeta,
      String transformName) {
    super(parent, variables, (BaseTransformMeta) inputMetadata, transMeta, transformName);
    input = (AzureWriterMeta) inputMetadata;
  }

  @Override
  public String open() {
    Shell parent = getParent();

    shell = new Shell(parent, SWT.DIALOG_TRIM | SWT.RESIZE | SWT.MAX | SWT.MIN);
    props.setLook(shell);
    setShellImage(shell, input);

    ModifyListener lsMod = e -> input.setChanged();
    changed = input.hasChanged();

    FormLayout formLayout = new FormLayout();
    formLayout.marginWidth = Const.FORM_MARGIN;
    formLayout.marginHeight = Const.FORM_MARGIN;

    shell.setLayout(formLayout);
    shell.setText("Azure Event Hubs Writer");

    int middle = props.getMiddlePct();
    int margin = Const.MARGIN;

    // Transform name line
    //
    Label wlTransformName = new Label(shell, SWT.RIGHT);
    wlTransformName.setText("Transform name");
    props.setLook(wlTransformName);
    fdlTransformName = new FormData();
    fdlTransformName.left = new FormAttachment(0, 0);
    fdlTransformName.right = new FormAttachment(middle, -margin);
    fdlTransformName.top = new FormAttachment(0, margin);
    wlTransformName.setLayoutData(fdlTransformName);
    wTransformName = new Text(shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    props.setLook(wTransformName);
    wTransformName.addModifyListener(lsMod);
    fdTransformName = new FormData();
    fdTransformName.left = new FormAttachment(middle, 0);
    fdTransformName.top = new FormAttachment(wlTransformName, 0, SWT.CENTER);
    fdTransformName.right = new FormAttachment(100, 0);
    wTransformName.setLayoutData(fdTransformName);
    Control lastControl = wTransformName;

    // Namespace
    //
    Label wlNamespace = new Label(shell, SWT.RIGHT);
    wlNamespace.setText("Event Hubs namespace");
    props.setLook(wlNamespace);
    FormData fdlNamespace = new FormData();
    fdlNamespace.left = new FormAttachment(0, 0);
    fdlNamespace.right = new FormAttachment(middle, -margin);
    fdlNamespace.top = new FormAttachment(lastControl, 2 * margin);
    wlNamespace.setLayoutData(fdlNamespace);
    wNamespace = new TextVar(variables, shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    props.setLook(wNamespace);
    wNamespace.addModifyListener(lsMod);
    FormData fdNamespace = new FormData();
    fdNamespace.left = new FormAttachment(middle, 0);
    fdNamespace.right = new FormAttachment(100, 0);
    fdNamespace.top = new FormAttachment(wlNamespace, 0, SWT.CENTER);
    wNamespace.setLayoutData(fdNamespace);
    lastControl = wNamespace;

    Label wlEventHub = new Label(shell, SWT.RIGHT);
    wlEventHub.setText("Event Hubs instance name");
    props.setLook(wlEventHub);
    FormData fdlEventHub = new FormData();
    fdlEventHub.left = new FormAttachment(0, 0);
    fdlEventHub.right = new FormAttachment(middle, -margin);
    fdlEventHub.top = new FormAttachment(lastControl, 2 * margin);
    wlEventHub.setLayoutData(fdlEventHub);
    wEventHub = new TextVar(variables, shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    props.setLook(wEventHub);
    wEventHub.addModifyListener(lsMod);
    FormData fdEventHub = new FormData();
    fdEventHub.left = new FormAttachment(middle, 0);
    fdEventHub.right = new FormAttachment(100, 0);
    fdEventHub.top = new FormAttachment(wlEventHub, 0, SWT.CENTER);
    wEventHub.setLayoutData(fdEventHub);
    lastControl = wEventHub;

    Label wlSasKeyName = new Label(shell, SWT.RIGHT);
    wlSasKeyName.setText("SAS Policy key name");
    props.setLook(wlSasKeyName);
    FormData fdlSasKeyName = new FormData();
    fdlSasKeyName.left = new FormAttachment(0, 0);
    fdlSasKeyName.right = new FormAttachment(middle, -margin);
    fdlSasKeyName.top = new FormAttachment(lastControl, 2 * margin);
    wlSasKeyName.setLayoutData(fdlSasKeyName);
    wSasKeyName = new TextVar(variables, shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    props.setLook(wSasKeyName);
    wSasKeyName.addModifyListener(lsMod);
    FormData fdSasKeyName = new FormData();
    fdSasKeyName.left = new FormAttachment(middle, 0);
    fdSasKeyName.right = new FormAttachment(100, 0);
    fdSasKeyName.top = new FormAttachment(wlSasKeyName, 0, SWT.CENTER);
    wSasKeyName.setLayoutData(fdSasKeyName);
    lastControl = wSasKeyName;

    Label wlSasKey = new Label(shell, SWT.RIGHT);
    wlSasKey.setText("SAS Key connection string");
    props.setLook(wlSasKey);
    FormData fdlSasKey = new FormData();
    fdlSasKey.left = new FormAttachment(0, 0);
    fdlSasKey.right = new FormAttachment(middle, -margin);
    fdlSasKey.top = new FormAttachment(lastControl, 2 * margin);
    wlSasKey.setLayoutData(fdlSasKey);
    wSasKey = new TextVar(variables, shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    wSasKey.setEchoChar('*');
    props.setLook(wSasKey);
    wSasKey.addModifyListener(lsMod);
    FormData fdSasKey = new FormData();
    fdSasKey.left = new FormAttachment(middle, 0);
    fdSasKey.right = new FormAttachment(100, 0);
    fdSasKey.top = new FormAttachment(wlSasKey, 0, SWT.CENTER);
    wSasKey.setLayoutData(fdSasKey);
    lastControl = wSasKey;

    Label wlBatchSize = new Label(shell, SWT.RIGHT);
    wlBatchSize.setText("Batch size");
    props.setLook(wlBatchSize);
    FormData fdlBatchSize = new FormData();
    fdlBatchSize.left = new FormAttachment(0, 0);
    fdlBatchSize.right = new FormAttachment(middle, -margin);
    fdlBatchSize.top = new FormAttachment(lastControl, 2 * margin);
    wlBatchSize.setLayoutData(fdlBatchSize);
    wBatchSize = new TextVar(variables, shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    props.setLook(wBatchSize);
    wBatchSize.addModifyListener(lsMod);
    FormData fdBatchSize = new FormData();
    fdBatchSize.left = new FormAttachment(middle, 0);
    fdBatchSize.right = new FormAttachment(100, 0);
    fdBatchSize.top = new FormAttachment(wlBatchSize, 0, SWT.CENTER);
    wBatchSize.setLayoutData(fdBatchSize);
    lastControl = wBatchSize;

    Label wlMessageField = new Label(shell, SWT.RIGHT);
    wlMessageField.setText("Message field");
    props.setLook(wlMessageField);
    FormData fdlMessageField = new FormData();
    fdlMessageField.left = new FormAttachment(0, 0);
    fdlMessageField.right = new FormAttachment(middle, -margin);
    fdlMessageField.top = new FormAttachment(lastControl, 2 * margin);
    wlMessageField.setLayoutData(fdlMessageField);
    wMessageField = new ComboVar(variables, shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    props.setLook(wMessageField);
    wMessageField.addModifyListener(lsMod);
    FormData fdMessageField = new FormData();
    fdMessageField.left = new FormAttachment(middle, 0);
    fdMessageField.right = new FormAttachment(100, 0);
    fdMessageField.top = new FormAttachment(wlMessageField, 0, SWT.CENTER);
    wMessageField.setLayoutData(fdMessageField);
    lastControl = wMessageField;

    // Position the buttons at the bottom of the dialog.
    //
    wOk = new Button(shell, SWT.PUSH);
    wOk.setText(BaseMessages.getString(PKG, "System.Button.OK"));
    wOk.addListener(SWT.Selection, e -> ok());
    wCancel = new Button(shell, SWT.PUSH);
    wCancel.setText(BaseMessages.getString(PKG, "System.Button.Cancel"));
    wCancel.addListener(SWT.Selection, e -> ok());
    setButtonPositions(new Button[] {wOk, wCancel}, margin, lastControl);

    getData();

    BaseDialog.defaultShellHandling(shell, c -> ok(), c -> cancel());

    return transformName;
  }

  private void cancel() {
    transformName = null;
    input.setChanged(changed);
    dispose();
  }

  public void getData() {

    wTransformName.setText(Const.NVL(transformName, ""));
    wNamespace.setText(Const.NVL(input.getNamespace(), ""));
    wEventHub.setText(Const.NVL(input.getEventHubName(), ""));
    wSasKeyName.setText(Const.NVL(input.getSasKeyName(), ""));
    wSasKey.setText(Const.NVL(input.getSasKey(), ""));
    wBatchSize.setText(Const.NVL(input.getBatchSize(), ""));
    wMessageField.setText(Const.NVL(input.getMessageField(), ""));

    try {
      wMessageField.setItems(
          pipelineMeta.getPrevTransformFields(variables, transformName).getFieldNames());
    } catch (HopTransformException e) {
      // Ignore
    }
  }

  private void ok() {
    if (StringUtils.isEmpty(wTransformName.getText())) {
      return;
    }
    transformName = wTransformName.getText(); // return value
    input.setNamespace(wNamespace.getText());
    input.setEventHubName(wEventHub.getText());
    input.setBatchSize(wBatchSize.getText());
    input.setSasKeyName(wSasKeyName.getText());
    input.setSasKey(wSasKey.getText());
    input.setMessageField(wMessageField.getText());
    dispose();
  }
}
