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
import org.apache.hop.ui.core.PropsUi;
import org.apache.hop.ui.core.dialog.BaseDialog;
import org.apache.hop.ui.core.widget.ComboVar;
import org.apache.hop.ui.core.widget.TextVar;
import org.apache.hop.ui.pipeline.transform.BaseTransformDialog;
import org.eclipse.swt.SWT;
import org.eclipse.swt.custom.ScrolledComposite;
import org.eclipse.swt.events.ModifyListener;
import org.eclipse.swt.graphics.Rectangle;
import org.eclipse.swt.layout.FillLayout;
import org.eclipse.swt.layout.FormAttachment;
import org.eclipse.swt.layout.FormData;
import org.eclipse.swt.layout.FormLayout;
import org.eclipse.swt.widgets.Composite;
import org.eclipse.swt.widgets.Control;
import org.eclipse.swt.widgets.Label;
import org.eclipse.swt.widgets.Shell;

public class AzureWriterDialog extends BaseTransformDialog {

  private static final Class<?> PKG =
      AzureWriterMeta.class; // for i18n purposes, needed by Translator2!!

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
      AzureWriterMeta inputMetadata,
      PipelineMeta pipelineMeta) {
    super(parent, variables, inputMetadata, pipelineMeta);
    input = inputMetadata;
  }

  @Override
  public String open() {
    createShell(BaseMessages.getString(PKG, "AzureWriterDialog.Shell.Title"));

    buildButtonBar().ok(e -> ok()).cancel(e -> cancel()).build();

    ModifyListener lsMod = e -> input.setChanged();
    changed = input.hasChanged();

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

    // Namespace
    //
    Label wlNamespace = new Label(wContent, SWT.RIGHT);
    wlNamespace.setText("Event Hubs namespace");
    PropsUi.setLook(wlNamespace);
    FormData fdlNamespace = new FormData();
    fdlNamespace.left = new FormAttachment(0, 0);
    fdlNamespace.right = new FormAttachment(middle, -margin);
    fdlNamespace.top = new FormAttachment(lastControl, margin);
    wlNamespace.setLayoutData(fdlNamespace);
    wNamespace = new TextVar(variables, wContent, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    PropsUi.setLook(wNamespace);
    wNamespace.addModifyListener(lsMod);
    FormData fdNamespace = new FormData();
    fdNamespace.left = new FormAttachment(middle, 0);
    fdNamespace.right = new FormAttachment(100, 0);
    fdNamespace.top = new FormAttachment(wlNamespace, 0, SWT.CENTER);
    wNamespace.setLayoutData(fdNamespace);
    lastControl = wNamespace;

    Label wlEventHub = new Label(wContent, SWT.RIGHT);
    wlEventHub.setText("Event Hubs instance name");
    PropsUi.setLook(wlEventHub);
    FormData fdlEventHub = new FormData();
    fdlEventHub.left = new FormAttachment(0, 0);
    fdlEventHub.right = new FormAttachment(middle, -margin);
    fdlEventHub.top = new FormAttachment(lastControl, margin);
    wlEventHub.setLayoutData(fdlEventHub);
    wEventHub = new TextVar(variables, wContent, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    PropsUi.setLook(wEventHub);
    wEventHub.addModifyListener(lsMod);
    FormData fdEventHub = new FormData();
    fdEventHub.left = new FormAttachment(middle, 0);
    fdEventHub.right = new FormAttachment(100, 0);
    fdEventHub.top = new FormAttachment(wlEventHub, 0, SWT.CENTER);
    wEventHub.setLayoutData(fdEventHub);
    lastControl = wEventHub;

    Label wlSasKeyName = new Label(wContent, SWT.RIGHT);
    wlSasKeyName.setText("SAS Policy key name");
    PropsUi.setLook(wlSasKeyName);
    FormData fdlSasKeyName = new FormData();
    fdlSasKeyName.left = new FormAttachment(0, 0);
    fdlSasKeyName.right = new FormAttachment(middle, -margin);
    fdlSasKeyName.top = new FormAttachment(lastControl, margin);
    wlSasKeyName.setLayoutData(fdlSasKeyName);
    wSasKeyName = new TextVar(variables, wContent, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    PropsUi.setLook(wSasKeyName);
    wSasKeyName.addModifyListener(lsMod);
    FormData fdSasKeyName = new FormData();
    fdSasKeyName.left = new FormAttachment(middle, 0);
    fdSasKeyName.right = new FormAttachment(100, 0);
    fdSasKeyName.top = new FormAttachment(wlSasKeyName, 0, SWT.CENTER);
    wSasKeyName.setLayoutData(fdSasKeyName);
    lastControl = wSasKeyName;

    Label wlSasKey = new Label(wContent, SWT.RIGHT);
    wlSasKey.setText("SAS Key connection string");
    PropsUi.setLook(wlSasKey);
    FormData fdlSasKey = new FormData();
    fdlSasKey.left = new FormAttachment(0, 0);
    fdlSasKey.right = new FormAttachment(middle, -margin);
    fdlSasKey.top = new FormAttachment(lastControl, margin);
    wlSasKey.setLayoutData(fdlSasKey);
    wSasKey = new TextVar(variables, wContent, SWT.SINGLE | SWT.LEFT | SWT.BORDER | SWT.PASSWORD);
    PropsUi.setLook(wSasKey);
    wSasKey.addModifyListener(lsMod);
    FormData fdSasKey = new FormData();
    fdSasKey.left = new FormAttachment(middle, 0);
    fdSasKey.right = new FormAttachment(100, 0);
    fdSasKey.top = new FormAttachment(wlSasKey, 0, SWT.CENTER);
    wSasKey.setLayoutData(fdSasKey);
    lastControl = wSasKey;

    Label wlBatchSize = new Label(wContent, SWT.RIGHT);
    wlBatchSize.setText("Batch size");
    PropsUi.setLook(wlBatchSize);
    FormData fdlBatchSize = new FormData();
    fdlBatchSize.left = new FormAttachment(0, 0);
    fdlBatchSize.right = new FormAttachment(middle, -margin);
    fdlBatchSize.top = new FormAttachment(lastControl, margin);
    wlBatchSize.setLayoutData(fdlBatchSize);
    wBatchSize = new TextVar(variables, wContent, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    PropsUi.setLook(wBatchSize);
    wBatchSize.addModifyListener(lsMod);
    FormData fdBatchSize = new FormData();
    fdBatchSize.left = new FormAttachment(middle, 0);
    fdBatchSize.right = new FormAttachment(100, 0);
    fdBatchSize.top = new FormAttachment(wlBatchSize, 0, SWT.CENTER);
    wBatchSize.setLayoutData(fdBatchSize);
    lastControl = wBatchSize;

    Label wlMessageField = new Label(wContent, SWT.RIGHT);
    wlMessageField.setText("Message field");
    PropsUi.setLook(wlMessageField);
    FormData fdlMessageField = new FormData();
    fdlMessageField.left = new FormAttachment(0, 0);
    fdlMessageField.right = new FormAttachment(middle, -margin);
    fdlMessageField.top = new FormAttachment(lastControl, margin);
    wlMessageField.setLayoutData(fdlMessageField);
    wMessageField = new ComboVar(variables, wContent, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    PropsUi.setLook(wMessageField);
    wMessageField.addModifyListener(lsMod);
    FormData fdMessageField = new FormData();
    fdMessageField.left = new FormAttachment(middle, 0);
    fdMessageField.right = new FormAttachment(100, 0);
    fdMessageField.top = new FormAttachment(wlMessageField, 0, SWT.CENTER);
    wMessageField.setLayoutData(fdMessageField);
    lastControl = wMessageField;

    wContent.pack();
    Rectangle bounds = wContent.getBounds();
    sc.setContent(wContent);
    sc.setExpandHorizontal(true);
    sc.setExpandVertical(true);
    sc.setMinWidth(bounds.width);
    sc.setMinHeight(bounds.height);

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

  public void getData() {
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
