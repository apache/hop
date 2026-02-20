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

package org.apache.hop.beam.transforms.kinesis;

import org.apache.hop.core.Const;
import org.apache.hop.core.util.Utils;
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

public class BeamKinesisConsumeDialog extends BaseTransformDialog {
  private static final Class<?> PKG = BeamKinesisConsume.class;
  private final BeamKinesisConsumeMeta input;

  private TextVar wAccessKey;
  private TextVar wSecretKey;

  private TextVar wStreamName;
  private TextVar wUniqueIdField;
  private TextVar wDataField;
  private ComboVar wDataType;

  private TextVar wPartitionKeyField;
  private TextVar wSequenceNumberField;
  private TextVar wSubSequenceNumberField;
  private TextVar wShardIdField;
  private TextVar wStreamNameField;

  private TextVar wMaxNumRecords;
  private TextVar wMaxReadTimeMs;
  private TextVar wUpToDateThresholdMs;
  private TextVar wRequestRecordsLimit;

  private Button wArrivalTimeWatermarkPolicy;
  private TextVar wArrivalTimeWatermarkPolicyMs;

  private Button wProcessingTimeWatermarkPolicy;

  private Button wFixedDelayRatePolicy;
  private TextVar wFixedDelayRatePolicyMs;

  private TextVar wMaxCapacityPerShard;

  public BeamKinesisConsumeDialog(
      Shell parent,
      IVariables variables,
      BeamKinesisConsumeMeta transformMeta,
      PipelineMeta pipelineMeta) {
    super(parent, variables, transformMeta, pipelineMeta);
    input = transformMeta;
  }

  @Override
  public String open() {
    createShell(BaseMessages.getString(PKG, "BeamKinesisConsumeDialog.DialogTitle"));

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

    Label wlAccessKey = new Label(wContent, SWT.RIGHT);
    wlAccessKey.setText(BaseMessages.getString(PKG, "BeamKinesisConsumeDialog.AccessKey"));
    PropsUi.setLook(wlAccessKey);
    FormData fdlAccessKey = new FormData();
    fdlAccessKey.left = new FormAttachment(0, 0);
    fdlAccessKey.top = new FormAttachment(0, margin);
    fdlAccessKey.right = new FormAttachment(middle, -margin);
    wlAccessKey.setLayoutData(fdlAccessKey);
    wAccessKey =
        new TextVar(variables, wContent, SWT.SINGLE | SWT.LEFT | SWT.BORDER | SWT.PASSWORD);
    PropsUi.setLook(wAccessKey);
    FormData fdAccessKey = new FormData();
    fdAccessKey.left = new FormAttachment(middle, 0);
    fdAccessKey.top = new FormAttachment(wlAccessKey, 0, SWT.CENTER);
    fdAccessKey.right = new FormAttachment(100, 0);
    wAccessKey.setLayoutData(fdAccessKey);
    lastControl = wAccessKey;

    Label wlSecretKey = new Label(wContent, SWT.RIGHT);
    wlSecretKey.setText(BaseMessages.getString(PKG, "BeamKinesisConsumeDialog.SecretKey"));
    PropsUi.setLook(wlSecretKey);
    FormData fdlSecretKey = new FormData();
    fdlSecretKey.left = new FormAttachment(0, 0);
    fdlSecretKey.top = new FormAttachment(lastControl, margin);
    fdlSecretKey.right = new FormAttachment(middle, -margin);
    wlSecretKey.setLayoutData(fdlSecretKey);
    wSecretKey =
        new TextVar(variables, wContent, SWT.SINGLE | SWT.LEFT | SWT.BORDER | SWT.PASSWORD);
    PropsUi.setLook(wSecretKey);
    FormData fdSecretKey = new FormData();
    fdSecretKey.left = new FormAttachment(middle, 0);
    fdSecretKey.top = new FormAttachment(wlSecretKey, 0, SWT.CENTER);
    fdSecretKey.right = new FormAttachment(100, 0);
    wSecretKey.setLayoutData(fdSecretKey);
    lastControl = wSecretKey;

    Label wlStreamName = new Label(wContent, SWT.RIGHT);
    wlStreamName.setText(BaseMessages.getString(PKG, "BeamKinesisConsumeDialog.StreamName"));
    PropsUi.setLook(wlStreamName);
    FormData fdlStreamName = new FormData();
    fdlStreamName.left = new FormAttachment(0, 0);
    fdlStreamName.top = new FormAttachment(lastControl, margin);
    fdlStreamName.right = new FormAttachment(middle, -margin);
    wlStreamName.setLayoutData(fdlStreamName);
    wStreamName = new TextVar(variables, wContent, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    PropsUi.setLook(wStreamName);
    FormData fdStreamName = new FormData();
    fdStreamName.left = new FormAttachment(middle, 0);
    fdStreamName.top = new FormAttachment(wlStreamName, 0, SWT.CENTER);
    fdStreamName.right = new FormAttachment(100, 0);
    wStreamName.setLayoutData(fdStreamName);
    lastControl = wStreamName;

    Label wlUniqueIdField = new Label(wContent, SWT.RIGHT);
    wlUniqueIdField.setText(BaseMessages.getString(PKG, "BeamKinesisConsumeDialog.UniqueIdField"));
    PropsUi.setLook(wlUniqueIdField);
    FormData fdlUniqueIdField = new FormData();
    fdlUniqueIdField.left = new FormAttachment(0, 0);
    fdlUniqueIdField.top = new FormAttachment(lastControl, margin);
    fdlUniqueIdField.right = new FormAttachment(middle, -margin);
    wlUniqueIdField.setLayoutData(fdlUniqueIdField);
    wUniqueIdField = new TextVar(variables, wContent, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    PropsUi.setLook(wUniqueIdField);
    FormData fdUniqueIdField = new FormData();
    fdUniqueIdField.left = new FormAttachment(middle, 0);
    fdUniqueIdField.top = new FormAttachment(wlUniqueIdField, 0, SWT.CENTER);
    fdUniqueIdField.right = new FormAttachment(100, 0);
    wUniqueIdField.setLayoutData(fdUniqueIdField);
    lastControl = wUniqueIdField;

    Label wlDataField = new Label(wContent, SWT.RIGHT);
    wlDataField.setText(BaseMessages.getString(PKG, "BeamKinesisConsumeDialog.DataField"));
    PropsUi.setLook(wlDataField);
    FormData fdlDataField = new FormData();
    fdlDataField.left = new FormAttachment(0, 0);
    fdlDataField.top = new FormAttachment(lastControl, margin);
    fdlDataField.right = new FormAttachment(middle, -margin);
    wlDataField.setLayoutData(fdlDataField);
    wDataField = new TextVar(variables, wContent, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    PropsUi.setLook(wDataField);
    FormData fdDataField = new FormData();
    fdDataField.left = new FormAttachment(middle, 0);
    fdDataField.top = new FormAttachment(wlDataField, 0, SWT.CENTER);
    fdDataField.right = new FormAttachment(100, 0);
    wDataField.setLayoutData(fdDataField);
    lastControl = wDataField;

    Label wlDataType = new Label(wContent, SWT.RIGHT);
    wlDataType.setText(BaseMessages.getString(PKG, "BeamKinesisConsumeDialog.DataType"));
    PropsUi.setLook(wlDataType);
    FormData fdlDataType = new FormData();
    fdlDataType.left = new FormAttachment(0, 0);
    fdlDataType.top = new FormAttachment(lastControl, margin);
    fdlDataType.right = new FormAttachment(middle, -margin);
    wlDataType.setLayoutData(fdlDataType);
    wDataType = new ComboVar(variables, wContent, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    wDataType.setItems(
        new String[] {
          "String",
        }); // TODO add "Avro Record"
    PropsUi.setLook(wDataType);
    FormData fdDataType = new FormData();
    fdDataType.left = new FormAttachment(middle, 0);
    fdDataType.top = new FormAttachment(wlDataType, 0, SWT.CENTER);
    fdDataType.right = new FormAttachment(100, 0);
    wDataType.setLayoutData(fdDataType);
    lastControl = wDataType;

    Label wlPartitionKeyField = new Label(wContent, SWT.RIGHT);
    wlPartitionKeyField.setText(
        BaseMessages.getString(PKG, "BeamKinesisConsumeDialog.PartitionKeyField"));
    PropsUi.setLook(wlPartitionKeyField);
    FormData fdlPartitionKeyField = new FormData();
    fdlPartitionKeyField.left = new FormAttachment(0, 0);
    fdlPartitionKeyField.top = new FormAttachment(lastControl, margin);
    fdlPartitionKeyField.right = new FormAttachment(middle, -margin);
    wlPartitionKeyField.setLayoutData(fdlPartitionKeyField);
    wPartitionKeyField = new TextVar(variables, wContent, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    PropsUi.setLook(wPartitionKeyField);
    FormData fdPartitionKeyField = new FormData();
    fdPartitionKeyField.left = new FormAttachment(middle, 0);
    fdPartitionKeyField.top = new FormAttachment(wlPartitionKeyField, 0, SWT.CENTER);
    fdPartitionKeyField.right = new FormAttachment(100, 0);
    wPartitionKeyField.setLayoutData(fdPartitionKeyField);
    lastControl = wPartitionKeyField;

    Label wlSequenceNumberField = new Label(wContent, SWT.RIGHT);
    wlSequenceNumberField.setText(
        BaseMessages.getString(PKG, "BeamKinesisConsumeDialog.SequenceNumberField"));
    PropsUi.setLook(wlSequenceNumberField);
    FormData fdlSequenceNumberField = new FormData();
    fdlSequenceNumberField.left = new FormAttachment(0, 0);
    fdlSequenceNumberField.top = new FormAttachment(lastControl, margin);
    fdlSequenceNumberField.right = new FormAttachment(middle, -margin);
    wlSequenceNumberField.setLayoutData(fdlSequenceNumberField);
    wSequenceNumberField = new TextVar(variables, wContent, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    PropsUi.setLook(wSequenceNumberField);
    FormData fdSequenceNumberField = new FormData();
    fdSequenceNumberField.left = new FormAttachment(middle, 0);
    fdSequenceNumberField.top = new FormAttachment(wlSequenceNumberField, 0, SWT.CENTER);
    fdSequenceNumberField.right = new FormAttachment(100, 0);
    wSequenceNumberField.setLayoutData(fdSequenceNumberField);
    lastControl = wSequenceNumberField;

    Label wlSubSequenceNumberField = new Label(wContent, SWT.RIGHT);
    wlSubSequenceNumberField.setText(
        BaseMessages.getString(PKG, "BeamKinesisConsumeDialog.SubSequenceNumberField"));
    PropsUi.setLook(wlSubSequenceNumberField);
    FormData fdlSubSequenceNumberField = new FormData();
    fdlSubSequenceNumberField.left = new FormAttachment(0, 0);
    fdlSubSequenceNumberField.top = new FormAttachment(lastControl, margin);
    fdlSubSequenceNumberField.right = new FormAttachment(middle, -margin);
    wlSubSequenceNumberField.setLayoutData(fdlSubSequenceNumberField);
    wSubSequenceNumberField = new TextVar(variables, wContent, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    PropsUi.setLook(wSubSequenceNumberField);
    FormData fdSubSequenceNumberField = new FormData();
    fdSubSequenceNumberField.left = new FormAttachment(middle, 0);
    fdSubSequenceNumberField.top = new FormAttachment(wlSubSequenceNumberField, 0, SWT.CENTER);
    fdSubSequenceNumberField.right = new FormAttachment(100, 0);
    wSubSequenceNumberField.setLayoutData(fdSubSequenceNumberField);
    lastControl = wSubSequenceNumberField;

    Label wlShardIdField = new Label(wContent, SWT.RIGHT);
    wlShardIdField.setText(BaseMessages.getString(PKG, "BeamKinesisConsumeDialog.ShardIdField"));
    PropsUi.setLook(wlShardIdField);
    FormData fdlShardIdField = new FormData();
    fdlShardIdField.left = new FormAttachment(0, 0);
    fdlShardIdField.top = new FormAttachment(lastControl, margin);
    fdlShardIdField.right = new FormAttachment(middle, -margin);
    wlShardIdField.setLayoutData(fdlShardIdField);
    wShardIdField = new TextVar(variables, wContent, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    PropsUi.setLook(wShardIdField);
    FormData fdShardIdField = new FormData();
    fdShardIdField.left = new FormAttachment(middle, 0);
    fdShardIdField.top = new FormAttachment(wlShardIdField, 0, SWT.CENTER);
    fdShardIdField.right = new FormAttachment(100, 0);
    wShardIdField.setLayoutData(fdShardIdField);
    lastControl = wShardIdField;

    Label wlStreamNameField = new Label(wContent, SWT.RIGHT);
    wlStreamNameField.setText(
        BaseMessages.getString(PKG, "BeamKinesisConsumeDialog.StreamNameField"));
    PropsUi.setLook(wlStreamNameField);
    FormData fdlStreamNameField = new FormData();
    fdlStreamNameField.left = new FormAttachment(0, 0);
    fdlStreamNameField.top = new FormAttachment(lastControl, margin);
    fdlStreamNameField.right = new FormAttachment(middle, -margin);
    wlStreamNameField.setLayoutData(fdlStreamNameField);
    wStreamNameField = new TextVar(variables, wContent, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    PropsUi.setLook(wStreamNameField);
    FormData fdStreamNameField = new FormData();
    fdStreamNameField.left = new FormAttachment(middle, 0);
    fdStreamNameField.top = new FormAttachment(wlStreamNameField, 0, SWT.CENTER);
    fdStreamNameField.right = new FormAttachment(100, 0);
    wStreamNameField.setLayoutData(fdStreamNameField);
    lastControl = wStreamNameField;

    Label wlMaxNumRecords = new Label(wContent, SWT.RIGHT);
    wlMaxNumRecords.setText(
        BaseMessages.getString(PKG, "BeamKinesisConsumeDialog.MaxNumRecords.Label"));
    wlMaxNumRecords.setToolTipText(
        BaseMessages.getString(PKG, "BeamKinesisConsumeDialog.MaxNumRecords.Tooltip"));
    PropsUi.setLook(wlMaxNumRecords);
    FormData fdlMaxNumRecords = new FormData();
    fdlMaxNumRecords.left = new FormAttachment(0, 0);
    fdlMaxNumRecords.top = new FormAttachment(lastControl, margin);
    fdlMaxNumRecords.right = new FormAttachment(middle, -margin);
    wlMaxNumRecords.setLayoutData(fdlMaxNumRecords);
    wMaxNumRecords = new TextVar(variables, wContent, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    wMaxNumRecords.setToolTipText(
        BaseMessages.getString(PKG, "BeamKinesisConsumeDialog.MaxNumRecords.Tooltip"));
    PropsUi.setLook(wMaxNumRecords);
    FormData fdMaxNumRecords = new FormData();
    fdMaxNumRecords.left = new FormAttachment(middle, 0);
    fdMaxNumRecords.top = new FormAttachment(wlMaxNumRecords, 0, SWT.CENTER);
    fdMaxNumRecords.right = new FormAttachment(100, 0);
    wMaxNumRecords.setLayoutData(fdMaxNumRecords);
    lastControl = wMaxNumRecords;

    Label wlMaxReadTimeMs = new Label(wContent, SWT.RIGHT);
    wlMaxReadTimeMs.setText(
        BaseMessages.getString(PKG, "BeamKinesisConsumeDialog.MaxReadTimeMs.Label"));
    wlMaxReadTimeMs.setToolTipText(
        BaseMessages.getString(PKG, "BeamKinesisConsumeDialog.MaxReadTimeMs.Tooltip"));
    PropsUi.setLook(wlMaxReadTimeMs);
    FormData fdlMaxReadTimeMs = new FormData();
    fdlMaxReadTimeMs.left = new FormAttachment(0, 0);
    fdlMaxReadTimeMs.top = new FormAttachment(lastControl, margin);
    fdlMaxReadTimeMs.right = new FormAttachment(middle, -margin);
    wlMaxReadTimeMs.setLayoutData(fdlMaxReadTimeMs);
    wMaxReadTimeMs = new TextVar(variables, wContent, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    wMaxReadTimeMs.setToolTipText(
        BaseMessages.getString(PKG, "BeamKinesisConsumeDialog.MaxReadTimeMs.Tooltip"));
    PropsUi.setLook(wMaxReadTimeMs);
    FormData fdMaxReadTimeMs = new FormData();
    fdMaxReadTimeMs.left = new FormAttachment(middle, 0);
    fdMaxReadTimeMs.top = new FormAttachment(wlMaxReadTimeMs, 0, SWT.CENTER);
    fdMaxReadTimeMs.right = new FormAttachment(100, 0);
    wMaxReadTimeMs.setLayoutData(fdMaxReadTimeMs);
    lastControl = wMaxReadTimeMs;

    Label wlUpToDateThresholdMs = new Label(wContent, SWT.RIGHT);
    wlUpToDateThresholdMs.setText(
        BaseMessages.getString(PKG, "BeamKinesisConsumeDialog.UpToDateThresholdMs.Label"));
    wlUpToDateThresholdMs.setToolTipText(
        BaseMessages.getString(PKG, "BeamKinesisConsumeDialog.UpToDateThresholdMs.Tooltip"));
    PropsUi.setLook(wlUpToDateThresholdMs);
    FormData fdlUpToDateThresholdMs = new FormData();
    fdlUpToDateThresholdMs.left = new FormAttachment(0, 0);
    fdlUpToDateThresholdMs.top = new FormAttachment(lastControl, margin);
    fdlUpToDateThresholdMs.right = new FormAttachment(middle, -margin);
    wlUpToDateThresholdMs.setLayoutData(fdlUpToDateThresholdMs);
    wUpToDateThresholdMs = new TextVar(variables, wContent, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    wUpToDateThresholdMs.setToolTipText(
        BaseMessages.getString(PKG, "BeamKinesisConsumeDialog.UpToDateThresholdMs.Tooltip"));
    PropsUi.setLook(wUpToDateThresholdMs);
    FormData fdUpToDateThresholdMs = new FormData();
    fdUpToDateThresholdMs.left = new FormAttachment(middle, 0);
    fdUpToDateThresholdMs.top = new FormAttachment(wlUpToDateThresholdMs, 0, SWT.CENTER);
    fdUpToDateThresholdMs.right = new FormAttachment(100, 0);
    wUpToDateThresholdMs.setLayoutData(fdUpToDateThresholdMs);
    lastControl = wUpToDateThresholdMs;

    Label wlRequestRecordsLimit = new Label(wContent, SWT.RIGHT);
    wlRequestRecordsLimit.setText(
        BaseMessages.getString(PKG, "BeamKinesisConsumeDialog.RequestRecordsLimit.Label"));
    wlRequestRecordsLimit.setToolTipText(
        BaseMessages.getString(PKG, "BeamKinesisConsumeDialog.RequestRecordsLimit.Tooltip"));
    PropsUi.setLook(wlRequestRecordsLimit);
    FormData fdlRequestRecordsLimit = new FormData();
    fdlRequestRecordsLimit.left = new FormAttachment(0, 0);
    fdlRequestRecordsLimit.top = new FormAttachment(lastControl, margin);
    fdlRequestRecordsLimit.right = new FormAttachment(middle, -margin);
    wlRequestRecordsLimit.setLayoutData(fdlRequestRecordsLimit);
    wRequestRecordsLimit = new TextVar(variables, wContent, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    wRequestRecordsLimit.setToolTipText(
        BaseMessages.getString(PKG, "BeamKinesisConsumeDialog.RequestRecordsLimit.Tooltip"));
    PropsUi.setLook(wRequestRecordsLimit);
    FormData fdRequestRecordsLimit = new FormData();
    fdRequestRecordsLimit.left = new FormAttachment(middle, 0);
    fdRequestRecordsLimit.top = new FormAttachment(wlRequestRecordsLimit, 0, SWT.CENTER);
    fdRequestRecordsLimit.right = new FormAttachment(100, 0);
    wRequestRecordsLimit.setLayoutData(fdRequestRecordsLimit);
    lastControl = wRequestRecordsLimit;

    Label wlArrivalTimeWatermarkPolicy = new Label(wContent, SWT.RIGHT);
    wlArrivalTimeWatermarkPolicy.setText(
        BaseMessages.getString(PKG, "BeamKinesisConsumeDialog.ArrivalTimeWatermarkPolicy.Label"));
    wlArrivalTimeWatermarkPolicy.setToolTipText(
        BaseMessages.getString(PKG, "BeamKinesisConsumeDialog.ArrivalTimeWatermarkPolicy.Tooltip"));
    PropsUi.setLook(wlArrivalTimeWatermarkPolicy);
    FormData fdlArrivalTimeWatermarkPolicy = new FormData();
    fdlArrivalTimeWatermarkPolicy.left = new FormAttachment(0, 0);
    fdlArrivalTimeWatermarkPolicy.top = new FormAttachment(lastControl, margin);
    fdlArrivalTimeWatermarkPolicy.right = new FormAttachment(middle, -margin);
    wlArrivalTimeWatermarkPolicy.setLayoutData(fdlArrivalTimeWatermarkPolicy);
    wArrivalTimeWatermarkPolicy = new Button(wContent, SWT.CHECK | SWT.LEFT);
    wArrivalTimeWatermarkPolicy.setToolTipText(
        BaseMessages.getString(PKG, "BeamKinesisConsumeDialog.ArrivalTimeWatermarkPolicy.Tooltip"));
    PropsUi.setLook(wArrivalTimeWatermarkPolicy);
    FormData fdArrivalTimeWatermarkPolicy = new FormData();
    fdArrivalTimeWatermarkPolicy.left = new FormAttachment(middle, 0);
    fdArrivalTimeWatermarkPolicy.top =
        new FormAttachment(wlArrivalTimeWatermarkPolicy, 0, SWT.CENTER);
    fdArrivalTimeWatermarkPolicy.right = new FormAttachment(100, 0);
    wArrivalTimeWatermarkPolicy.setLayoutData(fdArrivalTimeWatermarkPolicy);
    lastControl = wArrivalTimeWatermarkPolicy;

    Label wlArrivalTimeWatermarkPolicyMs = new Label(wContent, SWT.RIGHT);
    wlArrivalTimeWatermarkPolicyMs.setText(
        BaseMessages.getString(PKG, "BeamKinesisConsumeDialog.ArrivalTimeWatermarkPolicyMs.Label"));
    wlArrivalTimeWatermarkPolicyMs.setToolTipText(
        BaseMessages.getString(
            PKG, "BeamKinesisConsumeDialog.ArrivalTimeWatermarkPolicyMs.Tooltip"));
    PropsUi.setLook(wlArrivalTimeWatermarkPolicyMs);
    FormData fdlArrivalTimeWatermarkPolicyMs = new FormData();
    fdlArrivalTimeWatermarkPolicyMs.left = new FormAttachment(0, 0);
    fdlArrivalTimeWatermarkPolicyMs.top = new FormAttachment(lastControl, margin);
    fdlArrivalTimeWatermarkPolicyMs.right = new FormAttachment(middle, -margin);
    wlArrivalTimeWatermarkPolicyMs.setLayoutData(fdlArrivalTimeWatermarkPolicyMs);
    wArrivalTimeWatermarkPolicyMs =
        new TextVar(variables, wContent, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    wArrivalTimeWatermarkPolicyMs.setToolTipText(
        BaseMessages.getString(
            PKG, "BeamKinesisConsumeDialog.ArrivalTimeWatermarkPolicyMs.Tooltip"));
    PropsUi.setLook(wArrivalTimeWatermarkPolicyMs);
    FormData fdArrivalTimeWatermarkPolicyMs = new FormData();
    fdArrivalTimeWatermarkPolicyMs.left = new FormAttachment(middle, 0);
    fdArrivalTimeWatermarkPolicyMs.top =
        new FormAttachment(wlArrivalTimeWatermarkPolicyMs, 0, SWT.CENTER);
    fdArrivalTimeWatermarkPolicyMs.right = new FormAttachment(100, 0);
    wArrivalTimeWatermarkPolicyMs.setLayoutData(fdArrivalTimeWatermarkPolicyMs);
    lastControl = wArrivalTimeWatermarkPolicyMs;

    Label wlProcessingTimeWatermarkPolicy = new Label(wContent, SWT.RIGHT);
    wlProcessingTimeWatermarkPolicy.setText(
        BaseMessages.getString(
            PKG, "BeamKinesisConsumeDialog.ProcessingTimeWatermarkPolicy.Label"));
    wlProcessingTimeWatermarkPolicy.setToolTipText(
        BaseMessages.getString(
            PKG, "BeamKinesisConsumeDialog.ProcessingTimeWatermarkPolicy.Tooltip"));
    PropsUi.setLook(wlProcessingTimeWatermarkPolicy);
    FormData fdlProcessingTimeWatermarkPolicy = new FormData();
    fdlProcessingTimeWatermarkPolicy.left = new FormAttachment(0, 0);
    fdlProcessingTimeWatermarkPolicy.top = new FormAttachment(lastControl, margin);
    fdlProcessingTimeWatermarkPolicy.right = new FormAttachment(middle, -margin);
    wlProcessingTimeWatermarkPolicy.setLayoutData(fdlProcessingTimeWatermarkPolicy);
    wProcessingTimeWatermarkPolicy = new Button(wContent, SWT.CHECK | SWT.LEFT);
    wProcessingTimeWatermarkPolicy.setToolTipText(
        BaseMessages.getString(
            PKG, "BeamKinesisConsumeDialog.ProcessingTimeWatermarkPolicy.Tooltip"));
    PropsUi.setLook(wProcessingTimeWatermarkPolicy);
    FormData fdProcessingTimeWatermarkPolicy = new FormData();
    fdProcessingTimeWatermarkPolicy.left = new FormAttachment(middle, 0);
    fdProcessingTimeWatermarkPolicy.top =
        new FormAttachment(wlProcessingTimeWatermarkPolicy, 0, SWT.CENTER);
    fdProcessingTimeWatermarkPolicy.right = new FormAttachment(100, 0);
    wProcessingTimeWatermarkPolicy.setLayoutData(fdProcessingTimeWatermarkPolicy);
    lastControl = wProcessingTimeWatermarkPolicy;

    Label wlFixedDelayRatePolicy = new Label(wContent, SWT.RIGHT);
    wlFixedDelayRatePolicy.setText(
        BaseMessages.getString(PKG, "BeamKinesisConsumeDialog.FixedDelayRatePolicy.Label"));
    wlFixedDelayRatePolicy.setToolTipText(
        BaseMessages.getString(PKG, "BeamKinesisConsumeDialog.FixedDelayRatePolicy.Tooltip"));
    PropsUi.setLook(wlFixedDelayRatePolicy);
    FormData fdlFixedDelayRatePolicy = new FormData();
    fdlFixedDelayRatePolicy.left = new FormAttachment(0, 0);
    fdlFixedDelayRatePolicy.top = new FormAttachment(lastControl, margin);
    fdlFixedDelayRatePolicy.right = new FormAttachment(middle, -margin);
    wlFixedDelayRatePolicy.setLayoutData(fdlFixedDelayRatePolicy);
    wFixedDelayRatePolicy = new Button(wContent, SWT.CHECK | SWT.LEFT);
    wFixedDelayRatePolicy.setToolTipText(
        BaseMessages.getString(PKG, "BeamKinesisConsumeDialog.FixedDelayRatePolicy.Tooltip"));
    PropsUi.setLook(wFixedDelayRatePolicy);
    FormData fdFixedDelayRatePolicy = new FormData();
    fdFixedDelayRatePolicy.left = new FormAttachment(middle, 0);
    fdFixedDelayRatePolicy.top = new FormAttachment(wlFixedDelayRatePolicy, 0, SWT.CENTER);
    fdFixedDelayRatePolicy.right = new FormAttachment(100, 0);
    wFixedDelayRatePolicy.setLayoutData(fdFixedDelayRatePolicy);
    lastControl = wFixedDelayRatePolicy;

    Label wlFixedDelayRatePolicyMs = new Label(wContent, SWT.RIGHT);
    wlFixedDelayRatePolicyMs.setText(
        BaseMessages.getString(PKG, "BeamKinesisConsumeDialog.FixedDelayRatePolicyMs.Label"));
    wlFixedDelayRatePolicyMs.setToolTipText(
        BaseMessages.getString(PKG, "BeamKinesisConsumeDialog.FixedDelayRatePolicyMs.Tooltip"));
    PropsUi.setLook(wlFixedDelayRatePolicyMs);
    FormData fdlFixedDelayRatePolicyMs = new FormData();
    fdlFixedDelayRatePolicyMs.left = new FormAttachment(0, 0);
    fdlFixedDelayRatePolicyMs.top = new FormAttachment(lastControl, margin);
    fdlFixedDelayRatePolicyMs.right = new FormAttachment(middle, -margin);
    wlFixedDelayRatePolicyMs.setLayoutData(fdlFixedDelayRatePolicyMs);
    wFixedDelayRatePolicyMs = new TextVar(variables, wContent, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    wFixedDelayRatePolicyMs.setToolTipText(
        BaseMessages.getString(PKG, "BeamKinesisConsumeDialog.FixedDelayRatePolicyMs.Tooltip"));
    PropsUi.setLook(wFixedDelayRatePolicyMs);
    FormData fdFixedDelayRatePolicyMs = new FormData();
    fdFixedDelayRatePolicyMs.left = new FormAttachment(middle, 0);
    fdFixedDelayRatePolicyMs.top = new FormAttachment(wlFixedDelayRatePolicyMs, 0, SWT.CENTER);
    fdFixedDelayRatePolicyMs.right = new FormAttachment(100, 0);
    wFixedDelayRatePolicyMs.setLayoutData(fdFixedDelayRatePolicyMs);
    lastControl = wFixedDelayRatePolicyMs;

    Label wlMaxCapacityPerShard = new Label(wContent, SWT.RIGHT);
    wlMaxCapacityPerShard.setText(
        BaseMessages.getString(PKG, "BeamKinesisConsumeDialog.MaxCapacityPerShard.Label"));
    wlMaxCapacityPerShard.setToolTipText(
        BaseMessages.getString(PKG, "BeamKinesisConsumeDialog.MaxCapacityPerShard.Tooltip"));
    PropsUi.setLook(wlMaxCapacityPerShard);
    FormData fdlMaxCapacityPerShard = new FormData();
    fdlMaxCapacityPerShard.left = new FormAttachment(0, 0);
    fdlMaxCapacityPerShard.top = new FormAttachment(lastControl, margin);
    fdlMaxCapacityPerShard.right = new FormAttachment(middle, -margin);
    wlMaxCapacityPerShard.setLayoutData(fdlMaxCapacityPerShard);
    wMaxCapacityPerShard = new TextVar(variables, wContent, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    wMaxCapacityPerShard.setToolTipText(
        BaseMessages.getString(PKG, "BeamKinesisConsumeDialog.MaxCapacityPerShard.Tooltip"));
    PropsUi.setLook(wMaxCapacityPerShard);
    FormData fdMaxCapacityPerShard = new FormData();
    fdMaxCapacityPerShard.left = new FormAttachment(middle, 0);
    fdMaxCapacityPerShard.top = new FormAttachment(wlMaxCapacityPerShard, 0, SWT.CENTER);
    fdMaxCapacityPerShard.right = new FormAttachment(100, 0);
    wMaxCapacityPerShard.setLayoutData(fdMaxCapacityPerShard);
    lastControl = wMaxCapacityPerShard;

    wArrivalTimeWatermarkPolicy.addListener(SWT.Selection, e -> enableFields());
    wProcessingTimeWatermarkPolicy.addListener(SWT.Selection, e -> enableFields());
    wFixedDelayRatePolicy.addListener(SWT.Selection, e -> enableFields());

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

  private void enableFields() {
    /*
     wArrivalTimeWatermarkPolicy.addListener(SWT.Selection, e -> enableFields());
     wProcessingTimeWatermarkPolicy.addListener(SWT.Selection, e -> enableFields());
     wFixedDelayRatePolicy.addListener(SWT.Selection, e -> enableFields());
    */
  }

  /** Populate the widgets. */
  public void getData() {
    wSecretKey.setText(Const.NVL(input.getSecretKey(), ""));
    wAccessKey.setText(Const.NVL(input.getAccessKey(), ""));
    wStreamName.setText(Const.NVL(input.getStreamName(), ""));
    wUniqueIdField.setText(Const.NVL(input.getUniqueIdField(), ""));
    wDataField.setText(Const.NVL(input.getDataField(), ""));
    wDataType.setText(Const.NVL(input.getDataType(), ""));

    wPartitionKeyField.setText(Const.NVL(input.getPartitionKeyField(), ""));
    wSequenceNumberField.setText(Const.NVL(input.getSequenceNumberField(), ""));
    wSubSequenceNumberField.setText(Const.NVL(input.getSubSequenceNumberField(), ""));
    wShardIdField.setText(Const.NVL(input.getShardIdField(), ""));
    wStreamNameField.setText(Const.NVL(input.getStreamNameField(), ""));

    wMaxReadTimeMs.setText(Const.NVL(input.getMaxReadTimeMs(), ""));
    wUpToDateThresholdMs.setText(Const.NVL(input.getUpToDateThresholdMs(), ""));
    wRequestRecordsLimit.setText(Const.NVL(input.getRequestRecordsLimit(), ""));
    wArrivalTimeWatermarkPolicy.setSelection(input.isArrivalTimeWatermarkPolicy());
    wArrivalTimeWatermarkPolicyMs.setText(Const.NVL(input.getArrivalTimeWatermarkPolicyMs(), ""));
    wProcessingTimeWatermarkPolicy.setSelection(input.isProcessingTimeWatermarkPolicy());
    wFixedDelayRatePolicy.setSelection(input.isFixedDelayRatePolicy());
    wFixedDelayRatePolicyMs.setText(Const.NVL(input.getFixedDelayRatePolicyMs(), ""));
    wMaxCapacityPerShard.setText(Const.NVL(input.getMaxCapacityPerShard(), ""));
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

  private void getInfo(BeamKinesisConsumeMeta in) {
    transformName = wTransformName.getText(); // return value

    input.setAccessKey(wAccessKey.getText());
    input.setSecretKey(wSecretKey.getText());
    input.setStreamName(wStreamName.getText());
    input.setUniqueIdField(wUniqueIdField.getText());
    input.setDataField(wDataField.getText());
    input.setDataType(wDataType.getText());

    input.setPartitionKeyField(wPartitionKeyField.getText());
    input.setSequenceNumberField(wSequenceNumberField.getText());
    input.setSubSequenceNumberField(wSubSequenceNumberField.getText());
    input.setShardIdField(wShardIdField.getText());
    input.setStreamNameField(wStreamNameField.getText());

    input.setMaxNumRecords(wMaxNumRecords.getText());
    input.setMaxReadTimeMs(wMaxReadTimeMs.getText());
    input.setUpToDateThresholdMs(wUpToDateThresholdMs.getText());
    input.setRequestRecordsLimit(wRequestRecordsLimit.getText());
    input.setArrivalTimeWatermarkPolicy(wArrivalTimeWatermarkPolicy.getSelection());
    input.setArrivalTimeWatermarkPolicyMs(wArrivalTimeWatermarkPolicyMs.getText());
    input.setProcessingTimeWatermarkPolicy(wProcessingTimeWatermarkPolicy.getSelection());
    input.setFixedDelayRatePolicy(wFixedDelayRatePolicy.getSelection());
    input.setFixedDelayRatePolicyMs(wFixedDelayRatePolicyMs.getText());
    input.setMaxCapacityPerShard(wMaxCapacityPerShard.getText());

    input.setChanged();
  }
}
