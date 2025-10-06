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
 */

package org.apache.hop.pipeline.transforms.aws.sqs;

import org.apache.hop.core.annotations.Transform;
import org.apache.hop.core.exception.HopPluginException;
import org.apache.hop.core.exception.HopTransformException;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.core.row.value.ValueMetaFactory;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.metadata.api.HopMetadataProperty;
import org.apache.hop.metadata.api.IHopMetadataProvider;
import org.apache.hop.pipeline.transform.BaseTransformMeta;
import org.apache.hop.pipeline.transform.TransformMeta;

@Transform(
    id = "AwsSqSreader",
    image = "aws-sqs.svg",
    name = "i18n::SQSReaderTransform.Name",
    description = "i18n::SQSReaderTransform.TooltipDesc",
    categoryDescription = "i18n:org.apache.hop.pipeline.transform:BaseTransform.Category.Input",
    keywords = "",
    documentationUrl = "/pipeline/transforms/aws-sqs-reader.html")
public class SqsReaderMeta extends BaseTransformMeta<SqsReader, SqsReaderData> {

  @HopMetadataProperty(key = "AwsCredChain", injectionKey = "AWS_CRED_CHAIN")
  private String awsCredChain;

  @HopMetadataProperty(key = "aws_key", injectionKey = "AWS_KEY", password = true)
  private String awsKey;

  @HopMetadataProperty(key = "aws_key_secret", injectionKey = "AWS_KEY_SECRET", password = true)
  private String awsKeySecret;

  @HopMetadataProperty(key = "aws_region", injectionKey = "AWS_REGION")
  private String awsRegion;

  @HopMetadataProperty(key = "sqs_queue", injectionKey = "SQS_QUEUE")
  private String sqsQueue;

  @HopMetadataProperty(key = "field_message_id", injectionKey = "FIELD_MESSAGE_ID")
  private String tFldMessageID;

  @HopMetadataProperty(key = "field_message_body", injectionKey = "FIELD_MESSAGE_BODY")
  private String tFldMessageBody;

  @HopMetadataProperty(key = "field_receipt_handle", injectionKey = "FIELD_RECEIPT_HANDLE")
  private String tFldReceiptHandle;

  @HopMetadataProperty(key = "field_body_md5", injectionKey = "FIELD_BODY_MD5")
  private String tFldBodyMD5;

  @HopMetadataProperty(key = "field_sns_messages", injectionKey = "FIELD_SNS_MESSAGE")
  private String tFldSNSMessage;

  @HopMetadataProperty(key = "field_message_delete", injectionKey = "FIELD_MESSAGE_DELETE")
  private String tFldMessageDelete;

  @HopMetadataProperty(key = "field_max_messages", injectionKey = "FIELD_MAX_MESSAGES")
  private String tFldMaxMessages;

  /**
   * Constructor should call super() to make sure the base class has a chance to initialize
   * properly.
   */
  public SqsReaderMeta() {
    super();
  }

  /**
   * This method is called every time a new transform is created and should allocate/set the
   * transform configuration to sensible defaults. The values set here will be used by Hop Gui when
   * a new transform is created.
   */
  @Override
  public void setDefault() {}

  public String getAwsCredChain() {
    return awsCredChain = awsCredChain == null ? "N" : awsCredChain;
  }

  public void setAwsCredChain(String awsCredChain) {
    this.awsCredChain = awsCredChain;
  }

  public String getAwsKey() {
    return awsKey == null ? "" : awsKey;
  }

  public void setAwsKey(String aws_key) {
    this.awsKey = aws_key;
  }

  public String getAwsKeySecret() {
    return awsKeySecret == null ? "" : awsKeySecret;
  }

  public void setAwsKeySecret(String awsKeySecret) {
    this.awsKeySecret = awsKeySecret;
  }

  public String getAwsRegion() {
    return awsRegion == null ? "" : awsRegion;
  }

  public void setAwsRegion(String awsRegion) {
    this.awsRegion = awsRegion;
  }

  public String getSqsQueue() {
    return sqsQueue == null ? "" : sqsQueue;
  }

  public void setSqsQueue(String sqsQueue) {
    this.sqsQueue = sqsQueue;
  }

  public String getTFldMessageID() {
    return tFldMessageID == null ? "" : tFldMessageID;
  }

  public void setTFldMessageID(String tFldMessageID) {
    this.tFldMessageID = tFldMessageID;
  }

  public String getTFldMessageBody() {
    return tFldMessageBody == null ? "" : tFldMessageBody;
  }

  public void setTFldMessageBody(String tFldMessageBody) {
    this.tFldMessageBody = tFldMessageBody;
  }

  public String getTFldReceiptHandle() {
    return tFldReceiptHandle == null ? "" : tFldReceiptHandle;
  }

  public void setTFldReceiptHandle(String tFldReceiptHandle) {
    this.tFldReceiptHandle = tFldReceiptHandle;
  }

  public String getTFldBodyMD5() {
    return tFldBodyMD5 == null ? "" : tFldBodyMD5;
  }

  public void setTFldBodyMD5(String tFldBodyMD5) {
    this.tFldBodyMD5 = tFldBodyMD5;
  }

  public String getTFldSNSMessage() {
    return tFldSNSMessage == null ? "" : tFldSNSMessage;
  }

  public void setTFldSNSMessage(String tFldSNSMessage) {
    this.tFldSNSMessage = tFldSNSMessage;
  }

  public String getTFldMessageDelete() {
    return tFldMessageDelete == null ? "N" : tFldMessageDelete;
  }

  public void setTFldMessageDelete(String tFldMessageDelete) {
    this.tFldMessageDelete = tFldMessageDelete;
  }

  public String getTFldMaxMessages() {
    return tFldMaxMessages == null ? "0" : tFldMaxMessages;
  }

  public void setTFldMaxMessages(String tFldMaxMessages) {
    this.tFldMaxMessages = tFldMaxMessages;
  }

  /**
   * This method is used when a transform is duplicated in Hop Gui. It needs to return a deep copy
   * of this transform meta object. Be sure to create proper deep copies if the transform
   * configuration is stored in modifiable objects.
   *
   * <p>See org.apache.hop.pipeline.transforms.rowgenerator.RowGeneratorMeta.clone() for an example
   * on creating a deep copy.
   *
   * @return a deep copy of this
   */
  @Override
  public Object clone() {
    Object retval = super.clone();
    return retval;
  }

  /**
   * This method is called to determine the changes the transform is making to the row-stream. To
   * that end a RowMetaInterface object is passed in, containing the row-stream structure as it is
   * when entering the transform. This method must apply any changes the transform makes to the row
   * stream. Usually a transform adds fields to the row-stream.
   *
   * @param inputRowMeta the row structure coming in to the transform
   * @param origin the origin of the transform making the changes
   * @param info row structures of any info transforms coming in
   * @param nextTransform the description of a transform this transform is passing rows to
   * @param variables the variable variables for resolving variables
   * @param metadataProvider the metadataProvider to optionally read from
   */
  @Override
  public void getFields(
      IRowMeta inputRowMeta,
      String origin,
      IRowMeta[] info,
      TransformMeta nextTransform,
      IVariables variables,
      IHopMetadataProvider metadataProvider)
      throws HopTransformException {

    /*
     * This implementation appends the outputField to the row-stream
     */

    try {
      if (tFldMessageID != null && !tFldMessageID.equals("")) {
        String realMessageIDFieldName = variables.resolve(tFldMessageID);
        IValueMeta valueMeta =
            ValueMetaFactory.createValueMeta(realMessageIDFieldName, IValueMeta.TYPE_STRING);
        valueMeta.setName(realMessageIDFieldName.toUpperCase());
        valueMeta.setTrimType(IValueMeta.TRIM_TYPE_BOTH);
        valueMeta.setOrigin(origin);
        inputRowMeta.addValueMeta(valueMeta);
      }

      if (tFldMessageBody != null && !tFldMessageBody.equals("")) {
        String realMessageBodyFieldName = variables.resolve(tFldMessageBody);
        IValueMeta valueMeta =
            ValueMetaFactory.createValueMeta(realMessageBodyFieldName, IValueMeta.TYPE_STRING);
        valueMeta.setName(realMessageBodyFieldName.toUpperCase());
        valueMeta.setTrimType(IValueMeta.TRIM_TYPE_BOTH);
        valueMeta.setOrigin(origin);
        inputRowMeta.addValueMeta(valueMeta);
      }

      if (tFldReceiptHandle != null && !tFldReceiptHandle.equals("")) {
        String realReceiptHandleFieldName = variables.resolve(tFldReceiptHandle);
        IValueMeta valueMeta =
            ValueMetaFactory.createValueMeta(realReceiptHandleFieldName, IValueMeta.TYPE_STRING);
        valueMeta.setName(realReceiptHandleFieldName.toUpperCase());
        valueMeta.setTrimType(IValueMeta.TRIM_TYPE_BOTH);
        valueMeta.setOrigin(origin);
        inputRowMeta.addValueMeta(valueMeta);
      }

      if (tFldBodyMD5 != null && !tFldBodyMD5.equals("")) {
        String realBodyMD5FieldName = variables.resolve(tFldBodyMD5);
        IValueMeta valueMeta =
            ValueMetaFactory.createValueMeta(realBodyMD5FieldName, IValueMeta.TYPE_STRING);
        valueMeta.setName(realBodyMD5FieldName.toUpperCase());
        valueMeta.setTrimType(IValueMeta.TRIM_TYPE_BOTH);
        valueMeta.setOrigin(origin);
        inputRowMeta.addValueMeta(valueMeta);
      }

      if (tFldSNSMessage != null && !tFldSNSMessage.equals("")) {
        String realSNSMessageFieldName = variables.resolve(tFldSNSMessage);
        IValueMeta valueMeta =
            ValueMetaFactory.createValueMeta(realSNSMessageFieldName, IValueMeta.TYPE_STRING);
        valueMeta.setName(realSNSMessageFieldName.toUpperCase());
        valueMeta.setTrimType(IValueMeta.TRIM_TYPE_BOTH);
        valueMeta.setOrigin(origin);
        inputRowMeta.addValueMeta(valueMeta);
      }

    } catch (HopPluginException e) {
      logBasic(e.getMessage());
      throw new HopTransformException(e);
    }
  }
}
