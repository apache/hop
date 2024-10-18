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

package org.apache.hop.pipeline.transforms.aws.sns;

import java.util.List;
import org.apache.hop.core.CheckResult;
import org.apache.hop.core.ICheckResult;
import org.apache.hop.core.annotations.Transform;
import org.apache.hop.core.exception.HopPluginException;
import org.apache.hop.core.exception.HopTransformException;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.core.row.value.ValueMetaFactory;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.metadata.api.HopMetadataProperty;
import org.apache.hop.metadata.api.IHopMetadataProvider;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.BaseTransformMeta;
import org.apache.hop.pipeline.transform.TransformMeta;

@Transform(
    id = "SnsNotify",
    image = "aws-sns.svg",
    name = "i18n::SNSNotify.Name",
    description = "i18n::SNSNotify.Description",
    categoryDescription = "i18n:org.apache.hop.pipeline.transform:BaseTransform.Category.Output",
    keywords = "",
    documentationUrl = "/pipeline/transforms/aws-sns-notify.html")
public class SnsNotifyMeta extends BaseTransformMeta<SnsNotify, SnsNotifyData> {

  /**
   * The PKG member is used when looking up internationalized strings. The properties file with
   * localized keys is expected to reside in {the package of the class
   * specified}/messages/messages_{locale}.properties
   */
  private static final Class<?> PKG = SnsNotifyMeta.class; // for i18n purposes

  @HopMetadataProperty(key = "AwsCredChain", injectionKey = "AWS_CRED_CHAIN")
  private String awsCredChain;

  @HopMetadataProperty(key = "aws_key", injectionKey = "AWS_KEY")
  private String awsKey;

  @HopMetadataProperty(key = "aws_key_secret", injectionKey = "AWS_KEY_SECRET")
  private String awsKeySecret;

  @HopMetadataProperty(key = "aws_region", injectionKey = "AWS_REGION")
  private String awsRegion;

  @HopMetadataProperty(key = "notify_point", injectionKey = "NOTIFY_POINT")
  private String notifyPoint;

  @HopMetadataProperty(key = "chooser_topic_arn", injectionKey = "CHOOSER_TOPIC_ARN")
  private String cInputtopicArn;

  @HopMetadataProperty(key = "chooser_subject", injectionKey = "CHOOSER_SUBJECT")
  private String cInputSubject;

  @HopMetadataProperty(key = "chooser_message", injectionKey = "CHOOSER_MESSAGE")
  private String cInputMessage;

  @HopMetadataProperty(key = "field_topic_arn", injectionKey = "FIELD_TOPIC_ARN")
  private String tFldtopicARN;

  @HopMetadataProperty(key = "field_subject", injectionKey = "FIELD_SUBJECT")
  private String tFldSubject;

  @HopMetadataProperty(key = "field_message", injectionKey = "FIELD_MESSAGE")
  private String tFldMessage;

  @HopMetadataProperty(key = "value_topic_arn", injectionKey = "VALUE_TOPIC_ARN")
  private String tValuetopicARN;

  @HopMetadataProperty(key = "value_subject", injectionKey = "VALUE_SUBJECT")
  private String tValueSubject;

  @HopMetadataProperty(key = "value_message", injectionKey = "VALUE_MESSAGE")
  private String tValueMessage;

  @HopMetadataProperty(key = "field_message_id", injectionKey = "FIELD_MESSAGE_ID")
  private String tFldMessageID;

  /**
   * Constructor should call super() to make sure the base class has a chance to initialize
   * properly.
   */
  public SnsNotifyMeta() {
    super();
  }

  public String getAwsCredChain() {
    return awsCredChain == null ? "N" : awsCredChain;
  }

  public void setAwsCredChain(String awsCredChain) {
    this.awsCredChain = awsCredChain;
  }

  public String getAwsKey() {
    return awsKey == null ? "" : awsKey;
  }

  public void setAwsKey(String awsKey) {
    this.awsKey = awsKey;
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

  public String getNotifyPoint() {
    return notifyPoint == null ? "Send once with first row" : notifyPoint;
  }

  public String[] getNotifyPointValues() {

    return new String[] {
      "Send once with first row",
      // "Send once with last row",
      "Send for each row (be careful!)"
    };
  }

  public String getNotifyPointShort() {
    if (notifyPoint.contains("last")) {
      return "last";
    } else if (notifyPoint.contains("each")) {
      return "each";
    } else {
      return "first";
    }
  }

  public void setNotifyPoint(String notifyPoint) {
    this.notifyPoint = notifyPoint;
  }

  public String getCInputtopicArn() {
    return cInputtopicArn == null ? "N" : cInputtopicArn;
  }

  public void setCInputtopicArn(String cInputtopicArn) {
    this.cInputtopicArn = cInputtopicArn;
  }

  public String getTFldtopicARN() {
    return tFldtopicARN == null ? "" : tFldtopicARN;
  }

  public void setTFldtopicARN(String tFldtopicARN) {
    this.tFldtopicARN = tFldtopicARN;
  }

  public String getTValuetopicARN() {
    return tValuetopicARN == null ? "" : tValuetopicARN;
  }

  public void setTValuetopicARN(String tValuetopicARN) {
    this.tValuetopicARN = tValuetopicARN;
  }

  public String getCInputSubject() {
    return cInputSubject == null ? "N" : cInputSubject;
  }

  public void setCInputSubject(String cInputSubject) {
    this.cInputSubject = cInputSubject;
  }

  public String getTFldSubject() {
    return tFldSubject == null ? "" : tFldSubject;
  }

  public void setTFldSubject(String tFldSubject) {
    this.tFldSubject = tFldSubject;
  }

  public String getTValueSubject() {
    return tValueSubject == null ? "" : tValueSubject;
  }

  public void setTValueSubject(String tValueSubject) {
    this.tValueSubject = tValueSubject;
  }

  public String getCInputMessage() {
    return cInputMessage == null ? "N" : cInputMessage;
  }

  public void setCInputMessage(String cInputMessage) {
    this.cInputMessage = cInputMessage;
  }

  public String getTFldMessage() {
    return tFldMessage == null ? "N" : tFldMessage;
  }

  public void setTFldMessage(String tFldMessage) {
    this.tFldMessage = tFldMessage;
  }

  public String getTValueMessage() {
    return tValueMessage == null ? "" : tValueMessage;
  }

  public void setTValueMessage(String tValueMessage) {
    this.tValueMessage = tValueMessage;
  }

  public String getTFldMessageID() {
    return tFldMessageID == null ? "" : tFldMessageID;
  }

  public void setTFldMessageID(String tFldMessageID) {
    this.tFldMessageID = tFldMessageID;
  }

  /**
   * This method is used when a transform is duplicated in hop gui. It needs to return a deep copy
   * of this transform's meta object. Be sure to create proper deep copies if the transform
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
   * that end a IRowMeta object is passed in, containing the row-stream structure as it is when
   * entering the transform. This method must apply any changes the transform makes to the row
   * stream. Usually a transform adds fields to the row-stream.
   *
   * @param inputRowMeta the row structure coming in to the transform
   * @param name the name of the transform making the changes
   * @param info row structures of any info transform coming in
   * @param nextTransform the description of a transform this transform is passing rows to
   * @param space the variable space for resolving variables
   * @param metadataProvider the metadataProvider to optionally read from
   */
  @Override
  public void getFields(
      IRowMeta inputRowMeta,
      String name,
      IRowMeta[] info,
      TransformMeta nextTransform,
      IVariables space,
      IHopMetadataProvider metadataProvider)
      throws HopTransformException {

    /*
     * This implementation appends the outputField to the row-stream
     */

    try {

      if (tFldMessageID != null && !tFldMessageID.equals("")) {
        IValueMeta valueMeta =
            ValueMetaFactory.createValueMeta(tFldMessageID, IValueMeta.TYPE_STRING);
        valueMeta.setName(tFldMessageID.toUpperCase());
        valueMeta.setTrimType(IValueMeta.TRIM_TYPE_BOTH);
        valueMeta.setOrigin(name);
        inputRowMeta.addValueMeta(valueMeta);
      }

    } catch (HopPluginException e) {
      logBasic(e.getMessage());
      throw new HopTransformException(e);
    }
  }

  /**
   * This method is called when the user selects the "Verify Transformation" option in Hop Gui. A
   * list of remarks is passed in that this method should add to. Each remark is a comment, warning,
   * error, or ok. The method should perform as many checks as necessary to catch design-time
   * errors.
   *
   * <p>Typical checks include: - verify that all mandatory configuration is given - verify that the
   * transform receives any input, unless it's a row generating transform - verify that the
   * transform does not receive any input if it does not take them into account - verify that the
   * transform finds fields it relies on in the row-stream
   *
   * @param remarks the list of remarks to append to
   * @param transMeta the description of the pipeline
   * @param transformMeta the description of the transform
   * @param prev the structure of the incoming row-stream
   * @param input names of transforms sending input to the transform
   * @param output names of transforms this transform is sending output to
   * @param info fields coming in from info transform
   * @param metaStore metaStore to optionally read from
   */
  @Override
  public void check(
      List<ICheckResult> remarks,
      PipelineMeta transMeta,
      TransformMeta transformMeta,
      IRowMeta prev,
      String[] input,
      String[] output,
      IRowMeta info,
      IVariables space,
      IHopMetadataProvider metaStore) {

    CheckResult cr;

    // See if there are input streams leading to this transform!
    if (input.length > 0) {
      cr =
          new CheckResult(
              ICheckResult.TYPE_RESULT_OK,
              BaseMessages.getString(PKG, "SNSNotify.CheckResult.ReceivingRows.OK"),
              transformMeta);
      remarks.add(cr);
    } else {
      cr =
          new CheckResult(
              ICheckResult.TYPE_RESULT_ERROR,
              BaseMessages.getString(PKG, "SNSNotify.CheckResult.ReceivingRows.ERROR"),
              transformMeta);
      remarks.add(cr);
    }

    // Check for Credentials
    if ((getAwsCredChain() == "N")
        && (getAwsKey().isEmpty() || getAwsKeySecret().isEmpty() || getAwsRegion().isEmpty())) {
      cr =
          new CheckResult(
              ICheckResult.TYPE_RESULT_ERROR,
              BaseMessages.getString(PKG, "SNSNotify.CheckResult.AWSCredentials.ERROR"),
              transformMeta);
      remarks.add(cr);
    } else {
      cr =
          new CheckResult(
              ICheckResult.TYPE_RESULT_OK,
              BaseMessages.getString(PKG, "SNSNotify.CheckResult.AWSCredentials.OK"),
              transformMeta);
      remarks.add(cr);
    }

    // Check for NotifyPoint
    if (getNotifyPoint().isEmpty()) {
      cr =
          new CheckResult(
              ICheckResult.TYPE_RESULT_ERROR,
              BaseMessages.getString(PKG, "SNSNotify.CheckResult.NotifyPoint.ERROR"),
              transformMeta);
      remarks.add(cr);
    } else {
      cr =
          new CheckResult(
              ICheckResult.TYPE_RESULT_OK,
              BaseMessages.getString(PKG, "SNSNotify.CheckResult.NotifyPoint.OK"),
              transformMeta);
      remarks.add(cr);
    }

    // Check for NotifySettings
    boolean notifyPropsError = false;
    if ((getCInputtopicArn().equals("Y") && getTFldtopicARN().isEmpty())
        || (getCInputtopicArn().equals("N") && getTValuetopicARN().isEmpty())) {
      notifyPropsError = true;
      cr =
          new CheckResult(
              ICheckResult.TYPE_RESULT_OK,
              BaseMessages.getString(PKG, "SNSNotify.CheckResult.NotifyProps.topicARN.ERROR"),
              transformMeta);
      remarks.add(cr);
    }
    if ((getCInputSubject().equals("Y") && getTFldSubject().isEmpty())
        || (getCInputSubject().equals("N") && getTValueSubject().isEmpty())) {
      notifyPropsError = true;
      cr =
          new CheckResult(
              ICheckResult.TYPE_RESULT_OK,
              BaseMessages.getString(PKG, "SNSNotify.CheckResult.NotifyProps.Subject.ERROR"),
              transformMeta);
      remarks.add(cr);
    }
    if ((getCInputMessage().equals("Y") && getTFldMessage().isEmpty())
        || (getCInputMessage().equals("N") && getTValueMessage().isEmpty())) {
      notifyPropsError = true;
      cr =
          new CheckResult(
              ICheckResult.TYPE_RESULT_OK,
              BaseMessages.getString(PKG, "SNSNotify.CheckResult.NotifyProps.Message.ERROR"),
              transformMeta);
      remarks.add(cr);
    }

    if (!notifyPropsError) {
      cr =
          new CheckResult(
              ICheckResult.TYPE_RESULT_OK,
              BaseMessages.getString(PKG, "SNSNotify.CheckResult.NotifyProps.OK"),
              transformMeta);
      remarks.add(cr);
    }
  }

  @Override
  public boolean supportsErrorHandling() {
    return true;
  }
}
