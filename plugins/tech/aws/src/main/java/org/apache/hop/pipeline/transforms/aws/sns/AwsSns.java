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

import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.BaseTransform;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.sns.SnsClient;
import software.amazon.awssdk.services.sns.SnsClientBuilder;
import software.amazon.awssdk.services.sns.model.PublishRequest;
import software.amazon.awssdk.services.sns.model.PublishResponse;
import software.amazon.awssdk.services.sns.model.SnsException;

public class AwsSns {

  private SnsClient snsClient;
  private SnsNotifyMeta meta;
  private String awsKey;
  private String awsRegion;
  private String awsKeySecret;
  private BaseTransform baseTransform;
  private PipelineMeta pipelineMeta;
  private String awsCredChain;

  public AwsSns(
      SnsNotifyMeta transformMeta, PipelineMeta pipelineMeta, BaseTransform baseTransform) {

    this.meta = transformMeta;
    this.baseTransform = baseTransform;
    this.pipelineMeta = pipelineMeta;

    this.awsCredChain = this.baseTransform.resolve(meta.getAwsCredChain());
    this.awsKey = this.baseTransform.resolve(meta.getAwsKey());
    this.awsKeySecret = this.baseTransform.resolve(meta.getAwsKeySecret());
    this.awsRegion = this.baseTransform.resolve(meta.getAwsRegion());
  }

  /**
   * Establishing new Connection to Amazon Webservices
   *
   * @return true on successful connection
   */
  public boolean getAWSConnection() {
    try {
      baseTransform.logBasic("Starting connection to AWS SNS");

      SnsClientBuilder builder = SnsClient.builder().region(Region.of(this.awsRegion));

      if (this.awsCredChain.equalsIgnoreCase("N")) {
        AwsBasicCredentials awsCreds = AwsBasicCredentials.create(this.awsKey, this.awsKeySecret);
        snsClient = builder.credentialsProvider(StaticCredentialsProvider.create(awsCreds)).build();
        baseTransform.logBasic(
            "Connected to SNS in Region "
                + this.awsRegion
                + " with API-Key >>"
                + this.awsKey
                + "<<");
      } else {
        snsClient = builder.credentialsProvider(DefaultCredentialsProvider.create()).build();
        baseTransform.logBasic("Connected to SNS with provided Credentials Chain");
      }
      return true;

    } catch (Exception e) {
      baseTransform.logError(e.getMessage());
    }

    return false;
  }

  /** Disconnects from AWS */
  public void disconnectAWSConnection() {
    try {
      if (snsClient != null) {
        snsClient.close();
      }
      baseTransform.logBasic("Disconnected from SNS in Region " + this.awsRegion);
    } catch (Exception e) {
      baseTransform.logError(e.getMessage());
      baseTransform.setErrors(1);
    }
  }

  /**
   * Publish Message with Subject to a topicARN
   *
   * @param tARN AWS ARN for SNS topic
   * @param subj Subject for Message
   * @param msg Message Content
   * @return SNS messageID on successful publish
   */
  public String publishToSNS(String tARN, String subj, String msg) throws SnsException {

    String topicARN = baseTransform.resolve(tARN);
    String subject = baseTransform.resolve(subj);
    String message = baseTransform.resolve(msg);

    PublishRequest publishRequest =
        PublishRequest.builder().topicArn(topicARN).message(message).subject(subject).build();
    PublishResponse publishResult = snsClient.publish(publishRequest);
    String messageId = publishResult.messageId();
    baseTransform.logBasic(messageId);
    return messageId;
  }
}
