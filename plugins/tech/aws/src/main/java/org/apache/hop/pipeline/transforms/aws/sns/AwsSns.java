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

import com.amazonaws.AmazonClientException;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.auth.DefaultAWSCredentialsProviderChain;
import com.amazonaws.services.sns.AmazonSNSClient;
import com.amazonaws.services.sns.AmazonSNSClientBuilder;
import com.amazonaws.services.sns.model.AmazonSNSException;
import com.amazonaws.services.sns.model.PublishRequest;
import com.amazonaws.services.sns.model.PublishResult;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.BaseTransform;

public class AwsSns {

  private AmazonSNSClient snsClient;
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

      if (this.awsCredChain.equalsIgnoreCase("N")) {
        BasicAWSCredentials awsCreds = new BasicAWSCredentials(this.awsKey, this.awsKeySecret);
        snsClient =
            (AmazonSNSClient)
                AmazonSNSClientBuilder.standard()
                    .withCredentials(new AWSStaticCredentialsProvider(awsCreds))
                    .withRegion(this.awsRegion)
                    .build();

        baseTransform.logBasic(
            "Connected to SNS in Region "
                + this.awsRegion
                + " with API-Key >>"
                + this.awsKey
                + "<<");

      } else {
        AWSCredentialsProvider provider = new DefaultAWSCredentialsProviderChain();
        snsClient =
            (AmazonSNSClient) AmazonSNSClientBuilder.standard().withCredentials(provider).build();

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
      snsClient.shutdown();

      baseTransform.logBasic("Disconnected from SNS in Region " + this.awsRegion);

    } catch (AmazonClientException e) {
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
  public String publishToSNS(String tARN, String subj, String msg) throws AmazonSNSException {

    String topicARN = baseTransform.resolve(tARN);
    String subject = baseTransform.resolve(subj);
    String message = baseTransform.resolve(msg);

    try {

      PublishRequest publishRequest = new PublishRequest(topicARN, message, subject);
      PublishResult publishResult = snsClient.publish(publishRequest);
      String messageId = publishResult.getMessageId();
      baseTransform.logBasic(messageId);
      return messageId;

    } catch (AmazonSNSException e) {
      throw e;
    }
  }
}
