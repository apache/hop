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

import java.util.List;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.BaseTransform;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.SqsClientBuilder;
import software.amazon.awssdk.services.sqs.model.DeleteMessageRequest;
import software.amazon.awssdk.services.sqs.model.Message;
import software.amazon.awssdk.services.sqs.model.ReceiveMessageRequest;
import software.amazon.awssdk.services.sqs.model.ReceiveMessageResponse;
import software.amazon.awssdk.services.sqs.model.SqsException;

public class AwsSqsReader {

  private SqsClient sqsClient;
  private SqsReaderMeta meta;
  private String awsKey;
  private String awsRegion;
  private String awsKeySecret;
  private BaseTransform baseTransform;
  private PipelineMeta pipelineMeta;
  private String awsCredChain;
  private String deleteMessage;

  public AwsSqsReader(SqsReaderMeta transformMeta, PipelineMeta t, BaseTransform bst) {

    this.meta = transformMeta;
    this.baseTransform = bst;
    this.pipelineMeta = t;

    this.awsCredChain = this.baseTransform.resolve(meta.getAwsCredChain());
    this.awsKey = this.baseTransform.resolve(meta.getAwsKey());
    this.awsKeySecret = this.baseTransform.resolve(meta.getAwsKeySecret());
    this.awsRegion = this.baseTransform.resolve(meta.getAwsRegion());
    this.deleteMessage = this.baseTransform.resolve(meta.getTFldMessageDelete());
  }

  /**
   * Establishing new Connection to Amazon Webservices
   *
   * @return true on successful connection
   */
  public boolean getAWSConnection() {
    try {
      baseTransform.logBasic("Starting connection to AWS SQS");

      SqsClientBuilder builder = SqsClient.builder().region(Region.of(this.awsRegion));

      if (this.awsCredChain.equalsIgnoreCase("N")) {
        AwsBasicCredentials awsCreds = AwsBasicCredentials.create(this.awsKey, this.awsKeySecret);
        sqsClient = builder.credentialsProvider(StaticCredentialsProvider.create(awsCreds)).build();
      } else {
        sqsClient = builder.credentialsProvider(DefaultCredentialsProvider.create()).build();
        baseTransform.logBasic("Connected to SQS with provided Credentials Chain");
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
      if (sqsClient != null) {
        sqsClient.close();
      }
      baseTransform.logBasic("Disconnected from SQS");
    } catch (Exception e) {
      baseTransform.logError(e.getMessage());
      baseTransform.setErrors(1);
    }
  }

  /**
   * @param queueURL queue URL
   * @param numMsgs max number of messages
   * @param isPreview whether this is a preview
   * @return list of SQS messages
   * @throws SqsException on SQS errors
   */
  public List<Message> readMessages(String queueURL, int numMsgs, boolean isPreview)
      throws SqsException {

    int numMessages = (numMsgs > 10) ? 10 : numMsgs;

    ReceiveMessageRequest receiveMessageRequest =
        ReceiveMessageRequest.builder().queueUrl(queueURL).maxNumberOfMessages(numMessages).build();
    ReceiveMessageResponse response = sqsClient.receiveMessage(receiveMessageRequest);
    List<Message> messages = response.messages();

    baseTransform.logDebug(messages.size() + " Message(s) retrieved from queue");

    if (this.deleteMessage.equalsIgnoreCase("Y") && !isPreview) {
      for (Message m : messages) {
        sqsClient.deleteMessage(
            DeleteMessageRequest.builder()
                .queueUrl(queueURL)
                .receiptHandle(m.receiptHandle())
                .build());
      }
      baseTransform.logDebug(messages.size() + " Message(s) deleted from queue");
    }

    return messages;
  }
}
