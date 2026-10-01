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

package org.apache.hop.pipeline.transforms.jms.consumer;

import org.apache.commons.lang3.StringUtils;
import org.apache.hop.core.Const;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.row.RowMeta;
import org.apache.hop.pipeline.Pipeline;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.BaseTransform;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.apache.hop.pipeline.transforms.jms.shared.JmsConnection;
import org.apache.hop.pipeline.transforms.jms.shared.JmsProvider;
import org.apache.hop.pipeline.transforms.jms.shared.MessageQueueRecord;

public class JmsConsumer extends BaseTransform<JmsConsumerMeta, JmsConsumerData> {

  public JmsConsumer(
      TransformMeta transformMeta,
      JmsConsumerMeta meta,
      JmsConsumerData data,
      int copyNr,
      PipelineMeta pipelineMeta,
      Pipeline pipeline) {
    super(transformMeta, meta, data, copyNr, pipelineMeta, pipeline);
  }

  @Override
  public boolean init() {
    if (!super.init()) {
      return false;
    }
    try {
      JmsConnection connection =
          getMetadataProvider()
              .getSerializer(JmsConnection.class)
              .load(resolve(meta.getConnectionName()));
      if (connection == null) {
        logError(
            "JMS connection '" + meta.getConnectionName() + "' could not be found in the metadata");
        return false;
      }

      data.maxMessages = Const.toLong(resolve(meta.getMaxMessages()), 0L);
      data.receiveTimeout = Const.toLong(resolve(meta.getReceiveTimeout()), 5000L);
      if (data.receiveTimeout <= 0) {
        logError("The receive timeout must be greater than zero");
        return false;
      }

      data.provider =
          new JmsProvider(
              connection,
              meta.getDestination(),
              meta.isTopic(),
              meta.isTransacted(),
              resolve(meta.getMessageSelector()),
              resolve(meta.getDurableSubscription()),
              getLogChannel());
      data.provider.connect(this);
      return true;
    } catch (Exception e) {
      logError("Unable to start the JMS consumer", e);
      return false;
    }
  }

  @Override
  public boolean processRow() throws HopException {
    if (first) {
      first = false;
      data.outputRowMeta = new RowMeta();
      meta.getFields(data.outputRowMeta, getTransformName(), null, null, this, metadataProvider);
    }

    if (data.maxMessages > 0 && data.messageCount >= data.maxMessages) {
      setOutputDone();
      return false;
    }

    MessageQueueRecord record = data.provider.receive(data.receiveTimeout);
    if (record == null) {
      // Nothing waiting within the timeout: treat the destination as drained. A consumer that
      // should wait indefinitely belongs behind a scheduler, not a blocked pipeline.
      if (isDetailed()) {
        logDetailed(
            "No message received within "
                + data.receiveTimeout
                + "ms, stopping after "
                + data.messageCount
                + " message(s)");
      }
      setOutputDone();
      return false;
    }

    putRow(data.outputRowMeta, buildRow(record));

    // Acknowledge only once the row is on its way, so a failure upstream of this point leaves
    // the message on the broker rather than silently consuming it.
    data.provider.acknowledge(record);
    data.messageCount++;

    if (checkFeedback(getLinesRead()) && isBasic()) {
      logBasic("Read " + getLinesRead() + " messages from the JMS destination");
    }
    return true;
  }

  /** Builds the output row in the same order {@link JmsConsumerMeta#getFields} declares it. */
  private Object[] buildRow(MessageQueueRecord record) {
    Object[] row = new Object[data.outputRowMeta.size()];
    int index = 0;
    if (StringUtils.isNotEmpty(resolve(meta.getBodyField()))) {
      row[index++] = record.getBody();
    }
    if (StringUtils.isNotEmpty(resolve(meta.getKeyField()))) {
      row[index++] = record.getKey();
    }
    if (StringUtils.isNotEmpty(resolve(meta.getDestinationField()))) {
      row[index++] = record.getDestination();
    }
    if (StringUtils.isNotEmpty(resolve(meta.getMessageIdField()))) {
      row[index++] = record.getMessageId();
    }
    if (StringUtils.isNotEmpty(resolve(meta.getTimestampField()))) {
      row[index] = record.getTimestamp();
    }
    return row;
  }

  @Override
  public void dispose() {
    if (data.provider != null) {
      data.provider.close();
      data.provider = null;
    }
    super.dispose();
  }
}
