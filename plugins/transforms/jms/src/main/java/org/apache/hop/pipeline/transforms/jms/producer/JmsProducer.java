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

package org.apache.hop.pipeline.transforms.jms.producer;

import org.apache.commons.lang3.StringUtils;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.pipeline.Pipeline;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.BaseTransform;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.apache.hop.pipeline.transforms.jms.shared.JmsConnection;
import org.apache.hop.pipeline.transforms.jms.shared.JmsProvider;
import org.apache.hop.pipeline.transforms.jms.shared.MessageQueueRecord;

public class JmsProducer extends BaseTransform<JmsProducerMeta, JmsProducerData> {

  public JmsProducer(
      TransformMeta transformMeta,
      JmsProducerMeta meta,
      JmsProducerData data,
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
      if (StringUtils.isEmpty(meta.getBodyField())) {
        logError("No field was selected to use as the message body");
        return false;
      }

      data.provider =
          new JmsProvider(
              connection,
              meta.getDestination(),
              meta.isTopic(),
              meta.isTransacted(),
              null,
              null,
              getLogChannel());
      data.provider.connect(this);
      return true;
    } catch (Exception e) {
      logError("Unable to start the JMS producer", e);
      return false;
    }
  }

  @Override
  public boolean processRow() throws HopException {
    Object[] row = getRow();
    if (row == null) {
      setOutputDone();
      return false;
    }

    if (first) {
      first = false;
      data.bodyFieldIndex = getInputRowMeta().indexOfValue(resolve(meta.getBodyField()));
      if (data.bodyFieldIndex < 0) {
        throw new HopException(
            "Body field '"
                + resolve(meta.getBodyField())
                + "' could not be found in the input row: "
                + getInputRowMeta().getFieldNames().length
                + " field(s) available");
      }
      String keyField = resolve(meta.getKeyField());
      if (StringUtils.isNotEmpty(keyField)) {
        data.keyFieldIndex = getInputRowMeta().indexOfValue(keyField);
        if (data.keyFieldIndex < 0) {
          throw new HopException(
              "Key field '" + keyField + "' could not be found in the input row");
        }
      }
    }

    MessageQueueRecord record = new MessageQueueRecord();
    record.setBody(getInputRowMeta().getString(row, data.bodyFieldIndex));
    if (data.keyFieldIndex >= 0) {
      record.setKey(getInputRowMeta().getString(row, data.keyFieldIndex));
    }
    data.provider.send(record);

    // The row is passed through unchanged so the transform can sit mid-pipeline.
    putRow(getInputRowMeta(), row);

    if (checkFeedback(getLinesWritten()) && isBasic()) {
      logBasic("Sent " + getLinesWritten() + " messages to the JMS destination");
    }
    return true;
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
