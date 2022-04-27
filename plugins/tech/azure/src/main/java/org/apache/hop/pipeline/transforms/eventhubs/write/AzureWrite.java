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
 *
 */

package org.apache.hop.pipeline.transforms.eventhubs.write;

import com.microsoft.azure.eventhubs.ConnectionStringBuilder;
import com.microsoft.azure.eventhubs.EventData;
import com.microsoft.azure.eventhubs.EventHubClient;
import com.microsoft.azure.eventhubs.EventHubException;
import org.apache.hop.core.Const;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.exception.HopTransformException;
import org.apache.hop.pipeline.Pipeline;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.BaseTransform;
import org.apache.hop.pipeline.transform.TransformMeta;

import java.nio.charset.StandardCharsets;
import java.util.LinkedList;
import java.util.concurrent.Executors;

public class AzureWrite extends BaseTransform<AzureWriterMeta, AzureWriterData> {

  public AzureWrite(
      TransformMeta transformMeta,
      AzureWriterMeta meta,
      AzureWriterData data,
      int copyNr,
      PipelineMeta pipelineMeta,
      Pipeline pipeline) {
    super(transformMeta, meta, data, copyNr, pipelineMeta, pipeline);
  }

  @Override
  public boolean init() {

    data.batchSize = Const.toLong(resolve(meta.getBatchSize()), 1);
    data.list = new LinkedList<>();

    return super.init();
  }

  @Override
  public void dispose() {

    data.executorService.shutdown();

    super.dispose();
  }

  @Override
  public boolean processRow() throws HopException {

    // Input row
    //
    Object[] row = getRow();
    if (row == null) {

      // See if we have data left in the message buffer
      //
      if (data.batchSize > 1) {
        if (data.list.size() > 0) {
          try {
            data.eventHubClient.sendSync(data.list);
          } catch (EventHubException e) {
            throw new HopTransformException("Unable to send messages", e);
          }
          data.list = null;
        }
      }

      setOutputDone();
      return false;
    }

    if (first) {
      first = false;

      // get the output fields...
      //
      data.outputRowMeta = getInputRowMeta().clone();
      meta.getFields(
          data.outputRowMeta, getTransformName(), null, getTransformMeta(), this, metadataProvider);

      data.fieldIndex = getInputRowMeta().indexOfValue(meta.getMessageField());
      if (data.fieldIndex < 0) {
        throw new HopTransformException(
            "Unable to find field '" + meta.getMessageField() + "' in the Transform input");
      }

      log.logBasic("Creating connection string");

      String namespace = resolve(meta.getNamespace());
      String eventHubName = resolve(meta.getEventHubName());
      String sasKeyName = resolve(meta.getSasKeyName());
      String sasKey = resolve(meta.getSasKey());

      data.connectionStringBuilder =
          new ConnectionStringBuilder()
              .setNamespaceName(namespace)
              .setEventHubName(eventHubName)
              .setSasKeyName(sasKeyName)
              .setSasKey(sasKey);

      log.logBasic("Opening new executor service");
      data.executorService = Executors.newSingleThreadScheduledExecutor();
      log.logBasic("Creating event hub client");
      try {
        data.eventHubClient =
            EventHubClient.createFromConnectionStringSync(
                data.connectionStringBuilder.toString(), data.executorService);
      } catch (Exception e) {
        throw new HopTransformException("Unable to create event hub client", e);
      }
    }

    String message = getInputRowMeta().getString(row, data.fieldIndex);
    byte[] payloadBytes = message.getBytes(StandardCharsets.UTF_8);
    EventData sendEvent = EventData.create(payloadBytes);
    try {

      if (data.batchSize <= 1) {
        data.eventHubClient.sendSync(sendEvent);
      } else {
        data.list.add(sendEvent);

        if (data.list.size() >= data.batchSize) {
          data.eventHubClient.sendSync(data.list);
          data.list.clear();
        }
      }
    } catch (EventHubException e) {
      throw new HopTransformException("Unable to send message to event hubs", e);
    }

    // Pass the rows to the next Transforms
    //
    putRow(data.outputRowMeta, row);
    return true;
  }
}
