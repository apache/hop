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

package org.apache.hop.pipeline.transforms.eventhubs.listen;

import com.microsoft.azure.eventhubs.EventData;
import com.microsoft.azure.eventprocessorhost.CloseReason;
import com.microsoft.azure.eventprocessorhost.IEventProcessor;
import com.microsoft.azure.eventprocessorhost.PartitionContext;
import org.apache.commons.lang.StringUtils;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.row.RowDataUtil;

import java.sql.Timestamp;
import java.time.Instant;
import java.util.concurrent.atomic.AtomicBoolean;

public class AzureListenerEventProcessor implements IEventProcessor {

  private final AzureListener azureTransform;
  private final AzureListenerData azureData;

  private int checkpointBatchingSize;
  private int checkpointBatchingCount;

  private long lastIterationTime = -1;

  private long passedRowsCount;

  private AtomicBoolean wait = new AtomicBoolean(false);

  private PartitionContext lastContext;
  private EventData lastData;

  public AzureListenerEventProcessor(
      AzureListener Transform, AzureListenerData data, int checkpointBatchingSize) {
    this.azureTransform = Transform;
    this.azureData = data;
    this.checkpointBatchingSize = checkpointBatchingSize;

    passedRowsCount = 0L;
  }

  // OnOpen is called when a new event processor instance is created by the host. In a real
  // implementation, this
  // is the place to do initialization so that events can be processed when they arrive, such as
  // opening a database
  // connection.
  //
  @Override
  public void onOpen(PartitionContext context) {
    if (azureTransform.isDebug()) {
      azureTransform.logDebug("Partition " + context.getPartitionId() + " is opening");
    }
  }

  // OnClose is called when an event processor instance is being shut down. The reason argument
  // indicates whether the shut down
  // is because another host has stolen the lease for this partition or due to error or host
  // shutdown. In a real implementation,
  // this is the place to do cleanup for resources that were opened in onOpen.
  //
  @Override
  public void onClose(PartitionContext context, CloseReason reason) {
    if (azureTransform.isDebug()) {
      azureTransform.logDebug(
          "Partition " + context.getPartitionId() + " is closing for reason " + reason.toString());
    }
  }

  // onError is called when an error occurs in EventProcessorHost code that is tied to this
  // partition, such as a receiver failure.
  // It is NOT called for exceptions thrown out of onOpen/onClose/onEvents. EventProcessorHost is
  // responsible for recovering from
  // the error, if possible, or shutting the event processor down if not, in which case there will
  // be a call to onClose. The
  // notification provided to onError is primarily informational.
  //
  @Override
  public void onError(PartitionContext context, Throwable error) {
    azureTransform.logError(
        "Error on partition id " + context.getPartitionId() + " : " + error.toString(), error);
  }

  // onEvents is called when events are received on this partition of the Event Hub. The maximum
  // number of events in a batch
  // can be controlled via EventProcessorOptions. Also, if the "invoke processor after receive
  // timeout" option is set to true,
  // this method will be called with null when a receive timeout occurs.
  //
  @Override
  public void onEvents(PartitionContext context, Iterable<EventData> events) throws Exception {

    // First time calibration time
    //
    if (lastIterationTime < 0) {
      lastIterationTime = System.currentTimeMillis();
    }

    // See if we're not doing an iteration in the max wait time timer task...
    //
    while (wait.get() && !azureTransform.isStopped()) {
      Thread.sleep(10);
    }

    int eventCount = 0;
    for (EventData data : events) {

      // It is important to have a try-catch around the processing of each event. Throwing out of
      // onEvents deprives
      // you of the chance to process any remaining events in the batch.
      //
      azureTransform.incrementLinesInput();

      try {

        Object[] row = RowDataUtil.allocateRowData(azureData.outputRowMeta.size());
        int index = 0;

        // Message : String
        //
        if (StringUtils.isNotEmpty(azureData.outputField)) {
          row[index++] = new String(data.getBytes(), "UTF-8");
        }

        // Partition ID : String
        //
        if (StringUtils.isNotEmpty(azureData.partitionIdField)) {
          row[index++] = context.getPartitionId();
        }

        // Offset : String
        //
        if (StringUtils.isNotEmpty(azureData.offsetField)) {
          row[index++] = data.getSystemProperties().getOffset();
        }

        // Sequence number: Integer
        //
        if (StringUtils.isNotEmpty(azureData.sequenceNumberField)) {
          row[index++] = data.getSystemProperties().getSequenceNumber();
        }

        // Host: String
        //
        if (StringUtils.isNotEmpty(azureData.hostField)) {
          row[index++] = context.getOwner();
        }

        // Enqued Time: Timestamp
        //
        if (StringUtils.isNotEmpty(azureData.enqueuedTimeField)) {
          Instant enqueuedTime = data.getSystemProperties().getEnqueuedTime();
          row[index++] = Timestamp.from(enqueuedTime);
        }

        if (azureData.stt) {
          azureData.sttRowProducer.putRow(azureData.outputRowMeta, row);
          passedRowsCount++;
          lastContext = context;
          lastData = data;
        } else {
          // Just pass the row along, row safety is not of primary concern
          //
          azureTransform.putRow(azureData.outputRowMeta, row);
        }

        if (azureTransform.isDebug()) {
          azureTransform.logDebug(
              "Event read and passed for PartitionId ("
                  + context.getPartitionId()
                  + ","
                  + data.getSystemProperties().getOffset()
                  + ","
                  + data.getSystemProperties().getSequenceNumber()
                  + "): "
                  + new String(data.getBytes(), "UTF8")
                  + " ("
                  + index
                  + " values in row)");
        }

        eventCount++;

        // Checkpointing persists the current position in the event stream for this partition and
        // means that the next
        // time any host opens an event processor on this event hub+consumer group+partition
        // combination, it will start
        // receiving at the event after this one. Checkpointing is usually not a fast operation, so
        // there is a tradeoff
        // between checkpointing frequently (to minimize the number of events that will be
        // reprocessed after a crash, or
        // if the partition lease is stolen) and checkpointing infrequently (to reduce the impact on
        // event processing
        // performance). Checkpointing every five events is an arbitrary choice for this sample.
        //
        this.checkpointBatchingCount++;
        if ((checkpointBatchingCount % checkpointBatchingSize) == 0) {
          if (azureTransform.isDebug()) {
            azureTransform.logDebug(
                "Partition "
                    + context.getPartitionId()
                    + " checkpointing at "
                    + data.getSystemProperties().getOffset()
                    + ","
                    + data.getSystemProperties().getSequenceNumber());
          }

          if (azureData.stt) {

            if (azureTransform.isDetailed()) {
              azureTransform.logDetailed(
                  "Processing the rows sent to the batch transformation at event count "
                      + checkpointBatchingCount);
            }

            doOneIteration();
          } else {
            // Checkpoints are created asynchronously. It is important to wait for the result of
            // checkpointing
            // before exiting onEvents or before creating the next checkpoint, to detect errors and
            // to ensure proper ordering.
            //
            context.checkpoint(data).get();
          }
        }
      } catch (Exception e) {
        azureTransform.logError("Processing failed for an event: " + e.toString(), e);
        azureTransform.setErrors(1);
        azureTransform.stopAll();
      }
    }
    if (azureTransform.isDebug()) {
      azureTransform.logDebug(
          "Partition "
              + context.getPartitionId()
              + " batch size was "
              + eventCount
              + " for host "
              + context.getOwner());
    }
  }

  public synchronized void doOneIteration() throws HopException {
    azureData.sttExecutor.oneIteration();

    passedRowsCount = 0;

    // Keep track of when we last did an iteration.
    //
    lastIterationTime = System.currentTimeMillis();

    if (azureData.sttExecutor.isStopped() || azureData.sttExecutor.getErrors() > 0) {
      // Something went wrong, bail out, don't do checkpoint
      //
      azureData.sttPipeline.stopAll();
      azureTransform.setErrors(1);
      azureTransform.setStopped(true);
      azureTransform.stopAll();

      throw new HopException("Error in batch transformation, halting");
    }

    // Checkpoints are created asynchronously. It is important to wait for the result of
    // checkpointing
    // before exiting onEvents or before creating the next checkpoint, to detect errors and to
    // ensure proper ordering.
    //
    try {
      lastContext.checkpoint(lastData).get();
    } catch (Exception e) {
      throw new HopException("Failed to do checkpoint", e);
    }
  }

  public int getCheckpointBatchingSize() {
    return checkpointBatchingSize;
  }

  public void setCheckpointBatchingSize(int checkpointBatchingSize) {
    this.checkpointBatchingSize = checkpointBatchingSize;
  }

  public int getCheckpointBatchingCount() {
    return checkpointBatchingCount;
  }

  public void setCheckpointBatchingCount(int checkpointBatchingCount) {
    this.checkpointBatchingCount = checkpointBatchingCount;
  }

  public AzureListener getAzureTransform() {
    return azureTransform;
  }

  public AzureListenerData getAzureData() {
    return azureData;
  }

  public long getLastIterationTime() {
    return lastIterationTime;
  }

  public void setLastIterationTime(long lastIterationTime) {
    this.lastIterationTime = lastIterationTime;
  }

  public long getPassedRowsCount() {
    return passedRowsCount;
  }

  public void setPassedRowsCount(long passedRowsCount) {
    this.passedRowsCount = passedRowsCount;
  }

  public void startWait() {
    wait.set(true);
  }

  public void endWait() {
    wait.set(false);
  }
}
