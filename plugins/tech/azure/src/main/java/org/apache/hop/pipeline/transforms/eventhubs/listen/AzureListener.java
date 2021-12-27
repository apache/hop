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

import com.microsoft.azure.eventhubs.ConnectionStringBuilder;
import com.microsoft.azure.eventhubs.EventHubClient;
import com.microsoft.azure.eventprocessorhost.EventProcessorHost;
import com.microsoft.azure.eventprocessorhost.EventProcessorOptions;
import org.apache.commons.lang.StringUtils;
import org.apache.hop.core.Const;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.exception.HopTransformException;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.RowMeta;
import org.apache.hop.pipeline.Pipeline;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.SingleThreadedPipelineExecutor;
import org.apache.hop.pipeline.engines.local.LocalPipelineEngine;
import org.apache.hop.pipeline.transform.BaseTransform;
import org.apache.hop.pipeline.transform.ITransform;
import org.apache.hop.pipeline.transform.RowAdapter;
import org.apache.hop.pipeline.transform.TransformMeta;

import java.util.LinkedList;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.Executors;

public class AzureListener extends BaseTransform<AzureListenerMeta, AzureListenerData>
    implements ITransform<AzureListenerMeta, AzureListenerData> {

  public AzureListener(
      TransformMeta transformMeta,
      AzureListenerMeta meta,
      AzureListenerData data,
      int copyNr,
      PipelineMeta pipelineMeta,
      Pipeline pipeline) {
    super(transformMeta, meta, data, copyNr, pipelineMeta, pipeline);
  }

  @Override
  public boolean init() {

    data.batchSize = Const.toInt(resolve(meta.getBatchSize()), 100);
    data.prefetchSize = Const.toInt(resolve(meta.getPrefetchSize()), -1);
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

    // This thing is executed only once, rows are processed in the event processor later
    //

    // Get the output fields starting from nothing
    //
    data.outputRowMeta = new RowMeta();
    meta.getRegularRowMeta(data.outputRowMeta, this);

    data.outputField = resolve(meta.getOutputField());
    data.partitionIdField = resolve(meta.getPartitionIdField());
    data.offsetField = resolve(meta.getOffsetField());
    data.sequenceNumberField = resolve(meta.getSequenceNumberField());
    data.hostField = resolve(meta.getHostField());
    data.enqueuedTimeField = resolve(meta.getEnqueuedTimeField());

    String namespace = resolve(meta.getNamespace());
    String eventHubName = resolve(meta.getEventHubName());
    String sasKeyName = resolve(meta.getSasKeyName());
    String sasKey = resolve(meta.getSasKey());
    String consumerGroupName = resolve(meta.getConsumerGroupName());
    String storageContainerName = resolve(meta.getStorageContainerName());
    String storageConnectionString = resolve(meta.getStorageConnectionString());

    String batchTransformationFile = resolve(meta.getBatchPipeline());
    String batchInputTransform = resolve(meta.getBatchInputTransform());
    String batchOutputTransform = resolve(meta.getBatchOutputTransform());

    // Create a single threaded transformation
    //
    if (StringUtils.isNotEmpty(batchTransformationFile)
        && StringUtils.isNotEmpty(batchInputTransform)) {
      logBasic(
          "Passing rows to a batching transformation running single threaded : "
              + batchTransformationFile);
      data.stt = true;
      data.sttMaxWaitTime = Const.toLong(resolve(meta.getBatchMaxWaitTime()), -1L);
      data.sttPipelineMeta = meta.loadBatchPipelineMeta(meta, metadataProvider, this);
      data.sttPipelineMeta.setPipelineType(PipelineMeta.PipelineType.SingleThreaded);
      data.sttPipeline = new LocalPipelineEngine(data.sttPipelineMeta, this, this);
      data.sttPipeline.setParent(getPipeline());
      data.sttPipeline.setParentPipeline(getPipeline());

      // Leave a trace for Spoon...
      //
      getPipeline().addActiveSubPipeline(getTransformName(), data.sttPipeline);

      data.sttPipeline.prepareExecution();

      data.sttRowProducer = data.sttPipeline.addRowProducer(batchInputTransform, 0);

      if (StringUtils.isNotEmpty(batchOutputTransform)) {
        ITransform outputTransform = data.sttPipeline.findRunThread(batchOutputTransform);
        if (outputTransform == null) {
          throw new HopTransformException(
              "Unable to find output transform '" + batchOutputTransform + "'in batch pipeline");
        }
        outputTransform.addRowListener(
            new RowAdapter() {
              @Override
              public void rowWrittenEvent(IRowMeta rowMeta, Object[] row)
                  throws HopTransformException {
                AzureListener.this.putRow(rowMeta, row);
              }
            });
      }

      data.sttPipeline.startThreads();

      data.sttExecutor = new SingleThreadedPipelineExecutor(data.sttPipeline);

      boolean ok = data.sttExecutor.init();
      if (!ok) {
        logError("Initializing batch transformation failed");
        stopAll();
        setErrors(1);
        return false;
      }
    } else {
      data.stt = false;
    }

    log.logDetailed("Creating connection string builder");
    data.connectionStringBuilder =
        new ConnectionStringBuilder()
            .setNamespaceName(namespace)
            .setEventHubName(eventHubName)
            .setSasKeyName(sasKeyName)
            .setSasKey(sasKey);

    log.logDetailed("Opening new executor service");

    data.executorService = Executors.newSingleThreadScheduledExecutor();

    log.logDetailed("Creating event hub client");
    try {
      data.eventHubClient =
          EventHubClient.createFromConnectionStringSync(
              data.connectionStringBuilder.toString(), data.executorService);
    } catch (Exception e) {
      throw new HopTransformException("Unable to create event hub client", e);
    }

    EventProcessorHost host;

    try {
      host =
          EventProcessorHost.EventProcessorHostBuilder.newBuilder(
                  EventProcessorHost.createHostName("HopHost"), consumerGroupName)
              .useAzureStorageCheckpointLeaseManager(
                  storageConnectionString, storageContainerName, "hop")
              .useEventHubConnectionString(data.connectionStringBuilder.toString())
              .build();
    } catch (Exception e) {
      throw new HopException("Unable to set up events host processor", e);
    }
    log.logDetailed("Set up events host named " + host.getHostName());

    EventProcessorOptions options = new EventProcessorOptions();
    options.setExceptionNotification(new AzureListenerErrorNotificationHandler(AzureListener.this));

    if (!StringUtils.isNotEmpty(meta.getBatchSize())) {
      options.setMaxBatchSize(Const.toInt(resolve(meta.getBatchSize()), 100));
    }

    if (!StringUtils.isNotEmpty(meta.getPrefetchSize())) {
      options.setPrefetchCount(Const.toInt(resolve(meta.getPrefetchSize()), 100));
    }

    data.executorService = Executors.newSingleThreadScheduledExecutor();
    try {
      data.eventHubClient =
          EventHubClient.createFromConnectionStringSync(
              data.connectionStringBuilder.toString(), data.executorService);
    } catch (Exception e) {
      throw new HopTransformException("Unable to create event hub client", e);
    }

    // Create our event processor which is going to actually send rows to the batch transformation
    // (or not)
    // and get rows from an optional output Transform.
    //
    final AzureListenerEventProcessor eventProcessor =
        new AzureListenerEventProcessor(AzureListener.this, data, data.batchSize);

    // In case we have a while since an iteration was done sending rows to the batch transformation,
    // keep an eye out for the
    // maximum wait time.  If we go over that time, and we have records in the input of the batch,
    // call oneIteration.
    // We need to make sure to halt the rest though.
    //
    if (data.stt && data.sttMaxWaitTime > 0) {
      // Add a timer to check every max wait time to see whether or not we have to do an
      // iteration...
      //
      logBasic(
          "Checking for stalled rows every 100ms to see if we exceed the maximum wait time: "
              + data.sttMaxWaitTime);
      try {
        Timer timer = new Timer();
        TimerTask timerTask =
            new TimerTask() {
              @Override
              public void run() {
                // Do nothing if we haven't started yet.
                //
                if (eventProcessor.getLastIterationTime() > 0) {
                  if (eventProcessor.getPassedRowsCount() > 0) {
                    long now = System.currentTimeMillis();

                    long diff = now - eventProcessor.getLastIterationTime();
                    if (diff > data.sttMaxWaitTime) {
                      logDetailed(
                          "Stalled rows detected with wait time of " + ((double) diff / 1000));

                      // Call one iteration but halt anything else first.
                      //
                      try {
                        eventProcessor.startWait();
                        eventProcessor.doOneIteration();
                      } catch (Exception e) {
                        throw new RuntimeException(
                            "Error in batch iteration when max wait time was exceeded", e);
                      } finally {
                        eventProcessor.endWait();
                      }
                      logDetailed("Done processing after max wait time.");
                    }
                  }
                }
              }
            };
        // Check ten times per second
        //
        timer.schedule(timerTask, 100, 100);
      } catch (RuntimeException e) {
        throw new HopTransformException(
            "Error in batch iteration when max wait time was exceeded", e);
      }
    }

    try {
      host.registerEventProcessorFactory(partitionContext -> eventProcessor)
          .whenComplete(
              (unused, e) -> {
                // whenComplete passes the result of the previous stage through unchanged,
                // which makes it useful for logging a result without side effects.
                //
                if (e != null) {
                  logError("Failure while registering: " + e.toString());
                  if (e.getCause() != null) {
                    logError("Inner exception: " + e.getCause().toString());
                  }
                  setErrors(1);
                  stopAll();
                  setOutputDone();
                }
              })
          .thenAccept(
              unused -> {
                // This stage will only execute if registerEventProcessor succeeded.
                // If it completed exceptionally, this stage will be skipped.
                //
                // block until we need to stop...
                //
                while (!AzureListener.this.isStopped() && !AzureListener.this.outputIsDone()) {
                  try {
                    Thread.sleep(0, 100);
                  } catch (InterruptedException e) {
                    // Ignore
                  }
                }
              })
          .thenCompose(
              unused ->
                  // This stage will only execute if registerEventProcessor succeeded.
                  //
                  // Processing of events continues until unregisterEventProcessor is called.
                  // Unregistering shuts down the
                  // receivers on all currently owned leases, shuts down the instances of the event
                  // processor class, and
                  // releases the leases for other instances of EventProcessorHost to claim.
                  //
                  host.unregisterEventProcessor())
          .exceptionally(
              e -> {
                logError("Failure while unregistering: " + e.toString());
                if (e.getCause() != null) {
                  logError("Inner exception: " + e.getCause().toString());
                }
                return null;
              })
          .get(); // Wait for everything to finish before exiting main!

    } catch (Exception e) {
      throw new HopException("Error in event processor", e);
    }

    setOutputDone();
    return false;
  }
}
