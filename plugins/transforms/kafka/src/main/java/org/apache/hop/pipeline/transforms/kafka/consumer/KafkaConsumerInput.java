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

package org.apache.hop.pipeline.transforms.kafka.consumer;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Properties;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.commons.lang3.StringUtils;
import org.apache.hop.core.Const;
import org.apache.hop.core.Result;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.exception.HopTransformException;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.RowDataUtil;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.pipeline.Pipeline;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.SingleThreadedPipelineExecutor;
import org.apache.hop.pipeline.TransformWithMappingMeta;
import org.apache.hop.pipeline.config.PipelineRunConfiguration;
import org.apache.hop.pipeline.engines.local.LocalPipelineEngine;
import org.apache.hop.pipeline.engines.local.LocalPipelineRunConfiguration;
import org.apache.hop.pipeline.transform.BaseTransform;
import org.apache.hop.pipeline.transform.ITransform;
import org.apache.hop.pipeline.transform.ITransformMeta;
import org.apache.hop.pipeline.transform.RowAdapter;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.apache.hop.pipeline.transforms.injector.InjectorMeta;
import org.apache.hop.pipeline.transforms.kafka.shared.KafkaHeaders;
import org.apache.hop.pipeline.transforms.kafka.shared.KafkaOption;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;

/** Consume messages from a Kafka topic */
public class KafkaConsumerInput
    extends BaseTransform<KafkaConsumerInputMeta, KafkaConsumerInputData> {

  private static final Class<?> PKG = KafkaConsumerInputMeta.class;

  public KafkaConsumerInput(
      TransformMeta transformMeta,
      KafkaConsumerInputMeta meta,
      KafkaConsumerInputData data,
      int copyNr,
      PipelineMeta pipelineMeta,
      Pipeline pipeline) {
    super(transformMeta, meta, data, copyNr, pipelineMeta, pipeline);
  }

  /** Initialize and do work where other transforms need to wait for... */
  @Override
  public boolean init() {

    boolean superInit = super.init();
    if (!superInit) {
      return false;
    }

    try {
      data.outputRowMeta = meta.getRowMeta(getTransformName(), this);
    } catch (HopTransformException e) {
      logError("Error determining output row metadata", e);
    }

    data.incomingRowsBuffer = new ArrayList<>();
    data.batchDuration = Const.toInt(resolve(meta.getBatchDuration()), 0);
    data.batchSize = Const.toIntExpanded(resolve(meta.getBatchSize()), 0);
    data.stopWhenIdle = meta.isStopWhenIdle();
    data.maxIdleTimeMs = Const.toLong(resolve(meta.getMaxIdleTimeMs()), 500L);
    long maxConsume = Const.toLong(resolve(meta.getMaxConsumeDurationMs()), 0L);
    data.maxConsumeDurationMs = maxConsume > 0 ? maxConsume : 0L;
    data.startTime = System.currentTimeMillis();
    data.lastRecordTime = data.startTime;
    logBasic(
        "Kafka consumer batchDuration="
            + data.batchDuration
            + "ms, stopWhenIdle="
            + data.stopWhenIdle
            + ", maxIdleTimeMs="
            + data.maxIdleTimeMs
            + ", maxConsumeDurationMs="
            + data.maxConsumeDurationMs);

    data.consumer = buildKafkaConsumer(this, meta);

    // Subscribe to the topics...
    //
    Set<String> topics = meta.getTopics().stream().map(this::resolve).collect(Collectors.toSet());
    data.consumer.subscribe(topics);

    // Load and start the single threader transformation
    //
    try {
      initSubPipeline();
    } catch (Exception e) {
      logError("Error initializing sub-transformation", e);
      return false;
    }

    // Set Kafka consumer is closing flag to false
    data.isKafkaConsumerClosing = false;
    startMaxConsumeDeadlineWakeup();
    return true;
  }

  private void initSubPipeline() throws HopException {
    try {

      String realFilename = resolve(meta.getFilename());
      PipelineMeta subTransMeta = new PipelineMeta(realFilename, metadataProvider, this);
      subTransMeta.setMetadataProvider(metadataProvider);
      subTransMeta.setFilename(realFilename);
      logDetailed("Loaded sub-pipeline '" + realFilename + "'");

      PipelineRunConfiguration runConfiguration =
          new PipelineRunConfiguration(
              "Kafka",
              "",
              meta.getExecutionInformationLocation(),
              new ArrayList<>(),
              new LocalPipelineRunConfiguration(),
              meta.getExecutionDataProfile(),
              false);

      LocalPipelineEngine kafkaPipeline = new LocalPipelineEngine(subTransMeta, this, this);
      kafkaPipeline.setPipelineType(PipelineMeta.PipelineType.SingleThreaded);
      kafkaPipeline.setParentPipeline(getPipeline());
      kafkaPipeline.setPipelineRunConfiguration(runConfiguration);
      // Register under the consumer log channel. prepareExecution() captures the id, and the
      // execution-info timer later reads it again. Swapping the channel afterwards made every tick
      // miss the entry and keep it warm through a parent-id fallback.
      kafkaPipeline.setLogChannel(getLogChannel());
      kafkaPipeline.prepareExecution();
      kafkaPipeline.setLogLevel(getPipeline().getLogLevel());
      kafkaPipeline.setPreviousResult(new Result());
      TransformWithMappingMeta.replaceVariableValues(kafkaPipeline, this);
      TransformWithMappingMeta.addMissingVariables(kafkaPipeline, this);
      kafkaPipeline.activateParameters(kafkaPipeline);

      logDetailed("Initialized sub-pipeline '" + realFilename + "'");

      // Find the (first copy of the) "Get Record from Stream" transform
      //
      for (TransformMeta transformMeta : subTransMeta.getTransforms()) {
        ITransformMeta iTransform = transformMeta.getTransform();
        if (iTransform instanceof InjectorMeta) {
          if (data.rowProducer != null) {
            throw new HopException(
                "You can only have one copy of the injector transform '"
                    + transformMeta.getName()
                    + "' to accept the Kafka messages");
          }
          // Attach an injector to this transform
          //
          data.rowProducer = kafkaPipeline.addRowProducer(transformMeta.getName(), 0);
        }
      }

      if (data.rowProducer == null) {
        throw new HopException(
            "Unable to find an Injector transform in the Kafka pipeline. Such a transform is needed to accept data from this Kafka Consumer transform.");
      }

      // See if we need to grab result records from the sub-pipeline...
      //
      if (StringUtils.isNotEmpty(meta.getSubTransform())) {
        ITransform transform = kafkaPipeline.findRunThread(meta.getSubTransform());
        if (transform == null) {
          throw new HopException(
              "Unable to find transform '" + meta.getSubTransform() + "' to retrieve rows from");
        }
        transform.addRowListener(
            new RowAdapter() {

              @Override
              public void rowWrittenEvent(IRowMeta rowMeta, Object[] row)
                  throws HopTransformException {
                // Write this row to the next transform(s)
                //
                KafkaConsumerInput.this.putRow(rowMeta, row);
              }
            });
      }
      kafkaPipeline.startThreads();

      if (errorHandlingConditionIsSatisfied()) {
        data.executor = new SingleThreadedPipelineExecutor(kafkaPipeline, true);
      } else {
        // If the conditions for error handling are not met init SingleThreadedExecutor normally
        data.executor = new SingleThreadedPipelineExecutor(kafkaPipeline);
      }
      data.executor.setClearingMetricsPerIteration(
          StringUtils.isEmpty(meta.getExecutionInformationLocation()));

      // Initialize the sub-pipeline
      //
      boolean ok = data.executor.init();
      if (!ok) {
        throw new HopException("Initialization of sub-pipeline failed");
      }

      getPipeline().addActiveSubPipeline(getTransformName(), kafkaPipeline);
    } catch (Exception e) {
      throw new HopException("Unable to load and initialize sub pipeline", e);
    }
  }

  @Override
  public void dispose() {
    interruptMaxConsumeDeadlineWakeup();
    if (data.consumer != null) {
      data.consumer.wakeup();
      data.consumer.unsubscribe();
      data.consumer.close();
    }
    super.dispose();
  }

  public static Consumer buildKafkaConsumer(IVariables variables, KafkaConsumerInputMeta meta) {

    Thread.currentThread().setContextClassLoader(meta.getClass().getClassLoader());

    Properties config = new Properties();

    // Set all the configuration options...
    //
    for (KafkaOption option : meta.getOptions()) {
      String value = variables.resolve(option.getValue());
      if (StringUtils.isNotEmpty(value)) {
        config.put(option.getProperty(), variables.resolve(value));
      }
    }

    // The basics
    //
    config.put(
        ConsumerConfig.GROUP_ID_CONFIG,
        variables.resolve(Const.NVL(meta.getConsumerGroup(), "Apache Hop")));
    config.put(
        ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,
        variables.resolve(meta.getDirectBootstrapServers()));
    config.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, meta.isAutoCommit());

    // Timeout : max batch wait
    //
    int timeout = Const.toInt(variables.resolve(meta.getBatchDuration()), 0);
    if (timeout > 0) {
      config.put(ConsumerConfig.FETCH_MAX_WAIT_MS_CONFIG, timeout);
    }

    // The batch size : max poll size
    //
    int batch = Const.toIntExpanded(variables.resolve(meta.getBatchSize()), 0);
    if (batch > 0) {
      config.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, batch);
    }

    // Serializers...
    //
    String keySerializerClass = meta.getKeyField().getOutputType().getKafkaDeserializerClass();
    config.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, keySerializerClass);
    String valueSerializerClass =
        meta.getMessageField().getOutputType().getKafkaDeserializerClass();
    config.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, valueSerializerClass);

    // Other options?

    return new KafkaConsumer(config);
  }

  @Override
  public void stopRunning() throws HopException {
    data.isKafkaConsumerClosing = true;
    data.consumer.wakeup();
    super.stopRunning();
  }

  @Override
  public boolean processRow() throws HopException {

    // Poll records...
    // If we get any, process them...
    // When stop-when-idle is enabled, use a short poll timeout so idle time can be measured.
    // When a max consume duration is set, cap the poll to the remaining time so a long batch
    // duration cannot overshoot the deadline.
    //
    try {
      long now = System.currentTimeMillis();
      if (maxConsumeDurationReached(now, data.maxConsumeDurationMs, data.startTime)) {
        return stopGracefully(
            "Kafka consumer max consume duration of "
                + data.maxConsumeDurationMs
                + "ms reached, stopping gracefully");
      }
      long pollMs =
          pollTimeoutMs(
              data.stopWhenIdle,
              data.batchDuration,
              data.maxConsumeDurationMs,
              data.startTime,
              now);
      Duration duration = Duration.ofMillis(pollMs);
      ConsumerRecords<Object, Object> records = data.consumer.poll(duration);

      if (!data.isKafkaConsumerClosing) {
        if (records.isEmpty()) {
          // No records: still honor max consume duration. The deadline is wall-clock since
          // start, not "time since last message", so an idle topic must stop here.
          if (maxConsumeDurationReached(
              System.currentTimeMillis(), data.maxConsumeDurationMs, data.startTime)) {
            return stopGracefully(
                "Kafka consumer max consume duration of "
                    + data.maxConsumeDurationMs
                    + "ms reached, stopping gracefully");
          }
          // Optionally stop after max idle time.
          // Do not count idle until partitions are assigned — group join / rebalance can take
          // longer than maxIdleTimeMs and would otherwise stop before any poll can succeed.
          //
          if (data.stopWhenIdle) {
            if (data.consumer.assignment() == null || data.consumer.assignment().isEmpty()) {
              data.lastRecordTime = System.currentTimeMillis();
            } else if ((System.currentTimeMillis() - data.lastRecordTime) >= data.maxIdleTimeMs) {
              return stopGracefully(
                  "Kafka consumer idle timeout of "
                      + data.maxIdleTimeMs
                      + "ms exceeded, stopping gracefully");
            }
          }
        } else {
          // Grab the records...
          //
          for (ConsumerRecord<Object, Object> record : records) {
            Object[] outputRow = processMessageAsRow(record);
            data.rowProducer.putRow(data.outputRowMeta, outputRow);
            if (errorHandlingConditionIsSatisfied()) {
              data.incomingRowsBuffer.add(outputRow);
            }
            incrementLinesInput();
          }
          data.lastRecordTime = System.currentTimeMillis();
          if (isBasic()) {
            logBasic("Number of rows read: " + data.rowProducer.getRowSet().size());
          }
          // Pass them to the single threaded transformation and do an iteration...
          //
          data.executor.oneIteration();

          if (data.executor.isStopped() || data.executor.getErrors() > 0) {
            // An error occurred in the sub-transformation
            //
            if (isDebug()) {
              logDebug("Executor's reported errors #: " + data.executor.getErrors());
            }
            if (data.executor.getErrors() > 0 && errorHandlingConditionIsSatisfied()) {
              // If error handling is enabled return record that generates error in subpipeline
              // For future improvements in managing rows that generates error in sub pipeline
              // loop through the lines of the collected lines buffer even if we assume to have only
              // one line
              // in the buffer
              for (int i = 0; i < data.incomingRowsBuffer.size(); i++) {
                putError(
                    data.outputRowMeta,
                    data.incomingRowsBuffer.get(i),
                    1L,
                    "An error occurred while processing the subpipeline",
                    null,
                    "KAFKA001");
              }
            } else {
              // Otherwise proceed normally
              data.executor.getPipeline().stopAll();
              setOutputDone();
              stopAll();
              return false;
            }
          }

          // Confirm everything is processed. In case error handling is enabled, this is valid too
          // because it helps in
          // "removing" failing items from the kafka queue
          //
          data.consumer.commitAsync();
          data.executor.buildExecutionSummary();
          if (errorHandlingConditionIsSatisfied()) {
            data.incomingRowsBuffer.clear();
          }
        }

        if (maxConsumeDurationReached(
            System.currentTimeMillis(), data.maxConsumeDurationMs, data.startTime)) {
          return stopGracefully(
              "Kafka consumer max consume duration of "
                  + data.maxConsumeDurationMs
                  + "ms reached, stopping gracefully");
        }
      }
    } catch (WakeupException e) {
      // Deadline wakeup (no new messages, poll was still blocked) or the pipeline was stopped.
      if (data.maxConsumeDeadlineWakeup
          || maxConsumeDurationReached(
              System.currentTimeMillis(), data.maxConsumeDurationMs, data.startTime)) {
        return stopGracefully(
            "Kafka consumer max consume duration of "
                + data.maxConsumeDurationMs
                + "ms reached, stopping gracefully");
      }
      if (data.executor != null) {
        data.executor.getPipeline().stopAll();
      }
      setOutputDone();
      stopAll();
      return false;
    }

    if (data.executor.getErrors() > 0 && errorHandlingConditionIsSatisfied()) {
      // Once we got an error in the called sub-pipeline, to be really safe we re-initialize it
      // to be safe and having everything working properly once again
      // Load and start the single threader transformation
      //
      try {
        data.executor.getPipeline().stopAll();
        data.executor.dispose();

        data.rowProducer = null;

        initSubPipeline();
      } catch (Exception e) {
        logError("Error initializing sub-transformation", e);
        return false;
      }
    }
    return true;
  }

  /**
   * True when a max consume duration is configured and the wall clock since transform start has
   * reached it. {@code maxConsumeDurationMs <= 0} means no limit.
   */
  static boolean maxConsumeDurationReached(long now, long maxConsumeDurationMs, long startTime) {
    return maxConsumeDurationMs > 0 && (now - startTime) >= maxConsumeDurationMs;
  }

  /**
   * Poll timeout in milliseconds. Stop-when-idle and max-consume-duration use a short poll so the
   * deadline can be re-checked when no records arrive. A long or infinite poll would otherwise
   * never return on an idle topic, and the duration check after poll() would never run. A
   * configured max consume duration also caps the timeout to the remaining window.
   */
  static long pollTimeoutMs(
      boolean stopWhenIdle,
      long batchDuration,
      long maxConsumeDurationMs,
      long startTime,
      long now) {
    boolean shortPoll = stopWhenIdle || maxConsumeDurationMs > 0;
    long pollMs = shortPoll ? 100L : (batchDuration > 0 ? batchDuration : Long.MAX_VALUE);
    if (maxConsumeDurationMs > 0) {
      long remaining = maxConsumeDurationMs - (now - startTime);
      if (remaining <= 0) {
        return 0L;
      }
      pollMs = Math.min(pollMs, remaining);
    }
    return pollMs;
  }

  /**
   * {@code consumer.poll()} can block beyond the requested timeout (coordinator lookup, metadata, a
   * stuck fetch). Interrupt that wait when the max consume deadline is reached so an idle topic
   * still finishes.
   */
  private void startMaxConsumeDeadlineWakeup() {
    if (data.maxConsumeDurationMs <= 0 || data.consumer == null) {
      return;
    }
    final long deadline = data.startTime + data.maxConsumeDurationMs;
    data.maxConsumeDeadlineThread =
        new Thread(
            () -> {
              try {
                long sleepMs = deadline - System.currentTimeMillis();
                if (sleepMs > 0) {
                  Thread.sleep(sleepMs);
                }
                if (!data.isKafkaConsumerClosing && data.consumer != null) {
                  data.maxConsumeDeadlineWakeup = true;
                  data.consumer.wakeup();
                }
              } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
              }
            },
            "KafkaConsumer-maxConsumeDeadline");
    data.maxConsumeDeadlineThread.setDaemon(true);
    data.maxConsumeDeadlineThread.start();
  }

  private void interruptMaxConsumeDeadlineWakeup() {
    if (data.maxConsumeDeadlineThread != null) {
      data.maxConsumeDeadlineThread.interrupt();
      data.maxConsumeDeadlineThread = null;
    }
  }

  private boolean stopGracefully(String reason) {
    logBasic(reason);
    data.isKafkaConsumerClosing = true;
    interruptMaxConsumeDeadlineWakeup();
    if (data.consumer != null) {
      data.consumer.wakeup();
    }
    if (data.executor != null) {
      data.executor.getPipeline().stopAll();
    }
    setOutputDone();
    return false;
  }

  private boolean errorHandlingConditionIsSatisfied() {
    // Added a check to be sure that lines collecting for error handling is limited
    // to the case of batchSize = 1.
    return getTransformMeta().isDoingErrorHandling() && data.batchSize == 1;
  }

  public Object[] processMessageAsRow(ConsumerRecord<Object, Object> record) {

    Object[] rowData = RowDataUtil.allocateRowData(data.outputRowMeta.size());

    // Only fields carrying an output name are on the row, in the order KafkaConsumerInputMeta
    // adds them, so each value is placed conditionally rather than at a fixed index.
    int index = 0;
    index = putIfNamed(rowData, index, meta.getKeyField(), record.key());
    index = putIfNamed(rowData, index, meta.getMessageField(), record.value());
    index = putIfNamed(rowData, index, meta.getTopicField(), record.topic());
    index = putIfNamed(rowData, index, meta.getPartitionField(), (long) record.partition());
    index = putIfNamed(rowData, index, meta.getOffsetField(), record.offset());
    index = putIfNamed(rowData, index, meta.getTimestampField(), record.timestamp());
    putIfNamed(rowData, index, meta.getHeadersField(), KafkaHeaders.toJson(record.headers()));

    return rowData;
  }

  /**
   * Writes a value at the given index only when the field contributes a column, and reports the
   * next free index. A field with an empty output name is skipped by {@code
   * KafkaConsumerInputMeta.getRowMeta}, so writing it here would shift every later column.
   */
  private int putIfNamed(Object[] rowData, int index, KafkaConsumerField field, Object value) {
    if (field == null || StringUtils.isEmpty(field.getOutputName()) || index >= rowData.length) {
      return index;
    }
    rowData[index] = value;
    return index + 1;
  }
}
