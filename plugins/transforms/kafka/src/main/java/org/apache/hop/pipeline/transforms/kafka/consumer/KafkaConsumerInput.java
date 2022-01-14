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

import org.apache.commons.lang.StringUtils;
import org.apache.hop.core.Const;
import org.apache.hop.core.IRowSet;
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
import org.apache.hop.pipeline.engines.local.LocalPipelineEngine;
import org.apache.hop.pipeline.transform.*;
import org.apache.hop.pipeline.transforms.injector.InjectorMeta;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.errors.WakeupException;

import java.util.ArrayList;
import java.util.Properties;
import java.util.Set;
import java.util.stream.Collectors;

/** Consume messages from a Kafka topic */
public class KafkaConsumerInput
        extends BaseTransform<KafkaConsumerInputMeta, KafkaConsumerInputData>
        implements ITransform<KafkaConsumerInputMeta, KafkaConsumerInputData> {

  private static final Class<?> PKG = KafkaConsumerInputMeta.class; // For Translator

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
      log.logError("Error determining output row metadata", e);
    }

    data.incomingRowsBuffer= new ArrayList<>();
    data.batch = Const.toInt(resolve(meta.getBatchSize()), -1);

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
    return true;
  }

  private void initSubPipeline() throws HopException {
    try {

      String realFilename = resolve(meta.getFilename());
      PipelineMeta subTransMeta = new PipelineMeta(realFilename, metadataProvider, true, this);
      subTransMeta.setMetadataProvider(metadataProvider);
      subTransMeta.setFilename(realFilename);
      subTransMeta.setPipelineType(PipelineMeta.PipelineType.SingleThreaded);
      logDetailed("Loaded sub-pipeline '" + realFilename + "'");

      LocalPipelineEngine kafkaPipeline =
              new LocalPipelineEngine(subTransMeta, this, getPipeline());
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
      kafkaPipeline.setLogChannel(getLogChannel());
      kafkaPipeline.startThreads();

      if (errorHandlingConditionIsSatisfied()) {
        data.executor = new SingleThreadedPipelineExecutor(kafkaPipeline, true);
      } else {
        // If the conditions for error handling are not met init SingleThreadedExecutor normally
        data.executor = new SingleThreadedPipelineExecutor(kafkaPipeline);
      }

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
    if (data.consumer != null) {
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
    for (String option : meta.getConfig().keySet()) {
      String value = variables.resolve(meta.getConfig().get(option));
      if (StringUtils.isNotEmpty(value)) {
        config.put(option, variables.resolve(value));
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
    int batch = Const.toInt(variables.resolve(meta.getBatchSize()), 0);
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
    //
    try {
      ConsumerRecords<String, String> records =
              data.consumer.poll(data.batch > 0 ? data.batch : Long.MAX_VALUE);

      if (!data.isKafkaConsumerClosing) {
        if (records.isEmpty()) {
          // We can just skip this one, poll again next iteration of this method
          //
        } else {
          // Grab the records...
          //
          for (ConsumerRecord<String, String> record : records) {
            Object[] outputRow = processMessageAsRow(record);
            data.rowProducer.putRow(data.outputRowMeta, outputRow);
            if (errorHandlingConditionIsSatisfied()) {
              data.incomingRowsBuffer.add(outputRow);
            }
            incrementLinesInput();
          }
          logBasic("Number of rows read: " + data.rowProducer.getRowSet().size());
          // Pass them to the single threaded transformation and do an iteration...
          //
          data.executor.oneIteration();

          if (data.executor.isStopped() || data.executor.getErrors() > 0) {
            // An error occurred in the sub-transformation
            //
            logDebug("Executor's reported errors #: " + data.executor.getErrors());
            if (data.executor.getErrors() > 0 && errorHandlingConditionIsSatisfied()) {
              // If error handling is enabled return record that generates error in subpipeline
              // For future improvements in managing rows that generates error in sub pipeline
              // loop through the lines of the collected lines buffer even if we assume to have only one line
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

          // Confirm everything is processed. In case error handling is enabled, this is valid too because it helps in
          // "removing" failing items from the kafka queue
          //
          data.consumer.commitAsync();
          data.executor.buildExecutionSummary();
          if (errorHandlingConditionIsSatisfied()) {
            data.incomingRowsBuffer.clear();
          }
        }
      }
    } catch (WakeupException e) {
      // We're going to close kafka consumer because of pipeline has been stopped so stop executor
      // too
      data.executor.getPipeline().stopAll();
      setOutputDone();
      stopAll();
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

  private boolean errorHandlingConditionIsSatisfied () {
    // Added a check to be sure that lines collecting for error handling is limited
    // to the case of batchSize = 1.
    return getTransformMeta().isDoingErrorHandling() && data.batch == 1;
  }

  public Object[] processMessageAsRow(ConsumerRecord<String, String> record) {

    Object[] rowData = RowDataUtil.allocateRowData(data.outputRowMeta.size());

    int index = 0;
    rowData[index++] = record.key();
    rowData[index++] = record.value();
    rowData[index++] = record.topic();
    rowData[index++] = (long) record.partition();
    rowData[index++] = record.offset();
    rowData[index] = record.timestamp();

    return rowData;
  }
}
