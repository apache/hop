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

package org.apache.hop.spark.core;

import java.io.Serializable;
import java.net.InetAddress;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import org.apache.commons.lang3.StringUtils;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.exception.HopRuntimeException;
import org.apache.hop.core.exception.HopTransformException;
import org.apache.hop.core.logging.LogChannel;
import org.apache.hop.core.logging.LoggingObject;
import org.apache.hop.core.metadata.SerializableMetadataProvider;
import org.apache.hop.core.plugins.PluginRegistry;
import org.apache.hop.core.plugins.TransformPluginType;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.core.row.JsonRowMeta;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.core.variables.Variables;
import org.apache.hop.metadata.api.IHopMetadataProvider;
import org.apache.hop.pipeline.PipelineHopMeta;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.RowProducer;
import org.apache.hop.pipeline.SingleThreadedPipelineExecutor;
import org.apache.hop.pipeline.engines.local.LocalPipelineEngine;
import org.apache.hop.pipeline.transform.BaseTransform;
import org.apache.hop.pipeline.transform.ITransform;
import org.apache.hop.pipeline.transform.ITransformMeta;
import org.apache.hop.pipeline.transform.RowAdapter;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.apache.hop.pipeline.transform.TransformMetaDataCombi;
import org.apache.hop.pipeline.transforms.dummy.DummyMeta;
import org.apache.hop.pipeline.transforms.injector.InjectorField;
import org.apache.hop.pipeline.transforms.injector.InjectorMeta;
import org.apache.hop.spark.execution.SparkTransformExecutionSampling;
import org.apache.hop.spark.util.SparkConst;
import org.apache.spark.TaskContext;
import org.apache.spark.api.java.function.MapPartitionsFunction;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.sql.Row;

/**
 * Executes a single Hop transform once per Spark partition via {@link
 * SingleThreadedPipelineExecutor}. Metadata is passed as serializable strings (XML/JSON) so
 * executors never receive live Hop graph objects.
 */
public class HopMapPartitionsFn implements MapPartitionsFunction<Row, Row>, Serializable {
  private static final long serialVersionUID = 4L;

  /** Publish progress at least every this many input rows. */
  private static final int METRICS_ROW_INTERVAL = 1000;

  /** Publish progress at least every this many milliseconds. */
  private static final long METRICS_TIME_INTERVAL_MS = 1000L;

  private final List<SparkVariableValue> variableValues;
  private final String metastoreJson;
  private final String transformName;
  private final String transformPluginId;
  private final String transformMetaInterfaceXml;
  private final String inputRowMetaJson;
  private final String outputRowMetaJson;
  private final boolean inputTransform;
  private final List<String> targetTransforms;

  /** Names of info/side transforms (Stream Lookup "from", etc.). */
  private final List<String> infoTransforms;

  /** Parallel to {@link #infoTransforms}: row meta JSON per info stream. */
  private final List<String> infoRowMetaJsons;

  /** Parallel to {@link #infoTransforms}: broadcast Hop rows for each info stream. */
  private final List<Broadcast<List<Object[]>>> infoBroadcasts;

  private final SparkTransformMetricsAccumulator metricsAccumulator;
  private final SparkExecutionDataAccumulator sampleDataAccumulator;
  private final String runConfigName;
  private final String parentLogChannelId;
  private final String dataSamplersJson;

  public HopMapPartitionsFn(
      List<SparkVariableValue> variableValues,
      String metastoreJson,
      String transformName,
      String transformPluginId,
      String transformMetaInterfaceXml,
      String inputRowMetaJson,
      String outputRowMetaJson,
      boolean inputTransform,
      List<String> targetTransforms) {
    this(
        variableValues,
        metastoreJson,
        transformName,
        transformPluginId,
        transformMetaInterfaceXml,
        inputRowMetaJson,
        outputRowMetaJson,
        inputTransform,
        targetTransforms,
        null,
        null,
        null,
        null,
        null,
        null,
        null,
        null);
  }

  public HopMapPartitionsFn(
      List<SparkVariableValue> variableValues,
      String metastoreJson,
      String transformName,
      String transformPluginId,
      String transformMetaInterfaceXml,
      String inputRowMetaJson,
      String outputRowMetaJson,
      boolean inputTransform,
      List<String> targetTransforms,
      SparkTransformMetricsAccumulator metricsAccumulator) {
    this(
        variableValues,
        metastoreJson,
        transformName,
        transformPluginId,
        transformMetaInterfaceXml,
        inputRowMetaJson,
        outputRowMetaJson,
        inputTransform,
        targetTransforms,
        null,
        null,
        null,
        metricsAccumulator,
        null,
        null,
        null,
        null);
  }

  public HopMapPartitionsFn(
      List<SparkVariableValue> variableValues,
      String metastoreJson,
      String transformName,
      String transformPluginId,
      String transformMetaInterfaceXml,
      String inputRowMetaJson,
      String outputRowMetaJson,
      boolean inputTransform,
      List<String> targetTransforms,
      SparkTransformMetricsAccumulator metricsAccumulator,
      String runConfigName,
      String parentLogChannelId,
      String dataSamplersJson) {
    this(
        variableValues,
        metastoreJson,
        transformName,
        transformPluginId,
        transformMetaInterfaceXml,
        inputRowMetaJson,
        outputRowMetaJson,
        inputTransform,
        targetTransforms,
        null,
        null,
        null,
        metricsAccumulator,
        null,
        runConfigName,
        parentLogChannelId,
        dataSamplersJson);
  }

  public HopMapPartitionsFn(
      List<SparkVariableValue> variableValues,
      String metastoreJson,
      String transformName,
      String transformPluginId,
      String transformMetaInterfaceXml,
      String inputRowMetaJson,
      String outputRowMetaJson,
      boolean inputTransform,
      List<String> targetTransforms,
      SparkTransformMetricsAccumulator metricsAccumulator,
      SparkExecutionDataAccumulator sampleDataAccumulator,
      String runConfigName,
      String parentLogChannelId,
      String dataSamplersJson) {
    this(
        variableValues,
        metastoreJson,
        transformName,
        transformPluginId,
        transformMetaInterfaceXml,
        inputRowMetaJson,
        outputRowMetaJson,
        inputTransform,
        targetTransforms,
        null,
        null,
        null,
        metricsAccumulator,
        sampleDataAccumulator,
        runConfigName,
        parentLogChannelId,
        dataSamplersJson);
  }

  public HopMapPartitionsFn(
      List<SparkVariableValue> variableValues,
      String metastoreJson,
      String transformName,
      String transformPluginId,
      String transformMetaInterfaceXml,
      String inputRowMetaJson,
      String outputRowMetaJson,
      boolean inputTransform,
      List<String> targetTransforms,
      List<String> infoTransforms,
      List<String> infoRowMetaJsons,
      List<Broadcast<List<Object[]>>> infoBroadcasts,
      SparkTransformMetricsAccumulator metricsAccumulator,
      SparkExecutionDataAccumulator sampleDataAccumulator,
      String runConfigName,
      String parentLogChannelId,
      String dataSamplersJson) {
    this.variableValues = variableValues != null ? variableValues : List.of();
    this.metastoreJson = metastoreJson;
    this.transformName = transformName;
    this.transformPluginId = transformPluginId;
    this.transformMetaInterfaceXml = transformMetaInterfaceXml;
    this.inputRowMetaJson = inputRowMetaJson;
    this.outputRowMetaJson = outputRowMetaJson;
    this.inputTransform = inputTransform;
    this.targetTransforms = targetTransforms != null ? targetTransforms : List.of();
    this.infoTransforms = infoTransforms != null ? infoTransforms : List.of();
    this.infoRowMetaJsons = infoRowMetaJsons != null ? infoRowMetaJsons : List.of();
    this.infoBroadcasts = infoBroadcasts != null ? infoBroadcasts : List.of();
    this.metricsAccumulator = metricsAccumulator;
    this.sampleDataAccumulator = sampleDataAccumulator;
    this.runConfigName = runConfigName;
    this.parentLogChannelId = parentLogChannelId;
    this.dataSamplersJson = dataSamplersJson;
  }

  @Override
  public Iterator<Row> call(Iterator<Row> input) throws Exception {
    int copyNr = partitionId();
    String host = localHost();
    long partitionStartMs = System.currentTimeMillis();
    ITransform mainTransform = null;
    SparkTransformExecutionSampling sampling = null;
    try {
      SparkHop.init();

      IHopMetadataProvider metadataProvider = new SerializableMetadataProvider(metastoreJson);
      IVariables variables = new Variables();
      for (SparkVariableValue variableValue : variableValues) {
        if (StringUtils.isNotEmpty(variableValue.getVariable())) {
          variables.setVariable(variableValue.getVariable(), variableValue.getValue());
        }
      }

      IRowMeta inputRowMeta =
          StringUtils.isNotEmpty(inputRowMetaJson)
              ? JsonRowMeta.fromJson(inputRowMetaJson)
              : new org.apache.hop.core.row.RowMeta();
      IRowMeta outputRowMeta = JsonRowMeta.fromJson(outputRowMetaJson);

      PipelineMeta pipelineMeta = new PipelineMeta();
      pipelineMeta.setName(transformName);
      pipelineMeta.setPipelineType(PipelineMeta.PipelineType.SingleThreaded);
      pipelineMeta.setMetadataProvider(metadataProvider);

      if (infoTransforms.size() != infoRowMetaJsons.size()
          || infoTransforms.size() != infoBroadcasts.size()) {
        throw new HopException(
            "Info stream configuration mismatch for transform '"
                + transformName
                + "': names="
                + infoTransforms.size()
                + " metas="
                + infoRowMetaJsons.size()
                + " broadcasts="
                + infoBroadcasts.size());
      }

      List<IRowMeta> infoRowMetas = new ArrayList<>();
      for (String infoRowMetaJson : infoRowMetaJsons) {
        infoRowMetas.add(JsonRowMeta.fromJson(infoRowMetaJson));
      }

      TransformMeta mainInjectorTransformMeta = null;
      if (!inputTransform) {
        mainInjectorTransformMeta =
            createInjectorTransform(
                pipelineMeta, SparkConst.INJECTOR_TRANSFORM_NAME, inputRowMeta, 200, 200);
      }

      // Injectors for info/side streams (Stream Lookup, Validator, …) — Beam side-input pattern
      List<TransformMeta> infoTransformMetas = new ArrayList<>();
      for (int i = 0; i < infoTransforms.size(); i++) {
        infoTransformMetas.add(
            createInjectorTransform(
                pipelineMeta, infoTransforms.get(i), infoRowMetas.get(i), 200, 350 + 150 * i));
      }

      int targetLocationY = 200;
      List<TransformMeta> targetTransformMetas = new ArrayList<>();
      for (String targetTransform : targetTransforms) {
        DummyMeta dummyMeta = new DummyMeta();
        TransformMeta targetTransformMeta = new TransformMeta(targetTransform, dummyMeta);
        targetTransformMeta.setLocation(600, targetLocationY);
        targetLocationY += 150;
        targetTransformMetas.add(targetTransformMeta);
        pipelineMeta.addTransform(targetTransformMeta);
      }

      PluginRegistry registry = PluginRegistry.getInstance();
      ITransformMeta iTransformMeta =
          registry.loadClass(TransformPluginType.class, transformPluginId, ITransformMeta.class);
      if (iTransformMeta == null) {
        throw new HopException(
            "Unable to load transform plugin with ID "
                + transformPluginId
                + ", this plugin isn't in the plugin registry or classpath");
      }

      HopSparkUtil.loadTransformMetadataFromXml(
          transformName, iTransformMeta, transformMetaInterfaceXml, metadataProvider);

      TransformMeta transformMeta = new TransformMeta(transformName, iTransformMeta);
      transformMeta.setTransformPluginId(transformPluginId);
      transformMeta.setLocation(400, 200);
      pipelineMeta.addTransform(transformMeta);
      if (!inputTransform) {
        pipelineMeta.addPipelineHop(new PipelineHopMeta(mainInjectorTransformMeta, transformMeta));
      }
      for (TransformMeta infoTransformMeta : infoTransformMetas) {
        pipelineMeta.addPipelineHop(new PipelineHopMeta(infoTransformMeta, transformMeta));
      }
      for (TransformMeta targetTransformMeta : targetTransformMetas) {
        pipelineMeta.addPipelineHop(new PipelineHopMeta(transformMeta, targetTransformMeta));
      }

      // After injectors exist so Stream Lookup "from" can bind to the info injector by name
      iTransformMeta.searchInfoAndTargetTransforms(pipelineMeta.getTransforms());

      LocalPipelineEngine pipeline =
          new LocalPipelineEngine(
              pipelineMeta, variables, new LoggingObject("apache-spark-transform"));
      pipeline.setMetadataProvider(metadataProvider);
      pipeline
          .getPipelineRunConfiguration()
          .setName("spark-transform-local (" + transformName + ")");
      pipeline.prepareExecution();

      RowProducer rowProducer = null;
      TransformMetaDataCombi mainInjectorCombi = null;
      if (!inputTransform) {
        rowProducer = pipeline.addRowProducer(SparkConst.INJECTOR_TRANSFORM_NAME, 0);
        mainInjectorCombi = findCombi(pipeline, SparkConst.INJECTOR_TRANSFORM_NAME);
      }
      List<RowProducer> infoRowProducers = new ArrayList<>();
      for (String infoTransform : infoTransforms) {
        infoRowProducers.add(pipeline.addRowProducer(infoTransform, 0));
      }

      // Non-blocking getRow/putRow (Beam pattern). Default handler busy-waits on empty hops and
      // deadlocks Stream Lookup after readLookupValues when the main hop is empty in this thread.
      for (TransformMetaDataCombi c : pipeline.getTransforms()) {
        if (c.transform instanceof BaseTransform baseTransform) {
          baseTransform.setRowHandler(new SparkRowHandler(baseTransform));
        }
      }

      TransformMetaDataCombi transformCombi = findCombi(pipeline, transformName);
      mainTransform = transformCombi.transform;
      publishMetrics(mainTransform, copyNr, host, partitionStartMs, true, false);

      // Execution data sampling → driver accumulator + optional local registerData
      sampling =
          new SparkTransformExecutionSampling(
              transformName, parentLogChannelId, copyNr, sampleDataAccumulator);
      try {
        sampling.lookup(variables, metadataProvider, runConfigName, dataSamplersJson);
        if (sampling.isActive()) {
          sampling.registerExecutingTransform(pipeline);
          sampling.attach(variables, pipeline, mainTransform, inputRowMeta, outputRowMeta);
        }
      } catch (Throwable sampleEx) {
        // Non-fatal: pipeline must still process data. Catch Throwable so LinkageError
        // (Jackson dual classloaders on local[*]) cannot abort the Spark stage.
        LogChannel.GENERAL.logError(
            "Execution data sampling disabled for transform '"
                + transformName
                + "' on partition "
                + copyNr
                + " (non-fatal): "
                + sampleEx.getMessage(),
            sampleEx instanceof Exception ex ? ex : new Exception(sampleEx));
        sampling = null;
      }

      List<Object[]> resultRows = new ArrayList<>();
      if (targetTransforms.isEmpty()) {
        transformCombi.transform.addRowListener(
            new RowAdapter() {
              @Override
              public void rowWrittenEvent(IRowMeta rowMeta, Object[] row)
                  throws HopTransformException {
                resultRows.add(row);
              }
            });
      } else {
        for (String targetTransform : targetTransforms) {
          TransformMetaDataCombi targetCombi = findCombi(pipeline, targetTransform);
          targetCombi.transform.addRowListener(
              new RowAdapter() {
                @Override
                public void rowReadEvent(IRowMeta rowMeta, Object[] row)
                    throws HopTransformException {
                  resultRows.add(row);
                }
              });
        }
      }

      SingleThreadedPipelineExecutor executor = new SingleThreadedPipelineExecutor(pipeline);
      if (!executor.init()) {
        throw new HopException(
            "Error initializing single-threaded executor for transform '" + transformName + "'");
      }
      pipeline.startThreads();

      // Load info streams once per partition (before main rows), same pattern as Beam TransformFn:
      // put each side-input row through the info Injector via RowProducer, then processRow so the
      // hop into Stream Lookup (etc.) is filled. finished() + one more processRow flags the
      // injector done so readLookupValues / getRowFrom see isDone() and do not busy-wait.
      for (int i = 0; i < infoTransforms.size(); i++) {
        String infoName = infoTransforms.get(i);
        IRowMeta infoRowMeta = infoRowMetas.get(i);
        List<Object[]> infoData =
            infoBroadcasts.get(i) != null ? infoBroadcasts.get(i).value() : Collections.emptyList();
        RowProducer infoRowProducer = infoRowProducers.get(i);
        TransformMetaDataCombi infoCombi = findCombi(pipeline, infoName);
        for (Object[] infoRow : infoData) {
          infoRowProducer.putRow(infoRowMeta, infoRow);
          infoCombi.transform.processRow();
        }
        infoRowProducer.finished();
        infoCombi.transform.processRow();
      }

      List<Row> output = new ArrayList<>();
      MetricsThrottle throttle = new MetricsThrottle();
      final SparkTransformExecutionSampling samplingRef = sampling;

      if (inputTransform) {
        // Source transform: drive until finished with no external input
        boolean more = true;
        while (more && !pipeline.isFinished() && pipeline.getErrors() == 0) {
          resultRows.clear();
          more = executor.oneIteration();
          for (Object[] hopRow : resultRows) {
            output.add(HopSparkRowConverter.toSparkRow(outputRowMeta, hopRow));
          }
          if (throttle.shouldPublish(resultRows.size())) {
            publishMetrics(mainTransform, copyNr, host, partitionStartMs, true, false);
            flushSamplesQuietly(samplingRef, false);
          }
        }
      } else {
        long rowsSeen = 0;
        while (input.hasNext()) {
          Row sparkRow = input.next();
          Object[] hopRow = HopSparkRowConverter.toHopRow(inputRowMeta, sparkRow);
          resultRows.clear();
          rowProducer.putRow(inputRowMeta, hopRow, false);
          // Forward the main row onto the hop before Stream Lookup's info-first processRow
          // calls getRow() for the main stream (same timing Beam gets from topo order +
          // non-blocking handler).
          if (mainInjectorCombi != null) {
            mainInjectorCombi.transform.processRow();
          }
          executor.oneIteration();
          for (Object[] outHopRow : resultRows) {
            output.add(HopSparkRowConverter.toSparkRow(outputRowMeta, outHopRow));
          }
          rowsSeen++;
          if (throttle.shouldPublish(1) || rowsSeen % METRICS_ROW_INTERVAL == 0) {
            publishMetrics(mainTransform, copyNr, host, partitionStartMs, true, false);
            flushSamplesQuietly(samplingRef, false);
          }
        }
        if (rowProducer != null) {
          rowProducer.finished();
          if (mainInjectorCombi != null) {
            mainInjectorCombi.transform.processRow();
          }
          resultRows.clear();
          executor.oneIteration();
          for (Object[] outHopRow : resultRows) {
            output.add(HopSparkRowConverter.toSparkRow(outputRowMeta, outHopRow));
          }
        }
      }

      if (pipeline.getErrors() > 0) {
        publishMetrics(mainTransform, copyNr, host, partitionStartMs, false, true);
        throw new HopException(
            "Errors detected while executing transform '"
                + transformName
                + "' on a Spark partition");
      }

      executor.dispose();
      publishMetrics(mainTransform, copyNr, host, partitionStartMs, false, true);
      if (sampling != null) {
        sampling.close();
        sampling = null;
      }
      return output.iterator();
    } catch (Exception e) {
      if (mainTransform != null) {
        try {
          publishMetrics(mainTransform, copyNr, host, partitionStartMs, false, true);
        } catch (Exception ignored) {
          // best-effort metrics on failure
        }
      } else {
        publishErrorSlice(copyNr, host, partitionStartMs);
      }
      if (sampling != null) {
        try {
          sampling.close();
        } catch (Exception ignored) {
          // best-effort
        }
      }
      throw new HopRuntimeException(
          "Error executing Hop transform '" + transformName + "' in Spark mapPartitions", e);
    }
  }

  private static void flushSamplesQuietly(
      SparkTransformExecutionSampling sampling, boolean finished) {
    if (sampling == null || !sampling.isActive()) {
      return;
    }
    try {
      sampling.sendSamplesToLocation(finished);
    } catch (Exception e) {
      LogChannel.GENERAL.logError(
          "Error flushing transform samples to execution location (non-fatal)", e);
    }
  }

  private void publishMetrics(
      ITransform transform,
      int copyNr,
      String host,
      long startTimeMs,
      boolean running,
      boolean finished) {
    if (metricsAccumulator == null || transform == null) {
      return;
    }
    long endTimeMs = finished ? System.currentTimeMillis() : 0L;
    metricsAccumulator.add(
        new SparkTransformMetricSlice(
            transformName,
            copyNr,
            host,
            transform.getLinesRead(),
            transform.getLinesWritten(),
            transform.getLinesInput(),
            transform.getLinesOutput(),
            transform.getErrors(),
            running,
            finished,
            startTimeMs,
            endTimeMs));
  }

  private void publishErrorSlice(int copyNr, String host, long startTimeMs) {
    if (metricsAccumulator == null) {
      return;
    }
    long now = System.currentTimeMillis();
    long start = startTimeMs > 0 ? startTimeMs : now;
    metricsAccumulator.add(
        new SparkTransformMetricSlice(
            transformName, copyNr, host, 0, 0, 0, 0, 1, false, true, start, now));
  }

  private static int partitionId() {
    TaskContext ctx = TaskContext.get();
    return ctx != null ? ctx.partitionId() : 0;
  }

  private static String localHost() {
    try {
      return InetAddress.getLocalHost().getHostName();
    } catch (Exception e) {
      return null;
    }
  }

  private static TransformMeta createInjectorTransform(
      PipelineMeta pipelineMeta,
      String injectorTransformName,
      IRowMeta injectorRowMeta,
      int x,
      int y) {
    InjectorMeta injectorMeta = new InjectorMeta();
    for (IValueMeta valueMeta : injectorRowMeta.getValueMetaList()) {
      injectorMeta
          .getInjectorFields()
          .add(
              new InjectorField(
                  valueMeta.getName(),
                  valueMeta.getTypeDesc(),
                  Integer.toString(valueMeta.getLength()),
                  Integer.toString(valueMeta.getPrecision())));
    }
    TransformMeta injectorTransformMeta = new TransformMeta(injectorTransformName, injectorMeta);
    injectorTransformMeta.setLocation(x, y);
    pipelineMeta.addTransform(injectorTransformMeta);
    return injectorTransformMeta;
  }

  private static TransformMetaDataCombi findCombi(LocalPipelineEngine pipeline, String name) {
    for (TransformMetaDataCombi combi : pipeline.getTransforms()) {
      if (combi.transformName.equals(name)) {
        return combi;
      }
    }
    throw new HopRuntimeException(
        "Configuration error, transform '" + name + "' not found in mini-pipeline");
  }

  /** Throttles metric publishes by wall-clock time (row interval handled by caller). */
  private static final class MetricsThrottle implements Serializable {
    private static final long serialVersionUID = 1L;
    private long lastPublishMs = 0L;

    boolean shouldPublish(int rowsThisBatch) {
      long now = System.currentTimeMillis();
      if (lastPublishMs == 0L || now - lastPublishMs >= METRICS_TIME_INTERVAL_MS) {
        lastPublishMs = now;
        return true;
      }
      return false;
    }
  }
}
