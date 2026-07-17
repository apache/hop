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

package org.apache.hop.spark.pipeline.handler;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.logging.ILogChannel;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.JsonRowMeta;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.core.xml.XmlHandler;
import org.apache.hop.metadata.api.IHopMetadataProvider;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.ITransformIOMeta;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.apache.hop.pipeline.transform.stream.IStream;
import org.apache.hop.spark.core.HopMapPartitionsFn;
import org.apache.hop.spark.core.HopSparkRowConverter;
import org.apache.hop.spark.core.HopSparkUtil;
import org.apache.hop.spark.core.SparkExecutionDataAccumulator;
import org.apache.hop.spark.core.SparkInfoStreamSupport;
import org.apache.hop.spark.core.SparkTransformMetricsAccumulator;
import org.apache.hop.spark.core.SparkVariableValue;
import org.apache.hop.spark.engines.ISparkPipelineEngineRunConfiguration;
import org.apache.hop.spark.pipeline.ISparkPipelineTransformHandler;
import org.apache.hop.spark.util.SparkRunMode;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.StructType;

/**
 * Wraps a Hop transform in {@link HopMapPartitionsFn} so initialization happens once per Spark
 * partition (DISTRIBUTED) or once on the Spark driver (DRIVER_ONLY). Supports optional info/side
 * streams (Stream Lookup, etc.) via broadcast of collected rows.
 */
public class SparkGenericTransformHandler implements ISparkPipelineTransformHandler {

  /** Soft warning threshold when materializing input rows on the driver. */
  static final long DRIVER_ONLY_ROW_WARN_THRESHOLD = 100_000L;

  private SparkTransformMetricsAccumulator metricsAccumulator;
  private SparkExecutionDataAccumulator sampleDataAccumulator;
  private String runConfigName;
  private String parentLogChannelId;
  private String dataSamplersJson;

  public void setMetricsAccumulator(SparkTransformMetricsAccumulator metricsAccumulator) {
    this.metricsAccumulator = metricsAccumulator;
  }

  public void setSampleDataAccumulator(SparkExecutionDataAccumulator sampleDataAccumulator) {
    this.sampleDataAccumulator = sampleDataAccumulator;
  }

  public void setExecutionSamplingContext(
      String runConfigName, String parentLogChannelId, String dataSamplersJson) {
    this.runConfigName = runConfigName;
    this.parentLogChannelId = parentLogChannelId;
    this.dataSamplersJson = dataSamplersJson;
  }

  @Override
  public boolean isInput() {
    return false;
  }

  @Override
  public boolean isOutput() {
    return false;
  }

  @Override
  public void handleTransform(
      ILogChannel log,
      IVariables variables,
      String runConfigurationName,
      ISparkPipelineEngineRunConfiguration runConfiguration,
      IHopMetadataProvider metadataProvider,
      String metastoreJson,
      PipelineMeta pipelineMeta,
      TransformMeta transformMeta,
      Map<String, Dataset<Row>> transformDatasetMap,
      SparkSession spark,
      IRowMeta rowMeta,
      List<TransformMeta> previousTransforms,
      Dataset<Row> input)
      throws HopException {

    boolean inputTransform = input == null;
    // previousTransforms are main predecessors only (converter uses findPreviousTransforms(...,
    // false)). Multiple main predecessors are already unioned by HopPipelineMetaToSparkConverter
    // when row layouts match.

    // Ensure target stream subjects are bound (pipeline load usually does this already)
    transformMeta.getTransform().searchInfoAndTargetTransforms(pipelineMeta.getTransforms());
    List<String> targetNames = resolveTargetTransformNames(transformMeta);

    List<TransformMeta> nextTransforms = pipelineMeta.findNextTransforms(transformMeta);
    if (nextTransforms.size() > 1 && targetNames.isEmpty()) {
      throw new HopException(
          "Multiple outgoing hops from transform '"
              + transformMeta.getName()
              + "' are not supported on the native Spark engine unless they are declared as"
              + " target streams (e.g. Filter Rows true/false, Switch/Case). plugin id="
              + transformMeta.getTransformPluginId());
    }

    // Info/side streams: all previous including informational minus main previous
    List<TransformMeta> infoTransformMetas =
        resolveInfoTransforms(pipelineMeta, transformMeta, previousTransforms);

    List<String> infoNames = new ArrayList<>();
    List<String> infoRowMetaJsons = new ArrayList<>();
    List<Broadcast<List<Object[]>>> infoBroadcasts = new ArrayList<>();
    for (TransformMeta infoMeta : infoTransformMetas) {
      Dataset<Row> infoDs = transformDatasetMap.get(infoMeta.getName());
      if (infoDs == null) {
        throw new HopException(
            "Unable to find Dataset for transform '"
                + infoMeta.getName()
                + "' providing info for '"
                + transformMeta.getName()
                + "'");
      }
      IRowMeta infoRowMeta = pipelineMeta.getTransformFields(variables, infoMeta);
      Broadcast<List<Object[]>> broadcast =
          SparkInfoStreamSupport.broadcastInfoRows(spark, infoDs, infoRowMeta, infoMeta.getName());
      infoNames.add(infoMeta.getName());
      infoRowMetaJsons.add(JsonRowMeta.toJson(infoRowMeta));
      infoBroadcasts.add(broadcast);
      log.logBasic(
          "Broadcast info stream '"
              + infoMeta.getName()
              + "' → '"
              + transformMeta.getName()
              + "' rows="
              + broadcast.value().size());
    }

    IRowMeta outputRowMeta = pipelineMeta.getTransformFields(variables, transformMeta);
    StructType payloadSchema = HopSparkRowConverter.toStructType(outputRowMeta);
    boolean multiTarget = !targetNames.isEmpty();
    StructType mapPartitionsSchema =
        multiTarget ? HopSparkRowConverter.toTaggedStructType(outputRowMeta) : payloadSchema;

    String transformMetaXml =
        XmlHandler.openTag(TransformMeta.XML_TAG)
            + transformMeta.getTransform().getXml()
            + XmlHandler.closeTag(TransformMeta.XML_TAG);

    List<SparkVariableValue> variableValues = collectVariables(variables);
    String inputRowMetaJson =
        rowMeta != null && rowMeta.size() > 0 ? JsonRowMeta.toJson(rowMeta) : null;
    String outputRowMetaJson = JsonRowMeta.toJson(outputRowMeta);

    HopMapPartitionsFn mapFn =
        new HopMapPartitionsFn(
            variableValues,
            metastoreJson,
            transformMeta.getName(),
            transformMeta.getTransformPluginId(),
            transformMetaXml,
            inputRowMetaJson,
            outputRowMetaJson,
            inputTransform,
            targetNames,
            infoNames,
            infoRowMetaJsons,
            infoBroadcasts,
            metricsAccumulator,
            sampleDataAccumulator,
            runConfigName != null ? runConfigName : runConfigurationName,
            parentLogChannelId,
            dataSamplersJson);

    Dataset<Row> source;
    if (inputTransform) {
      // One empty row / one partition to drive source transforms (e.g. Row Generator)
      // or pure multi-info transforms with no main hop
      StructType emptySchema =
          new StructType()
              .add("_spark_hop_driver", org.apache.spark.sql.types.DataTypes.IntegerType, false);
      List<Row> driverRows = new ArrayList<>();
      driverRows.add(RowFactory.create(1));
      source = spark.createDataFrame(driverRows, emptySchema).repartition(1);
    } else {
      source = input;
    }

    SparkRunMode runMode = SparkRunMode.resolve(transformMeta, runConfiguration);
    warnIfNestedSparkExecutorDistributed(log, transformMeta, runMode);
    Dataset<Row> taggedOrPlain;
    if (runMode == SparkRunMode.DRIVER_ONLY) {
      taggedOrPlain = runOnDriver(log, transformMeta, spark, source, mapFn, mapPartitionsSchema);
    } else {
      JavaRDD<Row> outRdd =
          source
              .toJavaRDD()
              .mapPartitions(
                  iterator -> {
                    try {
                      return mapFn.call(iterator);
                    } catch (Exception e) {
                      throw new RuntimeException(e);
                    }
                  });
      taggedOrPlain = spark.createDataFrame(outRdd, mapPartitionsSchema);
    }

    if (!multiTarget) {
      transformDatasetMap.put(transformMeta.getName(), taggedOrPlain);
    } else {
      // Main key may be empty when all rows are routed to named targets (Filter/Switch)
      Dataset<Row> mainBranch =
          taggedOrPlain
              .filter(
                  taggedOrPlain
                      .col(HopSparkUtil.TARGET_TAG_COLUMN)
                      .equalTo(HopSparkUtil.MAIN_TARGET_TAG))
              .drop(HopSparkUtil.TARGET_TAG_COLUMN);
      transformDatasetMap.put(transformMeta.getName(), mainBranch);
      for (String targetName : targetNames) {
        Dataset<Row> branch =
            taggedOrPlain
                .filter(taggedOrPlain.col(HopSparkUtil.TARGET_TAG_COLUMN).equalTo(targetName))
                .drop(HopSparkUtil.TARGET_TAG_COLUMN);
        String tupleId = HopSparkUtil.createTargetTupleId(transformMeta.getName(), targetName);
        transformDatasetMap.put(tupleId, branch);
        log.logBasic(
            "Target stream '"
                + transformMeta.getName()
                + "' → '"
                + targetName
                + "' key="
                + tupleId);
      }
    }
    log.logBasic(
        "Handled transform '"
            + transformMeta.getName()
            + "' with generic Spark "
            + runMode.name()
            + " (plugin id="
            + transformMeta.getTransformPluginId()
            + (infoNames.isEmpty() ? "" : ", infoStreams=" + infoNames)
            + (targetNames.isEmpty() ? "" : ", targets=" + targetNames)
            + ")");
  }

  /**
   * Pipeline Executor that starts a nested Native Spark engine must run on the driver. Log a clear
   * warning when DISTRIBUTED would place that work on executors.
   */
  static void warnIfNestedSparkExecutorDistributed(
      ILogChannel log, TransformMeta transformMeta, SparkRunMode runMode) {
    if (runMode == SparkRunMode.DRIVER_ONLY || transformMeta == null) {
      return;
    }
    String pluginId = transformMeta.getTransformPluginId();
    if (!"PipelineExecutor".equals(pluginId)) {
      return;
    }
    log.logBasic(
        "Transform '"
            + transformMeta.getName()
            + "' is Pipeline Executor in DISTRIBUTED mode. Nested Native Spark child pipelines"
            + " must run on the driver (set Generic transform run mode DRIVER_ONLY or use"
            + " context action Spark Run Mode → Force Driver Only). DISTRIBUTED mapPartitions on"
            + " executors cannot safely own a SparkSession.");
  }

  /**
   * Run {@link HopMapPartitionsFn} once on the Spark driver so nested executions (Workflow
   * Executor, etc.) do not fan out across executor partitions.
   */
  static Dataset<Row> runOnDriver(
      ILogChannel log,
      TransformMeta transformMeta,
      SparkSession spark,
      Dataset<Row> source,
      HopMapPartitionsFn mapFn,
      StructType mapPartitionsSchema)
      throws HopException {
    try {
      List<Row> inputRows = source.collectAsList();
      if (inputRows.size() >= DRIVER_ONLY_ROW_WARN_THRESHOLD) {
        log.logBasic(
            "DRIVER_ONLY for transform '"
                + transformMeta.getName()
                + "': materializing "
                + inputRows.size()
                + " input rows on the Spark driver (threshold="
                + DRIVER_ONLY_ROW_WARN_THRESHOLD
                + "). Prefer DISTRIBUTED for high-volume data.");
      }

      Iterator<Row> outputIterator = mapFn.call(inputRows.iterator());
      List<Row> outputRows = new ArrayList<>();
      while (outputIterator.hasNext()) {
        outputRows.add(outputIterator.next());
      }
      log.logBasic(
          "DRIVER_ONLY transform '"
              + transformMeta.getName()
              + "': driver processed inputRows="
              + inputRows.size()
              + " outputRows="
              + outputRows.size());
      return spark.createDataFrame(outputRows, mapPartitionsSchema);
    } catch (HopException e) {
      throw e;
    } catch (Exception e) {
      throw new HopException(
          "Error running transform '"
              + transformMeta.getName()
              + "' in DRIVER_ONLY mode on the Spark driver",
          e);
    }
  }

  /**
   * Informational predecessors of {@code transformMeta} that are not already main previous
   * transforms (Beam: allPrevious − mainPrevious).
   */
  static List<TransformMeta> resolveInfoTransforms(
      PipelineMeta pipelineMeta,
      TransformMeta transformMeta,
      List<TransformMeta> mainPreviousTransforms) {
    List<TransformMeta> allPrevious = pipelineMeta.findPreviousTransforms(transformMeta, true);
    if (allPrevious == null || allPrevious.isEmpty()) {
      return List.of();
    }
    Set<String> mainNames = new HashSet<>();
    if (mainPreviousTransforms != null) {
      for (TransformMeta main : mainPreviousTransforms) {
        if (main != null) {
          mainNames.add(main.getName());
        }
      }
    }
    List<TransformMeta> info = new ArrayList<>();
    for (TransformMeta prev : allPrevious) {
      if (prev != null && !mainNames.contains(prev.getName())) {
        info.add(prev);
      }
    }
    return info;
  }

  /**
   * Names of transforms bound as {@link IStream.StreamType#TARGET} streams (Filter true/false,
   * Switch/Case cases, …). Empty when the transform only has a default main hop.
   */
  static List<String> resolveTargetTransformNames(TransformMeta transformMeta) {
    if (transformMeta == null || transformMeta.getTransform() == null) {
      return List.of();
    }
    ITransformIOMeta ioMeta = transformMeta.getTransform().getTransformIOMeta();
    if (ioMeta == null) {
      return List.of();
    }
    List<String> names = new ArrayList<>();
    for (IStream targetStream : ioMeta.getTargetStreams()) {
      if (targetStream != null && targetStream.getTransformMeta() != null) {
        names.add(targetStream.getTransformMeta().getName());
      }
    }
    return names;
  }

  private static List<SparkVariableValue> collectVariables(IVariables variables) {
    List<SparkVariableValue> list = new ArrayList<>();
    for (String name : variables.getVariableNames()) {
      list.add(new SparkVariableValue(name, variables.getVariable(name)));
    }
    return list;
  }
}
