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
import java.util.LinkedHashSet;
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
import org.apache.hop.spark.core.SparkHopPartitioning;
import org.apache.hop.spark.core.SparkInfoStreamSupport;
import org.apache.hop.spark.core.SparkInternalRows;
import org.apache.hop.spark.core.SparkTransformMetricsAccumulator;
import org.apache.hop.spark.core.SparkVariableValue;
import org.apache.hop.spark.engines.ISparkPipelineEngineRunConfiguration;
import org.apache.hop.spark.pipeline.HopPipelineMetaToSparkConverter;
import org.apache.hop.spark.pipeline.ISparkPipelineTransformHandler;
import org.apache.hop.spark.pipeline.SparkPersistedDatasets;
import org.apache.hop.spark.util.SparkRunMode;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.types.StructType;
import org.w3c.dom.Node;

/**
 * Wraps a Hop transform in {@link HopMapPartitionsFn} so initialization happens once per Spark
 * partition (DISTRIBUTED) or once on the Spark driver (DRIVER_ONLY). Supports optional info/side
 * streams (Stream Lookup, etc.) via broadcast of collected rows.
 */
public class SparkGenericTransformHandler implements ISparkPipelineTransformHandler {

  /** Soft warning threshold when materializing input rows on the driver. */
  static final long DRIVER_ONLY_ROW_WARN_THRESHOLD = 100_000L;

  /**
   * Set once Spark's internal row API turned out to be missing or different on this Spark build:
   * every later transform uses Spark's external {@code Row} without trying again.
   */
  private static volatile boolean internalRowsUnavailable;

  private SparkTransformMetricsAccumulator metricsAccumulator;
  private SparkPersistedDatasets persistedDatasets = new SparkPersistedDatasets();
  private SparkExecutionDataAccumulator sampleDataAccumulator;
  private String runConfigName;
  private String parentLogChannelId;
  private String dataSamplersJson;

  public void setMetricsAccumulator(SparkTransformMetricsAccumulator metricsAccumulator) {
    this.metricsAccumulator = metricsAccumulator;
  }

  /** The Datasets of the run, released when it ends: this handler keeps rows through it only. */
  public void setPersistedDatasets(SparkPersistedDatasets persistedDatasets) {
    this.persistedDatasets = persistedDatasets;
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
    handle(
        log,
        variables,
        runConfigurationName,
        runConfiguration,
        metastoreJson,
        pipelineMeta,
        transformMeta,
        List.of(),
        transformDatasetMap,
        spark,
        rowMeta,
        previousTransforms,
        input);
  }

  /**
   * Runs {@code head} and the transforms after it in one Hop mini-pipeline per partition, each
   * pulling the rows of the one before: the rows are converted from Spark to Hop once at the start
   * of the chain and back once at its end. Every transform of the chain must pass {@link #canFuse}.
   * The Dataset is registered under the name of the last transform.
   */
  public void handleFusedTransforms(
      ILogChannel log,
      IVariables variables,
      String runConfigurationName,
      ISparkPipelineEngineRunConfiguration runConfiguration,
      String metastoreJson,
      PipelineMeta pipelineMeta,
      TransformMeta head,
      List<TransformMeta> followers,
      Map<String, Dataset<Row>> transformDatasetMap,
      SparkSession spark,
      IRowMeta rowMeta,
      Dataset<Row> input)
      throws HopException {
    handle(
        log,
        variables,
        runConfigurationName,
        runConfiguration,
        metastoreJson,
        pipelineMeta,
        head,
        followers,
        transformDatasetMap,
        spark,
        rowMeta,
        pipelineMeta.findPreviousTransforms(head, false),
        input);
  }

  /**
   * Whether a transform can run in the same mini-pipeline as its neighbours: one main input and
   * nothing else coming in, no target streams or error hop, distributed over the partitions, and a
   * transform that the direct row path drives (no file names from a hop, no batch-driven child
   * pipeline).
   */
  public static boolean canFuse(
      PipelineMeta pipelineMeta,
      TransformMeta transformMeta,
      ISparkPipelineEngineRunConfiguration runConfiguration) {
    if (transformMeta == null || transformMeta.getTransform() == null) {
      return false;
    }
    String pluginId = transformMeta.getTransformPluginId();
    if (HopPipelineMetaToSparkConverter.HARD_BANNED_PLUGIN_IDS.containsKey(pluginId)
        || HopMapPartitionsFn.needsBatchComplete(pluginId)
        || transformMeta.isDoingErrorHandling()
        // Its rows go to their Hop partitions first
        || transformMeta.isPartitioned()) {
      return false;
    }
    try {
      if (needsOneOrderedStream(transformMeta)) {
        return false;
      }
    } catch (HopException e) {
      return false;
    }
    if (pipelineMeta.findPreviousTransforms(transformMeta, true).size() != 1
        || pipelineMeta.findPreviousTransforms(transformMeta, false).size() != 1) {
      return false;
    }
    transformMeta.getTransform().searchInfoAndTargetTransforms(pipelineMeta.getTransforms());
    if (!resolveTargetTransformNames(transformMeta).isEmpty()
        || readsInputsSeparately(transformMeta)
        || HopMapPartitionsFn.acceptsFilenamesFromHop(transformMeta.getTransform())) {
      return false;
    }
    return SparkRunMode.resolve(transformMeta, runConfiguration) == SparkRunMode.DISTRIBUTED;
  }

  /** Transforms that only give the local engine's results on one ordered stream of all rows. */
  static final Set<String> ONE_ORDERED_STREAM_PLUGIN_IDS =
      Set.of(
          "AnalyticQuery",
          "FieldsChangeSequence",
          // A value from the rows before it, groups of neighbouring rows with the same key
          "RepeatFields",
          "Denormaliser",
          "ReservoirSampling",
          "CombinationLookup",
          "DetectEmptyStream");

  /**
   * Whether the transform needs all its rows in one partition, in their order: it compares a row
   * with the rows before or after it (Analytic query, Add value fields changing sequence), samples
   * the whole stream (Reservoir sampling, Detect empty stream: an empty partition is no empty
   * stream), or adds rows to a table that every partition would look up and add at the same time
   * (Combination lookup/update, Dimension lookup/update when it updates). On several partitions
   * each one would work on its own part of the rows.
   */
  public static boolean needsOneOrderedStream(TransformMeta transformMeta) throws HopException {
    String pluginId = transformMeta.getTransformPluginId();
    if (ONE_ORDERED_STREAM_PLUGIN_IDS.contains(pluginId)) {
      return true;
    }
    if ("DimensionLookup".equals(pluginId)) {
      Node node =
          XmlHandler.getSubNode(
              XmlHandler.loadXmlString(transformMeta.getXml()), TransformMeta.XML_TAG);
      return "Y".equalsIgnoreCase(XmlHandler.getTagValue(node, "update"));
    }
    return false;
  }

  private void handle(
      ILogChannel log,
      IVariables variables,
      String runConfigurationName,
      ISparkPipelineEngineRunConfiguration runConfiguration,
      String metastoreJson,
      PipelineMeta pipelineMeta,
      TransformMeta transformMeta,
      List<TransformMeta> followers,
      Map<String, Dataset<Row>> transformDatasetMap,
      SparkSession spark,
      IRowMeta rowMeta,
      List<TransformMeta> previousTransforms,
      Dataset<Row> input)
      throws HopException {

    boolean inputTransform = input == null;
    // The transform whose output this Dataset is: the last one of a fused chain
    TransformMeta lastMeta =
        followers.isEmpty() ? transformMeta : followers.get(followers.size() - 1);
    // previousTransforms are main predecessors only (converter uses findPreviousTransforms(...,
    // false)). Multiple main predecessors are already unioned by HopPipelineMetaToSparkConverter
    // when row layouts match.

    // Ensure target stream subjects are bound (pipeline load usually does this already)
    transformMeta.getTransform().searchInfoAndTargetTransforms(pipelineMeta.getTransforms());
    List<String> targetNames = resolveTargetTransformNames(transformMeta);

    List<TransformMeta> nextTransforms = pipelineMeta.findNextTransforms(lastMeta);
    if (transformMeta.isDoingErrorHandling()) {
      throw new HopException(
          "Transform '"
              + transformMeta.getName()
              + "' sends rows with errors to an error hop, which the native Spark engine does not"
              + " support: the error rows would need a stream of their own. Validate in a"
              + " transform that writes a result field and route the rows with Filter Rows"
              + " instead. plugin id="
              + transformMeta.getTransformPluginId());
    }
    // Several outgoing hops that copy the rows: every next transform reads the same Dataset
    boolean copiesToSeveral =
        nextTransforms.size() > 1 && targetNames.isEmpty() && !lastMeta.isDistributes();
    if (nextTransforms.size() > 1 && targetNames.isEmpty() && lastMeta.isDistributes()) {
      throw new HopException(
          "Transform '"
              + lastMeta.getName()
              + "' distributes its rows over "
              + nextTransforms.size()
              + " outgoing hops (round robin), which the native Spark engine does not support."
              + " Copy the rows to every hop instead (data movement: copy), or use target streams"
              + " (Filter Rows true/false, Switch/Case). plugin id="
              + lastMeta.getTransformPluginId());
    }

    // Info/side streams: all previous including informational minus main previous
    List<TransformMeta> infoTransformMetas =
        new ArrayList<>(resolveInfoTransforms(pipelineMeta, transformMeta, previousTransforms));
    // Several main inputs the transform reads one by one (no union, see readsInputsSeparately):
    // each one is a stream of its own, like an info stream, named after its transform
    boolean separateInputs =
        inputTransform && previousTransforms != null && previousTransforms.size() > 1;
    if (separateInputs) {
      infoTransformMetas.addAll(0, previousTransforms);
    }

    List<String> infoNames = new ArrayList<>();
    List<String> infoRowMetaJsons = new ArrayList<>();
    List<Broadcast<List<Object[]>>> infoBroadcasts = new ArrayList<>();
    for (TransformMeta infoMeta : infoTransformMetas) {
      Dataset<Row> infoDs =
          separateInputs && previousTransforms.contains(infoMeta)
              ? HopPipelineMetaToSparkConverter.lookupPreviousDataset(
                  transformDatasetMap, infoMeta, transformMeta, log)
              : transformDatasetMap.get(infoMeta.getName());
      if (infoDs == null) {
        throw new HopException(
            "Unable to find Dataset for transform '"
                + infoMeta.getName()
                + "' providing info for '"
                + transformMeta.getName()
                + "'");
      }
      IRowMeta infoRowMeta =
          separateInputs && previousTransforms.contains(infoMeta)
              ? pipelineMeta.getTransformFields(variables, infoMeta, transformMeta, null)
              : pipelineMeta.getTransformFields(variables, infoMeta);
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

    IRowMeta outputRowMeta = pipelineMeta.getTransformFields(variables, lastMeta);
    // Workflow Executor / Pipeline Executor only emit on named TARGET streams.
    // getFields(next=null): WE clears the row; PE keeps the *input* layout (e.g. country_name).
    // Prefer a non-empty target-stream layout so Dummy "results" gets ExecutionTime/… and the
    // Spark Dataset schema matches hop rows written to those targets (Filter true/false share
    // the same layout as main, so this is safe for multi-output field-preserving transforms).
    if (!targetNames.isEmpty()) {
      for (String targetName : targetNames) {
        TransformMeta targetMeta = pipelineMeta.findTransform(targetName);
        if (targetMeta == null) {
          continue;
        }
        IRowMeta targetFields =
            pipelineMeta.getTransformFields(variables, transformMeta, targetMeta, null);
        if (targetFields != null && !targetFields.isEmpty()) {
          if (outputRowMeta == null
              || outputRowMeta.isEmpty()
              || !sameFieldLayout(outputRowMeta, targetFields)) {
            log.logBasic(
                "Transform '"
                    + transformMeta.getName()
                    + "' using target stream '"
                    + targetName
                    + "' layout ("
                    + targetFields.size()
                    + " fields) for Spark Dataset schema"
                    + (outputRowMeta != null && !outputRowMeta.isEmpty()
                        ? " (main reported " + outputRowMeta.size() + " field(s))"
                        : ""));
            outputRowMeta = targetFields;
          }
          break;
        }
      }
    }
    if (outputRowMeta == null) {
      outputRowMeta = new org.apache.hop.core.row.RowMeta();
    }
    StructType payloadSchema = HopSparkRowConverter.toStructType(outputRowMeta);
    boolean multiTarget = !targetNames.isEmpty();
    StructType mapPartitionsSchema =
        multiTarget ? HopSparkRowConverter.toTaggedStructType(outputRowMeta) : payloadSchema;

    String transformMetaXml =
        XmlHandler.openTag(TransformMeta.XML_TAG)
            + transformMeta.getTransform().getXml()
            + XmlHandler.closeTag(TransformMeta.XML_TAG);

    List<SparkVariableValue> variableValues = collectVariables(variables);
    if (transformMeta.isPartitioned()) {
      variableValues.add(new SparkVariableValue(SparkHopPartitioning.VAR_PARTITIONED, "Y"));
    }
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
    if (!followers.isEmpty()) {
      List<HopMapPartitionsFn.FusedStage> stages = new ArrayList<>();
      TransformMeta previous = transformMeta;
      for (TransformMeta follower : followers) {
        stages.add(
            new HopMapPartitionsFn.FusedStage(
                follower.getName(),
                follower.getTransformPluginId(),
                XmlHandler.openTag(TransformMeta.XML_TAG)
                    + follower.getTransform().getXml()
                    + XmlHandler.closeTag(TransformMeta.XML_TAG),
                JsonRowMeta.toJson(
                    pipelineMeta.getTransformFields(variables, previous, follower, null)),
                JsonRowMeta.toJson(pipelineMeta.getTransformFields(variables, follower))));
        previous = follower;
      }
      mapFn.setFusedStages(stages);
    }

    Dataset<Row> source;
    boolean oneOrderedStream = false;
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
      if (needsOneOrderedStream(transformMeta)) {
        // All rows in one partition, in their order: coalesce keeps the order of the partitions
        source = onePartition(spark, input);
        oneOrderedStream = true;
        log.logBasic(
            "Transform '"
                + transformMeta.getName()
                + "' runs on one partition with all rows in order: it works on the whole stream");
      }
    }

    SparkRunMode runMode = SparkRunMode.resolve(transformMeta, runConfiguration);
    warnIfNestedSparkExecutorDistributed(log, transformMeta, runMode);
    Dataset<Row> taggedOrPlain;
    if (runMode == SparkRunMode.DRIVER_ONLY) {
      taggedOrPlain = runOnDriver(log, transformMeta, spark, source, mapFn, mapPartitionsSchema);
    } else {
      taggedOrPlain =
          runConfiguration == null || runConfiguration.isInternalRowConversion()
              ? withInternalRows(log, spark, source, mapFn, mapPartitionsSchema)
              : null;
      if (taggedOrPlain == null) {
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
    }
    int sourcePartitions = sourcePartitions(variables, runConfiguration);
    if ((inputTransform || oneOrderedStream) && sourcePartitions > 1) {
      // A source runs once, in one partition: spread its rows so the transforms after it run in
      // parallel. In order: partition 1 holds the first rows, partition 2 the next ..., so a
      // transform that relies on the order of a file (a merge join on a sorted file) still sees
      // it when the partitions are read in turn
      taggedOrPlain = spreadInOrder(taggedOrPlain, sourcePartitions, persistedDatasets);
      log.logDetailed(
          "Spreading the rows of source '"
              + transformMeta.getName()
              + "' over "
              + sourcePartitions
              + " partitions");
    }

    if (copiesToSeveral) {
      // Read by every next transform: computed once
      taggedOrPlain = persistedDatasets.persist(taggedOrPlain);
    }
    if (!multiTarget) {
      transformDatasetMap.put(lastMeta.getName(), taggedOrPlain);
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
      transformDatasetMap.put(
          HopSparkUtil.createSharedOutputId(transformMeta.getName()), taggedOrPlain);
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
            + (followers.isEmpty()
                ? ""
                : ", fused with " + followers.stream().map(TransformMeta::getName).toList())
            + ")");
  }

  /**
   * The generic transform over Catalyst rows in and out: no conversion to Spark's external {@code
   * Row} on either side. Null when this Spark build doesn't offer that API (Spark Connect, another
   * Spark version or a vendor runtime with other internals): the caller then uses {@code Row}.
   */
  static Dataset<Row> withInternalRows(
      ILogChannel log,
      SparkSession spark,
      Dataset<Row> source,
      HopMapPartitionsFn mapFn,
      StructType schema) {
    if (internalRowsUnavailable) {
      return null;
    }
    try {
      if (!SparkInternalRows.supported(source, spark)) {
        return null;
      }
      mapFn.setInputSchemaJson(source.schema().json());
      JavaRDD<InternalRow> outRdd =
          SparkInternalRows.toRdd(source)
              .toJavaRDD()
              .mapPartitions(
                  iterator -> {
                    try {
                      return mapFn.callInternal(iterator);
                    } catch (Exception e) {
                      throw new RuntimeException(e);
                    }
                  });
      return SparkInternalRows.createDataFrame(spark, outRdd.rdd(), schema);
    } catch (Exception | LinkageError e) {
      // A missing class or method is an Error, not an Exception: catch both
      internalRowsUnavailable = true;
      if (mapFn != null) {
        mapFn.setInputSchemaJson(null);
      }
      log.logBasic(
          "Spark's internal row format is not available on this Spark build ("
              + e.getClass().getSimpleName()
              + ": "
              + e.getMessage()
              + "): generic transforms use Spark's regular rows");
      return null;
    }
  }

  /** For tests: try the internal row format again. */
  static void resetInternalRowsAvailability() {
    internalRowsUnavailable = false;
  }

  /** Column that numbers the rows of a source while they are spread over partitions. */
  static final String ROW_NR_COLUMN = "__hop_source_row_nr";

  /**
   * The rows in exactly one partition, in the order of their partitions. Also for no rows at all: a
   * shuffle of nothing can end up with no partitions (adaptive execution), and then a transform
   * that has to say something about an empty stream (Group By, Detect empty stream) never runs.
   */
  public static Dataset<Row> onePartition(SparkSession spark, Dataset<Row> rows) {
    Dataset<Row> empty =
        spark.createDataFrame(
            new JavaSparkContext(spark.sparkContext()).parallelize(List.<Row>of(), 1),
            rows.schema());
    return rows.unionByName(empty).coalesce(1);
  }

  /**
   * Spread the rows over partitions in contiguous, ordered ranges of their position: unlike {@code
   * repartition(n)}, which deals them out round robin, the order of the rows survives. The source
   * is computed once: the run keeps it in {@code persistedDatasets}.
   */
  static Dataset<Row> spreadInOrder(
      Dataset<Row> rows, int partitions, SparkPersistedDatasets persistedDatasets) {
    Column rowNr = org.apache.spark.sql.functions.col(ROW_NR_COLUMN);
    // Range partitioning samples its input in a job of its own: kept, so the source transform
    // runs once (generated values, a web service call ...) and the sample sees the same rows
    return persistedDatasets
        .persist(
            rows.withColumn(
                ROW_NR_COLUMN, org.apache.spark.sql.functions.monotonically_increasing_id()))
        .repartitionByRange(partitions, rowNr)
        .sortWithinPartitions(rowNr)
        .drop(ROW_NR_COLUMN);
  }

  /**
   * @return the source partitions of the run configuration, or 1 when it is empty or invalid
   */
  static int sourcePartitions(
      IVariables variables, ISparkPipelineEngineRunConfiguration runConfiguration) {
    if (runConfiguration == null) {
      return 1;
    }
    String value = variables.resolve(runConfiguration.getSourcePartitions());
    if (value == null || value.isBlank()) {
      return 1;
    }
    try {
      return Math.max(1, Integer.parseInt(value.trim()));
    } catch (NumberFormatException e) {
      return 1;
    }
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
  /** Transforms that read several main inputs one by one, by name or row set by row set. */
  static final Set<String> SEPARATE_INPUT_PLUGIN_IDS = Set.of("SortedMerge");

  /**
   * Whether a transform with several main inputs reads them one by one instead of as one merged
   * stream: Hop marks these by leaving their inputs out of the row layout check (Stream schema
   * merge, Multiway merge join, ...), plus Sorted merge, which merges sorted row sets.
   */
  public static boolean readsInputsSeparately(TransformMeta transformMeta) {
    return SEPARATE_INPUT_PLUGIN_IDS.contains(transformMeta.getTransformPluginId())
        || transformMeta.getTransform().excludeFromRowLayoutVerification();
  }

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
   * Switch/Case cases, Pipeline/Workflow Executor result hops, …). Empty when the transform only
   * has a default main hop.
   */
  static List<String> resolveTargetTransformNames(TransformMeta transformMeta) {
    if (transformMeta == null || transformMeta.getTransform() == null) {
      return List.of();
    }
    ITransformIOMeta ioMeta = transformMeta.getTransform().getTransformIOMeta();
    if (ioMeta == null) {
      return List.of();
    }
    // Once per target: Switch / case has a target stream per case value, and a target that gets
    // several values would be two transforms of one name in the partition's pipeline, the rows to
    // the second one lost
    Set<String> names = new LinkedHashSet<>();
    for (IStream targetStream : ioMeta.getTargetStreams()) {
      if (targetStream != null && targetStream.getTransformMeta() != null) {
        names.add(targetStream.getTransformMeta().getName());
      }
    }
    return new ArrayList<>(names);
  }

  /** Field names + types equal (order-sensitive). Used to detect PE main-vs-target layout drift. */
  static boolean sameFieldLayout(IRowMeta a, IRowMeta b) {
    if (a == b) {
      return true;
    }
    if (a == null || b == null || a.size() != b.size()) {
      return false;
    }
    for (int i = 0; i < a.size(); i++) {
      if (!a.getValueMeta(i).getName().equals(b.getValueMeta(i).getName())
          || a.getValueMeta(i).getType() != b.getValueMeta(i).getType()) {
        return false;
      }
    }
    return true;
  }

  private static List<SparkVariableValue> collectVariables(IVariables variables) {
    List<SparkVariableValue> list = new ArrayList<>();
    for (String name : variables.getVariableNames()) {
      list.add(new SparkVariableValue(name, variables.getVariable(name)));
    }
    return list;
  }
}
