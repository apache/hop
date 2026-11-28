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

package org.apache.hop.spark.pipeline;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Supplier;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.exception.HopRowException;
import org.apache.hop.core.logging.ILogChannel;
import org.apache.hop.core.logging.LogChannel;
import org.apache.hop.core.metadata.SerializableMetadataProvider;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.metadata.api.IHopMetadataProvider;
import org.apache.hop.pipeline.PipelineHopMeta;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.config.PipelineRunConfiguration;
import org.apache.hop.pipeline.transform.BaseTransform;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.apache.hop.pipeline.transforms.tableinput.TableInputMeta;
import org.apache.hop.spark.core.HopSparkUtil;
import org.apache.hop.spark.core.SparkExecutionDataAccumulator;
import org.apache.hop.spark.core.SparkHopPartitioning;
import org.apache.hop.spark.core.SparkNativeMetricsListener;
import org.apache.hop.spark.core.SparkTransformMetricsAccumulator;
import org.apache.hop.spark.engines.ISparkPipelineEngineRunConfiguration;
import org.apache.hop.spark.pipeline.handler.SparkAddSequenceHandler;
import org.apache.hop.spark.pipeline.handler.SparkBaseTransformHandler;
import org.apache.hop.spark.pipeline.handler.SparkDetectLastRowHandler;
import org.apache.hop.spark.pipeline.handler.SparkDummyHandler;
import org.apache.hop.spark.pipeline.handler.SparkFileInputHandler;
import org.apache.hop.spark.pipeline.handler.SparkFileOutputHandler;
import org.apache.hop.spark.pipeline.handler.SparkGenericTransformHandler;
import org.apache.hop.spark.pipeline.handler.SparkGroupByHandler;
import org.apache.hop.spark.pipeline.handler.SparkJdbcInputHandler;
import org.apache.hop.spark.pipeline.handler.SparkLakeTableInputHandler;
import org.apache.hop.spark.pipeline.handler.SparkLakeTableMaintenanceHandler;
import org.apache.hop.spark.pipeline.handler.SparkLakeTableMergeHandler;
import org.apache.hop.spark.pipeline.handler.SparkLakeTableOutputHandler;
import org.apache.hop.spark.pipeline.handler.SparkMemoryGroupByHandler;
import org.apache.hop.spark.pipeline.handler.SparkMergeJoinHandler;
import org.apache.hop.spark.pipeline.handler.SparkSampleRowsHandler;
import org.apache.hop.spark.pipeline.handler.SparkSortRowsHandler;
import org.apache.hop.spark.pipeline.handler.SparkSqlHandler;
import org.apache.hop.spark.pipeline.handler.SparkUniqueRowsByHashSetHandler;
import org.apache.hop.spark.pipeline.handler.SparkUniqueRowsHandler;
import org.apache.hop.spark.util.SparkConst;
import org.apache.hop.spark.util.SparkJobNames;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

/**
 * Converts a Hop {@link PipelineMeta} into a Spark {@link Dataset} lineage. Stateless transforms
 * use mapPartitions; shuffle-aware transforms use native Dataset handlers.
 */
public class HopPipelineMetaToSparkConverter {

  /**
   * Plugin ids with dedicated native handlers. Keep in lockstep with {@link
   * #addDefaultTransformHandlers()} and {@link
   * org.apache.hop.spark.engines.SparkPipelineEngine#supports}.
   */
  public static final Set<String> EXPLICIT_HANDLER_PLUGIN_IDS =
      Set.of(
          SparkConst.MEMORY_GROUP_BY_PLUGIN_ID,
          SparkConst.MERGE_JOIN_PLUGIN_ID,
          SparkConst.UNIQUE_ROWS_PLUGIN_ID,
          SparkConst.SORT_ROWS_PLUGIN_ID,
          SparkConst.SPARK_FILE_INPUT_PLUGIN_ID,
          SparkConst.SPARK_FILE_OUTPUT_PLUGIN_ID,
          SparkConst.SPARK_LAKE_TABLE_INPUT_PLUGIN_ID,
          SparkConst.SPARK_LAKE_TABLE_OUTPUT_PLUGIN_ID,
          SparkConst.SPARK_LAKE_TABLE_MERGE_PLUGIN_ID,
          SparkConst.SPARK_LAKE_TABLE_MAINTENANCE_PLUGIN_ID,
          SparkConst.SPARK_SQL_PLUGIN_ID,
          SparkConst.TABLE_INPUT_PLUGIN_ID,
          SparkConst.DUMMY_PLUGIN_ID,
          SparkConst.GROUP_BY_PLUGIN_ID,
          SparkConst.UNIQUE_ROWS_BY_HASH_SET_PLUGIN_ID,
          SparkConst.ADD_SEQUENCE_PLUGIN_ID,
          SparkConst.SAMPLE_ROWS_PLUGIN_ID,
          SparkConst.DETECT_LAST_ROW_PLUGIN_ID);

  /**
   * Native handlers that combine several input hops into one stream, like a generic transform,
   * instead of resolving named inputs themselves.
   */
  static final Set<String> UNION_INPUT_NATIVE_PLUGIN_IDS =
      Set.of(
          SparkConst.DUMMY_PLUGIN_ID,
          SparkConst.GROUP_BY_PLUGIN_ID,
          SparkConst.UNIQUE_ROWS_BY_HASH_SET_PLUGIN_ID);

  /**
   * Plugin ids that must not run as partition-local Hop mini-pipelines, with the reason shown to
   * the user: they give wrong rows or none on the native Spark engine. Keep in lockstep with {@link
   * org.apache.hop.spark.engines.SparkPipelineEngine#supports} and the "Native Spark=no" mark on
   * the transform's documentation page.
   */
  public static final Map<String, String> HARD_BANNED_PLUGIN_IDS =
      Map.ofEntries(
          Map.entry(
              SparkConst.JOIN_ROWS_PLUGIN_ID,
              "Join Rows is not supported on the native Spark engine. A cartesian product needs every row of every input in one place, but every partition would only combine the rows it happens to hold, so combinations would go missing. Add the same constant field to both inputs and use Merge Join on that field instead."),
          Map.entry(
              SparkConst.MASK_FIELDS_PLUGIN_ID,
              "Mask fields is not supported on the native Spark engine. Every partition keeps its own mapping and sequence, so the same source value would not stay the same token."),
          Map.entry(
              "BlockUntilTransformsFinish",
              "Block until transforms finish is not supported on the native Spark engine. It waits for other transforms of the same Hop pipeline, but a Spark partition only runs the transforms of its own stage, so the transforms it waits for are never found."),
          Map.entry(
              "ClosureGenerator",
              "Closure generator is not supported on the native Spark engine. It needs every parent-child row in one place, and on Spark every partition only sees its own rows."),
          Map.entry(
              "FilesFromResult",
              "Get files from result is not supported on the native Spark engine. The files of the previous result are not passed to the Spark partitions, so it reads no files."),
          Map.entry(
              "FilesToResult",
              "Set files in result is not supported on the native Spark engine. The result of a Spark run does not carry files, so the workflow or pipeline executor would receive none."),
          Map.entry(
              "MetaInject",
              "Metadata injection is not supported on the native Spark engine. It reads the rows to inject from other transforms of the same Hop pipeline, which a Spark partition doesn't have."),
          Map.entry(
              "MultiMapping",
              "Mapping (multiple inputs/outputs) is not supported on the native Spark engine. Its inputs and outputs exchange rows with other transforms of the Hop pipeline, and on Spark the pipeline hangs."),
          Map.entry(
              "PipelineDataProbe",
              "Pipeline data probe is not supported on the native Spark engine. Probes attach to transforms of Hop's own pipeline engine, so a Spark run sends no rows to the probe."),
          Map.entry(
              "RecordsFromStream",
              "Get records from stream is not supported on the native Spark engine. The rows a pipeline executor passes are not handed to the Spark partitions, so it reads no rows."),
          Map.entry(
              "RowsFromResult",
              "Get rows from result is not supported on the native Spark engine. The rows of the previous result are not passed to the Spark partitions, so it reads no rows."),
          Map.entry(
              "RowsToResult",
              "Copy rows to result is not supported on the native Spark engine. The result of a Spark run does not carry rows, so the workflow or pipeline executor would receive none."),
          Map.entry(
              "RuleAccumulator",
              "Rules accumulator is not supported on the native Spark engine. It evaluates the rules over all the rows it collects, and on Spark every partition evaluates only its own rows."),
          Map.entry(
              "SetVariable",
              "Set variables is not supported on the native Spark engine. It would set the variables in a Spark partition, and the parent workflow or pipeline never sees them."),
          Map.entry(
              "SortedMerge",
              "Sorted merge is not supported on the native Spark engine. It merges sorted input streams in one place, which Spark partitions don't give it. Use Sort rows (native Spark sort) on the combined rows instead."),
          Map.entry(
              "SortedSchemaMerge",
              "Sorted schema merge is not supported on the native Spark engine. It merges sorted input streams in one place, which Spark partitions don't give it. Use Sort rows (native Spark sort) on the combined rows instead."),
          Map.entry(
              "XMLJoin",
              "XML join is not supported on the native Spark engine. It reads the target XML from another transform's stream, which a Spark partition doesn't have."),
          Map.entry(
              "Append",
              "Append streams is not supported on the native Spark engine. It outputs every row of the head stream before those of the tail stream, an order Spark partitions don't keep."),
          Map.entry(
              "BlockingTransform",
              "Blocking transform is not supported on the native Spark engine. It waits for every input row, or passes only the last one, and every Spark partition only has its own rows."),
          Map.entry(
              "Injector",
              "Injector is not supported on the native Spark engine. It receives the rows an application hands to Hop's own pipeline engine."),
          Map.entry(
              "KafkaConsumer",
              "Kafka consumer is not supported on the native Spark engine, which has no support for unbounded streams. Use the Beam Kafka transforms on a Beam engine instead."),
          Map.entry(
              "MergeRows",
              "Merge rows (diff) is not supported on the native Spark engine. It compares two sorted streams row by row in one place, which Spark partitions don't give it."),
          Map.entry(
              "MultiwayMergeJoin",
              "Multiway merge join is not supported on the native Spark engine. It joins sorted streams row by row in one place, which Spark partitions don't give it. Use Merge join (native Spark join) instead."),
          Map.entry(
              "StreamSchema",
              "Stream schema merge is not supported on the native Spark engine. When Spark combines inputs with different layouts, rows go missing."));

  private final IVariables variables;
  private final PipelineMeta pipelineMeta;
  private final SerializableMetadataProvider metadataProvider;
  private final String metaStoreJson;
  private final String runConfigName;
  private final ISparkPipelineEngineRunConfiguration sparkRunConfiguration;
  private final Map<String, ISparkPipelineTransformHandler> transformHandlers;
  private final SparkGenericTransformHandler genericTransformHandler;
  private SparkTransformMetricsAccumulator metricsAccumulator;
  private SparkExecutionDataAccumulator sampleDataAccumulator;
  private String parentLogChannelId;
  private String dataSamplersJson;

  /** Datasets of the transforms without successors, in topological order (see createDataset). */
  private final Map<String, Dataset<Row>> leafDatasets = new LinkedHashMap<>();

  /** The Datasets this run keeps, by the converter and the handlers; released by unpersistAll. */
  private final SparkPersistedDatasets persistedDatasets = new SparkPersistedDatasets();

  public HopPipelineMetaToSparkConverter(
      IVariables variables,
      PipelineMeta pipelineMeta,
      IHopMetadataProvider metadataProvider,
      String runConfigName)
      throws HopException {
    this.variables = variables;
    this.pipelineMeta = pipelineMeta;
    this.metadataProvider = new SerializableMetadataProvider(metadataProvider);
    this.metaStoreJson = this.metadataProvider.toJson();
    this.runConfigName = runConfigName;

    PipelineRunConfiguration runConfiguration =
        metadataProvider.getSerializer(PipelineRunConfiguration.class).load(runConfigName);
    if (runConfiguration == null) {
      throw new HopException("Unable to load pipeline run configuration '" + runConfigName + "'");
    }
    if (!(runConfiguration.getEngineRunConfiguration()
        instanceof ISparkPipelineEngineRunConfiguration sparkConfig)) {
      throw new HopException(
          "Run configuration '"
              + runConfigName
              + "' is not a native Spark pipeline engine configuration");
    }
    this.sparkRunConfiguration = sparkConfig;
    this.transformHandlers = new HashMap<>();
    this.genericTransformHandler = new SparkGenericTransformHandler();
    this.genericTransformHandler.setPersistedDatasets(persistedDatasets);
    addDefaultTransformHandlers();
  }

  /**
   * Constructor used when the run configuration object is already available (unit tests / engine).
   */
  public HopPipelineMetaToSparkConverter(
      IVariables variables,
      PipelineMeta pipelineMeta,
      IHopMetadataProvider metadataProvider,
      String runConfigName,
      ISparkPipelineEngineRunConfiguration sparkRunConfiguration)
      throws HopException {
    this.variables = variables;
    this.pipelineMeta = pipelineMeta;
    this.metadataProvider = new SerializableMetadataProvider(metadataProvider);
    this.metaStoreJson = this.metadataProvider.toJson();
    this.runConfigName = runConfigName;
    this.sparkRunConfiguration = sparkRunConfiguration;
    this.transformHandlers = new HashMap<>();
    this.genericTransformHandler = new SparkGenericTransformHandler();
    this.genericTransformHandler.setPersistedDatasets(persistedDatasets);
    addDefaultTransformHandlers();
  }

  protected void addDefaultTransformHandlers() {
    // Lambdas, not constructor references: a constructor reference is linked here, which loads the
    // handler's class, and a handler of a transform plugin that isn't installed would stop the run
    // before register() can skip it
    register(SparkConst.MEMORY_GROUP_BY_PLUGIN_ID, () -> new SparkMemoryGroupByHandler());
    register(SparkConst.MERGE_JOIN_PLUGIN_ID, () -> new SparkMergeJoinHandler());
    register(SparkConst.UNIQUE_ROWS_PLUGIN_ID, () -> new SparkUniqueRowsHandler());
    register(SparkConst.SORT_ROWS_PLUGIN_ID, () -> new SparkSortRowsHandler());
    register(SparkConst.SPARK_FILE_INPUT_PLUGIN_ID, () -> new SparkFileInputHandler());
    register(SparkConst.SPARK_FILE_OUTPUT_PLUGIN_ID, () -> new SparkFileOutputHandler());
    register(SparkConst.SPARK_LAKE_TABLE_INPUT_PLUGIN_ID, () -> new SparkLakeTableInputHandler());
    register(SparkConst.SPARK_LAKE_TABLE_OUTPUT_PLUGIN_ID, () -> new SparkLakeTableOutputHandler());
    register(SparkConst.SPARK_LAKE_TABLE_MERGE_PLUGIN_ID, () -> new SparkLakeTableMergeHandler());
    register(
        SparkConst.SPARK_LAKE_TABLE_MAINTENANCE_PLUGIN_ID,
        () -> new SparkLakeTableMaintenanceHandler());
    register(SparkConst.SPARK_SQL_PLUGIN_ID, () -> new SparkSqlHandler());
    register(SparkConst.TABLE_INPUT_PLUGIN_ID, () -> new SparkJdbcInputHandler());
    register(SparkConst.DUMMY_PLUGIN_ID, () -> new SparkDummyHandler());
    register(SparkConst.GROUP_BY_PLUGIN_ID, () -> new SparkGroupByHandler(genericTransformHandler));
    register(
        SparkConst.ADD_SEQUENCE_PLUGIN_ID,
        () -> new SparkAddSequenceHandler(genericTransformHandler));
    register(SparkConst.SAMPLE_ROWS_PLUGIN_ID, () -> new SparkSampleRowsHandler());
    register(
        SparkConst.UNIQUE_ROWS_BY_HASH_SET_PLUGIN_ID,
        () -> new SparkUniqueRowsByHashSetHandler(genericTransformHandler));
    register(SparkConst.DETECT_LAST_ROW_PLUGIN_ID, () -> new SparkDetectLastRowHandler());
  }

  /**
   * Adds the handler of a transform. A handler that reads its transform's metadata class can't be
   * loaded when that transform plugin isn't installed; the transform isn't in any pipeline then.
   */
  private void register(String pluginId, Supplier<ISparkPipelineTransformHandler> handler) {
    try {
      ISparkPipelineTransformHandler created = handler.get();
      if (created instanceof SparkBaseTransformHandler baseHandler) {
        baseHandler.setPersistedDatasets(persistedDatasets);
      }
      transformHandlers.put(pluginId, created);
    } catch (LinkageError e) {
      LogChannel.GENERAL.logDebug(
          "No native Spark handler for transform plugin " + pluginId + ": " + e.getMessage());
    }
  }

  public void validatePipeline() throws HopException {
    for (TransformMeta transformMeta : pipelineMeta.getTransforms()) {
      validateTransformSparkUsage(transformMeta.getTransformPluginId(), transformMeta.getName());
    }
  }

  public static void validateTransformSparkUsage(String pluginId, String transformName)
      throws HopException {
    if (pluginId == null) {
      return;
    }
    String reason = HARD_BANNED_PLUGIN_IDS.get(pluginId);
    if (reason != null) {
      throw new HopException("Transform '" + transformName + "' (" + pluginId + "): " + reason);
    }
  }

  /**
   * Build the Dataset graph and return the sink Dataset (last transform with no successors, or the
   * last in topological order). Generic transforms are lazy: run an action on every Dataset in
   * {@link #getLeafDatasets()}, not only on the result, or the other branches never run.
   */
  public Dataset<Row> createDataset(SparkSession spark) throws HopException {
    try {
      ILogChannel log = LogChannel.GENERAL;
      validatePipeline();

      Map<String, Dataset<Row>> transformDatasetMap = new HashMap<>();
      List<TransformMeta> transforms = getSortedTransformsList();
      Map<String, List<TransformMeta>> fusedChains =
          sparkRunConfiguration == null || sparkRunConfiguration.isFuseGenericTransforms()
              ? findFusedChains(transforms)
              : Map.of();
      Set<String> fusedFollowers = new HashSet<>();
      for (List<TransformMeta> followers : fusedChains.values()) {
        followers.forEach(f -> fusedFollowers.add(f.getName()));
      }

      for (TransformMeta transformMeta : transforms) {
        if (fusedFollowers.contains(transformMeta.getName())) {
          // Runs inside the mini-pipeline of the first transform of its chain
          continue;
        }
        String pluginId = transformMeta.getTransformPluginId();

        List<TransformMeta> previousTransforms =
            pipelineMeta.findPreviousTransforms(transformMeta, false);

        // Per-transform handler selection: Table Input only runs through its native JDBC handler
        // when the partitioned read actually applies; any other configuration (parameters in the
        // SQL, per-row execution, incoming parameter stream, lookup, row limit) keeps the generic
        // handler so the multi-input union below and driver-side sampling are preserved.
        ISparkPipelineTransformHandler handler =
            transformHandlers.getOrDefault(pluginId, genericTransformHandler);
        if (SparkConst.TABLE_INPUT_PLUGIN_ID.equals(pluginId)
            && handler instanceof SparkJdbcInputHandler) {
          TableInputMeta tableInputMeta = new TableInputMeta();
          HopSparkUtil.loadTransformMetadata(
              tableInputMeta, transformMeta, metadataProvider, pipelineMeta);
          if (SparkJdbcInputHandler.isNativePartitionedRead(
              tableInputMeta, variables, !previousTransforms.isEmpty())) {
            handler = transformHandlers.get(pluginId);
          } else {
            handler = genericTransformHandler;
          }
        }
        boolean nativeHandler = handler != genericTransformHandler;

        Dataset<Row> input = null;
        IRowMeta rowMeta;
        if (previousTransforms.isEmpty()) {
          rowMeta = new org.apache.hop.core.row.RowMeta();
        } else if (nativeHandler
            && previousTransforms.size() > 1
            && !UNION_INPUT_NATIVE_PLUGIN_IDS.contains(pluginId)) {
          // Merge Join (etc.) resolves named inputs itself from transformDatasetMap
          TransformMeta previous = previousTransforms.get(0);
          input = lookupPreviousDataset(transformDatasetMap, previous, transformMeta, log);
          rowMeta =
              input != null
                  ? pipelineMeta.getTransformFields(variables, previous, transformMeta, null)
                  : new org.apache.hop.core.row.RowMeta();
        } else if (!nativeHandler
            && previousTransforms.size() > 1
            && SparkGenericTransformHandler.readsInputsSeparately(transformMeta)) {
          // Sorted merge, Stream schema merge, Multiway merge join ... read each input on its
          // own (by name, or row set by row set): no union, the generic handler gives every input
          // a stream of its own
          rowMeta = new org.apache.hop.core.row.RowMeta();
        } else {
          // One or more main predecessors: resolve each Dataset (incl. target-stream keys) and
          // union when layouts match (Hop multi-hop merge semantics).
          ResolvedInputs resolved =
              resolveAndUnionInputs(
                  log,
                  variables,
                  pipelineMeta,
                  transformMeta,
                  previousTransforms,
                  transformDatasetMap);
          input = resolved.dataset();
          rowMeta = resolved.rowMeta();
          if (input == null) {
            throw new HopException(
                "Previous Dataset(s) for transform '"
                    + transformMeta.getName()
                    + "' could not be found (previous="
                    + previousTransforms.stream().map(TransformMeta::getName).toList()
                    + "). Check that hops into this transform are enabled.");
          }
        }

        // Writers and info streams start Spark jobs while the graph is built: name them
        SparkJobNames.forTransform(
            spark, pipelineMeta.getName(), transformMeta.getName(), pluginId);
        if (transformMeta.isPartitioned()) {
          input = placeOnHopPartitions(log, transformMeta, previousTransforms, input);
        }

        List<TransformMeta> followers = fusedChains.get(transformMeta.getName());
        if (followers != null) {
          genericTransformHandler.handleFusedTransforms(
              log,
              variables,
              runConfigName,
              sparkRunConfiguration,
              metaStoreJson,
              pipelineMeta,
              transformMeta,
              followers,
              transformDatasetMap,
              spark,
              rowMeta,
              input);
          persistIfShared(log, followers.get(followers.size() - 1), transformDatasetMap);
          continue;
        }
        handler.handleTransform(
            log,
            variables,
            runConfigName,
            sparkRunConfiguration,
            metadataProvider,
            metaStoreJson,
            pipelineMeta,
            transformMeta,
            transformDatasetMap,
            spark,
            rowMeta,
            previousTransforms,
            input);

        persistIfShared(log, transformMeta, transformDatasetMap);
      }

      leafDatasets.clear();
      for (TransformMeta candidate : transforms) {
        if (pipelineMeta.findNextTransforms(candidate).isEmpty()) {
          Dataset<Row> leaf = transformDatasetMap.get(candidate.getName());
          if (leaf != null) {
            leafDatasets.put(candidate.getName(), leaf);
          }
        }
      }

      // Prefer a leaf transform as the result
      for (int i = transforms.size() - 1; i >= 0; i--) {
        TransformMeta candidate = transforms.get(i);
        if (pipelineMeta.findNextTransforms(candidate).isEmpty()) {
          Dataset<Row> leaf = transformDatasetMap.get(candidate.getName());
          if (leaf != null) {
            return leaf;
          }
        }
      }
      if (transforms.isEmpty()) {
        throw new HopException("Pipeline has no transforms to execute on Spark");
      }
      Dataset<Row> last = transformDatasetMap.get(transforms.get(transforms.size() - 1).getName());
      if (last == null) {
        throw new HopException("No Spark Dataset was produced for the pipeline");
      }
      return last;
    } catch (HopException e) {
      throw e;
    } catch (Exception e) {
      throw new HopException("Error converting Hop pipeline to Spark", e);
    }
  }

  /**
   * Chains of neighbouring generic transforms that can run in one Hop mini-pipeline per partition,
   * keyed by the first transform of the chain; the value holds the transforms after it, in order. A
   * link is fused when both transforms qualify ({@link SparkGenericTransformHandler#canFuse}), the
   * first has exactly one next transform and the second exactly one previous one (main or info).
   */
  Map<String, List<TransformMeta>> findFusedChains(List<TransformMeta> transforms) {
    Map<String, List<TransformMeta>> chains = new HashMap<>();
    Set<String> taken = new HashSet<>();
    for (TransformMeta head : transforms) {
      if (taken.contains(head.getName()) || !fusable(head)) {
        continue;
      }
      List<TransformMeta> followers = new ArrayList<>();
      TransformMeta current = head;
      while (true) {
        List<TransformMeta> next = pipelineMeta.findNextTransforms(current);
        if (next.size() != 1) {
          break;
        }
        TransformMeta candidate = next.get(0);
        if (taken.contains(candidate.getName())
            || !fusable(candidate)
            || pipelineMeta.findPreviousTransforms(candidate, true).size() != 1) {
          break;
        }
        followers.add(candidate);
        taken.add(candidate.getName());
        current = candidate;
      }
      if (!followers.isEmpty()) {
        taken.add(head.getName());
        chains.put(head.getName(), followers);
      }
    }
    return chains;
  }

  /**
   * The input of a partitioned transform, shuffled so that the rows of a key are on one Spark
   * partition (or spread evenly, for Rebalance). The rows of the only transform before it, placed
   * the same way, are already there: Hop hands them on partition by partition too.
   */
  private Dataset<Row> placeOnHopPartitions(
      ILogChannel log,
      TransformMeta transformMeta,
      List<TransformMeta> previousTransforms,
      Dataset<Row> input)
      throws HopException {
    String placement = SparkHopPartitioning.placement(transformMeta);
    if (input == null) {
      // An input transform, or one that reads each input on its own
      return null;
    }
    if (previousTransforms.size() == 1
        && keepsPartitions(previousTransforms.get(0))
        && placement.equals(SparkHopPartitioning.placement(previousTransforms.get(0)))) {
      log.logDetailed(
          "Transform '"
              + transformMeta.getName()
              + "' keeps the partitions of '"
              + previousTransforms.get(0).getName()
              + "'");
      return input;
    }
    log.logBasic(
        "Transform '"
            + transformMeta.getName()
            + "' is partitioned: its input is shuffled ("
            + placement
            + ")");
    return SparkHopPartitioning.repartition(input, transformMeta);
  }

  /**
   * Whether the output of a transform is still on the partitions of its input: it is partitioned
   * and runs partition by partition (a native handler other than Dummy shuffles its rows).
   */
  private boolean keepsPartitions(TransformMeta transformMeta) {
    String pluginId = transformMeta.getTransformPluginId();
    return transformMeta.isPartitioned()
        && (!transformHandlers.containsKey(pluginId)
            || SparkConst.DUMMY_PLUGIN_ID.equals(pluginId));
  }

  private boolean fusable(TransformMeta transformMeta) {
    return !transformHandlers.containsKey(transformMeta.getTransformPluginId())
        && SparkGenericTransformHandler.canFuse(pipelineMeta, transformMeta, sparkRunConfiguration);
  }

  /**
   * Persists the output of a transform that more than one transform reads (target streams of Filter
   * Rows / Switch / Case, a main and an info hop, ...). Without it every branch that runs an action
   * recomputes the transform and everything before it.
   */
  private void persistIfShared(
      ILogChannel log, TransformMeta transformMeta, Map<String, Dataset<Row>> transformDatasetMap) {
    String name = transformMeta.getName();
    Dataset<Row> shared = transformDatasetMap.get(HopSparkUtil.createSharedOutputId(name));
    if (shared == null && pipelineMeta.findNextTransforms(transformMeta).size() > 1) {
      shared = transformDatasetMap.get(name);
    }
    if (shared == null) {
      return;
    }
    persistedDatasets.persist(shared);
    log.logDetailed("Persisted the output of transform '" + name + "': several transforms read it");
  }

  /** Datasets of the transforms without successors, available after {@link #createDataset}. */
  public Map<String, Dataset<Row>> getLeafDatasets() {
    return leafDatasets;
  }

  /** Releases the Datasets kept by {@link #createDataset}, and by the transform handlers. */
  public void unpersistAll() {
    persistedDatasets.unpersistAll();
  }

  /**
   * Resolve Datasets for all main previous transforms (preferring target-stream keys), verify that
   * Hop row layouts match, and {@code union} them. Matches Beam Flatten / Hop multi-hop merge.
   */
  static ResolvedInputs resolveAndUnionInputs(
      ILogChannel log,
      IVariables variables,
      PipelineMeta pipelineMeta,
      TransformMeta transformMeta,
      List<TransformMeta> previousTransforms,
      Map<String, Dataset<Row>> transformDatasetMap)
      throws HopException {
    if (previousTransforms == null || previousTransforms.isEmpty()) {
      return new ResolvedInputs(null, new org.apache.hop.core.row.RowMeta());
    }

    List<Dataset<Row>> datasets = new ArrayList<>();
    IRowMeta referenceRowMeta = null;
    List<String> sourceNames = new ArrayList<>();

    for (TransformMeta previous : previousTransforms) {
      Dataset<Row> ds = lookupPreviousDataset(transformDatasetMap, previous, transformMeta, log);
      if (ds == null) {
        throw new HopException(
            "Previous Dataset for transform '"
                + previous.getName()
                + "' could not be found when handling '"
                + transformMeta.getName()
                + "'");
      }
      // Pass current as targetTransform so hops like Workflow Executor → results keep
      // execution-result fields (getFields clears the row when nextTransform is null).
      IRowMeta prevRowMeta =
          pipelineMeta.getTransformFields(variables, previous, transformMeta, null);
      if (referenceRowMeta == null) {
        referenceRowMeta = prevRowMeta;
      } else if (!transformMeta.getTransform().excludeFromRowLayoutVerification()) {
        try {
          BaseTransform.safeModeChecking(referenceRowMeta, prevRowMeta);
        } catch (HopRowException e) {
          throw new HopException(
              "Cannot combine data into transform '"
                  + transformMeta.getName()
                  + "': previous transforms do not share the same row layout. Hop requires"
                  + " identical field names, order, and types when multiple hops feed one"
                  + " transform. Mismatch involving '"
                  + previous.getName()
                  + "': "
                  + e.getMessage(),
              e);
        }
      }
      datasets.add(ds);
      sourceNames.add(previous.getName());
    }

    Dataset<Row> unioned = datasets.get(0);
    for (int i = 1; i < datasets.size(); i++) {
      // unionByName is more resilient if Spark column order drifts; layouts already checked
      unioned = unioned.unionByName(datasets.get(i));
    }
    if (datasets.size() > 1) {
      log.logBasic(
          "Combined "
              + datasets.size()
              + " previous Dataset(s) into transform '"
              + transformMeta.getName()
              + "': "
              + sourceNames);
    }
    return new ResolvedInputs(
        unioned,
        referenceRowMeta != null ? referenceRowMeta : new org.apache.hop.core.row.RowMeta());
  }

  /**
   * Prefer a target-stream Dataset when {@code previous} routed to {@code current} (Filter/Switch);
   * otherwise use the previous transform's main Dataset. Public so native handlers that resolve
   * their own named inputs (Spark SQL) reuse the same target-stream rules as the converter.
   */
  public static Dataset<Row> lookupPreviousDataset(
      Map<String, Dataset<Row>> transformDatasetMap,
      TransformMeta previous,
      TransformMeta current,
      ILogChannel log) {
    String targetKey = HopSparkUtil.createTargetTupleId(previous.getName(), current.getName());
    Dataset<Row> input = transformDatasetMap.get(targetKey);
    if (input != null) {
      if (log != null) {
        log.logBasic(
            "Transform '"
                + current.getName()
                + "' reading from previous target stream key: "
                + targetKey);
      }
      return input;
    }
    return transformDatasetMap.get(previous.getName());
  }

  /** Result of resolving one or more previous Datasets for a transform. */
  record ResolvedInputs(Dataset<Row> dataset, IRowMeta rowMeta) {}

  /**
   * Transforms that participate in at least one <strong>enabled</strong> hop (Beam-style active
   * graph). Fully disconnected transforms and transforms only linked by disabled hops are skipped
   * so sinks without an active input are not executed.
   *
   * <p>Unlike {@link PipelineMeta#getPipelineHopTransforms(boolean)}, this does <em>not</em> re-add
   * unused canvas transforms (those exist for painting only).
   */
  public static List<TransformMeta> collectActiveTransforms(PipelineMeta pipelineMeta) {
    if (pipelineMeta == null) {
      return List.of();
    }
    Set<TransformMeta> active = new LinkedHashSet<>();
    List<PipelineHopMeta> hops = pipelineMeta.getPipelineHops();
    if (hops != null) {
      for (PipelineHopMeta hop : hops) {
        if (hop == null || !hop.isEnabled()) {
          continue;
        }
        if (hop.getFromTransform() != null) {
          active.add(hop.getFromTransform());
        }
        if (hop.getToTransform() != null) {
          active.add(hop.getToTransform());
        }
      }
    }
    // Single-transform pipeline with no hops (rare design canvas)
    if (active.isEmpty() && pipelineMeta.nrTransforms() == 1) {
      active.add(pipelineMeta.getTransform(0));
    }
    return new ArrayList<>(active);
  }

  /** Topological order of active transforms (Kahn-style via repeated previous-set checks). */
  protected List<TransformMeta> getSortedTransformsList() throws HopException {
    List<TransformMeta> transforms = collectActiveTransforms(pipelineMeta);
    if (transforms.isEmpty() && pipelineMeta.nrTransforms() > 0) {
      throw new HopException(
          "No active transforms to execute on Spark: enable at least one hop between transforms"
              + " (disabled hops and disconnected transforms are skipped).");
    }
    List<TransformMeta> sorted = new ArrayList<>();
    Set<TransformMeta> handled = new HashSet<>();
    Set<TransformMeta> activeSet = new HashSet<>(transforms);

    while (sorted.size() < transforms.size()) {
      boolean progress = false;
      for (TransformMeta transformMeta : transforms) {
        if (handled.contains(transformMeta)) {
          continue;
        }
        // Include informational predecessors so Stream Lookup sources are built first
        List<TransformMeta> previous = pipelineMeta.findPreviousTransforms(transformMeta, true);
        boolean allPreviousHandled = true;
        for (TransformMeta prev : previous) {
          // Only require predecessors that are themselves active in the enabled graph
          if (activeSet.contains(prev) && !handled.contains(prev)) {
            allPreviousHandled = false;
            break;
          }
        }
        if (allPreviousHandled) {
          sorted.add(transformMeta);
          handled.add(transformMeta);
          progress = true;
        }
      }
      if (!progress) {
        // Cycle or unresolved graph — fall back to original order for remaining
        for (TransformMeta transformMeta : transforms) {
          if (!handled.contains(transformMeta)) {
            sorted.add(transformMeta);
            handled.add(transformMeta);
          }
        }
      }
    }
    return sorted;
  }

  public ISparkPipelineEngineRunConfiguration getSparkRunConfiguration() {
    return sparkRunConfiguration;
  }

  /**
   * Registers the driver-side metrics accumulator used by generic mapPartitions transforms and
   * native Dataset handlers. Must be called after the accumulator is registered with the Spark
   * context.
   */
  public void setMetricsAccumulator(SparkTransformMetricsAccumulator metricsAccumulator) {
    this.metricsAccumulator = metricsAccumulator;
    this.genericTransformHandler.setMetricsAccumulator(metricsAccumulator);
    for (ISparkPipelineTransformHandler handler : transformHandlers.values()) {
      if (handler instanceof SparkBaseTransformHandler baseHandler) {
        baseHandler.setMetricsAccumulator(metricsAccumulator);
      }
    }
  }

  /** Driver-side listener that attributes Spark's own SQL metrics to native handler stages. */
  public void setMetricsListener(SparkNativeMetricsListener metricsListener) {
    for (ISparkPipelineTransformHandler handler : transformHandlers.values()) {
      if (handler instanceof SparkBaseTransformHandler baseHandler) {
        baseHandler.setMetricsListener(metricsListener);
      }
    }
  }

  public SparkTransformMetricsAccumulator getMetricsAccumulator() {
    return metricsAccumulator;
  }

  /**
   * Accumulator that carries sample {@code ExecutionData} JSON from executors to the driver for
   * registration at the driver's execution info location.
   */
  public void setSampleDataAccumulator(SparkExecutionDataAccumulator sampleDataAccumulator) {
    this.sampleDataAccumulator = sampleDataAccumulator;
    this.genericTransformHandler.setSampleDataAccumulator(sampleDataAccumulator);
  }

  public SparkExecutionDataAccumulator getSampleDataAccumulator() {
    return sampleDataAccumulator;
  }

  /**
   * Context for executor-side execution data sampling. Call after the engine log channel exists.
   * Samples are shipped to the driver via {@link #setSampleDataAccumulator}.
   */
  public void setExecutionSamplingContext(String parentLogChannelId, String dataSamplersJson) {
    this.parentLogChannelId = parentLogChannelId;
    this.dataSamplersJson = dataSamplersJson;
    this.genericTransformHandler.setExecutionSamplingContext(
        runConfigName, parentLogChannelId, dataSamplersJson);
  }

  public String getParentLogChannelId() {
    return parentLogChannelId;
  }

  public String getDataSamplersJson() {
    return dataSamplersJson;
  }
}
