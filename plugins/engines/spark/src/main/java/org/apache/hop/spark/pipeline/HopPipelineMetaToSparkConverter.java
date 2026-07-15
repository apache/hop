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
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.logging.ILogChannel;
import org.apache.hop.core.logging.LogChannel;
import org.apache.hop.core.metadata.SerializableMetadataProvider;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.metadata.api.IHopMetadataProvider;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.config.PipelineRunConfiguration;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.apache.hop.spark.core.SparkExecutionDataAccumulator;
import org.apache.hop.spark.core.SparkTransformMetricsAccumulator;
import org.apache.hop.spark.engines.ISparkPipelineEngineRunConfiguration;
import org.apache.hop.spark.pipeline.handler.SparkBaseTransformHandler;
import org.apache.hop.spark.pipeline.handler.SparkFileInputHandler;
import org.apache.hop.spark.pipeline.handler.SparkFileOutputHandler;
import org.apache.hop.spark.pipeline.handler.SparkGenericTransformHandler;
import org.apache.hop.spark.pipeline.handler.SparkMemoryGroupByHandler;
import org.apache.hop.spark.pipeline.handler.SparkMergeJoinHandler;
import org.apache.hop.spark.pipeline.handler.SparkSortRowsHandler;
import org.apache.hop.spark.pipeline.handler.SparkUniqueRowsHandler;
import org.apache.hop.spark.util.SparkConst;
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
          SparkConst.SPARK_FILE_OUTPUT_PLUGIN_ID);

  /**
   * Plugin ids that must not run as partition-local Hop mini-pipelines. Keep in lockstep with
   * {@link org.apache.hop.spark.engines.SparkPipelineEngine#supports}.
   */
  public static final Map<String, String> HARD_BANNED_PLUGIN_IDS =
      Map.of(
          SparkConst.GROUP_BY_PLUGIN_ID,
          "Group By is not supported on the native Spark engine. Use Memory Group By (native Spark shuffle) instead, or run on Local/Beam.");

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
    addDefaultTransformHandlers();
  }

  protected void addDefaultTransformHandlers() {
    transformHandlers.put(SparkConst.MEMORY_GROUP_BY_PLUGIN_ID, new SparkMemoryGroupByHandler());
    transformHandlers.put(SparkConst.MERGE_JOIN_PLUGIN_ID, new SparkMergeJoinHandler());
    transformHandlers.put(SparkConst.UNIQUE_ROWS_PLUGIN_ID, new SparkUniqueRowsHandler());
    transformHandlers.put(SparkConst.SORT_ROWS_PLUGIN_ID, new SparkSortRowsHandler());
    transformHandlers.put(SparkConst.SPARK_FILE_INPUT_PLUGIN_ID, new SparkFileInputHandler());
    transformHandlers.put(SparkConst.SPARK_FILE_OUTPUT_PLUGIN_ID, new SparkFileOutputHandler());
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
   * last in topological order). Calling an action on the result materializes the pipeline.
   */
  public Dataset<Row> createDataset(SparkSession spark) throws HopException {
    try {
      ILogChannel log = LogChannel.GENERAL;
      validatePipeline();

      Map<String, Dataset<Row>> transformDatasetMap = new HashMap<>();
      List<TransformMeta> transforms = getSortedTransformsList();

      for (TransformMeta transformMeta : transforms) {
        String pluginId = transformMeta.getTransformPluginId();
        ISparkPipelineTransformHandler handler =
            transformHandlers.getOrDefault(pluginId, genericTransformHandler);
        boolean nativeHandler = transformHandlers.containsKey(pluginId);

        List<TransformMeta> previousTransforms =
            pipelineMeta.findPreviousTransforms(transformMeta, false);

        // Merge Join (and similar) can have multiple previous/info inputs; the native handler
        // resolves them from transformDatasetMap. Generic handlers still require a single input.
        if (!nativeHandler && previousTransforms.size() > 1) {
          throw new HopException(
              "Combining data from multiple previous transforms is not yet supported for transform '"
                  + transformMeta.getName()
                  + "' on the native Spark engine (plugin id="
                  + pluginId
                  + ")");
        }

        Dataset<Row> input = null;
        IRowMeta rowMeta;
        if (previousTransforms.isEmpty()) {
          rowMeta = new org.apache.hop.core.row.RowMeta();
        } else {
          TransformMeta previous = previousTransforms.get(0);
          input = transformDatasetMap.get(previous.getName());
          if (input == null && !nativeHandler) {
            throw new HopException(
                "Previous Dataset for transform '"
                    + previous.getName()
                    + "' could not be found when handling '"
                    + transformMeta.getName()
                    + "'");
          }
          rowMeta =
              input != null
                  ? pipelineMeta.getTransformFields(variables, previous)
                  : new org.apache.hop.core.row.RowMeta();
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

  /** Topological order of transforms (Kahn-style via repeated previous-set checks). */
  protected List<TransformMeta> getSortedTransformsList() {
    List<TransformMeta> transforms = new ArrayList<>(pipelineMeta.getTransforms());
    List<TransformMeta> sorted = new ArrayList<>();
    Set<TransformMeta> handled = new HashSet<>();

    while (sorted.size() < transforms.size()) {
      boolean progress = false;
      for (TransformMeta transformMeta : transforms) {
        if (handled.contains(transformMeta)) {
          continue;
        }
        List<TransformMeta> previous = pipelineMeta.findPreviousTransforms(transformMeta, false);
        boolean allPreviousHandled = true;
        for (TransformMeta prev : previous) {
          if (!handled.contains(prev)) {
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
