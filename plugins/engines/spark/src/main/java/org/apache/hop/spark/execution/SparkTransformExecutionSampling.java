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

package org.apache.hop.spark.execution;

import com.fasterxml.jackson.core.JsonProcessingException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.Timer;
import java.util.TimerTask;
import lombok.Getter;
import lombok.Setter;
import org.apache.commons.lang3.StringUtils;
import org.apache.hop.core.Const;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.exception.HopRuntimeException;
import org.apache.hop.core.exception.HopTransformException;
import org.apache.hop.core.json.HopJson;
import org.apache.hop.core.logging.LogChannel;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.RowBuffer;
import org.apache.hop.core.row.RowMetaBuilder;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.execution.Execution;
import org.apache.hop.execution.ExecutionBuilder;
import org.apache.hop.execution.ExecutionDataBuilder;
import org.apache.hop.execution.ExecutionDataSetMeta;
import org.apache.hop.execution.ExecutionInfoLocation;
import org.apache.hop.execution.ExecutionState;
import org.apache.hop.execution.ExecutionStateBuilder;
import org.apache.hop.execution.ExecutionType;
import org.apache.hop.execution.IExecutionInfoLocation;
import org.apache.hop.execution.profiling.ExecutionDataProfile;
import org.apache.hop.execution.sampler.ExecutionDataSamplerMeta;
import org.apache.hop.execution.sampler.IExecutionDataSampler;
import org.apache.hop.execution.sampler.IExecutionDataSamplerStore;
import org.apache.hop.metadata.api.IHopMetadataProvider;
import org.apache.hop.pipeline.Pipeline;
import org.apache.hop.pipeline.config.PipelineRunConfiguration;
import org.apache.hop.pipeline.engine.IEngineComponent;
import org.apache.hop.pipeline.engines.local.LocalPipelineEngine;
import org.apache.hop.pipeline.transform.ITransform;
import org.apache.hop.pipeline.transform.RowAdapter;
import org.apache.hop.pipeline.transform.stream.IStream;
import org.apache.hop.spark.core.SparkExecutionDataAccumulator;

/**
 * Executor-side sampling for Hop transforms running in Spark mapPartitions, ported from Beam {@code
 * TransformBaseFn}.
 *
 * <p>Samples are shipped to the driver via {@link SparkExecutionDataAccumulator} so the driver's
 * execution info location (which owns the parent pipeline {@code CacheEntry}) can call {@code
 * registerData}. Executor-local {@code registerData} is best-effort only (file locations).
 *
 * <p>Not serializable — construct after Hop/plugin init on the executor.
 */
@Getter
@Setter
public class SparkTransformExecutionSampling {

  private final String transformName;
  private final String parentLogChannelId;
  private final int copyNr;
  private final String ownerLogChannelId;

  private ExecutionInfoLocation executionInfoLocation;
  private List<IExecutionDataSampler> dataSamplers = List.of();
  private final List<IExecutionDataSamplerStore> dataSamplerStores = new ArrayList<>();
  private Timer executionInfoTimer;
  private LocalPipelineEngine pipeline;
  private ITransform transform;
  private boolean active;
  private SparkExecutionDataAccumulator sampleDataAccumulator;

  public SparkTransformExecutionSampling(
      String transformName, String parentLogChannelId, int copyNr) {
    this(transformName, parentLogChannelId, copyNr, null);
  }

  public SparkTransformExecutionSampling(
      String transformName,
      String parentLogChannelId,
      int copyNr,
      SparkExecutionDataAccumulator sampleDataAccumulator) {
    this.transformName = transformName;
    this.parentLogChannelId = parentLogChannelId;
    this.copyNr = copyNr;
    this.sampleDataAccumulator = sampleDataAccumulator;
    String parent =
        StringUtils.isNotEmpty(parentLogChannelId) ? parentLogChannelId : "spark-pipeline";
    this.ownerLogChannelId = parent + "|" + transformName + "|" + copyNr;
  }

  /**
   * Load location + data profile samplers from metadata. Sampling is only active when both an
   * execution information location and a non-empty data profile (or extra samplers JSON) are
   * present.
   */
  @SuppressWarnings("rawtypes")
  public void lookup(
      IVariables variables,
      IHopMetadataProvider metadataProvider,
      String runConfigName,
      String dataSamplersJson)
      throws HopException, JsonProcessingException {
    active = false;
    executionInfoLocation = null;
    dataSamplers = new ArrayList<>();
    dataSamplerStores.clear();

    if (StringUtils.isEmpty(runConfigName) || metadataProvider == null) {
      LogChannel.GENERAL.logDebug(
          "Spark sampling inactive for '"
              + transformName
              + "': runConfigName or metadataProvider missing");
      return;
    }

    PipelineRunConfiguration runConf =
        metadataProvider.getSerializer(PipelineRunConfiguration.class).load(runConfigName);
    if (runConf == null) {
      LogChannel.GENERAL.logBasic(
          "Spark sampling inactive for '"
              + transformName
              + "': run configuration '"
              + runConfigName
              + "' not found in metadata");
      return;
    }

    String locationName = runConf.getExecutionInfoLocationName();
    if (StringUtils.isEmpty(locationName)) {
      LogChannel.GENERAL.logDebug(
          "Spark sampling inactive for '"
              + transformName
              + "': no execution info location on run configuration '"
              + runConfigName
              + "'");
      return;
    }

    ExecutionInfoLocation location =
        metadataProvider.getSerializer(ExecutionInfoLocation.class).load(locationName);
    if (location == null) {
      LogChannel.GENERAL.logBasic(
          "Spark sampling inactive for '"
              + transformName
              + "': execution info location '"
              + locationName
              + "' not found in metadata");
      return;
    }

    String profileName = runConf.getExecutionDataProfileName();
    if (StringUtils.isNotEmpty(profileName)) {
      ExecutionDataProfile dataProfile =
          metadataProvider.getSerializer(ExecutionDataProfile.class).load(profileName);
      if (dataProfile != null && dataProfile.getSamplers() != null) {
        dataSamplers.addAll(dataProfile.getSamplers());
      } else {
        LogChannel.GENERAL.logBasic(
            "Spark sampling: data profile '"
                + profileName
                + "' missing or has no samplers for transform '"
                + transformName
                + "'");
      }
    }

    if (StringUtils.isNotEmpty(dataSamplersJson) && !"[]".equals(dataSamplersJson.trim())) {
      try {
        IExecutionDataSampler<?>[] extraSamplers =
            HopJson.newMapper().readValue(dataSamplersJson, IExecutionDataSampler[].class);
        if (extraSamplers != null) {
          dataSamplers.addAll(Arrays.asList(extraSamplers));
        }
      } catch (LinkageError | Exception e) {
        // local[*]: plugin CL may have a second Jackson → LinkageError on HopJson.newMapper().
        // Profile samplers above are enough; extra GUI samplers are best-effort.
        LogChannel.GENERAL.logError(
            "Unable to deserialize extra data samplers JSON for transform '"
                + transformName
                + "' (non-fatal): "
                + e.getMessage());
      }
    }

    if (dataSamplers.isEmpty()) {
      LogChannel.GENERAL.logBasic(
          "Spark sampling inactive for '"
              + transformName
              + "': no data samplers (profile='"
              + Const.NVL(profileName, "")
              + "', extraJson empty="
              + StringUtils.isEmpty(dataSamplersJson)
              + ")");
      return;
    }

    executionInfoLocation = location;
    // Location init still needed for optional executor-local registerData (file locations)
    IExecutionInfoLocation iLocation = executionInfoLocation.getExecutionInfoLocation();
    iLocation.initialize(variables, metadataProvider);
    active = true;
    LogChannel.GENERAL.logBasic(
        "Spark sampling active for '"
            + transformName
            + "' copy "
            + copyNr
            + " ("
            + dataSamplers.size()
            + " sampler(s), location='"
            + locationName
            + "', driverAccumulator="
            + (sampleDataAccumulator != null)
            + ")");
  }

  /** Register a transform execution node under the parent pipeline execution. */
  public void registerExecutingTransform(LocalPipelineEngine localPipeline) {
    if (!active || executionInfoLocation == null || localPipeline == null) {
      return;
    }
    try {
      ITransform t = localPipeline.getTransform(transformName, 0);
      if (t == null) {
        return;
      }
      List<IEngineComponent> copies = localPipeline.getComponentCopies(transformName);
      if (copies == null || copies.isEmpty()) {
        return;
      }
      IEngineComponent transformComponent = copies.get(0);
      Execution execution = ExecutionBuilder.fromTransform(localPipeline, t).build();
      execution.setParentId(parentLogChannelId);
      execution.setId(ownerLogChannelId);
      execution.setCopyNr(Integer.toString(copyNr));
      executionInfoLocation.getExecutionInfoLocation().registerExecution(execution);

      ExecutionState transformState =
          ExecutionStateBuilder.fromTransform(localPipeline, transformComponent).build();
      transformState.setParentId(parentLogChannelId);
      transformState.setId(ownerLogChannelId);
      transformState.setCopyNr(Integer.toString(copyNr));
      executionInfoLocation.getExecutionInfoLocation().updateExecutionState(transformState);
    } catch (Exception e) {
      LogChannel.GENERAL.logError(
          "Error registering transform execution for sampling (non-fatal)", e);
    }
  }

  /**
   * Attach profile samplers to transform OUTPUT rows and start a timer to flush samples to the
   * location.
   */
  @SuppressWarnings({"rawtypes", "unchecked"})
  public void attach(
      IVariables variables,
      LocalPipelineEngine localPipeline,
      ITransform transform,
      IRowMeta inputRowMeta,
      IRowMeta outputRowMeta) {
    if (!active || executionInfoLocation == null || dataSamplers.isEmpty() || transform == null) {
      return;
    }
    this.pipeline = localPipeline;
    this.transform = transform;

    ExecutionDataSamplerMeta dataSamplerMeta =
        new ExecutionDataSamplerMeta(
            transformName, Integer.toString(copyNr), ownerLogChannelId, false, false);

    for (IExecutionDataSampler dataSampler : dataSamplers) {
      IExecutionDataSamplerStore dataSamplerStore = dataSampler.createSamplerStore(dataSamplerMeta);
      dataSamplerStore.init(variables, inputRowMeta, outputRowMeta);
      dataSamplerStores.add(dataSamplerStore);
    }

    transform.addRowListener(
        new RowAdapter() {
          @Override
          public void rowWrittenEvent(IRowMeta rowMeta, Object[] row) throws HopTransformException {
            for (int s = 0; s < dataSamplers.size(); s++) {
              IExecutionDataSampler sampler = dataSamplers.get(s);
              IExecutionDataSamplerStore store = dataSamplerStores.get(s);
              try {
                sampler.sampleRow(store, IStream.StreamType.OUTPUT, rowMeta, row);
              } catch (HopException e) {
                throw new HopRuntimeException("Error sampling row on Spark partition", e);
              }
            }
          }
        });

    long delay = Const.toLong(executionInfoLocation.getDataLoggingDelay(), 5000L);
    long interval = Const.toLong(executionInfoLocation.getDataLoggingInterval(), 10000L);
    TimerTask task =
        new TimerTask() {
          @Override
          public void run() {
            try {
              sendSamplesToLocation(false);
            } catch (Exception e) {
              LogChannel.GENERAL.logError(
                  "Error sending transform samples to location (non-fatal)", e);
            }
          }
        };
    executionInfoTimer = new Timer("spark-sample-" + transformName + "-" + copyNr, true);
    executionInfoTimer.schedule(task, delay, interval);
  }

  /**
   * Push sampler stores + transform metrics. Prefer the driver accumulator (reliable for caching
   * locations); also best-effort {@code registerData} on this process for file locations.
   */
  @SuppressWarnings("rawtypes")
  public void sendSamplesToLocation(boolean finished) throws HopException {
    if (!active || executionInfoLocation == null || pipeline == null || transform == null) {
      return;
    }

    ExecutionDataBuilder dataBuilder =
        ExecutionDataBuilder.of()
            .withOwnerId(ownerLogChannelId)
            .withParentId(parentLogChannelId)
            .withExecutionType(ExecutionType.Transform)
            .withCollectionDate(new Date())
            .withFinished(finished);

    for (IExecutionDataSamplerStore store : dataSamplerStores) {
      dataBuilder.addDataSets(store.getSamples()).addSetMeta(store.getSamplesMetadata());
    }

    dataBuilder.addSetMeta(
        ownerLogChannelId,
        new ExecutionDataSetMeta(
            ownerLogChannelId,
            ownerLogChannelId,
            transformName,
            ownerLogChannelId,
            transformName + "." + ownerLogChannelId + " (Metrics)"));
    dataBuilder.addDataSet(
        ownerLogChannelId,
        new RowBuffer(
            new RowMetaBuilder().addString("metric").addInteger("value").build(),
            List.of(
                new Object[] {Pipeline.METRIC_NAME_INPUT, transform.getLinesInput()},
                new Object[] {Pipeline.METRIC_NAME_OUTPUT, transform.getLinesOutput()},
                new Object[] {Pipeline.METRIC_NAME_READ, transform.getLinesRead()},
                new Object[] {Pipeline.METRIC_NAME_WRITTEN, transform.getLinesWritten()},
                new Object[] {Pipeline.METRIC_NAME_REJECTED, transform.getLinesRejected()},
                new Object[] {Pipeline.METRIC_NAME_ERROR, transform.getErrors()})));

    var executionData = dataBuilder.build();

    // Primary path: ship JSON to the driver (owns parent CacheEntry for caching locations)
    if (sampleDataAccumulator != null) {
      try {
        String json = HopJson.newMapper().writeValueAsString(executionData);
        sampleDataAccumulator.addSample(ownerLogChannelId, json);
      } catch (LinkageError e) {
        // Dual Jackson on local[*] plugin CL — fall through to local registerData only
        LogChannel.GENERAL.logError(
            "Unable to serialize samples via HopJson for '"
                + transformName
                + "' (using local registerData only): "
                + e.getMessage());
      } catch (JsonProcessingException e) {
        throw new HopException(
            "Unable to serialize execution sample data for transform '" + transformName + "'", e);
      }
    }

    // Best-effort local registration (works for file locations when parent folder exists)
    IExecutionInfoLocation iLocation = executionInfoLocation.getExecutionInfoLocation();
    try {
      iLocation.registerData(executionData);
    } catch (Exception e) {
      if (sampleDataAccumulator == null) {
        throw e instanceof HopException hopEx
            ? hopEx
            : new HopException("Error registering execution data locally", e);
      }
      LogChannel.GENERAL.logDebug(
          "Local registerData skipped/failed for '"
              + transformName
              + "' (samples sent via driver accumulator): "
              + e.getMessage());
    }

    List<IEngineComponent> copies = pipeline.getComponentCopies(transformName);
    if (copies != null && !copies.isEmpty()) {
      ExecutionState transformState =
          ExecutionStateBuilder.fromTransform(pipeline, copies.get(0)).build();
      transformState.setParentId(parentLogChannelId);
      transformState.setId(ownerLogChannelId);
      transformState.setCopyNr(Integer.toString(copyNr));
      try {
        iLocation.updateExecutionState(transformState);
      } catch (Exception e) {
        LogChannel.GENERAL.logDebug(
            "Local updateExecutionState skipped for '" + transformName + "': " + e.getMessage());
      }
    }
  }

  /** Final sample flush, cancel timer, close location instance for this partition. */
  public void close() {
    if (!active) {
      return;
    }
    try {
      if (executionInfoTimer != null) {
        executionInfoTimer.cancel();
        executionInfoTimer = null;
      }
      try {
        sendSamplesToLocation(true);
      } catch (Exception e) {
        LogChannel.GENERAL.logError("Error sending final transform samples (non-fatal)", e);
      }
      if (executionInfoLocation != null
          && executionInfoLocation.getExecutionInfoLocation() != null) {
        try {
          executionInfoLocation.getExecutionInfoLocation().close();
        } catch (Exception e) {
          LogChannel.GENERAL.logError(
              "Error closing execution info location on partition (non-fatal)", e);
        }
      }
    } finally {
      active = false;
    }
  }

  /** Serialize extra GUI samplers for the mapPartitions closure (Beam-compatible). */
  public static String serializeDataSamplers(
      List<IExecutionDataSampler<? extends IExecutionDataSamplerStore>> dataSamplers)
      throws HopException {
    try {
      if (dataSamplers == null || dataSamplers.isEmpty()) {
        return "[]";
      }
      return HopJson.newMapper().writeValueAsString(dataSamplers);
    } catch (JsonProcessingException e) {
      throw new HopException("Unable to serialize execution data samplers", e);
    }
  }
}
