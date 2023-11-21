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

package org.apache.hop.pipeline.transforms.mapping;

import org.apache.commons.lang.StringUtils;
import org.apache.hop.core.Const;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.pipeline.Pipeline;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.RowProducer;
import org.apache.hop.pipeline.SingleThreadedPipelineExecutor;
import org.apache.hop.pipeline.TransformWithMappingMeta;
import org.apache.hop.pipeline.config.PipelineRunConfiguration;
import org.apache.hop.pipeline.engine.IEngineComponent;
import org.apache.hop.pipeline.engine.PipelineEngineFactory;
import org.apache.hop.pipeline.engines.local.LocalPipelineEngine;
import org.apache.hop.pipeline.engines.local.LocalPipelineRunConfiguration;
import org.apache.hop.pipeline.transform.BaseTransform;
import org.apache.hop.pipeline.transform.ITransform;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.apache.hop.pipeline.transforms.input.MappingInput;
import org.apache.hop.pipeline.transforms.output.MappingOutput;

import java.util.ArrayList;
import java.util.List;

/** Execute a mapping: a reusable pipeline */
public class SimpleMapping extends BaseTransform<SimpleMappingMeta, SimpleMappingData> {

  private static final Class<?> PKG = SimpleMappingMeta.class; // For Translator

  public SimpleMapping(
      TransformMeta transformMeta,
      SimpleMappingMeta meta,
      SimpleMappingData data,
      int copyNr,
      PipelineMeta pipelineMeta,
      Pipeline pipeline) {
    super(transformMeta, meta, data, copyNr, pipelineMeta, pipeline);
  }

  /**
   * Process a single row. In our case, we send one row of data to a piece of pipeline. In the
   * pipeline, we look up the MappingInput transform to send our rows to it. As a consequence, for
   * the time being, there can only be one MappingInput and one MappingOutput transform in the
   * Mapping.
   */
  @Override
  public boolean processRow() throws HopException {

    try {
      if (first) {
        first = false;
        data.wasStarted = true;

        // Rows read are injected into the one available Mapping Input transform.
        // This adds an input RowSet to the Mapping Input transform.
        //
        String mappingInputTransformName = data.mappingInput.getTransformName();
        RowProducer rowProducer = data.mappingPipeline.addRowProducer(mappingInputTransformName, 0);
        data.rowDataInputMapper = new RowDataInputMapper(meta.getInputMapping(), rowProducer);

        // Rows produced by the mapping are read and passed on.
        //
        String mappingOutputTransformName = data.mappingOutput.getTransformName();
        ITransform iOutputTransform =
            data.mappingPipeline.getTransform(mappingOutputTransformName, 0);
        RowOutputDataMapper outputDataMapper =
            new RowOutputDataMapper(meta.getInputMapping(), meta.getOutputMapping(), this::putRow);
        iOutputTransform.addRowListener(outputDataMapper);

        // Start the mapping/sub-pipeline threads
        //
        data.mappingPipeline.startThreads();
      }

      // The data we read we pass to the mapping
      //
      Object[] row = getRow();
      boolean rowWasPut = false;
      if (row != null) {
        while (!(data.mappingPipeline.isFinishedOrStopped() || rowWasPut)) {
          rowWasPut = data.rowDataInputMapper.putRow(getInputRowMeta(), row);
        }
      }

      if (!rowWasPut) {
        data.rowDataInputMapper.finished();
        data.mappingPipeline.waitUntilFinished();
        setOutputDone();
        return false;
      }

      return true;
    } catch (Throwable t) {
      // Some unexpected situation occurred.
      // Better to stop the mapping pipeline.
      //
      if (data.mappingPipeline != null) {
        data.mappingPipeline.stopAll();
      }

      // Forward the exception...
      //
      throw new HopException(t);
    }
  }

  public void prepareMappingExecution() throws HopException {
    if (data.isBeamContext()) {
      data.mappingPipelineMeta.setPipelineType(PipelineMeta.PipelineType.SingleThreaded);
    }
    SimpleMappingData simpleMappingData = getData();
    // Resolve pipeline full name in case variables are used and pipeline meta is not initialized in advance
    if (simpleMappingData.mappingPipelineMeta == null) {
      simpleMappingData.mappingPipelineMeta = new PipelineMeta(variables.resolve(meta.getFilename()), metadataProvider, variables);
      simpleMappingData.mappingPipelineMeta.clearChanged();

    }

    String runConfigName = resolve(meta.getRunConfigurationName());
    if (StringUtils.isEmpty(runConfigName)) {
      // Create the pipeline from meta-data...
      simpleMappingData.mappingPipeline =
          new LocalPipelineEngine(simpleMappingData.mappingPipelineMeta, this, this);
    } else {
      // Validate the run configuration to be a local one.
      //
      PipelineRunConfiguration runConfig =
          metadataProvider.getSerializer(PipelineRunConfiguration.class).load(runConfigName);
      if (runConfig == null) {
        throw new HopException("Unable to find run configuration with name " + runConfigName);
      }
      if (!(runConfig.getEngineRunConfiguration() instanceof LocalPipelineRunConfiguration)) {
        throw new HopException(
            "Apache Hop can only run simple mappings with a local pipeline engine, not with run configuration "
                + runConfigName);
      }

      // We can use the pipeline engine factory if we have a run configuration name.
      //
      simpleMappingData.mappingPipeline =
          (LocalPipelineEngine)
              PipelineEngineFactory.createPipelineEngine(
                  this, runConfigName, metadataProvider, simpleMappingData.mappingPipelineMeta);
    }

    // Copy the parameters over...
    //
    simpleMappingData.mappingPipeline.copyParametersFromDefinitions(
        simpleMappingData.mappingPipelineMeta);

    // Set the parameters values in the mapping.
    //
    TransformWithMappingMeta.activateParams(
        simpleMappingData.mappingPipeline,
        simpleMappingData.mappingPipeline,
        this,
        simpleMappingData.mappingPipelineMeta.listParameters(),
        meta.getMappingParameters().getVariables(),
        meta.getMappingParameters().getInputs(),
        meta.getMappingParameters().isInheritingAllVariables());

    // Leave a path up so that we can set variables in sub-pipelines...
    //
    simpleMappingData.mappingPipeline.setParentPipeline(getPipeline());
    simpleMappingData.mappingPipeline.setParent(this);

    // Pass down the safe mode flag to the mapping...
    //
    simpleMappingData.mappingPipeline.setSafeModeEnabled(getPipeline().isSafeModeEnabled());

    // Pass down the metrics gathering flag:
    //
    simpleMappingData.mappingPipeline.setGatheringMetrics(getPipeline().isGatheringMetrics());

    // We launch the pipeline in the processRow when the first row is
    // received.
    // This will allow the correct variables to be passed.
    // Otherwise, the parent is the init() thread which will be gone once the
    // init is done.
    //
    try {
      simpleMappingData.mappingPipeline.prepareExecution();
    } catch (HopException e) {
      throw new HopException(
          BaseMessages.getString(PKG, "SimpleMapping.Exception.UnableToPrepareExecutionOfMapping"),
          e);
    }

    // If there is no read/write logging transform set, we can insert the data from
    // the first mapping input/output transform...
    //
    List<MappingInput> mappingInputs = findMappingInputs(simpleMappingData.mappingPipeline);
    if (mappingInputs.isEmpty()) {
      throw new HopException(
          "The simple mapping transform needs one Mapping Input transform to write to in the sub-pipeline");
    }
    if (mappingInputs.size() > 1) {
      throw new HopException(
          "The simple mapping transform does not support multiple Mapping Input transforms to write to in the sub-pipeline");
    }
    simpleMappingData.mappingInput = mappingInputs.get(0);

    List<MappingOutput> mappingOutputs = findMappingOutputs(simpleMappingData.mappingPipeline);
    if (mappingOutputs.isEmpty()) {
      throw new HopException(
          "The simple mapping transform needs one Mapping Output transform to read from in the sub-pipeline");
    }
    if (mappingOutputs.size() > 1) {
      throw new HopException(
          "The simple mapping transform does not support multiple Mapping Output transforms to read from in the sub-pipeline");
    }
    simpleMappingData.mappingOutput = mappingOutputs.get(0);

    // Finally, add the mapping pipeline to the active sub-pipelines
    // map in the parent pipeline
    //
    getPipeline().addActiveSubPipeline(getTransformName(), simpleMappingData.mappingPipeline);
  }

  public static List<MappingInput> findMappingInputs(Pipeline mappingPipeline) {
    List<MappingInput> list = new ArrayList<>();

    List<IEngineComponent> components = mappingPipeline.getComponents();
    for (IEngineComponent component : components) {
      if (component instanceof MappingInput) {
        list.add((MappingInput) component);
      }
    }

    return list;
  }

  private List<MappingOutput> findMappingOutputs(Pipeline mappingPipeline) {
    List<MappingOutput> list = new ArrayList<>();

    List<IEngineComponent> components = mappingPipeline.getComponents();
    for (IEngineComponent component : components) {
      if (component instanceof MappingOutput) {
        list.add((MappingOutput) component);
      }
    }

    return list;
  }

  @Override
  public boolean init() {

    if (super.init()) {
      // First we need to load the mapping (pipeline)
      try {
        // Pass the MetaStore down to the metadata object...
        //
        data.mappingPipelineMeta =
            SimpleMappingMeta.loadMappingMeta(meta, getMetadataProvider(), this);
        if (data.mappingPipelineMeta != null) { // Do we have a mapping at all?

          // OK, now prepare the execution of the mapping.
          // This includes the allocation of RowSet buffers, the creation of the
          // sub-pipeline threads, etc.
          //
          prepareMappingExecution();

          // Create the single threaded executor on top of the pipeline.
          // We don't want to process one-row batches in a parallel engine where we need to wait for
          // the threads to finish.
          //
          if (data.isBeamContext()) {
            data.executor = new SingleThreadedPipelineExecutor(data.mappingPipeline);
          }

          // That's all for now...
          return true;
        } else {
          logError("No valid mapping was specified!");
          return false;
        }
      } catch (Exception e) {
        logError("Unable to load the mapping pipeline because of an error : " + e.toString());
        logError(Const.getStackTracker(e));
      }
    }
    return false;
  }

  @Override
  public void dispose() {
    if (data.isBeamContext()) {
      try {
        data.executor.dispose();
      } catch (Exception e) {
        logError("Error calling dispose() on single threaded Simple Mapping executor", e);
        setErrors(1);
      }
    } else {
      // Close the running pipeline
      if (data.wasStarted) {
        if (!data.mappingPipeline.isFinished()) {
          // Wait until the child pipeline has finished.
          data.mappingPipeline.waitUntilFinished();
        }
      }
    }

    // See if there was an error in the sub-pipeline, in that case, flag error etc.
    if (getData().mappingPipeline.getErrors() > 0) {
      logError(BaseMessages.getString(PKG, "SimpleMapping.Log.ErrorOccurredInSubPipeline"));
      setErrors(1);
    }

    super.dispose();
  }

  @Override
  public void batchComplete() throws HopException {
    if (data.isBeamContext()) {
      // Execute all transforms single-threaded, one after the other.
      // This way input rows can end up in the mapping output transform.
      //
      data.executor.oneIteration();
    }
  }

  @Override
  public void startBundle() throws HopException {
    //    if (!first) {
    //      prepareMappingExecution();
    //    }
  }

  @Override
  public void finishBundle() throws HopException {
    //    setOutputDone();
    //    first=true;
    //    data.wasStarted=false;
  }

  @Override
  public void stopRunning() {
    if (data.mappingPipeline != null) {
      data.mappingPipeline.stopAll();
    }
  }

  @Override
  public void stopAll() {
    // Stop the mapping transform.
    if (data.mappingPipeline != null) {
      data.mappingPipeline.stopAll();
    }

    // Also stop this transform
    super.stopAll();
  }

  public Pipeline getMappingPipeline() {
    return data.mappingPipeline;
  }
}
