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

package org.apache.hop.pipeline.transforms.multimapping;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.apache.commons.lang3.StringUtils;
import org.apache.hop.core.Const;
import org.apache.hop.core.IRowSet;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.exception.HopTransformException;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.pipeline.Pipeline;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.RowProducer;
import org.apache.hop.pipeline.TransformWithMappingMeta;
import org.apache.hop.pipeline.config.PipelineRunConfiguration;
import org.apache.hop.pipeline.engine.PipelineEngineFactory;
import org.apache.hop.pipeline.engines.local.LocalPipelineEngine;
import org.apache.hop.pipeline.engines.local.LocalPipelineRunConfiguration;
import org.apache.hop.pipeline.transform.BaseTransform;
import org.apache.hop.pipeline.transform.IRowListener;
import org.apache.hop.pipeline.transform.ITransform;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.apache.hop.pipeline.transforms.input.MappingInput;
import org.apache.hop.pipeline.transforms.mapping.MappingIODefinition;
import org.apache.hop.pipeline.transforms.mapping.MappingTransforms;
import org.apache.hop.pipeline.transforms.mapping.MappingValueRename;
import org.apache.hop.pipeline.transforms.mapping.RowDataInputMapper;
import org.apache.hop.pipeline.transforms.mapping.RowOutputDataMapper;
import org.apache.hop.pipeline.transforms.output.MappingOutput;

/** Execute a mapping with 0..N inputs and outputs using row producers and row listeners. */
public class MultiMapping extends BaseTransform<MultiMappingMeta, MultiMappingData> {

  private static final Class<?> PKG = MultiMappingMeta.class;

  public MultiMapping(
      TransformMeta transformMeta,
      MultiMappingMeta meta,
      MultiMappingData data,
      int copyNr,
      PipelineMeta pipelineMeta,
      Pipeline pipeline) {
    super(transformMeta, meta, data, copyNr, pipelineMeta, pipeline);
  }

  @Override
  public boolean processRow() throws HopException {
    try {
      if (first) {
        first = false;
        data.wasStarted = true;
        wireInputProducers();
        wireOutputListeners();
        data.mappingPipeline.startThreads();
        drainInfoStreams();
        data.infoDrained = true;
      }

      if (!data.mainRowSets.isEmpty()) {
        Object[] row = nextMainRow();
        if (row != null) {
          return true;
        }
      }

      finishProducers();
      data.mappingPipeline.waitUntilFinished();
      setOutputDone();
      return false;
    } catch (Exception e) {
      if (data.mappingPipeline != null) {
        data.mappingPipeline.stopAll();
      }
      throw new HopException(e);
    }
  }

  private void wireInputProducers() throws HopException {
    List<MappingInput> mappingInputs = MappingTransforms.findMappingInputs(data.mappingPipeline);
    Set<String> referencedInputs = new HashSet<>();

    List<MappingIODefinition> inputDefinitions = new ArrayList<>();
    for (MultiMappingInputDefinition definition : meta.getInputMappings()) {
      inputDefinitions.add(definition.toIoDefinition());
    }

    if (inputDefinitions.isEmpty() && mappingInputs.size() == 1) {
      MappingIODefinition auto =
          new MappingIODefinition(null, mappingInputs.get(0).getTransformName());
      auto.setMainDataPath(true);
      inputDefinitions.add(auto);
    }

    for (MappingIODefinition definition : inputDefinitions) {
      MappingInput mappingInput =
          MappingTransforms.findMappingInput(
              data.mappingPipeline, definition.getOutputTransformName());
      if (mappingInput == null) {
        throw new HopException(
            BaseMessages.getString(
                PKG,
                "MultiMapping.Exception.MappingInputNotFound",
                Const.NVL(definition.getOutputTransformName(), "")));
      }
      referencedInputs.add(mappingInput.getTransformName());

      RowProducer rowProducer =
          data.mappingPipeline.addRowProducer(mappingInput.getTransformName(), 0);
      RowDataInputMapper mapper = new RowDataInputMapper(definition, rowProducer);
      data.allInputMappers.add(mapper);

      if (MappingTransforms.isInfoMapping(definition)) {
        IRowSet rowSet = findInputRowSet(definition.getInputTransformName());
        if (rowSet == null) {
          throw new HopException(
              BaseMessages.getString(
                  PKG,
                  "MultiMapping.Exception.InfoRowSetNotFound",
                  definition.getInputTransformName()));
        }
        data.infoRowSets.add(rowSet);
        data.rowSetMappers.put(rowSet, mapper);
      } else if (StringUtils.isNotEmpty(definition.getInputTransformName())) {
        IRowSet rowSet = findInputRowSet(definition.getInputTransformName());
        if (rowSet == null) {
          throw new HopException(
              BaseMessages.getString(
                  PKG,
                  "MultiMapping.Exception.InputRowSetNotFound",
                  definition.getInputTransformName()));
        }
        data.mainRowSets.add(rowSet);
        data.rowSetMappers.put(rowSet, mapper);
      } else {
        for (IRowSet rowSet : getInputRowSets()) {
          if (!isInfoRowSet(rowSet) && !data.rowSetMappers.containsKey(rowSet)) {
            data.mainRowSets.add(rowSet);
            data.rowSetMappers.put(rowSet, mapper);
          }
        }
      }
    }

    for (MappingInput mappingInput : mappingInputs) {
      if (!referencedInputs.contains(mappingInput.getTransformName())) {
        RowProducer rowProducer =
            data.mappingPipeline.addRowProducer(mappingInput.getTransformName(), 0);
        rowProducer.finished();
      }
    }
  }

  private boolean isInfoRowSet(IRowSet rowSet) {
    for (IRowSet infoRowSet : data.infoRowSets) {
      if (infoRowSet == rowSet) {
        return true;
      }
    }
    String origin = rowSet.getOriginTransformName();
    for (MultiMappingInputDefinition definition : meta.getInputMappings()) {
      if (MappingTransforms.isInfoMapping(definition.toIoDefinition())
          && origin != null
          && origin.equalsIgnoreCase(definition.getInputTransformName())) {
        return true;
      }
    }
    return false;
  }

  private void wireOutputListeners() throws HopException {
    List<MappingOutput> mappingOutputs = MappingTransforms.findMappingOutputs(data.mappingPipeline);
    List<MappingValueRename> inputRenames = new ArrayList<>();
    for (MultiMappingInputDefinition inputDefinition : meta.getInputMappings()) {
      if (inputDefinition.isRenamingOnOutput()) {
        MultiMappingMeta.addInputRenames(
            inputRenames, inputDefinition.toIoDefinition().getValueRenames());
      }
    }
    MappingIODefinition combinedInputRenames = new MappingIODefinition();
    combinedInputRenames.setRenamingOnOutput(!inputRenames.isEmpty());
    combinedInputRenames.setValueRenames(inputRenames);

    Set<String> claimedTargets = new HashSet<>();
    for (MultiMappingOutputDefinition definition : meta.getOutputMappings()) {
      if (MappingTransforms.isTargetMapping(definition.toIoDefinition())) {
        claimedTargets.add(definition.getOutputTransformName());
      }
    }

    List<MappingIODefinition> outputDefinitions = new ArrayList<>();
    for (MultiMappingOutputDefinition definition : meta.getOutputMappings()) {
      outputDefinitions.add(definition.toIoDefinition());
    }
    if (outputDefinitions.isEmpty() && mappingOutputs.size() == 1) {
      MappingIODefinition auto =
          new MappingIODefinition(mappingOutputs.get(0).getTransformName(), null);
      auto.setMainDataPath(true);
      outputDefinitions.add(auto);
    }

    Set<String> referencedOutputs = new HashSet<>();
    for (MappingIODefinition definition : outputDefinitions) {
      MappingOutput mappingOutput =
          MappingTransforms.findMappingOutput(
              data.mappingPipeline, definition.getInputTransformName());
      if (mappingOutput == null) {
        throw new HopException(
            BaseMessages.getString(
                PKG,
                "MultiMapping.Exception.MappingOutputNotFound",
                Const.NVL(definition.getInputTransformName(), "")));
      }
      referencedOutputs.add(mappingOutput.getTransformName());

      List<IRowSet> targetRowSets = resolveOutputRowSets(definition, claimedTargets);
      RowOutputDataMapper outputDataMapper =
          new RowOutputDataMapper(
              combinedInputRenames,
              definition,
              (rowMeta, row) -> putRowsTo(rowMeta, row, targetRowSets));
      mappingOutput.addRowListener(outputDataMapper);
    }
  }

  private List<IRowSet> resolveOutputRowSets(
      MappingIODefinition definition, Set<String> claimedTargets) throws HopTransformException {
    List<IRowSet> rowSets = new ArrayList<>();
    if (StringUtils.isNotEmpty(definition.getOutputTransformName())) {
      IRowSet rowSet = findOutputRowSet(definition.getOutputTransformName());
      if (rowSet != null) {
        rowSets.add(rowSet);
      }
      return rowSets;
    }
    for (IRowSet rowSet : getOutputRowSets()) {
      String destination = rowSet.getDestinationTransformName();
      if (destination == null || !claimedTargets.contains(destination)) {
        rowSets.add(rowSet);
      }
    }
    return rowSets;
  }

  private void putRowsTo(IRowMeta rowMeta, Object[] row, List<IRowSet> rowSets)
      throws HopTransformException {
    if (rowSets.isEmpty()) {
      return;
    }
    for (IRowSet rowSet : rowSets) {
      putRowTo(rowMeta, row, rowSet);
    }
  }

  private void drainInfoStreams() throws HopException {
    for (IRowSet rowSet : data.infoRowSets) {
      RowDataInputMapper mapper = data.rowSetMappers.get(rowSet);
      Object[] row = getRowFrom(rowSet);
      while (row != null && !isStopped() && !data.mappingPipeline.isFinishedOrStopped()) {
        IRowMeta rowMeta = rowSet.getRowMeta();
        boolean put = false;
        while (!(data.mappingPipeline.isFinishedOrStopped() || put)) {
          put = mapper.putRow(rowMeta, row);
        }
        if (!put) {
          break;
        }
        row = getRowFrom(rowSet);
      }
      mapper.finished();
      data.finishedRowSets.add(rowSet);
    }
  }

  private Object[] nextMainRow() throws HopException {
    int attempts = data.mainRowSets.size();
    for (int i = 0; i < attempts; i++) {
      if (data.mainRowSetIndex >= data.mainRowSets.size()) {
        data.mainRowSetIndex = 0;
      }
      IRowSet rowSet = data.mainRowSets.get(data.mainRowSetIndex);
      data.mainRowSetIndex++;
      if (data.finishedRowSets.contains(rowSet)) {
        continue;
      }
      Object[] row = getRowFrom(rowSet);
      if (row == null) {
        RowDataInputMapper mapper = data.rowSetMappers.get(rowSet);
        if (mapper != null) {
          mapper.finished();
        }
        data.finishedRowSets.add(rowSet);
        continue;
      }
      RowDataInputMapper mapper = data.rowSetMappers.get(rowSet);
      IRowMeta rowMeta = rowSet.getRowMeta();
      boolean put = false;
      while (!(data.mappingPipeline.isFinishedOrStopped() || put)) {
        put = mapper.putRow(rowMeta, row);
      }
      if (!put) {
        return null;
      }
      return row;
    }
    return null;
  }

  private void finishProducers() {
    if (data.producersFinished) {
      return;
    }
    data.producersFinished = true;
    for (RowDataInputMapper mapper : data.allInputMappers) {
      mapper.finished();
    }
  }

  public void prepareMappingExecution() throws HopException {
    if (data.mappingPipelineMeta == null) {
      data.mappingPipelineMeta =
          new PipelineMeta(variables.resolve(meta.getFilename()), metadataProvider, variables);
      data.mappingPipelineMeta.clearChanged();
    }

    String runConfigName = resolve(meta.getRunConfigurationName());
    if (StringUtils.isEmpty(runConfigName)) {
      data.mappingPipeline = new LocalPipelineEngine(data.mappingPipelineMeta, this, this);
    } else {
      PipelineRunConfiguration runConfig =
          metadataProvider.getSerializer(PipelineRunConfiguration.class).load(runConfigName);
      if (runConfig == null) {
        throw new HopException("Unable to find run configuration with name " + runConfigName);
      }
      if (!(runConfig.getEngineRunConfiguration() instanceof LocalPipelineRunConfiguration)) {
        throw new HopException(
            "Apache Hop can only run multi mappings with a local pipeline engine, not with run configuration "
                + runConfigName);
      }
      data.mappingPipeline =
          (LocalPipelineEngine)
              PipelineEngineFactory.createPipelineEngine(
                  getPipeline(), runConfigName, metadataProvider, data.mappingPipelineMeta);
    }

    data.mappingPipeline.copyParametersFromDefinitions(data.mappingPipelineMeta);
    TransformWithMappingMeta.activateParams(
        data.mappingPipeline,
        data.mappingPipeline,
        this,
        data.mappingPipelineMeta.listParameters(),
        meta.getMappingParameters().getVariables(),
        meta.getMappingParameters().getInputs(),
        meta.getMappingParameters().isInheritingAllVariables());

    data.mappingPipeline.setParentPipeline(getPipeline());
    data.mappingPipeline.setParent(this);
    data.mappingPipeline.setSafeModeEnabled(getPipeline().isSafeModeEnabled());
    data.mappingPipeline.setGatheringMetrics(getPipeline().isGatheringMetrics());

    try {
      data.mappingPipeline.prepareExecution();
    } catch (HopException e) {
      throw new HopException(
          BaseMessages.getString(PKG, "MultiMapping.Exception.UnableToPrepareExecutionOfMapping"),
          e);
    }

    getPipeline().addActiveSubPipeline(getTransformName(), data.mappingPipeline);
  }

  @Override
  public boolean init() {
    if (super.init()) {
      try {
        data.mappingPipelineMeta =
            TransformWithMappingMeta.loadMappingMeta(meta, getMetadataProvider(), this);
        if (data.mappingPipelineMeta != null) {
          prepareMappingExecution();
          return true;
        } else {
          logError("No valid mapping was specified!");
          return false;
        }
      } catch (Exception e) {
        logError(
            "Unable to load the mapping pipeline '"
                + resolve(meta.getFilename())
                + "' (PROJECT_HOME="
                + getVariable("PROJECT_HOME")
                + "): "
                + e);
        logError(Const.getStackTracker(e));
        setErrors(1);
      }
    }
    return false;
  }

  @Override
  public void dispose() {
    try {
      if (data.mappingPipeline != null) {
        if (data.wasStarted && !data.mappingPipeline.isFinished()) {
          data.mappingPipeline.waitUntilFinished();
        }
        if (data.mappingPipeline.getErrors() > 0) {
          logError(BaseMessages.getString(PKG, "MultiMapping.Log.ErrorOccurredInSubPipeline"));
          setErrors(1);
        }
      }
    } finally {
      super.dispose();
    }
  }

  @Override
  public void stopRunning() {
    if (data.mappingPipeline != null) {
      data.mappingPipeline.stopAll();
    }
  }

  @Override
  public void stopAll() {
    if (data.mappingPipeline != null) {
      data.mappingPipeline.stopAll();
    }
    super.stopAll();
  }

  @Override
  public void addRowListener(IRowListener rowListener) {
    super.addRowListener(rowListener);
    if (data.mappingPipeline == null) {
      return;
    }
    for (MappingOutput mappingOutput : MappingTransforms.findMappingOutputs(data.mappingPipeline)) {
      mappingOutput.addRowListener(rowListener);
    }
  }

  public Pipeline getMappingPipeline() {
    return data.mappingPipeline;
  }

  ITransform getMappingInputTransform() {
    List<MappingInput> inputs = MappingTransforms.findMappingInputs(data.mappingPipeline);
    return inputs.isEmpty() ? null : inputs.get(0);
  }
}
