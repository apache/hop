/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hop.beam.transforms.io;

import org.apache.beam.sdk.values.PCollection;
import org.apache.commons.lang.StringUtils;
import org.apache.hop.beam.core.HopRow;
import org.apache.hop.beam.core.transform.BeamInputTransform;
import org.apache.hop.beam.core.util.JsonRowMeta;
import org.apache.hop.beam.engines.IBeamPipelineEngineRunConfiguration;
import org.apache.hop.beam.metadata.FileDefinition;
import org.apache.hop.beam.pipeline.IBeamPipelineTransformHandler;
import org.apache.hop.core.annotations.Transform;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.exception.HopPluginException;
import org.apache.hop.core.exception.HopTransformException;
import org.apache.hop.core.logging.ILogChannel;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.metadata.api.HopMetadataProperty;
import org.apache.hop.metadata.api.IHopMetadataProvider;
import org.apache.hop.metadata.api.IHopMetadataSerializer;
import org.apache.hop.pipeline.Pipeline;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.BaseTransformMeta;
import org.apache.hop.pipeline.transform.ITransformMeta;
import org.apache.hop.pipeline.transform.TransformMeta;

import java.util.List;
import java.util.Map;

@Transform(
    id = "BeamInput",
    name = "Beam Input",
    description = "Describes a Beam Input",
    image = "beam-input.svg",
    categoryDescription = "i18n:org.apache.hop.pipeline.transform:BaseTransform.Category.BigData",
    documentationUrl = "https://hop.apache.org/manual/latest/pipeline/transforms/beaminput.html")
public class BeamInputMeta extends BaseTransformMeta
    implements ITransformMeta<BeamInput, BeamInputData>, IBeamPipelineTransformHandler {

  @HopMetadataProperty(key = "input_location")
  private String inputLocation;

  @HopMetadataProperty(key = "file_description_name")
  private String fileDefinitionName;

  public BeamInputMeta() {}

  @Override
  public BeamInput createTransform(
      TransformMeta transformMeta,
      BeamInputData data,
      int copyNr,
      PipelineMeta pipelineMeta,
      Pipeline pipeline) {
    return new BeamInput(transformMeta, this, data, copyNr, pipelineMeta, pipeline);
  }

  @Override
  public BeamInputData getTransformData() {
    return new BeamInputData();
  }

  @Override
  public String getDialogClassName() {
    return BeamInputDialog.class.getName();
  }

  @Override
  public void getFields(
      IRowMeta inputRowMeta,
      String name,
      IRowMeta[] info,
      TransformMeta nextTransform,
      IVariables variables,
      IHopMetadataProvider metadataProvider)
      throws HopTransformException {

    if (metadataProvider != null) {
      FileDefinition fileDefinition = loadFileDefinition(metadataProvider);

      try {
        inputRowMeta.clear();
        inputRowMeta.addRowMeta(fileDefinition.getRowMeta());
      } catch (HopPluginException e) {
        throw new HopTransformException(
            "Unable to get row layout of file definition '" + fileDefinition.getName() + "'", e);
      }
    }
  }

  public FileDefinition loadFileDefinition(IHopMetadataProvider metadataProvider)
      throws HopTransformException {
    if (StringUtils.isEmpty(fileDefinitionName)) {
      throw new HopTransformException("No file description name provided");
    }
    FileDefinition fileDefinition;
    try {
      IHopMetadataSerializer<FileDefinition> serializer =
          metadataProvider.getSerializer(FileDefinition.class);
      fileDefinition = serializer.load(fileDefinitionName);
    } catch (Exception e) {
      throw new HopTransformException(
          "Unable to load file description '" + fileDefinitionName + "' from the metadata", e);
    }
    if (fileDefinition == null) {
      throw new HopTransformException(
          "Unable to find file definition '" + fileDefinitionName + "' in the metadata");
    }

    return fileDefinition;
  }

  @Override
  public boolean isInput() {
    return true;
  }

  @Override
  public boolean isOutput() {
    return false;
  }

  @Override
  public void handleTransform(
      ILogChannel log,
      IVariables variables,
      IBeamPipelineEngineRunConfiguration runConfiguration,
      IHopMetadataProvider metadataProvider,
      PipelineMeta pipelineMeta,
      List<String> transformPluginClasses,
      List<String> xpPluginClasses,
      TransformMeta transformMeta,
      Map<String, PCollection<HopRow>> transformCollectionMap,
      org.apache.beam.sdk.Pipeline pipeline,
      IRowMeta rowMeta,
      List<TransformMeta> previousTransforms,
      PCollection<HopRow> input)
      throws HopException {
    // Input handling
    //
    FileDefinition inputFileDefinition = loadFileDefinition(metadataProvider);
    IRowMeta fileRowMeta = inputFileDefinition.getRowMeta();

    // Apply the PBegin to HopRow transform:
    //
    String fileInputLocation = variables.resolve(inputLocation);

    BeamInputTransform beamInputTransform =
        new BeamInputTransform(
            transformMeta.getName(),
            transformMeta.getName(),
            fileInputLocation,
            variables.resolve(inputFileDefinition.getSeparator()),
            JsonRowMeta.toJson(fileRowMeta),
            transformPluginClasses,
            xpPluginClasses);
    PCollection<HopRow> afterInput = pipeline.apply(beamInputTransform);
    transformCollectionMap.put(transformMeta.getName(), afterInput);
    log.logBasic("Handled transform (INPUT) : " + transformMeta.getName());
  }

  /**
   * Gets inputLocation
   *
   * @return value of inputLocation
   */
  public String getInputLocation() {
    return inputLocation;
  }

  /** @param inputLocation The inputLocation to set */
  public void setInputLocation(String inputLocation) {
    this.inputLocation = inputLocation;
  }

  /**
   * Gets fileDescriptionName
   *
   * @return value of fileDescriptionName
   */
  public String getFileDefinitionName() {
    return fileDefinitionName;
  }

  /** @param fileDefinitionName The fileDescriptionName to set */
  public void setFileDefinitionName(String fileDefinitionName) {
    this.fileDefinitionName = fileDefinitionName;
  }
}
