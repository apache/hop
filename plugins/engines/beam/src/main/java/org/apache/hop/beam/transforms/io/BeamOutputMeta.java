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
import org.apache.hop.beam.core.transform.BeamOutputTransform;
import org.apache.hop.beam.core.util.JsonRowMeta;
import org.apache.hop.beam.engines.IBeamPipelineEngineRunConfiguration;
import org.apache.hop.beam.metadata.FieldDefinition;
import org.apache.hop.beam.metadata.FileDefinition;
import org.apache.hop.beam.pipeline.IBeamPipelineTransformHandler;
import org.apache.hop.core.annotations.Transform;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.exception.HopTransformException;
import org.apache.hop.core.logging.ILogChannel;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.IValueMeta;
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
    id = "BeamOutput",
    image = "beam-output.svg",
    name = "Beam Output",
    description = "Describes a Beam Output",
    categoryDescription = "i18n:org.apache.hop.pipeline.transform:BaseTransform.Category.BigData",
    documentationUrl = "https://hop.apache.org/manual/latest/pipeline/transforms/beamoutput.html")
public class BeamOutputMeta extends BaseTransformMeta
    implements ITransformMeta<BeamOutput, BeamOutputData>, IBeamPipelineTransformHandler {

  @HopMetadataProperty(key = "output_location")
  private String outputLocation;

  @HopMetadataProperty(key = "file_description_name")
  private String fileDefinitionName;

  @HopMetadataProperty(key = "file_prefix")
  private String filePrefix;

  @HopMetadataProperty(key = "file_suffix")
  private String fileSuffix;

  @HopMetadataProperty private boolean windowed;

  @Override
  public BeamOutput createTransform(
      TransformMeta transformMeta,
      BeamOutputData data,
      int copyNr,
      PipelineMeta pipelineMeta,
      Pipeline pipeline) {
    return new BeamOutput(transformMeta, this, data, copyNr, pipelineMeta, pipeline);
  }

  @Override
  public BeamOutputData getTransformData() {
    return new BeamOutputData();
  }

  @Override
  public String getDialogClassName() {
    return BeamOutputDialog.class.getName();
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

    return fileDefinition;
  }

  @Override
  public boolean isInput() {
    return false;
  }

  @Override
  public boolean isOutput() {
    return true;
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

    FileDefinition outputFileDefinition;
    if (StringUtils.isEmpty(fileDefinitionName)) {
      // Create a default file definition using standard output and sane defaults...
      //
      outputFileDefinition = getDefaultFileDefinition();
    } else {
      outputFileDefinition = loadFileDefinition(metadataProvider);
    }

    // Empty file definition? Add all fields in the output
    //
    addAllFieldsToEmptyFileDefinition(rowMeta, outputFileDefinition);

    // Apply the output transform from HopRow to PDone
    //
    if (rowMeta == null || rowMeta.isEmpty()) {
      throw new HopException(
          "No output fields found in the file definition or from previous transforms");
    }

    BeamOutputTransform beamOutputTransform =
        new BeamOutputTransform(
            transformMeta.getName(),
            variables.resolve(outputLocation),
            variables.resolve(filePrefix),
            variables.resolve(fileSuffix),
            variables.resolve(outputFileDefinition.getSeparator()),
            variables.resolve(outputFileDefinition.getEnclosure()),
            windowed,
            JsonRowMeta.toJson(rowMeta),
            transformPluginClasses,
            xpPluginClasses);

    // Which transform do we apply this transform to?
    // Ignore info hops until we figure that out.
    //
    if (previousTransforms.size() > 1) {
      throw new HopException("Combining data from multiple transforms is not supported yet!");
    }
    TransformMeta previousTransform = previousTransforms.get(0);

    // No need to store this, it's PDone.
    //
    input.apply(beamOutputTransform);
    log.logBasic(
        "Handled transform (OUTPUT) : "
            + transformMeta.getName()
            + ", gets data from "
            + previousTransform.getName());
  }

  private FileDefinition getDefaultFileDefinition() {
    FileDefinition fileDefinition = new FileDefinition();

    fileDefinition.setName("Default");
    fileDefinition.setEnclosure("\"");
    fileDefinition.setSeparator(",");

    return fileDefinition;
  }

  private void addAllFieldsToEmptyFileDefinition(IRowMeta rowMeta, FileDefinition fileDefinition) {
    if (fileDefinition.getFieldDefinitions().isEmpty()) {
      for (IValueMeta valueMeta : rowMeta.getValueMetaList()) {
        fileDefinition
            .getFieldDefinitions()
            .add(
                new FieldDefinition(
                    valueMeta.getName(),
                    valueMeta.getTypeDesc(),
                    valueMeta.getLength(),
                    valueMeta.getPrecision(),
                    valueMeta.getConversionMask()));
      }
    }
  }

  /**
   * Gets outputLocation
   *
   * @return value of outputLocation
   */
  public String getOutputLocation() {
    return outputLocation;
  }

  /** @param outputLocation The outputLocation to set */
  public void setOutputLocation(String outputLocation) {
    this.outputLocation = outputLocation;
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

  /**
   * Gets filePrefix
   *
   * @return value of filePrefix
   */
  public String getFilePrefix() {
    return filePrefix;
  }

  /** @param filePrefix The filePrefix to set */
  public void setFilePrefix(String filePrefix) {
    this.filePrefix = filePrefix;
  }

  /**
   * Gets fileSuffix
   *
   * @return value of fileSuffix
   */
  public String getFileSuffix() {
    return fileSuffix;
  }

  /** @param fileSuffix The fileSuffix to set */
  public void setFileSuffix(String fileSuffix) {
    this.fileSuffix = fileSuffix;
  }

  /**
   * Gets windowed
   *
   * @return value of windowed
   */
  public boolean isWindowed() {
    return windowed;
  }

  /** @param windowed The windowed to set */
  public void setWindowed(boolean windowed) {
    this.windowed = windowed;
  }
}
