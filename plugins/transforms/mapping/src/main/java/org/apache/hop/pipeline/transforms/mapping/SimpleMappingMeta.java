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

import java.util.ArrayList;
import java.util.List;
import org.apache.commons.lang.StringUtils;
import org.apache.hop.core.CheckResult;
import org.apache.hop.core.ICheckResult;
import org.apache.hop.core.annotations.ActionTransformType;
import org.apache.hop.core.annotations.Transform;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.exception.HopTransformException;
import org.apache.hop.core.file.IHasFilename;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.metadata.api.HopMetadataProperty;
import org.apache.hop.metadata.api.IHopMetadataProvider;
import org.apache.hop.pipeline.ISubPipelineAwareMeta;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.PipelineMeta.PipelineType;
import org.apache.hop.pipeline.TransformWithMappingMeta;
import org.apache.hop.pipeline.transform.ITransformIOMeta;
import org.apache.hop.pipeline.transform.TransformIOMeta;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.apache.hop.pipeline.transforms.input.MappingInputMeta;
import org.apache.hop.resource.ResourceEntry;
import org.apache.hop.resource.ResourceEntry.ResourceType;
import org.apache.hop.resource.ResourceReference;

/** Meta-data for the Mapping transform: contains name of the (sub-) pipeline to execute */
@Transform(
    id = "SimpleMapping",
    name = "i18n::BaseTransform.TypeLongDesc.SimpleMapping",
    description = "i18n::BaseTransform.TypeTooltipDesc.SimpleMapping",
    image = "MAP.svg",
    categoryDescription = "i18n:org.apache.hop.pipeline.transform:BaseTransform.Category.Mapping",
    keywords = "i18n::SimpleMappingMeta.keyword",
    documentationUrl = "/pipeline/transforms/simple-mapping.html",
    actionTransformTypes = {ActionTransformType.HOP_FILE, ActionTransformType.HOP_PIPELINE})
public class SimpleMappingMeta extends TransformWithMappingMeta<SimpleMapping, SimpleMappingData>
    implements ISubPipelineAwareMeta {

  private static final Class<?> PKG = SimpleMappingMeta.class;

  @HopMetadataProperty(key = "runConfiguration")
  private String runConfigurationName;

  @HopMetadataProperty(key = "mappings")
  private IOMappings ioMappings;

  public SimpleMappingMeta() {
    super();
    ioMappings = new IOMappings();
  }

  public SimpleMappingMeta(SimpleMappingMeta m) {
    super(m);
    this.runConfigurationName = m.runConfigurationName;
    this.ioMappings = new IOMappings(m.ioMappings);
  }

  @Override
  public SimpleMappingMeta clone() {
    return new SimpleMappingMeta(this);
  }

  @Override
  public void setDefault() {
    MappingIODefinition inputDefinition = new MappingIODefinition(null, null);
    inputDefinition.setMainDataPath(true);
    inputDefinition.setRenamingOnOutput(true);
    ioMappings.setInputMapping(inputDefinition);

    MappingIODefinition outputDefinition = new MappingIODefinition(null, null);
    outputDefinition.setMainDataPath(true);
    ioMappings.setOutputMapping(outputDefinition);
  }

  @Override
  public void getFields(
      IRowMeta row,
      String origin,
      IRowMeta[] info,
      TransformMeta nextTransform,
      IVariables variables,
      IHopMetadataProvider metadataProvider)
      throws HopTransformException {
    // First load some interesting data...

    // Then see which fields get added to the row.
    //
    PipelineMeta mappingPipelineMeta;
    try {
      mappingPipelineMeta = loadMappingMeta(this, metadataProvider, variables);
    } catch (HopException e) {
      throw new HopTransformException(
          BaseMessages.getString(PKG, "SimpleMappingMeta.Exception.UnableToLoadMappingPipeline"),
          e);
    }

    // Before we ask the mapping outputs anything, we should teach the mapping
    // input transforms in the sub- pipeline about the data coming in...
    //
    IRowMeta inputRowMeta;

    // The row metadata, what we pass to the mapping input transform
    // definition.getOutputTransform(), is "row"
    // However, we do need to re-map some fields...
    //
    inputRowMeta = row.clone();
    if (!inputRowMeta.isEmpty()) {
      for (MappingValueRename valueRename : ioMappings.getInputMapping().getValueRenames()) {
        IValueMeta valueMeta = inputRowMeta.searchValueMeta(valueRename.getSourceValueName());
        if (valueMeta == null) {
          throw new HopTransformException(
              BaseMessages.getString(
                  PKG,
                  "SimpleMappingMeta.Exception.UnableToFindField",
                  valueRename.getSourceValueName()));
        }
        valueMeta.setName(valueRename.getTargetValueName());
      }
    }

    // What is this mapping input transform?
    //
    TransformMeta mappingInputTransform = mappingPipelineMeta.findMappingInputTransform(null);
    TransformMeta mappingOutputTransform = mappingPipelineMeta.findMappingOutputTransform(null);

    // We're certain of these classes at least
    //
    MappingInputMeta mappingInputMeta = (MappingInputMeta) mappingInputTransform.getTransform();

    // Inform the mapping input transform about what it's going to receive...
    //
    mappingInputMeta.setInputRowMeta(inputRowMeta);

    // Now we know wat's going to come out of the mapping pipeline...
    // This is going to be the full row that's being written.
    //
    IRowMeta mappingOutputRowMeta =
        mappingPipelineMeta.getTransformFields(variables, mappingOutputTransform);

    // We're renaming some stuff back:
    //
    if (ioMappings.getInputMapping().isRenamingOnOutput()) {
      for (MappingValueRename rename : ioMappings.getInputMapping().getValueRenames()) {
        IValueMeta valueMeta = mappingOutputRowMeta.searchValueMeta(rename.getTargetValueName());
        if (valueMeta != null) {
          valueMeta.setName(rename.getSourceValueName());
        }
      }
    }

    // Also rename output values back
    //
    for (MappingValueRename rename : ioMappings.getOutputMapping().getValueRenames()) {
      IValueMeta valueMeta = mappingOutputRowMeta.searchValueMeta(rename.getSourceValueName());
      if (valueMeta != null) {
        valueMeta.setName(rename.getTargetValueName());
      }
    }

    row.clear();
    row.addRowMeta(mappingOutputRowMeta);
  }

  public String[] getInfoTransforms() {
    return null;
  }

  public String[] getTargetTransforms() {
    return null;
  }

  @Override
  public void check(
      List<ICheckResult> remarks,
      PipelineMeta pipelineMeta,
      TransformMeta transformMeta,
      IRowMeta prev,
      String[] input,
      String[] output,
      IRowMeta info,
      IVariables variables,
      IHopMetadataProvider metadataProvider) {
    CheckResult cr;
    if (prev == null || prev.size() == 0) {
      cr =
          new CheckResult(
              ICheckResult.TYPE_RESULT_WARNING,
              BaseMessages.getString(PKG, "SimpleMappingMeta.CheckResult.NotReceivingAnyFields"),
              transformMeta);
      remarks.add(cr);
    } else {
      cr =
          new CheckResult(
              ICheckResult.TYPE_RESULT_OK,
              BaseMessages.getString(
                  PKG, "SimpleMappingMeta.CheckResult.TransformReceivingFields", prev.size() + ""),
              transformMeta);
      remarks.add(cr);
    }

    // See if we have input streams leading to this transform!
    if (input.length > 0) {
      cr =
          new CheckResult(
              ICheckResult.TYPE_RESULT_OK,
              BaseMessages.getString(
                  PKG, "SimpleMappingMeta.CheckResult.TransformReceivingFieldsFromOtherTransforms"),
              transformMeta);
      remarks.add(cr);
    } else {
      cr =
          new CheckResult(
              ICheckResult.TYPE_RESULT_ERROR,
              BaseMessages.getString(PKG, "SimpleMappingMeta.CheckResult.NoInputReceived"),
              transformMeta);
      remarks.add(cr);
    }
  }

  /**
   * @return the mappingParameters
   */
  public MappingParameters getMappingParameters() {
    return ioMappings.getMappingParameters();
  }

  /**
   * @param mappingParameters the mappingParameters to set
   */
  public void setMappingParameters(MappingParameters mappingParameters) {
    ioMappings.setMappingParameters(mappingParameters);
  }

  @Override
  public List<ResourceReference> getResourceDependencies(
      IVariables variables, TransformMeta transformMeta) {
    List<ResourceReference> references = new ArrayList<>(5);
    String realFilename = variables.resolve(filename);
    ResourceReference reference = new ResourceReference(transformMeta);
    references.add(reference);

    if (StringUtils.isNotEmpty(realFilename)) {
      // Add the filename to the references, including a reference to this transform
      // meta data.
      //
      reference.getEntries().add(new ResourceEntry(realFilename, ResourceType.ACTIONFILE));
    }
    return references;
  }

  @Override
  public ITransformIOMeta getTransformIOMeta() {
    ITransformIOMeta ioMeta = super.getTransformIOMeta(false);
    if (ioMeta == null) {
      ioMeta = new TransformIOMeta(true, true, false, false, false, false);
      setTransformIOMeta(ioMeta);
    }
    return ioMeta;
  }

  @Override
  public boolean excludeFromRowLayoutVerification() {
    return false;
  }

  @Override
  public void searchInfoAndTargetTransforms(List<TransformMeta> transforms) {
    // Do nothing
  }

  @Override
  public PipelineType[] getSupportedPipelineTypes() {
    return new PipelineType[] {
      PipelineType.Normal,
    };
  }

  /**
   * @return The objects referenced in the transform, like a mapping, a pipeline, a workflow, ...
   */
  @Override
  public String[] getReferencedObjectDescriptions() {
    return new String[] {
      BaseMessages.getString(PKG, "SimpleMappingMeta.ReferencedObject.Description"),
    };
  }

  private boolean isMapppingDefined() {
    return StringUtils.isNotEmpty(filename);
  }

  @Override
  public boolean[] isReferencedObjectEnabled() {
    return new boolean[] {
      isMapppingDefined(),
    };
  }

  /**
   * Load the referenced object
   *
   * @param index the object index to load
   * @param metadataProvider the MetaStore to use
   * @param variables the variable variables to use
   * @return the referenced object once loaded
   * @throws HopException
   */
  @Override
  public IHasFilename loadReferencedObject(
      int index, IHopMetadataProvider metadataProvider, IVariables variables) throws HopException {
    return loadMappingMeta(this, metadataProvider, variables);
  }

  public MappingIODefinition getInputMapping() {
    return ioMappings.getInputMapping();
  }

  public void setInputMapping(MappingIODefinition inputMapping) {
    ioMappings.setInputMapping(inputMapping);
  }

  public MappingIODefinition getOutputMapping() {
    return ioMappings.getOutputMapping();
  }

  public void setOutputMapping(MappingIODefinition outputMapping) {
    ioMappings.setOutputMapping(outputMapping);
  }

  /**
   * Gets runConfigurationName
   *
   * @return value of runConfigurationName
   */
  public String getRunConfigurationName() {
    return runConfigurationName;
  }

  /**
   * Sets runConfigurationName
   *
   * @param runConfigurationName value of runConfigurationName
   */
  public void setRunConfigurationName(String runConfigurationName) {
    this.runConfigurationName = runConfigurationName;
  }

  /**
   * Gets ioMappings
   *
   * @return value of ioMappings
   */
  public IOMappings getIoMappings() {
    return ioMappings;
  }

  /**
   * Sets ioMappings
   *
   * @param ioMappings value of ioMappings
   */
  public void setIoMappings(IOMappings ioMappings) {
    this.ioMappings = ioMappings;
  }
}
