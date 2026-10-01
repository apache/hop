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
import java.util.List;
import lombok.Getter;
import lombok.Setter;
import org.apache.commons.lang3.StringUtils;
import org.apache.hop.core.CheckResult;
import org.apache.hop.core.Const;
import org.apache.hop.core.ICheckResult;
import org.apache.hop.core.annotations.ActionTransformType;
import org.apache.hop.core.annotations.Transform;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.exception.HopTransformException;
import org.apache.hop.core.file.IHasFilename;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.core.util.Utils;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.metadata.api.HopMetadataProperty;
import org.apache.hop.metadata.api.HopMetadataPropertyType;
import org.apache.hop.metadata.api.IHopMetadataProvider;
import org.apache.hop.pipeline.ISubPipelineAwareMeta;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.PipelineMeta.PipelineType;
import org.apache.hop.pipeline.TransformWithMappingMeta;
import org.apache.hop.pipeline.transform.ITransformIOMeta;
import org.apache.hop.pipeline.transform.TransformIOMeta;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.apache.hop.pipeline.transform.stream.IStream;
import org.apache.hop.pipeline.transform.stream.IStream.StreamType;
import org.apache.hop.pipeline.transform.stream.Stream;
import org.apache.hop.pipeline.transform.stream.StreamIcon;
import org.apache.hop.pipeline.transforms.input.MappingInputMeta;
import org.apache.hop.pipeline.transforms.mapping.MappingIODefinition;
import org.apache.hop.pipeline.transforms.mapping.MappingParameters;
import org.apache.hop.pipeline.transforms.mapping.MappingTransforms;
import org.apache.hop.pipeline.transforms.mapping.MappingValueRename;
import org.apache.hop.pipeline.transforms.mapping.SimpleMappingMeta;
import org.apache.hop.resource.ResourceEntry;
import org.apache.hop.resource.ResourceEntry.ResourceType;
import org.apache.hop.resource.ResourceReference;

/** Meta-data for the Multi Mapping transform: 0..N inputs and outputs to a child pipeline. */
@Transform(
    id = "MultiMapping",
    name = "i18n::MultiMapping.Name",
    description = "i18n::MultiMapping.Description",
    image = "MMAP.svg",
    categoryDescription = "i18n:org.apache.hop.pipeline.transform:BaseTransform.Category.Mapping",
    keywords = "i18n::MultiMappingMeta.keyword",
    documentationUrl = "/pipeline/transforms/multi-mapping.html",
    actionTransformTypes = {ActionTransformType.HOP_FILE, ActionTransformType.HOP_PIPELINE})
@Getter
@Setter
public class MultiMappingMeta extends TransformWithMappingMeta<MultiMapping, MultiMappingData>
    implements ISubPipelineAwareMeta {

  private static final Class<?> PKG = MultiMappingMeta.class;

  private static final IStream NEW_INFO_STREAM =
      new Stream(
          StreamType.INFO,
          null,
          BaseMessages.getString(PKG, "MultiMappingMeta.InfoStream.New.Description"),
          StreamIcon.INFO,
          null);

  private static final IStream NEW_TARGET_STREAM =
      new Stream(
          StreamType.TARGET,
          null,
          BaseMessages.getString(PKG, "MultiMappingMeta.TargetStream.New.Description"),
          StreamIcon.TARGET,
          null);

  @HopMetadataProperty(
      key = "runConfiguration",
      hopMetadataPropertyType = HopMetadataPropertyType.PIPELINE_RUN_CONFIG,
      injectionKey = "RUN_CONFIGURATION",
      injectionKeyDescription = "MultiMappingMeta.Injection.RUN_CONFIGURATION")
  private String runConfigurationName;

  @HopMetadataProperty(key = "mappings")
  private MultiIOMappings ioMappings;

  public MultiMappingMeta() {
    super();
    ioMappings = new MultiIOMappings();
  }

  @Override
  public void setDefault() {
    MultiMappingInputDefinition inputDefinition = new MultiMappingInputDefinition(null, null);
    inputDefinition.setMainDataPath(true);
    inputDefinition.setRenamingOnOutput(true);
    ioMappings.getInputMappings().add(inputDefinition);

    MultiMappingOutputDefinition outputDefinition = new MultiMappingOutputDefinition(null, null);
    outputDefinition.setMainDataPath(true);
    ioMappings.getOutputMappings().add(outputDefinition);
  }

  public MappingParameters getMappingParameters() {
    return ioMappings.getMappingParameters();
  }

  public void setMappingParameters(MappingParameters mappingParameters) {
    ioMappings.setMappingParameters(mappingParameters);
  }

  public List<MultiMappingInputDefinition> getInputMappings() {
    return ioMappings.getInputMappings();
  }

  public List<MultiMappingOutputDefinition> getOutputMappings() {
    return ioMappings.getOutputMappings();
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
    PipelineMeta mappingPipelineMeta;
    try {
      mappingPipelineMeta = loadMappingMeta(this, metadataProvider, variables);
    } catch (HopException e) {
      throw new HopTransformException(
          BaseMessages.getString(PKG, "MultiMappingMeta.Exception.UnableToLoadMappingPipeline"), e);
    }

    List<MappingValueRename> inputRenameList = new ArrayList<>();
    String[] infoTransforms = getInfoTransforms();

    for (MultiMappingInputDefinition inputDefinition : ioMappings.getInputMappings()) {
      MappingIODefinition definition = inputDefinition.toIoDefinition();
      IRowMeta inputRowMeta = resolveInputRowMeta(row, info, infoTransforms, definition);

      TransformMeta mappingInputTransform =
          SimpleMappingMeta.findMappingInputTransform(
              mappingPipelineMeta, definition.getOutputTransformName());
      MappingInputMeta mappingInputMeta = (MappingInputMeta) mappingInputTransform.getTransform();
      mappingInputMeta.setInputRowMeta(inputRowMeta);

      if (definition.isRenamingOnOutput()) {
        addInputRenames(inputRenameList, definition.getValueRenames());
      }
    }

    MultiMappingOutputDefinition outputDefinition = findOutputDefinition(nextTransform);
    if (outputDefinition == null) {
      // No output mappings: this transform produces no fields.
      row.clear();
      return;
    }

    TransformMeta mappingOutputTransform =
        SimpleMappingMeta.findMappingOutputTransform(
            mappingPipelineMeta, outputDefinition.getInputTransformName());
    IRowMeta mappingOutputRowMeta =
        mappingPipelineMeta.getTransformFields(variables, mappingOutputTransform);

    if (!inputRenameList.isEmpty()) {
      for (MappingValueRename rename : inputRenameList) {
        IValueMeta valueMeta = mappingOutputRowMeta.searchValueMeta(rename.getTargetValueName());
        if (valueMeta != null) {
          valueMeta.setName(rename.getSourceValueName());
        }
      }
    }
    for (MappingValueRename rename : outputDefinition.toIoDefinition().getValueRenames()) {
      IValueMeta valueMeta = mappingOutputRowMeta.searchValueMeta(rename.getSourceValueName());
      if (valueMeta != null) {
        valueMeta.setName(rename.getTargetValueName());
      }
    }

    row.clear();
    row.addRowMeta(mappingOutputRowMeta);
  }

  private IRowMeta resolveInputRowMeta(
      IRowMeta row, IRowMeta[] info, String[] infoTransforms, MappingIODefinition definition)
      throws HopTransformException {
    if (definition.isMainDataPath() || Utils.isEmpty(definition.getInputTransformName())) {
      IRowMeta inputRowMeta = row.clone();
      applyRenames(inputRowMeta, definition.getValueRenames());
      return inputRowMeta;
    }

    int infoIndex =
        infoTransforms == null
            ? -1
            : Const.indexOfString(definition.getInputTransformName(), infoTransforms);
    if (infoIndex < 0) {
      throw new HopTransformException(
          BaseMessages.getString(
              PKG,
              "MultiMappingMeta.Exception.UnableToFindMetadataInfo",
              definition.getInputTransformName()));
    }
    if (info != null && infoIndex < info.length && info[infoIndex] != null) {
      IRowMeta inputRowMeta = info[infoIndex].clone();
      applyRenames(inputRowMeta, definition.getValueRenames());
      return inputRowMeta;
    }
    return null;
  }

  private static void applyRenames(IRowMeta inputRowMeta, List<MappingValueRename> valueRenames)
      throws HopTransformException {
    if (inputRowMeta == null || inputRowMeta.isEmpty() || valueRenames == null) {
      return;
    }
    for (MappingValueRename valueRename : valueRenames) {
      IValueMeta valueMeta = inputRowMeta.searchValueMeta(valueRename.getSourceValueName());
      if (valueMeta == null) {
        throw new HopTransformException(
            BaseMessages.getString(
                PKG,
                "MultiMappingMeta.Exception.UnableToFindField",
                valueRename.getSourceValueName()));
      }
      valueMeta.setName(valueRename.getTargetValueName());
    }
  }

  MultiMappingOutputDefinition findOutputDefinition(TransformMeta nextTransform) {
    if (nextTransform != null) {
      for (MultiMappingOutputDefinition definition : ioMappings.getOutputMappings()) {
        if (nextTransform.getName().equals(definition.getOutputTransformName())) {
          return definition;
        }
      }
    }
    MultiMappingOutputDefinition main = null;
    for (MultiMappingOutputDefinition definition : ioMappings.getOutputMappings()) {
      if (definition.isMainDataPath() || Utils.isEmpty(definition.getOutputTransformName())) {
        main = definition;
      }
    }
    return main;
  }

  static void addInputRenames(
      List<MappingValueRename> renameList, List<MappingValueRename> addRenameList) {
    if (addRenameList == null) {
      return;
    }
    for (MappingValueRename rename : addRenameList) {
      if (renameList.indexOf(rename) < 0) {
        renameList.add(rename);
      }
    }
  }

  public String[] getInfoTransforms() {
    List<String> names = new ArrayList<>();
    for (MultiMappingInputDefinition definition : ioMappings.getInputMappings()) {
      if (MappingTransforms.isInfoMapping(definition.toIoDefinition())) {
        names.add(definition.getInputTransformName());
      }
    }
    return names.isEmpty() ? null : names.toArray(String[]::new);
  }

  public String[] getTargetTransforms() {
    List<String> names = new ArrayList<>();
    for (MultiMappingOutputDefinition definition : ioMappings.getOutputMappings()) {
      if (MappingTransforms.isTargetMapping(definition.toIoDefinition())) {
        names.add(definition.getOutputTransformName());
      }
    }
    return names.isEmpty() ? null : names.toArray(String[]::new);
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
    if (StringUtils.isEmpty(filename)) {
      remarks.add(
          new CheckResult(
              ICheckResult.TYPE_RESULT_ERROR,
              BaseMessages.getString(PKG, "MultiMappingMeta.CheckResult.NoMappingSpecified"),
              transformMeta));
      return;
    }

    remarks.add(
        new CheckResult(
            ICheckResult.TYPE_RESULT_OK,
            BaseMessages.getString(PKG, "MultiMappingMeta.CheckResult.MappingPipelineSpecified"),
            transformMeta));

    if (prev == null || prev.isEmpty()) {
      remarks.add(
          new CheckResult(
              ICheckResult.TYPE_RESULT_WARNING,
              BaseMessages.getString(PKG, "MultiMappingMeta.CheckResult.NotReceivingAnyFields"),
              transformMeta));
    } else {
      remarks.add(
          new CheckResult(
              ICheckResult.TYPE_RESULT_OK,
              BaseMessages.getString(
                  PKG, "MultiMappingMeta.CheckResult.TransformReceivingFields", prev.size() + ""),
              transformMeta));
    }

    if (input.length > 0) {
      remarks.add(
          new CheckResult(
              ICheckResult.TYPE_RESULT_OK,
              BaseMessages.getString(
                  PKG, "MultiMappingMeta.CheckResult.TransformReceivingFieldsFromOtherTransforms"),
              transformMeta));
    } else {
      remarks.add(
          new CheckResult(
              ICheckResult.TYPE_RESULT_WARNING,
              BaseMessages.getString(PKG, "MultiMappingMeta.CheckResult.NoInputReceived"),
              transformMeta));
    }

    try {
      PipelineMeta mappingPipelineMeta = loadMappingMeta(this, metadataProvider, variables);
      for (MultiMappingInputDefinition definition : ioMappings.getInputMappings()) {
        if (StringUtils.isNotEmpty(definition.getInputTransformName())
            && pipelineMeta.findTransform(definition.getInputTransformName()) == null) {
          remarks.add(
              new CheckResult(
                  ICheckResult.TYPE_RESULT_ERROR,
                  BaseMessages.getString(
                      PKG,
                      "MultiMappingMeta.CheckResult.ParentTransformNotFound",
                      definition.getInputTransformName()),
                  transformMeta));
        }
        if (MappingTransforms.isInfoMapping(definition.toIoDefinition())
            && Utils.isEmpty(definition.getInputTransformName())) {
          remarks.add(
              new CheckResult(
                  ICheckResult.TYPE_RESULT_WARNING,
                  BaseMessages.getString(
                      PKG, "MultiMappingMeta.CheckResult.InfoMappingMissingSource"),
                  transformMeta));
        }
        SimpleMappingMeta.findMappingInputTransform(
            mappingPipelineMeta, definition.getOutputTransformName());
      }
      for (MultiMappingOutputDefinition definition : ioMappings.getOutputMappings()) {
        if (StringUtils.isNotEmpty(definition.getOutputTransformName())
            && pipelineMeta.findTransform(definition.getOutputTransformName()) == null) {
          remarks.add(
              new CheckResult(
                  ICheckResult.TYPE_RESULT_ERROR,
                  BaseMessages.getString(
                      PKG,
                      "MultiMappingMeta.CheckResult.ParentTransformNotFound",
                      definition.getOutputTransformName()),
                  transformMeta));
        }
        SimpleMappingMeta.findMappingOutputTransform(
            mappingPipelineMeta, definition.getInputTransformName());
      }
    } catch (HopException e) {
      remarks.add(
          new CheckResult(
              ICheckResult.TYPE_RESULT_ERROR,
              BaseMessages.getString(
                      PKG, "MultiMappingMeta.CheckResult.UnableToLoadMappingPipeline")
                  + " : "
                  + e.getMessage(),
              transformMeta));
    }
  }

  @Override
  public List<ResourceReference> getResourceDependencies(
      IVariables variables, TransformMeta transformMeta) {
    List<ResourceReference> references = new ArrayList<>(5);
    String realFilename = variables.resolve(filename);
    ResourceReference reference = new ResourceReference(transformMeta);
    references.add(reference);
    if (StringUtils.isNotEmpty(realFilename)) {
      reference.getEntries().add(new ResourceEntry(realFilename, ResourceType.ACTIONFILE));
    }
    return references;
  }

  @Override
  public ITransformIOMeta getTransformIOMeta() {
    ITransformIOMeta ioMeta = super.getTransformIOMeta(false);
    if (ioMeta == null) {
      ioMeta = new TransformIOMeta(true, true, true, false, true, true);
      for (MultiMappingInputDefinition definition : ioMappings.getInputMappings()) {
        MappingIODefinition ioDefinition = definition.toIoDefinition();
        if (MappingTransforms.isInfoMapping(ioDefinition)) {
          ioMeta.addStream(
              new Stream(
                  StreamType.INFO,
                  definition.getInputTransform(),
                  BaseMessages.getString(PKG, "MultiMappingMeta.InfoStream.Description"),
                  StreamIcon.INFO,
                  definition.getInputTransformName()));
        }
      }
      for (MultiMappingOutputDefinition definition : ioMappings.getOutputMappings()) {
        MappingIODefinition ioDefinition = definition.toIoDefinition();
        if (MappingTransforms.isTargetMapping(ioDefinition)) {
          ioMeta.addStream(
              new Stream(
                  StreamType.TARGET,
                  null,
                  BaseMessages.getString(
                      PKG,
                      "MultiMappingMeta.TargetStream.Description",
                      Const.NVL(definition.getOutputTransformName(), "")),
                  StreamIcon.TARGET,
                  definition.getOutputTransformName()));
        }
      }
      setTransformIOMeta(ioMeta);
    }
    return ioMeta;
  }

  @Override
  public void searchInfoAndTargetTransforms(List<TransformMeta> transforms) {
    for (MultiMappingInputDefinition definition : ioMappings.getInputMappings()) {
      if (MappingTransforms.isInfoMapping(definition.toIoDefinition())) {
        definition.setInputTransform(
            TransformMeta.findTransform(transforms, definition.getInputTransformName()));
      }
    }
    List<IStream> infoStreams = getTransformIOMeta().getInfoStreams();
    int infoIndex = 0;
    for (MultiMappingInputDefinition definition : ioMappings.getInputMappings()) {
      if (MappingTransforms.isInfoMapping(definition.toIoDefinition())
          && infoIndex < infoStreams.size()) {
        infoStreams.get(infoIndex++).setTransformMeta(definition.getInputTransform());
      }
    }
    List<IStream> targetStreams = getTransformIOMeta().getTargetStreams();
    int targetIndex = 0;
    for (MultiMappingOutputDefinition definition : ioMappings.getOutputMappings()) {
      if (MappingTransforms.isTargetMapping(definition.toIoDefinition())
          && targetIndex < targetStreams.size()) {
        targetStreams
            .get(targetIndex++)
            .setTransformMeta(
                TransformMeta.findTransform(transforms, definition.getOutputTransformName()));
      }
    }
  }

  @Override
  public List<IStream> getOptionalStreams() {
    List<IStream> list = new ArrayList<>();
    list.add(NEW_INFO_STREAM);
    list.add(NEW_TARGET_STREAM);
    return list;
  }

  @Override
  public void handleStreamSelection(IStream stream) {
    if (stream == NEW_INFO_STREAM) {
      MultiMappingInputDefinition definition = new MultiMappingInputDefinition();
      definition.setMainDataPath(false);
      if (stream.getTransformMeta() != null) {
        definition.setInputTransformName(stream.getTransformMeta().getName());
        definition.setInputTransform(stream.getTransformMeta());
      }
      ioMappings.getInputMappings().add(definition);
      resetTransformIoMeta();
    } else if (stream == NEW_TARGET_STREAM) {
      MultiMappingOutputDefinition definition = new MultiMappingOutputDefinition();
      definition.setMainDataPath(false);
      if (stream.getTransformMeta() != null) {
        definition.setOutputTransformName(stream.getTransformMeta().getName());
      }
      ioMappings.getOutputMappings().add(definition);
      resetTransformIoMeta();
    }
  }

  @Override
  public boolean cleanAfterHopFromRemove(TransformMeta toTransform) {
    if (toTransform == null) {
      return false;
    }
    boolean changed = false;
    String toName = toTransform.getName();
    for (MultiMappingOutputDefinition definition : ioMappings.getOutputMappings()) {
      if (toName.equals(definition.getOutputTransformName())) {
        definition.setOutputTransformName(null);
        changed = true;
      }
    }
    if (changed) {
      resetTransformIoMeta();
    }
    return changed;
  }

  @Override
  public boolean cleanAfterHopToRemove(TransformMeta fromTransform) {
    if (fromTransform == null) {
      return false;
    }
    boolean changed = false;
    String fromName = fromTransform.getName();
    for (MultiMappingInputDefinition definition : ioMappings.getInputMappings()) {
      if (fromName.equals(definition.getInputTransformName())) {
        definition.setInputTransformName(null);
        definition.setInputTransform(null);
        changed = true;
      }
    }
    if (changed) {
      resetTransformIoMeta();
    }
    return changed;
  }

  @Override
  public boolean excludeFromRowLayoutVerification() {
    return true;
  }

  @Override
  public boolean excludeFromCopyDistributeVerification() {
    return true;
  }

  @Override
  public PipelineType[] getSupportedPipelineTypes() {
    return new PipelineType[] {PipelineType.Normal};
  }

  @Override
  public String[] getReferencedObjectDescriptions() {
    return new String[] {
      BaseMessages.getString(PKG, "MultiMappingMeta.ReferencedObject.Description"),
    };
  }

  private boolean isMappingDefined() {
    return StringUtils.isNotEmpty(filename);
  }

  @Override
  public boolean[] isReferencedObjectEnabled() {
    return new boolean[] {isMappingDefined()};
  }

  @Override
  public IHasFilename loadReferencedObject(
      int index, IHopMetadataProvider metadataProvider, IVariables variables) throws HopException {
    return loadMappingMeta(this, metadataProvider, variables);
  }

  @Override
  public boolean supportsDrillDown() {
    return true;
  }
}
