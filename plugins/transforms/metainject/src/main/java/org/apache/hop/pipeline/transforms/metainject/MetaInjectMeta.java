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

package org.apache.hop.pipeline.transforms.metainject;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import lombok.Getter;
import lombok.Setter;
import org.apache.hop.core.annotations.ActionTransformType;
import org.apache.hop.core.annotations.Transform;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.exception.HopPluginException;
import org.apache.hop.core.exception.HopTransformException;
import org.apache.hop.core.file.IHasFilename;
import org.apache.hop.core.logging.LogChannel;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.util.CurrentDirectoryResolver;
import org.apache.hop.core.util.Utils;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.metadata.api.HopMetadataProperty;
import org.apache.hop.metadata.api.IHopMetadataProvider;
import org.apache.hop.pipeline.ISubPipelineAwareMeta;
import org.apache.hop.pipeline.PipelineHopMeta;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.BaseTransformMeta;
import org.apache.hop.pipeline.transform.ITransformMetaChangeListener;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.apache.hop.resource.IResourceNaming;
import org.apache.hop.resource.ResourceDefinition;
import org.apache.hop.resource.ResourceEntry;
import org.apache.hop.resource.ResourceReference;

@Transform(
    id = "MetaInject",
    image = "GenericTransform.svg",
    name = "i18n::MetaInject.Name",
    description = "i18n::MetaInject.Description",
    categoryDescription = "i18n:org.apache.hop.pipeline.transform:BaseTransform.Category.Transform",
    keywords = "i18n::MetaInjectMeta.keyword",
    documentationUrl = "/pipeline/transforms/metainject.html",
    actionTransformTypes = {ActionTransformType.HOP_FILE, ActionTransformType.HOP_PIPELINE})
@Getter
@Setter
public class MetaInjectMeta extends BaseTransformMeta<MetaInject, MetaInjectData>
    implements ITransformMetaChangeListener, ISubPipelineAwareMeta {

  private static final Class<?> PKG = MetaInjectMeta.class;

  @HopMetadataProperty(
      key = "filename",
      injectionKey = "FILE_NAME",
      injectionKeyDescription = "MetaInject.Injection.FILE_NAME")
  private String templateFileName;

  @HopMetadataProperty(
      key = "source_transform",
      injectionKey = "SOURCE_TRANSFORM_NAME",
      injectionKeyDescription = "MetaInject.Injection.SOURCE_TRANSFORM_NAME")
  private String sourceTransformName;

  @HopMetadataProperty(
      key = "source_output_field",
      groupKey = "source_output_fields",
      injectionGroupKey = "SOURCE_OUTPUT_FIELDS",
      injectionGroupDescription = "MetaInject.Injection.SOURCE_OUTPUT_FIELDS")
  private List<MetaInjectOutputField> sourceOutputFields;

  @HopMetadataProperty(
      key = "mapping",
      groupKey = "mappings",
      injectionGroupKey = "MAPPING_FIELDS",
      injectionGroupDescription = "MetaInject.Injection.MAPPING_FIELDS")
  private List<MetaInjectMapping> mappings;

  @HopMetadataProperty(
      key = "target_file",
      injectionKey = "TARGET_FILE",
      injectionKeyDescription = "MetaInject.Injection.TARGET_FILE")
  private String targetFile;

  @HopMetadataProperty(
      key = "no_execution",
      injectionKey = "NO_EXECUTION",
      injectionKeyDescription = "MetaInject.Injection.NO_EXECUTION")
  private boolean noExecution;

  @HopMetadataProperty(
      key = "allow_empty_stream_on_execution",
      injectionKey = "ALLOW_EMPTY_STREAM_ON_EXECUTION",
      injectionKeyDescription = "MetaInject.Injection.ALLOW_EMPTY_STREAM_ON_EXECUTION")
  private boolean allowEmptyStreamOnExecution;

  @HopMetadataProperty(
      key = "stream_source_transform",
      injectionKey = "STREAMING_SOURCE_TRANSFORM",
      injectionKeyDescription = "MetaInject.Injection.STREAMING_SOURCE_TRANSFORM")
  private String streamSourceTransformName;

  @HopMetadataProperty(
      key = "stream_target_transform",
      injectionKey = "STREAMING_TARGET_TRANSFORM",
      injectionKeyDescription = "MetaInject.Injection.STREAMING_TARGET_TRANSFORM")
  private String streamTargetTransformName;

  @HopMetadataProperty(
      key = "run_configuration",
      injectionKey = "RUN_CONFIGURATION",
      injectionKeyDescription = "MetaInject.Injection.RUN_CONFIGURATION")
  private String runConfigurationName;

  @HopMetadataProperty(
      key = "create_parent_folder",
      injectionKey = "CREATE_PARENT_FOLDER",
      injectionKeyDescription = "MetaInject.Injection.CREATE_PARENT_FOLDER")
  private boolean createParentFolder;

  public MetaInjectMeta() {
    super();
    mappings = new ArrayList<>();
    sourceOutputFields = new ArrayList<>();
    createParentFolder = true;
  }

  public MetaInjectMeta(MetaInjectMeta m) {
    this();
    this.allowEmptyStreamOnExecution = m.allowEmptyStreamOnExecution;
    this.createParentFolder = m.createParentFolder;
    this.noExecution = m.noExecution;
    this.runConfigurationName = m.runConfigurationName;
    this.sourceTransformName = m.sourceTransformName;
    this.streamSourceTransformName = m.streamSourceTransformName;
    this.streamTargetTransformName = m.streamTargetTransformName;
    this.targetFile = m.targetFile;
    this.templateFileName = m.templateFileName;
    m.sourceOutputFields.forEach(f -> this.sourceOutputFields.add(new MetaInjectOutputField(f)));
    m.mappings.forEach(map -> this.mappings.add(new MetaInjectMapping(map)));
  }

  @Override
  public Object clone() {
    return new MetaInjectMeta(this);
  }

  @Override
  public void getFields(
      IRowMeta rowMeta,
      String origin,
      IRowMeta[] info,
      TransformMeta nextTransform,
      IVariables variables,
      IHopMetadataProvider metadataProvider)
      throws HopTransformException {

    rowMeta.clear(); // No defined output is expected from this transform.
    if (!Utils.isEmpty(sourceTransformName)) {
      for (MetaInjectOutputField field : sourceOutputFields) {
        try {
          rowMeta.addValueMeta(field.createValueMeta());
        } catch (HopPluginException e) {
          throw new HopTransformException(
              "Error creating value meta for output field '" + field.getName() + "'", e);
        }
      }
    }
  }

  public static synchronized PipelineMeta loadPipelineMeta(
      MetaInjectMeta injectMeta, IHopMetadataProvider metadataProvider, IVariables variables)
      throws HopException {
    PipelineMeta mappingPipelineMeta = null;

    CurrentDirectoryResolver resolver = new CurrentDirectoryResolver();
    IVariables tmpSpace =
        resolver.resolveCurrentDirectory(
            variables, injectMeta.getParentTransformMeta(), injectMeta.getTemplateFileName());

    String realFilename = tmpSpace.resolve(injectMeta.getTemplateFileName());
    try {
      // Load the meta-data from file.
      //
      mappingPipelineMeta = new PipelineMeta(realFilename, metadataProvider, tmpSpace);
      LogChannel.GENERAL.logDetailed(
          "Loading Mapping from repository",
          "Mapping transformation was loaded from XML file [" + realFilename + "]");
    } catch (Exception e) {
      throw new HopException(
          BaseMessages.getString(
              PKG, "MetaInjectMeta.Exception.UnableToLoadPipelineFromFile", realFilename),
          e);
    }

    // Pass some important information to the mapping transformation metadata:
    //
    mappingPipelineMeta.setFilename(mappingPipelineMeta.getFilename());

    return mappingPipelineMeta;
  }

  /** package-local visibility for testing purposes */
  PipelineMeta loadPipelineMeta(IHopMetadataProvider metadataProvider, IVariables variables)
      throws HopException {
    return MetaInjectMeta.loadPipelineMeta(this, metadataProvider, variables);
  }

  @Override
  public List<ResourceReference> getResourceDependencies(
      IVariables variables, TransformMeta transformMeta) {

    List<ResourceReference> references = new ArrayList<>(5);
    String realFilename = variables.resolve(templateFileName);
    ResourceReference reference = new ResourceReference(transformMeta);
    references.add(reference);

    if (!Utils.isEmpty(realFilename)) {
      // Add the filename to the references, including a reference to this transform
      // meta data.
      //
      reference
          .getEntries()
          .add(new ResourceEntry(realFilename, ResourceEntry.ResourceType.ACTIONFILE));
    }
    return references;
  }

  @Override
  public String exportResources(
      IVariables variables,
      Map<String, ResourceDefinition> definitions,
      IResourceNaming resourceNamingInterface,
      IHopMetadataProvider metadataProvider)
      throws HopException {
    try {
      // Try to load the transformation from repository or file.
      // Modify this recursively too...
      //
      // NOTE: there is no need to clone this transform because the caller is
      // responsible for this.
      //
      // First load the executor transformation metadata...
      //
      PipelineMeta executorPipelineMeta = loadPipelineMeta(metadataProvider, variables);

      // Also go down into the mapping transformation and export the files
      // there. (mapping recursively down)
      //
      String proposedNewFilename =
          executorPipelineMeta.exportResources(
              variables, definitions, resourceNamingInterface, metadataProvider);

      // Set the correct filename inside the XML.
      //
      executorPipelineMeta.setFilename(proposedNewFilename);

      // change it in the entry
      //
      templateFileName = proposedNewFilename;

      return proposedNewFilename;
    } catch (Exception e) {
      throw new HopException(
          BaseMessages.getString(
              PKG, "MetaInjectMeta.Exception.UnableToLoadTrans", templateFileName));
    }
  }

  @Override
  public boolean excludeFromCopyDistributeVerification() {
    return true;
  }

  @Override
  public boolean excludeFromRowLayoutVerification() {
    return true;
  }

  /**
   * @return The objects referenced in the transform, like a mapping, a pipeline, a workflow, ...
   */
  @Override
  public String[] getReferencedObjectDescriptions() {
    return new String[] {
      BaseMessages.getString(PKG, "MetaInjectMeta.ReferencedObject.Description"),
    };
  }

  private boolean isTransformationDefined() {
    return !Utils.isEmpty(templateFileName);
  }

  @Override
  public boolean[] isReferencedObjectEnabled() {
    return new boolean[] {
      isTransformationDefined(),
    };
  }

  @Override
  public String getActiveReferencedObjectDescription() {
    return BaseMessages.getString(PKG, "MetaInjectMeta.ReferencedObjectAfterInjection.Description");
  }

  /**
   * Load the referenced object
   *
   * @param index the object index to load
   * @param metadataProvider metadataProvider
   * @param variables the variable variables to use
   * @return the referenced object once loaded
   * @throws HopException in case there was an error loading the template pipeline.
   */
  @Override
  public IHasFilename loadReferencedObject(
      int index, IHopMetadataProvider metadataProvider, IVariables variables) throws HopException {
    return loadPipelineMeta(this, metadataProvider, variables);
  }

  @Override
  public void onTransformChange(
      PipelineMeta pipelineMeta, TransformMeta oldMeta, TransformMeta newMeta) {
    for (int i = 0; i < pipelineMeta.nrPipelineHops(); i++) {
      PipelineHopMeta hopMeta = pipelineMeta.getPipelineHop(i);
      if (hopMeta.getFromTransform().equals(oldMeta)) {
        TransformMeta toTransformMeta = hopMeta.getToTransform();
        if ((toTransformMeta.getTransform() instanceof MetaInjectMeta toMeta)
            && (toTransformMeta.equals(this.getParentTransformMeta()))) {
          List<MetaInjectMapping> sourceMappings = toMeta.getMappings();
          for (MetaInjectMapping sourceMapping : sourceMappings) {
            if (sourceMapping.getSourceTransformName() != null
                && sourceMapping.getSourceTransformName().equals(oldMeta.getName())) {
              sourceMapping.setSourceTransformName(newMeta.getName());
            }
          }
        }
      }
    }
  }

  @Override
  public String getFilename() {
    return null;
  }

  @Override
  public boolean supportsDrillDown() {
    return true;
  }
}
