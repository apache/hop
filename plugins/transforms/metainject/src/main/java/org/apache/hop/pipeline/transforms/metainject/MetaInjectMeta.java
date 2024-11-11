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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import org.apache.hop.core.Const;
import org.apache.hop.core.annotations.ActionTransformType;
import org.apache.hop.core.annotations.Transform;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.exception.HopPluginException;
import org.apache.hop.core.exception.HopTransformException;
import org.apache.hop.core.exception.HopXmlException;
import org.apache.hop.core.file.IHasFilename;
import org.apache.hop.core.injection.Injection;
import org.apache.hop.core.injection.InjectionDeep;
import org.apache.hop.core.injection.InjectionSupported;
import org.apache.hop.core.logging.LogChannel;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.value.ValueMetaFactory;
import org.apache.hop.core.util.CurrentDirectoryResolver;
import org.apache.hop.core.util.Utils;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.core.xml.XmlHandler;
import org.apache.hop.i18n.BaseMessages;
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
import org.w3c.dom.Node;

@Transform(
    id = "MetaInject",
    image = "GenericTransform.svg",
    name = "i18n::MetaInject.Name",
    description = "i18n::MetaInject.Description",
    categoryDescription = "i18n:org.apache.hop.pipeline.transform:BaseTransform.Category.Transform",
    keywords = "i18n::MetaInjectMeta.keyword",
    documentationUrl = "/pipeline/transforms/metainject.html",
    actionTransformTypes = {ActionTransformType.HOP_FILE, ActionTransformType.HOP_PIPELINE})
@InjectionSupported(
    localizationPrefix = "MetaInject.Injection.",
    groups = {"SOURCE_OUTPUT_FIELDS", "MAPPING_FIELDS"})
public class MetaInjectMeta extends BaseTransformMeta<MetaInject, MetaInjectData>
    implements ITransformMetaChangeListener, ISubPipelineAwareMeta {

  private static final Class<?> PKG = MetaInjectMeta.class;

  private static final String MAPPINGS = "mappings";
  private static final String MAPPING = "mapping";

  private static final String FILENAME = "filename";
  private static final String RUN_CONFIG = "run_configuration";
  private static final String TARGET_FILE = "target_file";
  private static final String CREATE_PARENT_FOLDER = "create_parent_folder";
  private static final String NO_EXECUTION = "no_execution";
  private static final String SOURCE_TRANSFORM = "source_transform";

  private static final String STREAM_SOURCE_TRANSFORM = "stream_source_transform";
  private static final String STREAM_TARGET_TRANSFORM = "stream_target_transform";
  private static final String TARGET_TRANSFORM_NAME = "target_transform_name";
  private static final String TARGET_ATTRIBUTE_KEY = "target_attribute_key";
  private static final String TARGET_DETAIL = "target_detail";
  private static final String SOURCE_FIELD = "source_field";
  private static final String SOURCE_OUTPUT_FIELDS = "source_output_fields";
  private static final String SOURCE_OUTPUT_FIELD = "source_output_field";
  private static final String SOURCE_OUTPUT_FIELD_NAME = "source_output_field_name";
  private static final String SOURCE_OUTPUT_FIELD_TYPE = "source_output_field_type";
  private static final String SOURCE_OUTPUT_FIELD_LENGTH = "source_output_field_length";
  private static final String SOURCE_OUTPUT_FIELD_PRECISION = "source_output_field_precision";

  private static final String GROUP_AND_NAME_DELIMITER = ".";
  private static final String CONST_SPACE = "      ";
  private static final String CONST_SPACE_LONG = "        ";

  // description of the transformation to execute...
  //
  @Injection(name = "FILE_NAME")
  private String fileName;

  @Injection(name = "SOURCE_TRANSFORM_NAME")
  private String sourceTransformName;

  @InjectionDeep private List<MetaInjectOutputField> sourceOutputFields;

  private Map<TargetTransformAttribute, SourceTransformField> targetSourceMapping;

  @InjectionDeep private List<MetaInjectMapping> metaInjectMapping;

  @Injection(name = "TARGET_FILE")
  private String targetFile;

  @Injection(name = "NO_EXECUTION")
  private boolean noExecution;

  @Injection(name = "STREAMING_SOURCE_TRANSFORM")
  private String streamSourceTransformName;

  private TransformMeta streamSourceTransform;

  @Injection(name = "STREAMING_TARGET_TRANSFORM")
  private String streamTargetTransformName;

  private String runConfigurationName;

  private boolean createParentFolder;

  public MetaInjectMeta() {
    super(); // allocate BaseTransformMeta
    targetSourceMapping = new HashMap<>();
    sourceOutputFields = new ArrayList<>();
  }

  @Override
  public Object clone() {
    Object retval = super.clone();
    return retval;
  }

  @Override
  public String getXml() {
    actualizeMetaInjectMapping();
    StringBuilder retval = new StringBuilder(500);

    retval.append("    ").append(XmlHandler.addTagValue(FILENAME, fileName));
    retval.append("    ").append(XmlHandler.addTagValue(RUN_CONFIG, runConfigurationName));

    retval.append("    ").append(XmlHandler.addTagValue(SOURCE_TRANSFORM, sourceTransformName));
    retval.append("    ").append(XmlHandler.openTag(SOURCE_OUTPUT_FIELDS));
    for (MetaInjectOutputField field : sourceOutputFields) {
      retval.append(CONST_SPACE).append(XmlHandler.openTag(SOURCE_OUTPUT_FIELD));
      retval
          .append(CONST_SPACE_LONG)
          .append(XmlHandler.addTagValue(SOURCE_OUTPUT_FIELD_NAME, field.getName()));
      retval
          .append(CONST_SPACE_LONG)
          .append(XmlHandler.addTagValue(SOURCE_OUTPUT_FIELD_TYPE, field.getTypeDescription()));
      retval
          .append(CONST_SPACE_LONG)
          .append(XmlHandler.addTagValue(SOURCE_OUTPUT_FIELD_LENGTH, field.getLength()));
      retval
          .append(CONST_SPACE_LONG)
          .append(XmlHandler.addTagValue(SOURCE_OUTPUT_FIELD_PRECISION, field.getPrecision()));
      retval.append(CONST_SPACE).append(XmlHandler.closeTag(SOURCE_OUTPUT_FIELD));
    }
    retval.append("    ").append(XmlHandler.closeTag(SOURCE_OUTPUT_FIELDS));

    retval.append("    ").append(XmlHandler.addTagValue(TARGET_FILE, targetFile));
    retval.append("    ").append(XmlHandler.addTagValue(CREATE_PARENT_FOLDER, createParentFolder));
    retval.append("    ").append(XmlHandler.addTagValue(NO_EXECUTION, noExecution));

    if ((streamSourceTransformName == null) && (streamSourceTransform != null)) {
      streamSourceTransformName = streamSourceTransform.getName();
    }
    retval
        .append("    ")
        .append(XmlHandler.addTagValue(STREAM_SOURCE_TRANSFORM, streamSourceTransformName));
    retval
        .append("    ")
        .append(XmlHandler.addTagValue(STREAM_TARGET_TRANSFORM, streamTargetTransformName));

    retval.append("    ").append(XmlHandler.openTag(MAPPINGS));
    for (TargetTransformAttribute target : targetSourceMapping.keySet()) {
      retval.append(CONST_SPACE).append(XmlHandler.openTag(MAPPING));
      SourceTransformField source = targetSourceMapping.get(target);
      retval
          .append(CONST_SPACE_LONG)
          .append(XmlHandler.addTagValue(TARGET_TRANSFORM_NAME, target.getTransformName()));
      retval
          .append(CONST_SPACE_LONG)
          .append(XmlHandler.addTagValue(TARGET_ATTRIBUTE_KEY, target.getAttributeKey()));
      retval
          .append(CONST_SPACE_LONG)
          .append(XmlHandler.addTagValue(TARGET_DETAIL, target.isDetail()));
      retval
          .append(CONST_SPACE_LONG)
          .append(XmlHandler.addTagValue(SOURCE_TRANSFORM, source.getTransformName()));
      retval
          .append(CONST_SPACE_LONG)
          .append(XmlHandler.addTagValue(SOURCE_FIELD, source.getField()));
      retval.append(CONST_SPACE).append(XmlHandler.closeTag(MAPPING));
    }
    retval.append("    ").append(XmlHandler.closeTag(MAPPINGS));

    return retval.toString();
  }

  @Override
  public void loadXml(Node transformNode, IHopMetadataProvider metadataProvider)
      throws HopXmlException {
    try {
      fileName = XmlHandler.getTagValue(transformNode, FILENAME);
      runConfigurationName = XmlHandler.getTagValue(transformNode, RUN_CONFIG);

      sourceTransformName = XmlHandler.getTagValue(transformNode, SOURCE_TRANSFORM);
      Node outputFieldsNode = XmlHandler.getSubNode(transformNode, SOURCE_OUTPUT_FIELDS);
      List<Node> outputFieldNodes = XmlHandler.getNodes(outputFieldsNode, SOURCE_OUTPUT_FIELD);
      sourceOutputFields = new ArrayList<>();
      for (Node outputFieldNode : outputFieldNodes) {
        String name = XmlHandler.getTagValue(outputFieldNode, SOURCE_OUTPUT_FIELD_NAME);
        String typeName = XmlHandler.getTagValue(outputFieldNode, SOURCE_OUTPUT_FIELD_TYPE);
        int length =
            Const.toInt(XmlHandler.getTagValue(outputFieldNode, SOURCE_OUTPUT_FIELD_LENGTH), -1);
        int precision =
            Const.toInt(XmlHandler.getTagValue(outputFieldNode, SOURCE_OUTPUT_FIELD_PRECISION), -1);
        int type = ValueMetaFactory.getIdForValueMeta(typeName);
        sourceOutputFields.add(new MetaInjectOutputField(name, type, length, precision));
      }

      targetFile = XmlHandler.getTagValue(transformNode, TARGET_FILE);
      createParentFolder =
          "Y".equalsIgnoreCase(XmlHandler.getTagValue(transformNode, CREATE_PARENT_FOLDER));
      noExecution = "Y".equalsIgnoreCase(XmlHandler.getTagValue(transformNode, NO_EXECUTION));

      streamSourceTransformName = XmlHandler.getTagValue(transformNode, STREAM_SOURCE_TRANSFORM);
      streamTargetTransformName = XmlHandler.getTagValue(transformNode, STREAM_TARGET_TRANSFORM);

      Node mappingsNode = XmlHandler.getSubNode(transformNode, MAPPINGS);
      int nrMappings = XmlHandler.countNodes(mappingsNode, MAPPING);
      for (int i = 0; i < nrMappings; i++) {
        Node mappingNode = XmlHandler.getSubNodeByNr(mappingsNode, MAPPING, i);
        String targetTransformName = XmlHandler.getTagValue(mappingNode, TARGET_TRANSFORM_NAME);
        String targetAttributeKey = XmlHandler.getTagValue(mappingNode, TARGET_ATTRIBUTE_KEY);
        boolean targetDetail =
            "Y".equalsIgnoreCase(XmlHandler.getTagValue(mappingNode, TARGET_DETAIL));
        String sourceTransformName = XmlHandler.getTagValue(mappingNode, SOURCE_TRANSFORM);
        String sourceField = XmlHandler.getTagValue(mappingNode, SOURCE_FIELD);

        TargetTransformAttribute target =
            new TargetTransformAttribute(targetTransformName, targetAttributeKey, targetDetail);
        SourceTransformField source = new SourceTransformField(sourceTransformName, sourceField);
        targetSourceMapping.put(target, source);
      }

      MetaInjectMigration.migrateFrom70(targetSourceMapping);
    } catch (Exception e) {
      throw new HopXmlException("Unable to load transform info from XML", e);
    }
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

  public Map<TargetTransformAttribute, SourceTransformField> getTargetSourceMapping() {
    return targetSourceMapping;
  }

  public void setTargetSourceMapping(
      Map<TargetTransformAttribute, SourceTransformField> targetSourceMapping) {
    this.targetSourceMapping = targetSourceMapping;
  }

  /**
   * @return the fileName
   */
  public String getFileName() {
    return fileName;
  }

  /**
   * @param fileName the fileName to set
   */
  public void setFileName(String fileName) {
    this.fileName = fileName;
  }

  public static final synchronized PipelineMeta loadPipelineMeta(
      MetaInjectMeta injectMeta, IHopMetadataProvider metadataProvider, IVariables variables)
      throws HopException {
    PipelineMeta mappingPipelineMeta = null;

    CurrentDirectoryResolver resolver = new CurrentDirectoryResolver();
    IVariables tmpSpace =
        resolver.resolveCurrentDirectory(
            variables, injectMeta.getParentTransformMeta(), injectMeta.getFileName());

    String realFilename = tmpSpace.resolve(injectMeta.getFileName());
    try {
      // OK, load the meta-data from file...
      //
      // Don't set internal variables: they belong to the parent thread!
      //
      if (mappingPipelineMeta == null) {
        mappingPipelineMeta = new PipelineMeta(realFilename, metadataProvider, tmpSpace);
        LogChannel.GENERAL.logDetailed(
            "Loading Mapping from repository",
            "Mapping transformation was loaded from XML file [" + realFilename + "]");
      }
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
    String realFilename = variables.resolve(fileName);
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

      // To get a relative path to it, we inject
      // ${Internal.Entry.Current.Directory}
      //
      String newFilename = proposedNewFilename;

      // Set the correct filename inside the XML.
      //
      executorPipelineMeta.setFilename(newFilename);

      // change it in the entry
      //
      fileName = newFilename;

      return proposedNewFilename;
    } catch (Exception e) {
      throw new HopException(
          BaseMessages.getString(PKG, "MetaInjectMeta.Exception.UnableToLoadTrans", fileName));
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
   * @return the sourceTransformName
   */
  public String getSourceTransformName() {
    return sourceTransformName;
  }

  /**
   * @param sourceTransformName the sourceTransformName to set
   */
  public void setSourceTransformName(String sourceTransformName) {
    this.sourceTransformName = sourceTransformName;
  }

  /**
   * @return the targetFile
   */
  public String getTargetFile() {
    return targetFile;
  }

  /**
   * @param targetFile the targetFile to set
   */
  public void setTargetFile(String targetFile) {
    this.targetFile = targetFile;
  }

  /**
   * @return the noExecution
   */
  public boolean isNoExecution() {
    return noExecution;
  }

  /**
   * @param noExecution the noExecution to set
   */
  public void setNoExecution(boolean noExecution) {
    this.noExecution = noExecution;
  }

  public String getRunConfigurationName() {
    return runConfigurationName;
  }

  public void setRunConfigurationName(String runConfigurationName) {
    this.runConfigurationName = runConfigurationName;
  }

  public boolean isCreateParentFolder() {
    return createParentFolder;
  }

  public void setCreateParentFolder(boolean createParentFolder) {
    this.createParentFolder = createParentFolder;
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
    return !Utils.isEmpty(fileName);
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
   * @throws HopException
   */
  @Override
  public IHasFilename loadReferencedObject(
      int index, IHopMetadataProvider metadataProvider, IVariables variables) throws HopException {
    return loadPipelineMeta(this, metadataProvider, variables);
  }

  public String getStreamSourceTransformName() {
    return streamSourceTransformName;
  }

  public void setStreamSourceTransformName(String streamSourceTransformName) {
    this.streamSourceTransformName = streamSourceTransformName;
  }

  public TransformMeta getStreamSourceTransform() {
    return streamSourceTransform;
  }

  public void setStreamSourceTransform(TransformMeta streamSourceTransform) {
    this.streamSourceTransform = streamSourceTransform;
  }

  public String getStreamTargetTransformName() {
    return streamTargetTransformName;
  }

  public void setStreamTargetTransformName(String streamTargetTransformName) {
    this.streamTargetTransformName = streamTargetTransformName;
  }

  @Override
  public void searchInfoAndTargetTransforms(List<TransformMeta> transforms) {
    streamSourceTransform = TransformMeta.findTransform(transforms, streamSourceTransformName);
  }

  public List<MetaInjectOutputField> getSourceOutputFields() {
    return sourceOutputFields;
  }

  public void setSourceOutputFields(List<MetaInjectOutputField> sourceOutputFields) {
    this.sourceOutputFields = sourceOutputFields;
  }

  public List<MetaInjectMapping> getMetaInjectMapping() {
    return metaInjectMapping;
  }

  public void setMetaInjectMapping(List<MetaInjectMapping> metaInjectMapping) {
    this.metaInjectMapping = metaInjectMapping;
  }

  public void actualizeMetaInjectMapping() {
    if (metaInjectMapping == null || metaInjectMapping.isEmpty()) {
      return;
    }
    Map<TargetTransformAttribute, SourceTransformField> targetToSourceMap =
        convertToMap(metaInjectMapping);
    setTargetSourceMapping(targetToSourceMap);
  }

  /** package-local visibility for testing purposes */
  static Map<TargetTransformAttribute, SourceTransformField> convertToMap(
      List<MetaInjectMapping> metaInjectMapping) {
    Map<TargetTransformAttribute, SourceTransformField> targetToSourceMap = new HashMap<>();
    for (MetaInjectMapping mappingEntry : metaInjectMapping) {
      if (!isMappingEntryFilled(mappingEntry)) {
        continue;
      }
      TargetTransformAttribute targetTransformAttribute =
          createTargetTransformAttribute(mappingEntry);
      SourceTransformField sourceTransformField = createSourceTransformField(mappingEntry);
      targetToSourceMap.put(targetTransformAttribute, sourceTransformField);
    }
    return targetToSourceMap;
  }

  private static TargetTransformAttribute createTargetTransformAttribute(
      MetaInjectMapping mappingEntry) {
    String targetFieldName = mappingEntry.getTargetField();
    if (targetFieldName.contains(GROUP_AND_NAME_DELIMITER)) {
      String[] targetFieldGroupAndName = targetFieldName.split("\\" + GROUP_AND_NAME_DELIMITER);
      return new TargetTransformAttribute(
          mappingEntry.getTargetTransform(), targetFieldGroupAndName[1], true);
    }
    return new TargetTransformAttribute(
        mappingEntry.getTargetTransform(), mappingEntry.getTargetField(), false);
  }

  private static boolean isMappingEntryFilled(MetaInjectMapping mappingEntry) {
    if (mappingEntry.getSourceTransform() == null
        || mappingEntry.getSourceField() == null
        || mappingEntry.getTargetTransform() == null
        || mappingEntry.getTargetField() == null) {
      return false;
    }
    return true;
  }

  private static SourceTransformField createSourceTransformField(MetaInjectMapping mappingEntry) {
    return new SourceTransformField(
        mappingEntry.getSourceTransform(), mappingEntry.getSourceField());
  }

  @Override
  public void setDefault() {
    super.setDefault();
    createParentFolder = true;
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
          Map<TargetTransformAttribute, SourceTransformField> sourceMapping =
              toMeta.getTargetSourceMapping();
          for (Entry<TargetTransformAttribute, SourceTransformField> entry :
              sourceMapping.entrySet()) {
            SourceTransformField value = entry.getValue();
            if (value.getTransformName() != null
                && value.getTransformName().equals(oldMeta.getName())) {
              value.setTransformName(newMeta.getName());
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
}
