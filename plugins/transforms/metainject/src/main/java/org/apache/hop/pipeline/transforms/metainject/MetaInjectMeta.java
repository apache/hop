/*! ******************************************************************************
 *
 * Hop : The Hop Orchestration Platform
 *
 * Copyright (C) 2002-2017 by Hitachi Vantara : http://www.pentaho.com
 * http://www.project-hop.org
 *
 *******************************************************************************
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 ******************************************************************************/

package org.apache.hop.pipeline.transforms.metainject;

import org.apache.hop.core.Const;
import org.apache.hop.core.annotations.Transform;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.exception.HopPluginException;
import org.apache.hop.core.exception.HopTransformException;
import org.apache.hop.core.exception.HopXmlException;
import org.apache.hop.core.file.IHasFilename;
import org.apache.hop.core.injection.Injection;
import org.apache.hop.core.injection.InjectionDeep;
import org.apache.hop.core.injection.InjectionSupported;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.value.ValueMetaFactory;
import org.apache.hop.core.util.CurrentDirectoryResolver;
import org.apache.hop.core.util.StringUtil;
import org.apache.hop.core.util.Utils;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.core.xml.XmlHandler;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.metadata.api.IHopMetadataProvider;
import org.apache.hop.pipeline.ISubPipelineAwareMeta;
import org.apache.hop.pipeline.Pipeline;
import org.apache.hop.pipeline.PipelineHopMeta;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.*;
import org.apache.hop.resource.IResourceNaming;
import org.apache.hop.resource.ResourceDefinition;
import org.apache.hop.resource.ResourceEntry;
import org.apache.hop.resource.ResourceReference;
import org.w3c.dom.Node;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

@Transform( id = "MetaInject",
    image = "GenericTransform.svg",
    i18nPackageName = "org.apache.hop.pipeline.transforms.metainject",
    name = "BaseTransform.TypeLongDesc.MetaInject",
    description = "BaseTransform.TypeTooltipDesc.MetaInject",
    categoryDescription = "",
    documentationUrl = "https://www.project-hop.org/manual/latest/plugins/transforms/metainject.html" )
@InjectionSupported( localizationPrefix = "MetaInject.Injection.", groups = { "SOURCE_OUTPUT_FIELDS",
  "MAPPING_FIELDS" } )
public class MetaInjectMeta extends BaseTransformMeta implements ITransformMeta<MetaInject, MetaInjectData>, ITransformMetaChangeListener, ISubPipelineAwareMeta {

  private static final Class<?> PKG = MetaInjectMeta.class; // for i18n purposes, needed by Translator2!!

  private static final String MAPPINGS = "mappings";
  private static final String MAPPING = "mapping";

//  private static final String SPECIFICATION_METHOD = "specification_method";
//  private static final String TRANS_OBJECT_ID = "trans_object_id";
  private static final String PIPELINE_NAME = "pipeline_name";
  private static final String FILENAME = "filename";
  private static final String DIRECTORY_PATH = "directory_path";
  private static final String TARGET_FILE = "target_file";
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

  private static final String MAPPING_SOURCE_FIELD = "mapping_source_field";
  private static final String MAPPING_SOURCE_TRANSFORM = "mapping_source_transform";
  private static final String MAPPING_TARGET_DETAIL = "mapping_target_detail";
  private static final String MAPPING_TARGET_ATTRIBUTE_KEY = "mapping_target_attribute_key";
  private static final String MAPPING_TARGET_TRANSFORM_NAME = "mapping_target_transform_name";

  private static final String GROUP_AND_NAME_DELIMITER = ".";

  // description of the transformation to execute...
  //
  @Injection( name = "PIPELINE_NAME" )
  private String pipelineName;

  @Injection( name = "FILE_NAME" )
  private String fileName;

  @Injection( name = "DIRECTORY_PATH" )
  private String directoryPath;

//  private ObjectId transObjectId;

  @Injection( name = "PIPELINE_SPECIFICATION_METHOD" )
  private ObjectLocationSpecificationMethod specificationMethod;

  @Injection( name = "SOURCE_TRANSFORM_NAME" )
  private String sourceTransformName;

  @InjectionDeep
  private List<MetaInjectOutputField> sourceOutputFields;

  private Map<TargetTransformAttribute, SourceTransformField> targetSourceMapping;

  @InjectionDeep
  private List<MetaInjectMapping> metaInjectMapping;

  @Injection( name = "TARGET_FILE" )
  private String targetFile;

  @Injection( name = "NO_EXECUTION" )
  private boolean noExecution;

  @Injection( name = "STREAMING_SOURCE_TRANSFORM" )
  private String streamSourceTransformName;

  private TransformMeta streamSourceTransform;

  @Injection( name = "STREAMING_TARGET_TRANSFORM" )
  private String streamTargetTransformName;

  public MetaInjectMeta() {
    super(); // allocate BaseTransformMeta
    specificationMethod = ObjectLocationSpecificationMethod.FILENAME;
    targetSourceMapping = new HashMap<TargetTransformAttribute, SourceTransformField>();
    sourceOutputFields = new ArrayList<MetaInjectOutputField>();
  }

  // TODO: deep copy
  @Override
  public Object clone() {
    Object retval = super.clone();
    return retval;
  }

  @Override
  public void setDefault() {
  }

  @Override
  public String getXml() {
    actualizeMetaInjectMapping();
    StringBuilder retval = new StringBuilder( 500 );

//    retval.append( "    " ).append( XmlHandler.addTagValue( SPECIFICATION_METHOD, specificationMethod == null ? null
//      : specificationMethod.getCode() ) );
//    retval.append( "    " ).append( XmlHandler.addTagValue( TRANS_OBJECT_ID, transObjectId == null ? null
//      : transObjectId.toString() ) );
    retval.append( "    " ).append( XmlHandler.addTagValue( PIPELINE_NAME, pipelineName) );
    retval.append( "    " ).append( XmlHandler.addTagValue( FILENAME, fileName ) );
    retval.append( "    " ).append( XmlHandler.addTagValue( DIRECTORY_PATH, directoryPath ) );

    retval.append( "    " ).append( XmlHandler.addTagValue( SOURCE_TRANSFORM, sourceTransformName) );
    retval.append( "    " ).append( XmlHandler.openTag( SOURCE_OUTPUT_FIELDS ) );
    for ( MetaInjectOutputField field : sourceOutputFields ) {
      retval.append( "      " ).append( XmlHandler.openTag( SOURCE_OUTPUT_FIELD ) );
      retval.append( "        " ).append( XmlHandler.addTagValue( SOURCE_OUTPUT_FIELD_NAME, field.getName() ) );
      retval.append( "        " ).append( XmlHandler.addTagValue( SOURCE_OUTPUT_FIELD_TYPE, field
        .getTypeDescription() ) );
      retval.append( "        " ).append( XmlHandler.addTagValue( SOURCE_OUTPUT_FIELD_LENGTH, field.getLength() ) );
      retval.append( "        " ).append( XmlHandler.addTagValue( SOURCE_OUTPUT_FIELD_PRECISION, field
        .getPrecision() ) );
      retval.append( "      " ).append( XmlHandler.closeTag( SOURCE_OUTPUT_FIELD ) );
    }
    retval.append( "    " ).append( XmlHandler.closeTag( SOURCE_OUTPUT_FIELDS ) );

    retval.append( "    " ).append( XmlHandler.addTagValue( TARGET_FILE, targetFile ) );
    retval.append( "    " ).append( XmlHandler.addTagValue( NO_EXECUTION, noExecution ) );

    if ( ( streamSourceTransformName == null ) && ( streamSourceTransform != null ) ) {
      streamSourceTransformName = streamSourceTransform.getName();
    }
    retval.append( "    " ).append( XmlHandler.addTagValue( STREAM_SOURCE_TRANSFORM, streamSourceTransformName) );
    retval.append( "    " ).append( XmlHandler.addTagValue( STREAM_TARGET_TRANSFORM, streamTargetTransformName) );

    retval.append( "    " ).append( XmlHandler.openTag( MAPPINGS ) );
    for ( TargetTransformAttribute target : targetSourceMapping.keySet() ) {
      retval.append( "      " ).append( XmlHandler.openTag( MAPPING ) );
      SourceTransformField source = targetSourceMapping.get( target );
      retval.append( "        " ).append( XmlHandler.addTagValue( TARGET_TRANSFORM_NAME, target.getTransformName() ) );
      retval.append( "        " ).append( XmlHandler.addTagValue( TARGET_ATTRIBUTE_KEY, target.getAttributeKey() ) );
      retval.append( "        " ).append( XmlHandler.addTagValue( TARGET_DETAIL, target.isDetail() ) );
      retval.append( "        " ).append( XmlHandler.addTagValue( SOURCE_TRANSFORM, source.getTransformName() ) );
      retval.append( "        " ).append( XmlHandler.addTagValue( SOURCE_FIELD, source.getField() ) );
      retval.append( "      " ).append( XmlHandler.closeTag( MAPPING ) );
    }
    retval.append( "    " ).append( XmlHandler.closeTag( MAPPINGS ) );

    return retval.toString();
  }

  @Override
  public void loadXml(Node transformNode, IHopMetadataProvider metadataProvider ) throws HopXmlException {
    try {

      pipelineName = XmlHandler.getTagValue( transformNode, PIPELINE_NAME );
      fileName = XmlHandler.getTagValue( transformNode, FILENAME );
      directoryPath = XmlHandler.getTagValue( transformNode, DIRECTORY_PATH );

      sourceTransformName = XmlHandler.getTagValue( transformNode, SOURCE_TRANSFORM );
      Node outputFieldsNode = XmlHandler.getSubNode( transformNode, SOURCE_OUTPUT_FIELDS );
      List<Node> outputFieldNodes = XmlHandler.getNodes( outputFieldsNode, SOURCE_OUTPUT_FIELD );
      sourceOutputFields = new ArrayList<MetaInjectOutputField>();
      for ( Node outputFieldNode : outputFieldNodes ) {
        String name = XmlHandler.getTagValue( outputFieldNode, SOURCE_OUTPUT_FIELD_NAME );
        String typeName = XmlHandler.getTagValue( outputFieldNode, SOURCE_OUTPUT_FIELD_TYPE );
        int length = Const.toInt( XmlHandler.getTagValue( outputFieldNode, SOURCE_OUTPUT_FIELD_LENGTH ), -1 );
        int precision = Const.toInt( XmlHandler.getTagValue( outputFieldNode, SOURCE_OUTPUT_FIELD_PRECISION ), -1 );
        int type = ValueMetaFactory.getIdForValueMeta( typeName );
        sourceOutputFields.add( new MetaInjectOutputField( name, type, length, precision ) );
      }

      targetFile = XmlHandler.getTagValue( transformNode, TARGET_FILE );
      noExecution = "Y".equalsIgnoreCase( XmlHandler.getTagValue( transformNode, NO_EXECUTION ) );

      streamSourceTransformName = XmlHandler.getTagValue( transformNode, STREAM_SOURCE_TRANSFORM );
      streamTargetTransformName = XmlHandler.getTagValue( transformNode, STREAM_TARGET_TRANSFORM );

      Node mappingsNode = XmlHandler.getSubNode( transformNode, MAPPINGS );
      int nrMappings = XmlHandler.countNodes( mappingsNode, MAPPING );
      for ( int i = 0; i < nrMappings; i++ ) {
        Node mappingNode = XmlHandler.getSubNodeByNr( mappingsNode, MAPPING, i );
        String targetTransformName = XmlHandler.getTagValue( mappingNode, TARGET_TRANSFORM_NAME );
        String targetAttributeKey = XmlHandler.getTagValue( mappingNode, TARGET_ATTRIBUTE_KEY );
        boolean targetDetail = "Y".equalsIgnoreCase( XmlHandler.getTagValue( mappingNode, TARGET_DETAIL ) );
        String sourceTransformName = XmlHandler.getTagValue( mappingNode, SOURCE_TRANSFORM );
        String sourceField = XmlHandler.getTagValue( mappingNode, SOURCE_FIELD );

        TargetTransformAttribute target = new TargetTransformAttribute( targetTransformName, targetAttributeKey, targetDetail );
        SourceTransformField source = new SourceTransformField( sourceTransformName, sourceField );
        targetSourceMapping.put( target, source );
      }

      MetaInjectMigration.migrateFrom70( targetSourceMapping );
    } catch ( Exception e ) {
      throw new HopXmlException( "Unable to load transform info from XML", e );
    }
  }


  @Override
  public void getFields(IRowMeta rowMeta, String origin, IRowMeta[] info, TransformMeta nextTransform,
                        IVariables space, IHopMetadataProvider metadataProvider ) throws HopTransformException {

    rowMeta.clear(); // No defined output is expected from this transform.
    if ( !Utils.isEmpty(sourceTransformName) ) {
      for ( MetaInjectOutputField field : sourceOutputFields ) {
        try {
          rowMeta.addValueMeta(field.createValueMeta() );
        } catch ( HopPluginException e ) {
          throw new HopTransformException( "Error creating value meta for output field '" + field.getName() + "'", e );
        }
      }
    }
  }

  @Override
  public MetaInjectData getTransformData() {
    return new MetaInjectData();
  }

  public Map<TargetTransformAttribute, SourceTransformField> getTargetSourceMapping() {
    return targetSourceMapping;
  }

  public void setTargetSourceMapping( Map<TargetTransformAttribute, SourceTransformField> targetSourceMapping ) {
    this.targetSourceMapping = targetSourceMapping;
  }

  /**
   * @return the pipeline name
   */
  public String getPipelineName() {
    return pipelineName;
  }

  /**
   * @param pipelineName the pipelineName to set
   */
  public void setPipelineName(String pipelineName ) {
    this.pipelineName = pipelineName;
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
  public void setFileName( String fileName ) {
    this.fileName = fileName;
  }

  /**
   * @return the directoryPath
   */
  public String getDirectoryPath() {
    return directoryPath;
  }

  /**
   * @param directoryPath the directoryPath to set
   */
  public void setDirectoryPath( String directoryPath ) {
    this.directoryPath = directoryPath;
  }

/*
  @Override
  public String[] getDirectories() {
    return new String[]{ directoryPath };
  }

  @Override
  public void setDirectories( String[] directories ) {
    this.directoryPath = directories[0];
  }
*/

  /**
   * @return the transObjectId
   */
/*
  public ObjectId getTransObjectId() {
    return transObjectId;
  }
*/

/*
  */
//    @param transObjectId the transObjectId to set
   /*

  public void setTransObjectId( ObjectId transObjectId ) {
    this.transObjectId = transObjectId;
  }
*/

//  @Injection( name = "TRANS_OBJECT_ID" )
/*
  public void setTransStringObjectId( String transStringObjectId ) {
    this.transObjectId = new StringObjectId( transStringObjectId );
  }
*/

  /**
   * @return the specificationMethod
   */
  public ObjectLocationSpecificationMethod getSpecificationMethod() {
    return specificationMethod;
  }

//  @Override
//  public ObjectLocationSpecificationMethod[] getSpecificationMethods() {
//    return new ObjectLocationSpecificationMethod[] { specificationMethod };
//  }

  /**
   * @param specificationMethod the specificationMethod to set
   */
  public void setSpecificationMethod( ObjectLocationSpecificationMethod specificationMethod ) {
    this.specificationMethod = specificationMethod;
  }

/*
  @Override
  public PipelineMeta fetchPipelineMeta(ITransformMeta transformMeta, IMetaStore metastore, IVariables space ) throws HopException {
    return ( transformMeta != null && transformMeta instanceof MetaInjectMeta )
        ? loadPipelineMeta( (MetaInjectMeta) transformMeta, metastore, space ) : null;

  }
*/

/*
  @Deprecated
  public static final synchronized PipelineMeta loadPipelineMeta( MetaInjectMeta mappingMeta,
                                                                     IVariables space ) throws HopException {
    return loadPipelineMeta( mappingMeta, space );
  }
*/

  public static final synchronized PipelineMeta loadPipelineMeta(MetaInjectMeta injectMeta,
                                                                 IHopMetadataProvider metadataProvider, IVariables space )
    throws HopException {
    PipelineMeta mappingPipelineMeta = null;

    CurrentDirectoryResolver resolver = new CurrentDirectoryResolver();
    IVariables tmpSpace =
      resolver.resolveCurrentDirectory( space, injectMeta.getParentTransformMeta(), injectMeta.getFileName() );

    switch ( injectMeta.getSpecificationMethod() ) {
      case FILENAME:
        String realFilename = tmpSpace.environmentSubstitute( injectMeta.getFileName() );
        try {
          // OK, load the meta-data from file...
          //
          // Don't set internal variables: they belong to the parent thread!
          //
          if ( mappingPipelineMeta == null ) {
            mappingPipelineMeta = new PipelineMeta( realFilename, metadataProvider, false, tmpSpace);
            mappingPipelineMeta.getLogChannel().logDetailed( "Loading Mapping from repository",
              "Mapping transformation was loaded from XML file [" + realFilename + "]" );
          }
        } catch ( Exception e ) {
          throw new HopException( BaseMessages.getString( PKG,
            "MetaInjectMeta.Exception.UnableToLoadTransformationFromFile", realFilename ), e );
        }
        break;

      default:
        break;
    }

    // Pass some important information to the mapping transformation metadata:
    //
    mappingPipelineMeta.copyVariablesFrom( space );
    mappingPipelineMeta.setFilename( mappingPipelineMeta.getFilename() );

    return mappingPipelineMeta;
  }

  /**
   * package-local visibility for testing purposes
   */
  PipelineMeta loadPipelineMeta(IHopMetadataProvider metadataProvider, IVariables space ) throws HopException {
    return MetaInjectMeta.loadPipelineMeta( this, metadataProvider, space );
  }

  @Override
  public List<ResourceReference> getResourceDependencies(PipelineMeta pipelineMeta, TransformMeta transformInfo ) {
    List<ResourceReference> references = new ArrayList<ResourceReference>( 5 );
    String realFilename = pipelineMeta.environmentSubstitute( fileName );
    String realPipelineName = pipelineMeta.environmentSubstitute(pipelineName);
    String realDirectoryPath = pipelineMeta.environmentSubstitute( directoryPath );
    ResourceReference reference = new ResourceReference( transformInfo );
    references.add( reference );

    if ( !Utils.isEmpty( realFilename ) ) {
      // Add the filename to the references, including a reference to this transform
      // meta data.
      //
      reference.getEntries().add( new ResourceEntry( realFilename, ResourceEntry.ResourceType.ACTIONFILE ) );
    } else if ( !Utils.isEmpty( realPipelineName ) ) {
      // Add the trans name (including full repository path) to dependencies
      String realTransformation = realDirectoryPath + "/" + realPipelineName;
      reference.getEntries().add( new ResourceEntry( realTransformation, ResourceEntry.ResourceType.ACTIONFILE ) );
    }
    return references;
  }

  @Override
  public String exportResources(IVariables space, Map<String, ResourceDefinition> definitions,
                                IResourceNaming resourceNamingInterface, IHopMetadataProvider metadataProvider )
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
      PipelineMeta executorPipelineMeta = loadPipelineMeta( metadataProvider, space );

      // Also go down into the mapping transformation and export the files
      // there. (mapping recursively down)
      //
      String proposedNewFilename =
        executorPipelineMeta.exportResources( executorPipelineMeta, definitions, resourceNamingInterface, metadataProvider );

      // To get a relative path to it, we inject
      // ${Internal.Entry.Current.Directory}
      //
      String newFilename = proposedNewFilename;

      // Set the correct filename inside the XML.
      //
      executorPipelineMeta.setFilename( newFilename );

      // change it in the entry
      //
      fileName = newFilename;

      setSpecificationMethod( ObjectLocationSpecificationMethod.FILENAME );

      return proposedNewFilename;
    } catch ( Exception e ) {
      throw new HopException( BaseMessages.getString( PKG, "MetaInjectMeta.Exception.UnableToLoadTrans",
        fileName ) );
    }
  }

  @Override
  public boolean excludeFromCopyDistributeVerification() {
    return true;
  }

  @Override
  public ITransform createTransform(TransformMeta transformMeta, MetaInjectData data, int copyNr, PipelineMeta pipelineMeta, Pipeline pipeline) {
    return new MetaInject(transformMeta, this, data, copyNr, pipelineMeta, pipeline);
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
  public void setSourceTransformName(String sourceTransformName ) {
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
  public void setTargetFile( String targetFile ) {
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
  public void setNoExecution( boolean noExecution ) {
    this.noExecution = noExecution;
  }

  /**
   * @return The objects referenced in the transform, like a mapping, a transformation, a job, ...
   */
  @Override
  public String[] getReferencedObjectDescriptions() {
    return new String[] { BaseMessages.getString( PKG, "MetaInjectMeta.ReferencedObject.Description" ), };
  }

  private boolean isTransformationDefined() {
    return !Utils.isEmpty( fileName ) || ( !Utils.isEmpty( this.directoryPath ) && !StringUtil
      .isEmpty(pipelineName) );
  }

  @Override
  public boolean[] isReferencedObjectEnabled() {
    return new boolean[] { isTransformationDefined(), };
  }

  @Override
  public String getActiveReferencedObjectDescription() {
    return BaseMessages.getString( PKG, "MetaInjectMeta.ReferencedObjectAfterInjection.Description" );
  }

//  @Override
//  @Deprecated
//  public Object loadReferencedObject( int index, IVariables space ) throws HopException {
//    return loadReferencedObject( index, null, space );
//  }

  /**
   * Load the referenced object
   *
   * @param index     the object index to load
   * @param metadataProvider metadataProvider
   * @param space     the variable space to use
   * @return the referenced object once loaded
   * @throws HopException
   */
  @Override
  public IHasFilename loadReferencedObject(int index, IHopMetadataProvider metadataProvider, IVariables space )
    throws HopException {
    return loadPipelineMeta( this, metadataProvider, space );
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
  public void searchInfoAndTargetTransforms( List<TransformMeta> transforms ) {
    streamSourceTransform = TransformMeta.findTransform( transforms, streamSourceTransformName);
  }

  public List<MetaInjectOutputField> getSourceOutputFields() {
    return sourceOutputFields;
  }

  public void setSourceOutputFields( List<MetaInjectOutputField> sourceOutputFields ) {
    this.sourceOutputFields = sourceOutputFields;
  }

  public List<MetaInjectMapping> getMetaInjectMapping() {
    return metaInjectMapping;
  }

  public void setMetaInjectMapping( List<MetaInjectMapping> metaInjectMapping ) {
    this.metaInjectMapping = metaInjectMapping;
  }

  public void actualizeMetaInjectMapping() {
    if ( metaInjectMapping == null || metaInjectMapping.isEmpty() ) {
      return;
    }
    Map<TargetTransformAttribute, SourceTransformField> targetToSourceMap = convertToMap( metaInjectMapping );
    setTargetSourceMapping( targetToSourceMap );
  }

  /**
   * package-local visibility for testing purposes
   */
  static Map<TargetTransformAttribute, SourceTransformField> convertToMap(List<MetaInjectMapping> metaInjectMapping ) {
    Map<TargetTransformAttribute, SourceTransformField> targetToSourceMap = new HashMap<TargetTransformAttribute, SourceTransformField>();
    for ( MetaInjectMapping mappingEntry : metaInjectMapping ) {
      if ( !isMappingEntryFilled( mappingEntry ) ) {
        continue;
      }
      TargetTransformAttribute targetTransformAttribute = createTargetTransformAttribute( mappingEntry );
      SourceTransformField sourceTransformField = createSourceTransformField( mappingEntry );
      targetToSourceMap.put(targetTransformAttribute, sourceTransformField);
    }
    return targetToSourceMap;
  }

  private static TargetTransformAttribute createTargetTransformAttribute(MetaInjectMapping mappingEntry ) {
    String targetFieldName = mappingEntry.getTargetField();
    if ( targetFieldName.contains( GROUP_AND_NAME_DELIMITER ) ) {
      String[] targetFieldGroupAndName = targetFieldName.split( "\\" + GROUP_AND_NAME_DELIMITER );
      return new TargetTransformAttribute( mappingEntry.getTargetTransform(), targetFieldGroupAndName[ 1 ], true );
    }
    return new TargetTransformAttribute( mappingEntry.getTargetTransform(), mappingEntry.getTargetField(), false );
  }

  private static boolean isMappingEntryFilled( MetaInjectMapping mappingEntry ) {
    if ( mappingEntry.getSourceTransform() == null || mappingEntry.getSourceField() == null || mappingEntry
      .getTargetTransform() == null || mappingEntry.getTargetField() == null ) {
      return false;
    }
    return true;
  }

  private static SourceTransformField createSourceTransformField(MetaInjectMapping mappingEntry ) {
    return new SourceTransformField( mappingEntry.getSourceTransform(), mappingEntry.getSourceField() );
  }

  @Override
  public void onTransformChange( PipelineMeta pipelineMeta, TransformMeta oldMeta, TransformMeta newMeta ) {
    for ( int i = 0; i < pipelineMeta.nrPipelineHops(); i++ ) {
      PipelineHopMeta hopMeta = pipelineMeta.getPipelineHop( i );
      if ( hopMeta.getFromTransform().equals( oldMeta ) ) {
        TransformMeta toTransformMeta = hopMeta.getToTransform();
        if ( ( toTransformMeta.getTransform() instanceof MetaInjectMeta ) && ( toTransformMeta.equals( this
          .getParentTransformMeta() ) ) ) {
          MetaInjectMeta toMeta = (MetaInjectMeta) toTransformMeta.getTransform();
          Map<TargetTransformAttribute, SourceTransformField> sourceMapping = toMeta.getTargetSourceMapping();
          for ( Entry<TargetTransformAttribute, SourceTransformField> entry : sourceMapping.entrySet() ) {
            SourceTransformField value = entry.getValue();
            if ( value.getTransformName() != null && value.getTransformName().equals( oldMeta.getName() ) ) {
              value.setTransformName( newMeta.getName() );
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
