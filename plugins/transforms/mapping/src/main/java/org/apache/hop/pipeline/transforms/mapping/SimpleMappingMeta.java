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
import org.apache.hop.core.CheckResult;
import org.apache.hop.core.Const;
import org.apache.hop.core.ICheckResult;
import org.apache.hop.core.annotations.Transform;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.exception.HopTransformException;
import org.apache.hop.core.exception.HopXmlException;
import org.apache.hop.core.file.IHasFilename;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.core.xml.XmlHandler;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.metadata.api.IHopMetadataProvider;
import org.apache.hop.pipeline.ISubPipelineAwareMeta;
import org.apache.hop.pipeline.Pipeline;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.PipelineMeta.PipelineType;
import org.apache.hop.pipeline.TransformWithMappingMeta;
import org.apache.hop.pipeline.transform.ITransformIOMeta;
import org.apache.hop.pipeline.transform.ITransformMeta;
import org.apache.hop.pipeline.transform.TransformIOMeta;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.apache.hop.pipeline.transforms.input.MappingInputMeta;
import org.apache.hop.resource.ResourceEntry;
import org.apache.hop.resource.ResourceEntry.ResourceType;
import org.apache.hop.resource.ResourceReference;
import org.w3c.dom.Node;

import java.util.ArrayList;
import java.util.List;

/**
 * Meta-data for the Mapping transform: contains name of the (sub-) pipeline to execute
 *
 * @author Matt
 * @since 22-nov-2005
 */

@Transform(
  id = "SimpleMapping",
  name = "i18n::BaseTransform.TypeLongDesc.SimpleMapping",
  description = "i18n::BaseTransform.TypeTooltipDesc.SimpleMapping",
  image = "MAP.svg",
  categoryDescription = "i18n:org.apache.hop.pipeline.transform:BaseTransform.Category.Mapping"
)
public class SimpleMappingMeta extends TransformWithMappingMeta<SimpleMapping, SimpleMappingData> implements ITransformMeta<SimpleMapping, SimpleMappingData>, ISubPipelineAwareMeta {

  private static final Class<?> PKG = SimpleMappingMeta.class; // For Translator

  private MappingIODefinition inputMapping;
  private MappingIODefinition outputMapping;
  private MappingParameters mappingParameters;

  private IHopMetadataProvider metadataProvider;

  public SimpleMappingMeta() {
    super(); // allocate BaseTransformMeta

    inputMapping = new MappingIODefinition();
    outputMapping = new MappingIODefinition();

    mappingParameters = new MappingParameters();
  }

  public void loadXml( Node transformNode, IHopMetadataProvider metadataProvider ) throws HopXmlException {
    try {
      filename = XmlHandler.getTagValue( transformNode, "filename" );

      Node mappingsNode = XmlHandler.getSubNode( transformNode, "mappings" );

      if ( mappingsNode == null ) {
        throw new HopXmlException( "Unable to find <mappings> element in the transform XML" );
      }

      // Read all the input mapping definitions...
      //
      Node inputNode = XmlHandler.getSubNode( mappingsNode, "input" );
      Node mappingNode = XmlHandler.getSubNode( inputNode, MappingIODefinition.XML_TAG );
      if ( mappingNode != null ) {
        inputMapping = new MappingIODefinition( mappingNode );
      } else {
        inputMapping = new MappingIODefinition(); // empty
      }
      Node outputNode = XmlHandler.getSubNode( mappingsNode, "output" );
      mappingNode = XmlHandler.getSubNode( outputNode, MappingIODefinition.XML_TAG );
      if ( mappingNode != null ) {
        outputMapping = new MappingIODefinition( mappingNode );
      } else {
        outputMapping = new MappingIODefinition(); // empty
      }

      // Load the mapping parameters too..
      //
      Node mappingParametersNode = XmlHandler.getSubNode( mappingsNode, MappingParameters.XML_TAG );
      mappingParameters = new MappingParameters( mappingParametersNode );

    } catch ( Exception e ) {
      throw new HopXmlException( BaseMessages.getString(
        PKG, "SimpleMappingMeta.Exception.ErrorLoadingPipelineTransformFromXML" ), e );
    }
  }

  public Object clone() {
    Object retval = super.clone();
    return retval;
  }

  public String getXml() {
    StringBuilder retval = new StringBuilder( 300 );

    retval.append( "    " ).append( XmlHandler.addTagValue( "filename", filename ) );

    retval.append( "    " ).append( XmlHandler.openTag( "mappings" ) ).append( Const.CR );

    retval.append( "      " ).append( XmlHandler.openTag( "input" ) ).append( Const.CR );
    retval.append( inputMapping.getXml() );
    retval.append( "      " ).append( XmlHandler.closeTag( "input" ) ).append( Const.CR );

    retval.append( "      " ).append( XmlHandler.openTag( "output" ) ).append( Const.CR );
    retval.append( outputMapping.getXml() );
    retval.append( "      " ).append( XmlHandler.closeTag( "output" ) ).append( Const.CR );

    // Add the mapping parameters too
    //
    retval.append( "      " ).append( mappingParameters.getXml() ).append( Const.CR );

    retval.append( "    " ).append( XmlHandler.closeTag( "mappings" ) ).append( Const.CR );

    return retval.toString();
  }

  public void setDefault() {

    MappingIODefinition inputDefinition = new MappingIODefinition( null, null );
    inputDefinition.setMainDataPath( true );
    inputDefinition.setRenamingOnOutput( true );
    inputMapping = inputDefinition;

    MappingIODefinition outputDefinition = new MappingIODefinition( null, null );
    outputDefinition.setMainDataPath( true );
    outputMapping = outputDefinition;
  }

  public void getFields( IRowMeta row, String origin, IRowMeta[] info, TransformMeta nextTransform,
                         IVariables variables, IHopMetadataProvider metadataProvider ) throws HopTransformException {
    // First load some interesting data...

    // Then see which fields get added to the row.
    //
    PipelineMeta mappingPipelineMeta;
    try {
      mappingPipelineMeta = loadMappingMeta( this, metadataProvider, variables );
    } catch ( HopException e ) {
      throw new HopTransformException( BaseMessages.getString(
        PKG, "SimpleMappingMeta.Exception.UnableToLoadMappingPipeline" ), e );
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
    if ( !inputRowMeta.isEmpty() ) {
      for ( MappingValueRename valueRename : inputMapping.getValueRenames() ) {
        IValueMeta valueMeta = inputRowMeta.searchValueMeta( valueRename.getSourceValueName() );
        if ( valueMeta == null ) {
          throw new HopTransformException( BaseMessages.getString( PKG, "SimpleMappingMeta.Exception.UnableToFindField", valueRename.getSourceValueName() ) );
        }
        valueMeta.setName( valueRename.getTargetValueName() );
      }
    }

    // What is this mapping input transform?
    //
    TransformMeta mappingInputTransform = mappingPipelineMeta.findMappingInputTransform( null );
    TransformMeta mappingOutputTransform = mappingPipelineMeta.findMappingOutputTransform( null );

    // We're certain of these classes at least
    //
    MappingInputMeta mappingInputMeta = (MappingInputMeta) mappingInputTransform.getTransform();

    // Inform the mapping input transform about what it's going to receive...
    //
    mappingInputMeta.setInputRowMeta( inputRowMeta );

    // Now we know wat's going to come out of the mapping pipeline...
    // This is going to be the full row that's being written.
    //
    IRowMeta mappingOutputRowMeta = mappingPipelineMeta.getTransformFields( variables, mappingOutputTransform );

    // We're renaming some stuff back:
    //
    if (inputMapping.isRenamingOnOutput()) {
      for (MappingValueRename rename : inputMapping.getValueRenames()) {
        IValueMeta valueMeta = mappingOutputRowMeta.searchValueMeta( rename.getTargetValueName() );
        if (valueMeta!=null) {
          valueMeta.setName( rename.getSourceValueName() );
        }
      }
    }

    // Also rename output values back
    //
    for (MappingValueRename rename : outputMapping.getValueRenames()) {
      IValueMeta valueMeta = mappingOutputRowMeta.searchValueMeta( rename.getSourceValueName() );
      if (valueMeta!=null) {
        valueMeta.setName( rename.getTargetValueName() );
      }
    }

    row.clear();
    row.addRowMeta( mappingOutputRowMeta );
  }

  public String[] getInfoTransforms() {
    return null;
  }

  public String[] getTargetTransforms() {
    return null;
  }


  public void check( List<ICheckResult> remarks, PipelineMeta pipelineMeta, TransformMeta transformMeta,
                     IRowMeta prev, String[] input, String[] output, IRowMeta info, IVariables variables,
                     IHopMetadataProvider metadataProvider ) {
    CheckResult cr;
    if ( prev == null || prev.size() == 0 ) {
      cr =
        new CheckResult( ICheckResult.TYPE_RESULT_WARNING, BaseMessages.getString(
          PKG, "SimpleMappingMeta.CheckResult.NotReceivingAnyFields" ), transformMeta );
      remarks.add( cr );
    } else {
      cr =
        new CheckResult( ICheckResult.TYPE_RESULT_OK, BaseMessages.getString(
          PKG, "SimpleMappingMeta.CheckResult.TransformReceivingFields", prev.size() + "" ), transformMeta );
      remarks.add( cr );
    }

    // See if we have input streams leading to this transform!
    if ( input.length > 0 ) {
      cr =
        new CheckResult( ICheckResult.TYPE_RESULT_OK, BaseMessages.getString(
          PKG, "SimpleMappingMeta.CheckResult.TransformReceivingFieldsFromOtherTransforms" ), transformMeta );
      remarks.add( cr );
    } else {
      cr =
        new CheckResult( ICheckResult.TYPE_RESULT_ERROR, BaseMessages.getString(
          PKG, "SimpleMappingMeta.CheckResult.NoInputReceived" ), transformMeta );
      remarks.add( cr );
    }
  }

  public SimpleMapping createTransform( TransformMeta transformMeta, SimpleMappingData data, int cnr, PipelineMeta tr,
                                Pipeline pipeline ) {
    return new SimpleMapping( transformMeta, this, data, cnr, tr, pipeline );
  }

  public SimpleMappingData getTransformData() {
    return new SimpleMappingData();
  }

  /**
   * @return the mappingParameters
   */
  public MappingParameters getMappingParameters() {
    return mappingParameters;
  }

  /**
   * @param mappingParameters the mappingParameters to set
   */
  public void setMappingParameters( MappingParameters mappingParameters ) {
    this.mappingParameters = mappingParameters;
  }

  @Override public List<ResourceReference> getResourceDependencies( IVariables variables, TransformMeta transformMeta ) {
    List<ResourceReference> references = new ArrayList<>( 5 );
    String realFilename = variables.resolve( filename );
    ResourceReference reference = new ResourceReference( transformMeta );
    references.add( reference );

    if ( StringUtils.isNotEmpty( realFilename ) ) {
      // Add the filename to the references, including a reference to this transform
      // meta data.
      //
      reference.getEntries().add( new ResourceEntry( realFilename, ResourceType.ACTIONFILE ) );
    }
    return references;
  }

  @Override
  public ITransformIOMeta getTransformIOMeta() {
    ITransformIOMeta ioMeta = super.getTransformIOMeta( false );
    if ( ioMeta == null ) {
      ioMeta = new TransformIOMeta( true, true, false, false, false, false );
      setTransformIOMeta( ioMeta );
    }
    return ioMeta;
  }

  public boolean excludeFromRowLayoutVerification() {
    return false;
  }

  @Override
  public void searchInfoAndTargetTransforms( List<TransformMeta> transforms ) {
  }

  public PipelineType[] getSupportedPipelineTypes() {
    return new PipelineType[] { PipelineType.Normal, };
  }

  /**
   * @return The objects referenced in the transform, like a mapping, a pipeline, a workflow, ...
   */
  public String[] getReferencedObjectDescriptions() {
    return new String[] { BaseMessages.getString( PKG, "SimpleMappingMeta.ReferencedObject.Description" ), };
  }

  private boolean isMapppingDefined() {
    return StringUtils.isNotEmpty( filename );
  }

  public boolean[] isReferencedObjectEnabled() {
    return new boolean[] { isMapppingDefined(), };
  }

  @Deprecated
  public IHasFilename loadReferencedObject( int index, IVariables variables ) throws HopException {
    return loadReferencedObject( index, null, variables );
  }

  /**
   * Load the referenced object
   *
   * @param index     the object index to load
   * @param metadataProvider the MetaStore to use
   * @param variables     the variable variables to use
   * @return the referenced object once loaded
   * @throws HopException
   */
  public IHasFilename loadReferencedObject( int index, IHopMetadataProvider metadataProvider, IVariables variables ) throws HopException {
    return loadMappingMeta( this, metadataProvider, variables );
  }

  public IHopMetadataProvider getMetadataProvider() {
    return metadataProvider;
  }

  public void setMetadataProvider( IHopMetadataProvider metadataProvider ) {
    this.metadataProvider = metadataProvider;
  }

  public MappingIODefinition getInputMapping() {
    return inputMapping;
  }

  public void setInputMapping( MappingIODefinition inputMapping ) {
    this.inputMapping = inputMapping;
  }

  public MappingIODefinition getOutputMapping() {
    return outputMapping;
  }

  public void setOutputMapping( MappingIODefinition outputMapping ) {
    this.outputMapping = outputMapping;
  }

}
