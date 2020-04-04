/*! ******************************************************************************
 *
 * Hop : The Hop Orchestration Platform
 *
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

package org.apache.hop.pipeline.transforms.simplemapping;

import org.apache.commons.lang.StringUtils;
import org.apache.hop.core.CheckResult;
import org.apache.hop.core.CheckResultInterface;
import org.apache.hop.core.Const;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.exception.HopTransformException;
import org.apache.hop.core.exception.HopXMLException;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.core.variables.iVariables;
import org.apache.hop.core.xml.XMLHandler;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.metastore.api.IMetaStore;
import org.apache.hop.resource.ResourceEntry;
import org.apache.hop.resource.ResourceEntry.ResourceType;
import org.apache.hop.resource.ResourceReference;
import org.apache.hop.pipeline.ISubPipelineAwareMeta;
import org.apache.hop.pipeline.TransformWithMappingMeta;
import org.apache.hop.pipeline.Pipeline;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.PipelineMeta.PipelineType;
import org.apache.hop.pipeline.transform.ITransformData;
import org.apache.hop.pipeline.transform.TransformIOMeta;
import org.apache.hop.pipeline.transform.TransformIOMetaInterface;
import org.apache.hop.pipeline.transform.ITransform;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.apache.hop.pipeline.transform.TransformMetaInterface;
import org.apache.hop.pipeline.transforms.mapping.MappingIODefinition;
import org.apache.hop.pipeline.transforms.mapping.MappingParameters;
import org.apache.hop.pipeline.transforms.mapping.MappingValueRename;
import org.apache.hop.pipeline.transforms.mappinginput.MappingInputMeta;
import org.apache.hop.pipeline.transforms.mappingoutput.MappingOutputMeta;
import org.w3c.dom.Node;

import java.util.ArrayList;
import java.util.List;

/**
 * Meta-data for the Mapping transform: contains name of the (sub-) pipeline to execute
 *
 * @author Matt
 * @since 22-nov-2005
 */

public class SimpleMappingMeta extends TransformWithMappingMeta implements TransformMetaInterface, ISubPipelineAwareMeta {

  private static Class<?> PKG = SimpleMappingMeta.class; // for i18n purposes, needed by Translator!!

  private MappingIODefinition inputMapping;
  private MappingIODefinition outputMapping;
  private MappingParameters mappingParameters;

  private IMetaStore metaStore;

  public SimpleMappingMeta() {
    super(); // allocate BaseTransformMeta

    inputMapping = new MappingIODefinition();
    outputMapping = new MappingIODefinition();

    mappingParameters = new MappingParameters();
  }

  public void loadXML( Node transformNode, IMetaStore metaStore ) throws HopXMLException {
    try {
      fileName = XMLHandler.getTagValue( transformNode, "filename" );

      Node mappingsNode = XMLHandler.getSubNode( transformNode, "mappings" );

      if ( mappingsNode == null ) {
        throw new HopXMLException( "Unable to find <mappings> element in the transform XML" );
      }

      // Read all the input mapping definitions...
      //
      Node inputNode = XMLHandler.getSubNode( mappingsNode, "input" );
      Node mappingNode = XMLHandler.getSubNode( inputNode, MappingIODefinition.XML_TAG );
      if ( mappingNode != null ) {
        inputMapping = new MappingIODefinition( mappingNode );
      } else {
        inputMapping = new MappingIODefinition(); // empty
      }
      Node outputNode = XMLHandler.getSubNode( mappingsNode, "output" );
      mappingNode = XMLHandler.getSubNode( outputNode, MappingIODefinition.XML_TAG );
      if ( mappingNode != null ) {
        outputMapping = new MappingIODefinition( mappingNode );
      } else {
        outputMapping = new MappingIODefinition(); // empty
      }

      // Load the mapping parameters too..
      //
      Node mappingParametersNode = XMLHandler.getSubNode( mappingsNode, MappingParameters.XML_TAG );
      mappingParameters = new MappingParameters( mappingParametersNode );

    } catch ( Exception e ) {
      throw new HopXMLException( BaseMessages.getString(
        PKG, "SimpleMappingMeta.Exception.ErrorLoadingPipelineTransformFromXML" ), e );
    }
  }

  public Object clone() {
    Object retval = super.clone();
    return retval;
  }

  public String getXML() {
    StringBuilder retval = new StringBuilder( 300 );

    retval.append( "    " ).append( XMLHandler.addTagValue( "filename", fileName ) );

    retval.append( "    " ).append( XMLHandler.openTag( "mappings" ) ).append( Const.CR );

    retval.append( "      " ).append( XMLHandler.openTag( "input" ) ).append( Const.CR );
    retval.append( inputMapping.getXML() );
    retval.append( "      " ).append( XMLHandler.closeTag( "input" ) ).append( Const.CR );

    retval.append( "      " ).append( XMLHandler.openTag( "output" ) ).append( Const.CR );
    retval.append( outputMapping.getXML() );
    retval.append( "      " ).append( XMLHandler.closeTag( "output" ) ).append( Const.CR );

    // Add the mapping parameters too
    //
    retval.append( "      " ).append( mappingParameters.getXML() ).append( Const.CR );

    retval.append( "    " ).append( XMLHandler.closeTag( "mappings" ) ).append( Const.CR );

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
                         iVariables variables, IMetaStore metaStore ) throws HopTransformException {
    // First load some interesting data...

    // Then see which fields get added to the row.
    //
    PipelineMeta mappingPipelineMeta = null;
    try {
      mappingPipelineMeta =
        loadMappingMeta( this, metaStore, variables, mappingParameters.isInheritingAllVariables() );
    } catch ( HopException e ) {
      throw new HopTransformException( BaseMessages.getString(
        PKG, "SimpleMappingMeta.Exception.UnableToLoadMappingPipeline" ), e );
    }

    // The field structure may depend on the input parameters as well (think of parameter replacements in MDX queries
    // for instance)
    if ( mappingParameters != null && mappingPipelineMeta != null ) {

      // Just set the variables in the pipeline statically.
      // This just means: set a number of variables or parameter values:
      //
      TransformWithMappingMeta.activateParams( mappingPipelineMeta, mappingPipelineMeta, variables, mappingPipelineMeta.listParameters(),
        mappingParameters.getVariable(), mappingParameters.getInputField(), mappingParameters.isInheritingAllVariables() );
    }

    // Keep track of all the fields that need renaming...
    //
    List<MappingValueRename> inputRenameList = new ArrayList<MappingValueRename>();

    //
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
          throw new HopTransformException( BaseMessages.getString(
            PKG, "SimpleMappingMeta.Exception.UnableToFindField", valueRename.getSourceValueName() ) );
        }
        valueMeta.setName( valueRename.getTargetValueName() );
      }
    }

    // What is this mapping input transform?
    //
    TransformMeta mappingInputTransform = mappingPipelineMeta.findMappingInputTransform( null );

    // We're certain it's a MappingInput transform...
    //
    MappingInputMeta mappingInputMeta = (MappingInputMeta) mappingInputTransform.getTransformMetaInterface();

    // Inform the mapping input transform about what it's going to receive...
    //
    mappingInputMeta.setInputRowMeta( inputRowMeta );

    // What values are we changing names for: already done!
    //
    mappingInputMeta.setValueRenames( null );

    // Keep a list of the input rename values that need to be changed back at
    // the output
    //
    if ( inputMapping.isRenamingOnOutput() ) {
      SimpleMapping.addInputRenames( inputRenameList, inputMapping.getValueRenames() );
    }

    TransformMeta mappingOutputTransform = mappingPipelineMeta.findMappingOutputTransform( null );

    // We know it's a mapping output transform...
    MappingOutputMeta mappingOutputMeta = (MappingOutputMeta) mappingOutputTransform.getTransformMetaInterface();

    // Change a few columns.
    mappingOutputMeta.setOutputValueRenames( outputMapping.getValueRenames() );

    // Perhaps we need to change a few input columns back to the original?
    //
    mappingOutputMeta.setInputValueRenames( inputRenameList );

    // Now we know wat's going to come out of there...
    // This is going to be the full row, including all the remapping, etc.
    //
    IRowMeta mappingOutputRowMeta = mappingPipelineMeta.getTransformFields( mappingOutputTransform );

    row.clear();
    row.addRowMeta( mappingOutputRowMeta );
  }

  public String[] getInfoTransforms() {
    return null;
  }

  public String[] getTargetTransforms() {
    return null;
  }


  public void check( List<CheckResultInterface> remarks, PipelineMeta pipelineMeta, TransformMeta transformMeta,
                     IRowMeta prev, String[] input, String[] output, IRowMeta info, iVariables variables,
                     IMetaStore metaStore ) {
    CheckResult cr;
    if ( prev == null || prev.size() == 0 ) {
      cr =
        new CheckResult( CheckResultInterface.TYPE_RESULT_WARNING, BaseMessages.getString(
          PKG, "SimpleMappingMeta.CheckResult.NotReceivingAnyFields" ), transformMeta );
      remarks.add( cr );
    } else {
      cr =
        new CheckResult( CheckResultInterface.TYPE_RESULT_OK, BaseMessages.getString(
          PKG, "SimpleMappingMeta.CheckResult.TransformReceivingFields", prev.size() + "" ), transformMeta );
      remarks.add( cr );
    }

    // See if we have input streams leading to this transform!
    if ( input.length > 0 ) {
      cr =
        new CheckResult( CheckResultInterface.TYPE_RESULT_OK, BaseMessages.getString(
          PKG, "SimpleMappingMeta.CheckResult.TransformReceivingFieldsFromOtherTransforms" ), transformMeta );
      remarks.add( cr );
    } else {
      cr =
        new CheckResult( CheckResultInterface.TYPE_RESULT_ERROR, BaseMessages.getString(
          PKG, "SimpleMappingMeta.CheckResult.NoInputReceived" ), transformMeta );
      remarks.add( cr );
    }
  }

  public ITransform getTransform( TransformMeta transformMeta, ITransformData data, int cnr, PipelineMeta tr,
                                Pipeline pipeline ) {
    return new SimpleMapping( transformMeta, this, data, cnr, tr, pipeline );
  }

  public ITransformData getTransformData() {
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

  @Override
  public List<ResourceReference> getResourceDependencies( PipelineMeta pipelineMeta, TransformMeta transformInfo ) {
    List<ResourceReference> references = new ArrayList<ResourceReference>( 5 );
    String realFilename = pipelineMeta.environmentSubstitute( fileName );
    ResourceReference reference = new ResourceReference( transformInfo );
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
  public TransformIOMetaInterface getTransformIOMeta() {
    TransformIOMetaInterface ioMeta = super.getTransformIOMeta( false );
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
   * @return The objects referenced in the transform, like a mapping, a pipeline, a job, ...
   */
  public String[] getReferencedObjectDescriptions() {
    return new String[] { BaseMessages.getString( PKG, "SimpleMappingMeta.ReferencedObject.Description" ), };
  }

  private boolean isMapppingDefined() {
    return StringUtils.isNotEmpty( fileName );
  }

  public boolean[] isReferencedObjectEnabled() {
    return new boolean[] { isMapppingDefined(), };
  }

  @Deprecated
  public Object loadReferencedObject( int index, iVariables variables ) throws HopException {
    return loadReferencedObject( index, null, variables );
  }

  /**
   * Load the referenced object
   *
   * @param index     the object index to load
   * @param metaStore the MetaStore to use
   * @param variables     the variable space to use
   * @return the referenced object once loaded
   * @throws HopException
   */
  public Object loadReferencedObject( int index, IMetaStore metaStore, iVariables variables ) throws HopException {
    return loadMappingMeta( this, metaStore, variables );
  }

  public IMetaStore getMetaStore() {
    return metaStore;
  }

  public void setMetaStore( IMetaStore metaStore ) {
    this.metaStore = metaStore;
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

  @Override
  public List<MappingIODefinition> getInputMappings() {
    final List<MappingIODefinition> inputMappings = new ArrayList();
    inputMappings.add( inputMapping );
    return inputMappings;
  }

  @Override
  public List<MappingIODefinition> getOutputMappings() {
    final List<MappingIODefinition> outputMappings = new ArrayList();
    outputMappings.add( outputMapping );
    return outputMappings;
  }
}
