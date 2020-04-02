/*! ******************************************************************************
 *
 * Pentaho Data Integration
 *
 * Copyright (C) 2002-2018 by Hitachi Vantara : http://www.pentaho.com
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

package org.apache.hop.pipeline.transforms.mapping;

import org.apache.commons.lang.StringUtils;
import org.apache.hop.core.CheckResult;
import org.apache.hop.core.CheckResultInterface;
import org.apache.hop.core.Const;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.exception.HopTransformException;
import org.apache.hop.core.exception.HopXMLException;
import org.apache.hop.core.parameters.UnknownParamException;
import org.apache.hop.core.row.RowMetaInterface;
import org.apache.hop.core.row.ValueMetaInterface;
import org.apache.hop.core.util.Utils;
import org.apache.hop.core.variables.VariableSpace;
import org.apache.hop.core.xml.XMLHandler;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.metastore.api.IMetaStore;
import org.apache.hop.pipeline.ISubPipelineAwareMeta;
import org.apache.hop.pipeline.Pipeline;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.PipelineMeta.PipelineType;
import org.apache.hop.pipeline.TransformWithMappingMeta;
import org.apache.hop.pipeline.transform.TransformIOMeta;
import org.apache.hop.pipeline.transform.TransformIOMetaInterface;
import org.apache.hop.pipeline.transform.TransformInterface;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.apache.hop.pipeline.transform.TransformMetaInterface;
import org.apache.hop.pipeline.transform.errorhandling.Stream;
import org.apache.hop.pipeline.transform.errorhandling.StreamIcon;
import org.apache.hop.pipeline.transform.errorhandling.StreamInterface.StreamType;
import org.apache.hop.pipeline.transforms.mappinginput.MappingInputMeta;
import org.apache.hop.pipeline.transforms.mappingoutput.MappingOutputMeta;
import org.apache.hop.resource.ResourceEntry;
import org.apache.hop.resource.ResourceEntry.ResourceType;
import org.apache.hop.resource.ResourceReference;
import org.w3c.dom.Node;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Meta-data for the Mapping transform: contains name of the (sub-) pipeline to execute
 *
 * @author Matt
 * @since 22-nov-2005
 */

public class MappingMeta extends TransformWithMappingMeta<Mapping,MappingData> implements TransformMetaInterface<Mapping, MappingData>,
  ISubPipelineAwareMeta {

  private static Class<?> PKG = MappingMeta.class;
  private List<MappingIODefinition> inputMappings;
  private List<MappingIODefinition> outputMappings;
  private MappingParameters mappingParameters;

  private boolean allowingMultipleInputs;
  private boolean allowingMultipleOutputs;

  private IMetaStore metaStore;

  public MappingMeta() {
    super(); // allocate BaseTransformMeta

    inputMappings = new ArrayList<MappingIODefinition>();
    outputMappings = new ArrayList<MappingIODefinition>();
    mappingParameters = new MappingParameters();
  }

  public void loadXML( Node transformNode, IMetaStore metaStore ) throws HopXMLException {
    try {
      fileName = XMLHandler.getTagValue( transformNode, "filename" );

      Node mappingsNode = XMLHandler.getSubNode( transformNode, "mappings" );
      inputMappings.clear();
      outputMappings.clear();

      if ( mappingsNode != null ) {
        // Read all the input mapping definitions...
        //
        Node inputNode = XMLHandler.getSubNode( mappingsNode, "input" );
        int nrInputMappings = XMLHandler.countNodes( inputNode, MappingIODefinition.XML_TAG );
        for ( int i = 0; i < nrInputMappings; i++ ) {
          Node mappingNode = XMLHandler.getSubNodeByNr( inputNode, MappingIODefinition.XML_TAG, i );
          MappingIODefinition inputMappingDefinition = new MappingIODefinition( mappingNode );
          inputMappings.add( inputMappingDefinition );
        }
        Node outputNode = XMLHandler.getSubNode( mappingsNode, "output" );
        int nrOutputMappings = XMLHandler.countNodes( outputNode, MappingIODefinition.XML_TAG );
        for ( int i = 0; i < nrOutputMappings; i++ ) {
          Node mappingNode = XMLHandler.getSubNodeByNr( outputNode, MappingIODefinition.XML_TAG, i );
          MappingIODefinition outputMappingDefinition = new MappingIODefinition( mappingNode );
          outputMappings.add( outputMappingDefinition );
        }

        // Load the mapping parameters too..
        //
        Node mappingParametersNode = XMLHandler.getSubNode( mappingsNode, MappingParameters.XML_TAG );
        mappingParameters = new MappingParameters( mappingParametersNode );
      } else {
        // backward compatibility...
        //
        Node inputNode = XMLHandler.getSubNode( transformNode, "input" );
        Node outputNode = XMLHandler.getSubNode( transformNode, "output" );

        int nrInput = XMLHandler.countNodes( inputNode, "connector" );
        int nrOutput = XMLHandler.countNodes( outputNode, "connector" );

        // null means: auto-detect
        //
        MappingIODefinition inputMappingDefinition = new MappingIODefinition();
        inputMappingDefinition.setMainDataPath( true );

        for ( int i = 0; i < nrInput; i++ ) {
          Node inputConnector = XMLHandler.getSubNodeByNr( inputNode, "connector", i );
          String inputField = XMLHandler.getTagValue( inputConnector, "field" );
          String inputMapping = XMLHandler.getTagValue( inputConnector, "mapping" );
          inputMappingDefinition.getValueRenames().add( new MappingValueRename( inputField, inputMapping ) );
        }

        // null means: auto-detect
        //
        MappingIODefinition outputMappingDefinition = new MappingIODefinition();
        outputMappingDefinition.setMainDataPath( true );

        for ( int i = 0; i < nrOutput; i++ ) {
          Node outputConnector = XMLHandler.getSubNodeByNr( outputNode, "connector", i );
          String outputField = XMLHandler.getTagValue( outputConnector, "field" );
          String outputMapping = XMLHandler.getTagValue( outputConnector, "mapping" );
          outputMappingDefinition.getValueRenames().add( new MappingValueRename( outputMapping, outputField ) );
        }

        // Don't forget to add these to the input and output mapping
        // definitions...
        //
        inputMappings.add( inputMappingDefinition );
        outputMappings.add( outputMappingDefinition );

        // The default is to have no mapping parameters: the concept didn't
        // exist before.
        //
        mappingParameters = new MappingParameters();

      }

      String multiInput = XMLHandler.getTagValue( transformNode, "allow_multiple_input" );
      allowingMultipleInputs =
        Utils.isEmpty( multiInput ) ? inputMappings.size() > 1 : "Y".equalsIgnoreCase( multiInput );
      String multiOutput = XMLHandler.getTagValue( transformNode, "allow_multiple_output" );
      allowingMultipleOutputs =
        Utils.isEmpty( multiOutput ) ? outputMappings.size() > 1 : "Y".equalsIgnoreCase( multiOutput );

    } catch ( Exception e ) {
      throw new HopXMLException( BaseMessages.getString(
        PKG, "MappingMeta.Exception.ErrorLoadingPipelineTransformFromXML" ), e );
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
    for ( int i = 0; i < inputMappings.size(); i++ ) {
      retval.append( inputMappings.get( i ).getXML() );
    }
    retval.append( "      " ).append( XMLHandler.closeTag( "input" ) ).append( Const.CR );

    retval.append( "      " ).append( XMLHandler.openTag( "output" ) ).append( Const.CR );
    for ( int i = 0; i < outputMappings.size(); i++ ) {
      retval.append( outputMappings.get( i ).getXML() );
    }
    retval.append( "      " ).append( XMLHandler.closeTag( "output" ) ).append( Const.CR );

    // Add the mapping parameters too
    //
    retval.append( "      " ).append( mappingParameters.getXML() ).append( Const.CR );

    retval.append( "    " ).append( XMLHandler.closeTag( "mappings" ) ).append( Const.CR );

    retval.append( "    " ).append( XMLHandler.addTagValue( "allow_multiple_input", allowingMultipleInputs ) );
    retval.append( "    " ).append( XMLHandler.addTagValue( "allow_multiple_output", allowingMultipleOutputs ) );

    return retval.toString();
  }

  public void setDefault() {

    MappingIODefinition inputDefinition = new MappingIODefinition( null, null );
    inputDefinition.setMainDataPath( true );
    inputDefinition.setRenamingOnOutput( true );
    inputMappings.add( inputDefinition );
    MappingIODefinition outputDefinition = new MappingIODefinition( null, null );
    outputDefinition.setMainDataPath( true );
    outputMappings.add( outputDefinition );

    allowingMultipleInputs = false;
    allowingMultipleOutputs = false;
  }

  public void getFields( RowMetaInterface row, String origin, RowMetaInterface[] info, TransformMeta nextTransform,
                         VariableSpace space, IMetaStore metaStore ) throws HopTransformException {
    // First load some interesting data...

    // Then see which fields get added to the row.
    //
    PipelineMeta mappingPipelineMeta = null;
    try {
      mappingPipelineMeta = loadMappingMeta( this, metaStore, space );
    } catch ( HopException e ) {
      throw new HopTransformException( BaseMessages.getString(
        PKG, "MappingMeta.Exception.UnableToLoadMappingPipeline" ), e );
    }

    // The field structure may depend on the input parameters as well (think of parameter replacements in MDX queries
    // for instance)
    if ( mappingParameters != null ) {

      // See if we need to pass all variables from the parent or not...
      //
      if ( mappingParameters.isInheritingAllVariables() ) {
        mappingPipelineMeta.copyVariablesFrom( space );
      }

      // Just set the variables in the pipeline statically.
      // This just means: set a number of variables or parameter values:
      //
      List<String> subParams = Arrays.asList( mappingPipelineMeta.listParameters() );

      for ( int i = 0; i < mappingParameters.getVariable().length; i++ ) {
        String name = mappingParameters.getVariable()[ i ];
        String value = space.environmentSubstitute( mappingParameters.getInputField()[ i ] );
        if ( !Utils.isEmpty( name ) && !Utils.isEmpty( value ) ) {
          if ( subParams.contains( name ) ) {
            try {
              mappingPipelineMeta.setParameterValue( name, value );
            } catch ( UnknownParamException e ) {
              // this is explicitly checked for up front
            }
          }
          mappingPipelineMeta.setVariable( name, value );

        }
      }
    }

    // Keep track of all the fields that need renaming...
    //
    List<MappingValueRename> inputRenameList = new ArrayList<MappingValueRename>();

    /*
     * Before we ask the mapping outputs anything, we should teach the mapping input transforms in the sub-pipeline
     * about the data coming in...
     */
    for ( MappingIODefinition definition : inputMappings ) {

      RowMetaInterface inputRowMeta;

      if ( definition.isMainDataPath() || Utils.isEmpty( definition.getInputTransformName() ) ) {
        // The row metadata, what we pass to the mapping input transform
        // definition.getOutputTransform(), is "row"
        // However, we do need to re-map some fields...
        //
        inputRowMeta = row.clone();
        if ( !inputRowMeta.isEmpty() ) {
          for ( MappingValueRename valueRename : definition.getValueRenames() ) {
            ValueMetaInterface valueMeta = inputRowMeta.searchValueMeta( valueRename.getSourceValueName() );
            if ( valueMeta == null ) {
              throw new HopTransformException( BaseMessages.getString(
                PKG, "MappingMeta.Exception.UnableToFindField", valueRename.getSourceValueName() ) );
            }
            valueMeta.setName( valueRename.getTargetValueName() );
          }
        }
      } else {
        // The row metadata that goes to the info mapping input comes from the
        // specified transform
        // In fact, it's one of the info transforms that is going to contain this
        // information...
        //
        String[] infoTransforms = getInfoTransforms();
        int infoTransformIndex = Const.indexOfString( definition.getInputTransformName(), infoTransforms );
        if ( infoTransformIndex < 0 ) {
          throw new HopTransformException( BaseMessages.getString(
            PKG, "MappingMeta.Exception.UnableToFindMetadataInfo", definition.getInputTransformName() ) );
        }
        if ( info[ infoTransformIndex ] != null ) {
          inputRowMeta = info[ infoTransformIndex ].clone();
        } else {
          inputRowMeta = null;
        }
      }

      // What is this mapping input transform?
      //
      TransformMeta mappingInputTransform = mappingPipelineMeta.findMappingInputTransform( definition.getOutputTransformName() );

      // We're certain it's a MappingInput transform...
      //
      MappingInputMeta mappingInputMeta = (MappingInputMeta) mappingInputTransform.getTransformMetaInterface();

      // Inform the mapping input transform about what it's going to receive...
      //
      mappingInputMeta.setInputRowMeta( inputRowMeta );

      // What values are we changing names for?
      //
      mappingInputMeta.setValueRenames( definition.getValueRenames() );

      // Keep a list of the input rename values that need to be changed back at
      // the output
      //
      if ( definition.isRenamingOnOutput() ) {
        Mapping.addInputRenames( inputRenameList, definition.getValueRenames() );
      }
    }

    // All the mapping transforms now know what they will be receiving.
    // That also means that the sub- pipeline / mapping has everything it
    // needs.
    // So that means that the MappingOutput transforms know exactly what the output
    // is going to be.
    // That could basically be anything.
    // It also could have absolutely no resemblance to what came in on the
    // input.
    // The relative old approach is therefore no longer suited.
    //
    // OK, but what we *can* do is have the MappingOutput transform rename the
    // appropriate fields.
    // The mapping transform will tell this transform how it's done.
    //
    // Let's look for the mapping output transform that is relevant for this actual
    // call...
    //
    MappingIODefinition mappingOutputDefinition = null;
    if ( nextTransform == null ) {
      // This is the main transform we read from...
      // Look up the main transform to write to.
      // This is the output mapping definition with "main path" enabled.
      //
      for ( MappingIODefinition definition : outputMappings ) {
        if ( definition.isMainDataPath() || Utils.isEmpty( definition.getOutputTransformName() ) ) {
          // This is the definition to use...
          //
          mappingOutputDefinition = definition;
        }
      }
    } else {
      // Is there an output mapping definition for this transform?
      // If so, we can look up the Mapping output transform to see what has changed.
      //

      for ( MappingIODefinition definition : outputMappings ) {
        if ( nextTransform.getName().equals( definition.getOutputTransformName() )
          || definition.isMainDataPath() || Utils.isEmpty( definition.getOutputTransformName() ) ) {
          mappingOutputDefinition = definition;
        }
      }
    }

    if ( mappingOutputDefinition == null ) {
      throw new HopTransformException( BaseMessages.getString(
        PKG, "MappingMeta.Exception.UnableToFindMappingDefinition" ) );
    }

    // OK, now find the mapping output transform in the mapping...
    // This method in PipelineMeta takes into account a number of things, such as
    // the transform not specified, etc.
    // The method never returns null but throws an exception.
    //
    TransformMeta mappingOutputTransform =
      mappingPipelineMeta.findMappingOutputTransform( mappingOutputDefinition.getInputTransformName() );

    // We know it's a mapping output transform...
    MappingOutputMeta mappingOutputMeta = (MappingOutputMeta) mappingOutputTransform.getTransformMetaInterface();

    // Change a few columns.
    mappingOutputMeta.setOutputValueRenames( mappingOutputDefinition.getValueRenames() );

    // Perhaps we need to change a few input columns back to the original?
    //
    mappingOutputMeta.setInputValueRenames( inputRenameList );

    // Now we know wat's going to come out of there...
    // This is going to be the full row, including all the remapping, etc.
    //
    RowMetaInterface mappingOutputRowMeta = mappingPipelineMeta.getTransformFields( mappingOutputTransform );

    row.clear();
    row.addRowMeta( mappingOutputRowMeta );
  }

  public String[] getInfoTransforms() {
    String[] infoTransforms = getTransformIOMeta().getInfoTransformNames();
    // Return null instead of empty array to preserve existing behavior
    return infoTransforms.length == 0 ? null : infoTransforms;
  }

  public String[] getTargetTransforms() {

    List<String> targetTransforms = new ArrayList<>();
    // The info transforms are those transforms that are specified in the input mappings
    for ( MappingIODefinition definition : outputMappings ) {
      if ( !definition.isMainDataPath() && !Utils.isEmpty( definition.getOutputTransformName() ) ) {
        targetTransforms.add( definition.getOutputTransformName() );
      }
    }
    if ( targetTransforms.isEmpty() ) {
      return null;
    }

    return targetTransforms.toArray( new String[ targetTransforms.size() ] );
  }


  @Deprecated
  public static final synchronized PipelineMeta loadMappingMeta( MappingMeta mappingMeta,
                                                                 VariableSpace space ) throws HopException {
    return loadMappingMeta( mappingMeta, null, space );
  }

  public void check( List<CheckResultInterface> remarks, PipelineMeta pipelineMeta, TransformMeta transformMeta,
                     RowMetaInterface prev, String[] input, String[] output, RowMetaInterface info, VariableSpace space,
                     IMetaStore metaStore ) {
    CheckResult cr;
    if ( prev == null || prev.size() == 0 ) {
      cr =
        new CheckResult( CheckResultInterface.TYPE_RESULT_WARNING, BaseMessages.getString(
          PKG, "MappingMeta.CheckResult.NotReceivingAnyFields" ), transformMeta );
      remarks.add( cr );
    } else {
      cr =
        new CheckResult( CheckResultInterface.TYPE_RESULT_OK, BaseMessages.getString(
          PKG, "MappingMeta.CheckResult.TransformReceivingFields", prev.size() + "" ), transformMeta );
      remarks.add( cr );
    }

    // See if we have input streams leading to this transform!
    if ( input.length > 0 ) {
      cr =
        new CheckResult( CheckResultInterface.TYPE_RESULT_OK, BaseMessages.getString(
          PKG, "MappingMeta.CheckResult.TransformReceivingFieldsFromOtherTransforms" ), transformMeta );
      remarks.add( cr );
    } else {
      cr =
        new CheckResult( CheckResultInterface.TYPE_RESULT_ERROR, BaseMessages.getString(
          PKG, "MappingMeta.CheckResult.NoInputReceived" ), transformMeta );
      remarks.add( cr );
    }
  }

  public TransformInterface createTransform( TransformMeta transformMeta, MappingData transformDataInterface, int cnr, PipelineMeta tr,
                                             Pipeline pipeline ) {
    return new Mapping( transformMeta, transformDataInterface, cnr, tr, pipeline );
  }

  public MappingData getTransformData() {
    return new MappingData();
  }

  @Override
  public List<MappingIODefinition> getInputMappings() {
    return inputMappings;
  }

  /**
   * @param inputMappings the inputMappings to set
   */
  public void setInputMappings( List<MappingIODefinition> inputMappings ) {
    this.inputMappings = inputMappings;
    resetTransformIoMeta();
  }

  @Override
  public List<MappingIODefinition> getOutputMappings() {
    return outputMappings;
  }

  /**
   * @param outputMappings the outputMappings to set
   */
  public void setOutputMappings( List<MappingIODefinition> outputMappings ) {
    this.outputMappings = outputMappings;
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
      // TODO Create a dynamic TransformIOMeta so that we can more easily manipulate the info streams?
      ioMeta = new TransformIOMeta( true, true, true, false, true, false );
      for ( MappingIODefinition def : inputMappings ) {
        if ( isInfoMapping( def ) ) {
          Stream stream =
            new Stream( StreamType.INFO, def.getInputTransform(), BaseMessages.getString(
              PKG, "MappingMeta.InfoStream.Description" ), StreamIcon.INFO, null );
          ioMeta.addStream( stream );
        }
      }
      setTransformIOMeta( ioMeta );
    }
    return ioMeta;
  }

  private boolean isInfoMapping( MappingIODefinition def ) {
    return !def.isMainDataPath() && !Utils.isEmpty( def.getInputTransformName() );
  }

  public boolean excludeFromRowLayoutVerification() {
    return true;
  }

  @Override
  public void searchInfoAndTargetTransforms( List<TransformMeta> transforms ) {
    // Assign all TransformMeta references for Input Mappings that are INFO inputs
    for ( MappingIODefinition def : inputMappings ) {
      if ( isInfoMapping( def ) ) {
        def.setInputTransform( TransformMeta.findTransform( transforms, def.getInputTransformName() ) );
      }
    }
  }

  public PipelineType[] getSupportedPipelineTypes() {
    return new PipelineType[] { PipelineType.Normal, };
  }

  /**
   * @return the allowingMultipleInputs
   */
  public boolean isAllowingMultipleInputs() {
    return allowingMultipleInputs;
  }

  /**
   * @param allowingMultipleInputs the allowingMultipleInputs to set
   */
  public void setAllowingMultipleInputs( boolean allowingMultipleInputs ) {
    this.allowingMultipleInputs = allowingMultipleInputs;
  }

  /**
   * @return the allowingMultipleOutputs
   */
  public boolean isAllowingMultipleOutputs() {
    return allowingMultipleOutputs;
  }

  /**
   * @param allowingMultipleOutputs the allowingMultipleOutputs to set
   */
  public void setAllowingMultipleOutputs( boolean allowingMultipleOutputs ) {
    this.allowingMultipleOutputs = allowingMultipleOutputs;
  }

  /**
   * @return The objects referenced in the transform, like a mapping, a pipeline, a job, ...
   */
  public String[] getReferencedObjectDescriptions() {
    return new String[] { BaseMessages.getString( PKG, "MappingMeta.ReferencedObject.Description" ), };
  }

  private boolean isMapppingDefined() {
    return StringUtils.isNotEmpty( fileName );
  }

  public boolean[] isReferencedObjectEnabled() {
    return new boolean[] { isMapppingDefined(), };
  }

  @Deprecated
  public Object loadReferencedObject( int index, VariableSpace space ) throws HopException {
    return loadReferencedObject( index, null, space );
  }

  /**
   * Load the referenced object
   *
   * @param index     the object index to load
   * @param metaStore the MetaStore to use
   * @param space     the variable space to use
   * @return the referenced object once loaded
   * @throws HopException
   */
  public Object loadReferencedObject( int index, IMetaStore metaStore, VariableSpace space ) throws HopException {
    return loadMappingMeta( this, metaStore, space );
  }

  public IMetaStore getMetaStore() {
    return metaStore;
  }

  public void setMetaStore( IMetaStore metaStore ) {
    this.metaStore = metaStore;
  }

}
