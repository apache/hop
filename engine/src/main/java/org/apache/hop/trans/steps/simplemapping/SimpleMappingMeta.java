/*! ******************************************************************************
 *
 * Pentaho Data Integration
 *
 * Copyright (C) 2002-2019 by Hitachi Vantara : http://www.pentaho.com
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

package org.apache.hop.trans.steps.simplemapping;

import org.apache.commons.lang.StringUtils;
import org.apache.hop.core.CheckResult;
import org.apache.hop.core.CheckResultInterface;
import org.apache.hop.core.Const;
import org.apache.hop.core.database.DatabaseMeta;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.exception.HopStepException;
import org.apache.hop.core.exception.HopXMLException;
import org.apache.hop.core.row.RowMetaInterface;
import org.apache.hop.core.row.ValueMetaInterface;
import org.apache.hop.core.util.Utils;
import org.apache.hop.core.variables.VariableSpace;
import org.apache.hop.core.xml.XMLHandler;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.resource.ResourceEntry;
import org.apache.hop.resource.ResourceEntry.ResourceType;
import org.apache.hop.resource.ResourceReference;
import org.apache.hop.trans.ISubTransAwareMeta;
import org.apache.hop.trans.StepWithMappingMeta;
import org.apache.hop.trans.Trans;
import org.apache.hop.trans.TransMeta;
import org.apache.hop.trans.TransMeta.TransformationType;
import org.apache.hop.trans.step.StepDataInterface;
import org.apache.hop.trans.step.StepIOMeta;
import org.apache.hop.trans.step.StepIOMetaInterface;
import org.apache.hop.trans.step.StepInterface;
import org.apache.hop.trans.step.StepMeta;
import org.apache.hop.trans.step.StepMetaInterface;
import org.apache.hop.trans.steps.mapping.MappingIODefinition;
import org.apache.hop.trans.steps.mapping.MappingParameters;
import org.apache.hop.trans.steps.mapping.MappingValueRename;
import org.apache.hop.trans.steps.mappinginput.MappingInputMeta;
import org.apache.hop.trans.steps.mappingoutput.MappingOutputMeta;
import org.apache.hop.metastore.api.IMetaStore;
import org.w3c.dom.Node;

import java.util.ArrayList;
import java.util.List;

/**
 * Meta-data for the Mapping step: contains name of the (sub-)transformation to execute
 *
 * @since 22-nov-2005
 * @author Matt
 *
 */

public class SimpleMappingMeta extends StepWithMappingMeta implements StepMetaInterface, ISubTransAwareMeta {

  private static Class<?> PKG = SimpleMappingMeta.class; // for i18n purposes, needed by Translator2!!

  private MappingIODefinition inputMapping;
  private MappingIODefinition outputMapping;
  private MappingParameters mappingParameters;

  private IMetaStore metaStore;

  public SimpleMappingMeta() {
    super(); // allocate BaseStepMeta

    inputMapping = new MappingIODefinition();
    outputMapping = new MappingIODefinition();

    mappingParameters = new MappingParameters();
  }

  public void loadXML( Node stepnode, IMetaStore metaStore ) throws HopXMLException {
    try {
      fileName = XMLHandler.getTagValue( stepnode, "filename" );

      Node mappingsNode = XMLHandler.getSubNode( stepnode, "mappings" );

      if ( mappingsNode == null ) {
        throw new HopXMLException( "Unable to find <mappings> element in the step XML" );
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
        PKG, "SimpleMappingMeta.Exception.ErrorLoadingTransformationStepFromXML" ), e );
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

  public void getFields( RowMetaInterface row, String origin, RowMetaInterface[] info, StepMeta nextStep,
    VariableSpace space, IMetaStore metaStore ) throws HopStepException {
    // First load some interesting data...

    // Then see which fields get added to the row.
    //
    TransMeta mappingTransMeta = null;
    try {
      mappingTransMeta =
        loadMappingMeta( this, metaStore, space, mappingParameters.isInheritingAllVariables() );
    } catch ( HopException e ) {
      throw new HopStepException( BaseMessages.getString(
        PKG, "SimpleMappingMeta.Exception.UnableToLoadMappingTransformation" ), e );
    }

    // The field structure may depend on the input parameters as well (think of parameter replacements in MDX queries
    // for instance)
    if ( mappingParameters != null && mappingTransMeta != null ) {

      // Just set the variables in the transformation statically.
      // This just means: set a number of variables or parameter values:
      //
      StepWithMappingMeta.activateParams( mappingTransMeta, mappingTransMeta, space, mappingTransMeta.listParameters(),
        mappingParameters.getVariable(), mappingParameters.getInputField(), mappingParameters.isInheritingAllVariables() );
    }

    // Keep track of all the fields that need renaming...
    //
    List<MappingValueRename> inputRenameList = new ArrayList<MappingValueRename>();

    //
    // Before we ask the mapping outputs anything, we should teach the mapping
    // input steps in the sub-transformation about the data coming in...
    //

    RowMetaInterface inputRowMeta;

    // The row metadata, what we pass to the mapping input step
    // definition.getOutputStep(), is "row"
    // However, we do need to re-map some fields...
    //
    inputRowMeta = row.clone();
    if ( !inputRowMeta.isEmpty() ) {
      for ( MappingValueRename valueRename : inputMapping.getValueRenames() ) {
        ValueMetaInterface valueMeta = inputRowMeta.searchValueMeta( valueRename.getSourceValueName() );
        if ( valueMeta == null ) {
          throw new HopStepException( BaseMessages.getString(
            PKG, "SimpleMappingMeta.Exception.UnableToFindField", valueRename.getSourceValueName() ) );
        }
        valueMeta.setName( valueRename.getTargetValueName() );
      }
    }

    // What is this mapping input step?
    //
    StepMeta mappingInputStep = mappingTransMeta.findMappingInputStep( null );

    // We're certain it's a MappingInput step...
    //
    MappingInputMeta mappingInputMeta = (MappingInputMeta) mappingInputStep.getStepMetaInterface();

    // Inform the mapping input step about what it's going to receive...
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

    StepMeta mappingOutputStep = mappingTransMeta.findMappingOutputStep( null );

    // We know it's a mapping output step...
    MappingOutputMeta mappingOutputMeta = (MappingOutputMeta) mappingOutputStep.getStepMetaInterface();

    // Change a few columns.
    mappingOutputMeta.setOutputValueRenames( outputMapping.getValueRenames() );

    // Perhaps we need to change a few input columns back to the original?
    //
    mappingOutputMeta.setInputValueRenames( inputRenameList );

    // Now we know wat's going to come out of there...
    // This is going to be the full row, including all the remapping, etc.
    //
    RowMetaInterface mappingOutputRowMeta = mappingTransMeta.getStepFields( mappingOutputStep );

    row.clear();
    row.addRowMeta( mappingOutputRowMeta );
  }

  public String[] getInfoSteps() {
    return null;
  }

  public String[] getTargetSteps() {
    return null;
  }



  public void check( List<CheckResultInterface> remarks, TransMeta transMeta, StepMeta stepMeta,
    RowMetaInterface prev, String[] input, String[] output, RowMetaInterface info, VariableSpace space,
    IMetaStore metaStore ) {
    CheckResult cr;
    if ( prev == null || prev.size() == 0 ) {
      cr =
        new CheckResult( CheckResultInterface.TYPE_RESULT_WARNING, BaseMessages.getString(
          PKG, "SimpleMappingMeta.CheckResult.NotReceivingAnyFields" ), stepMeta );
      remarks.add( cr );
    } else {
      cr =
        new CheckResult( CheckResultInterface.TYPE_RESULT_OK, BaseMessages.getString(
          PKG, "SimpleMappingMeta.CheckResult.StepReceivingFields", prev.size() + "" ), stepMeta );
      remarks.add( cr );
    }

    // See if we have input streams leading to this step!
    if ( input.length > 0 ) {
      cr =
        new CheckResult( CheckResultInterface.TYPE_RESULT_OK, BaseMessages.getString(
          PKG, "SimpleMappingMeta.CheckResult.StepReceivingFieldsFromOtherSteps" ), stepMeta );
      remarks.add( cr );
    } else {
      cr =
        new CheckResult( CheckResultInterface.TYPE_RESULT_ERROR, BaseMessages.getString(
          PKG, "SimpleMappingMeta.CheckResult.NoInputReceived" ), stepMeta );
      remarks.add( cr );
    }
  }

  public StepInterface getStep( StepMeta stepMeta, StepDataInterface stepDataInterface, int cnr, TransMeta tr,
    Trans trans ) {
    return new SimpleMapping( stepMeta, stepDataInterface, cnr, tr, trans );
  }

  public StepDataInterface getStepData() {
    return new SimpleMappingData();
  }

  /**
   * @return the mappingParameters
   */
  public MappingParameters getMappingParameters() {
    return mappingParameters;
  }

  /**
   * @param mappingParameters
   *          the mappingParameters to set
   */
  public void setMappingParameters( MappingParameters mappingParameters ) {
    this.mappingParameters = mappingParameters;
  }

  @Override
  public List<ResourceReference> getResourceDependencies( TransMeta transMeta, StepMeta stepInfo ) {
    List<ResourceReference> references = new ArrayList<ResourceReference>( 5 );
    String realFilename = transMeta.environmentSubstitute( fileName );
    ResourceReference reference = new ResourceReference( stepInfo );
    references.add( reference );

    if ( StringUtils.isNotEmpty( realFilename ) ) {
      // Add the filename to the references, including a reference to this step
      // meta data.
      //
      reference.getEntries().add( new ResourceEntry( realFilename, ResourceType.ACTIONFILE ) );
    }
    return references;
  }

  @Override
  public StepIOMetaInterface getStepIOMeta() {
    StepIOMetaInterface ioMeta = super.getStepIOMeta( false );
    if ( ioMeta == null ) {
      ioMeta = new StepIOMeta( true, true, false, false, false, false );
      setStepIOMeta( ioMeta );
    }
    return ioMeta;
  }

  public boolean excludeFromRowLayoutVerification() {
    return false;
  }

  @Override
  public void searchInfoAndTargetSteps( List<StepMeta> steps ) {
  }

  public TransformationType[] getSupportedTransformationTypes() {
    return new TransformationType[] { TransformationType.Normal, };
  }

  /**
   * @return The objects referenced in the step, like a mapping, a transformation, a job, ...
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
  public Object loadReferencedObject( int index, VariableSpace space ) throws HopException {
    return loadReferencedObject( index, null, space );
  }

  /**
   * Load the referenced object
   *
   * @param index
   *          the object index to load
   * @param metaStore
   *          the MetaStore to use
   * @param space
   *          the variable space to use
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
