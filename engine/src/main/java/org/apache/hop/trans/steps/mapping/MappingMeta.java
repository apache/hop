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

package org.apache.hop.trans.steps.mapping;

import org.apache.commons.lang.StringUtils;
import org.apache.hop.core.CheckResult;
import org.apache.hop.core.CheckResultInterface;
import org.apache.hop.core.Const;
import org.apache.hop.core.database.DatabaseMeta;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.exception.HopStepException;
import org.apache.hop.core.exception.HopXMLException;
import org.apache.hop.core.parameters.UnknownParamException;
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
import org.apache.hop.trans.step.errorhandling.Stream;
import org.apache.hop.trans.step.errorhandling.StreamIcon;
import org.apache.hop.trans.step.errorhandling.StreamInterface.StreamType;
import org.apache.hop.trans.steps.mappinginput.MappingInputMeta;
import org.apache.hop.trans.steps.mappingoutput.MappingOutputMeta;
import org.apache.hop.metastore.api.IMetaStore;
import org.w3c.dom.Node;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Meta-data for the Mapping step: contains name of the (sub-)transformation to execute
 *
 * @since 22-nov-2005
 * @author Matt
 *
 */

public class MappingMeta extends StepWithMappingMeta implements StepMetaInterface,
  ISubTransAwareMeta {

  private static Class<?> PKG = MappingMeta.class;
  private List<MappingIODefinition> inputMappings;
  private List<MappingIODefinition> outputMappings;
  private MappingParameters mappingParameters;

  private boolean allowingMultipleInputs;
  private boolean allowingMultipleOutputs;

  private IMetaStore metaStore;

  public MappingMeta() {
    super(); // allocate BaseStepMeta

    inputMappings = new ArrayList<MappingIODefinition>();
    outputMappings = new ArrayList<MappingIODefinition>();
    mappingParameters = new MappingParameters();
  }

  public void loadXML( Node stepnode, IMetaStore metaStore ) throws HopXMLException {
    try {
      fileName = XMLHandler.getTagValue( stepnode, "filename" );

      Node mappingsNode = XMLHandler.getSubNode( stepnode, "mappings" );
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
        Node inputNode = XMLHandler.getSubNode( stepnode, "input" );
        Node outputNode = XMLHandler.getSubNode( stepnode, "output" );

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

      String multiInput = XMLHandler.getTagValue( stepnode, "allow_multiple_input" );
      allowingMultipleInputs =
        Utils.isEmpty( multiInput ) ? inputMappings.size() > 1 : "Y".equalsIgnoreCase( multiInput );
      String multiOutput = XMLHandler.getTagValue( stepnode, "allow_multiple_output" );
      allowingMultipleOutputs =
        Utils.isEmpty( multiOutput ) ? outputMappings.size() > 1 : "Y".equalsIgnoreCase( multiOutput );

    } catch ( Exception e ) {
      throw new HopXMLException( BaseMessages.getString(
        PKG, "MappingMeta.Exception.ErrorLoadingTransformationStepFromXML" ), e );
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

  public void getFields( RowMetaInterface row, String origin, RowMetaInterface[] info, StepMeta nextStep,
    VariableSpace space, IMetaStore metaStore ) throws HopStepException {
    // First load some interesting data...

    // Then see which fields get added to the row.
    //
    TransMeta mappingTransMeta = null;
    try {
      mappingTransMeta = loadMappingMeta( this, metaStore, space );
    } catch ( HopException e ) {
      throw new HopStepException( BaseMessages.getString(
        PKG, "MappingMeta.Exception.UnableToLoadMappingTransformation" ), e );
    }

    // The field structure may depend on the input parameters as well (think of parameter replacements in MDX queries
    // for instance)
    if ( mappingParameters != null ) {

      // See if we need to pass all variables from the parent or not...
      //
      if ( mappingParameters.isInheritingAllVariables() ) {
        mappingTransMeta.copyVariablesFrom( space );
      }

      // Just set the variables in the transformation statically.
      // This just means: set a number of variables or parameter values:
      //
      List<String> subParams = Arrays.asList( mappingTransMeta.listParameters() );

      for ( int i = 0; i < mappingParameters.getVariable().length; i++ ) {
        String name = mappingParameters.getVariable()[i];
        String value = space.environmentSubstitute( mappingParameters.getInputField()[i] );
        if ( !Utils.isEmpty( name ) && !Utils.isEmpty( value ) ) {
          if ( subParams.contains( name ) ) {
            try {
              mappingTransMeta.setParameterValue( name, value );
            } catch ( UnknownParamException e ) {
              // this is explicitly checked for up front
            }
          }
          mappingTransMeta.setVariable( name, value );

        }
      }
    }

    // Keep track of all the fields that need renaming...
    //
    List<MappingValueRename> inputRenameList = new ArrayList<MappingValueRename>();

    /*
     * Before we ask the mapping outputs anything, we should teach the mapping input steps in the sub-transformation
     * about the data coming in...
     */
    for ( MappingIODefinition definition : inputMappings ) {

      RowMetaInterface inputRowMeta;

      if ( definition.isMainDataPath() || Utils.isEmpty( definition.getInputStepname() ) ) {
        // The row metadata, what we pass to the mapping input step
        // definition.getOutputStep(), is "row"
        // However, we do need to re-map some fields...
        //
        inputRowMeta = row.clone();
        if ( !inputRowMeta.isEmpty() ) {
          for ( MappingValueRename valueRename : definition.getValueRenames() ) {
            ValueMetaInterface valueMeta = inputRowMeta.searchValueMeta( valueRename.getSourceValueName() );
            if ( valueMeta == null ) {
              throw new HopStepException( BaseMessages.getString(
                PKG, "MappingMeta.Exception.UnableToFindField", valueRename.getSourceValueName() ) );
            }
            valueMeta.setName( valueRename.getTargetValueName() );
          }
        }
      } else {
        // The row metadata that goes to the info mapping input comes from the
        // specified step
        // In fact, it's one of the info steps that is going to contain this
        // information...
        //
        String[] infoSteps = getInfoSteps();
        int infoStepIndex = Const.indexOfString( definition.getInputStepname(), infoSteps );
        if ( infoStepIndex < 0 ) {
          throw new HopStepException( BaseMessages.getString(
            PKG, "MappingMeta.Exception.UnableToFindMetadataInfo", definition.getInputStepname() ) );
        }
        if ( info[infoStepIndex] != null ) {
          inputRowMeta = info[infoStepIndex].clone();
        } else {
          inputRowMeta = null;
        }
      }

      // What is this mapping input step?
      //
      StepMeta mappingInputStep = mappingTransMeta.findMappingInputStep( definition.getOutputStepname() );

      // We're certain it's a MappingInput step...
      //
      MappingInputMeta mappingInputMeta = (MappingInputMeta) mappingInputStep.getStepMetaInterface();

      // Inform the mapping input step about what it's going to receive...
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

    // All the mapping steps now know what they will be receiving.
    // That also means that the sub-transformation / mapping has everything it
    // needs.
    // So that means that the MappingOutput steps know exactly what the output
    // is going to be.
    // That could basically be anything.
    // It also could have absolutely no resemblance to what came in on the
    // input.
    // The relative old approach is therefore no longer suited.
    //
    // OK, but what we *can* do is have the MappingOutput step rename the
    // appropriate fields.
    // The mapping step will tell this step how it's done.
    //
    // Let's look for the mapping output step that is relevant for this actual
    // call...
    //
    MappingIODefinition mappingOutputDefinition = null;
    if ( nextStep == null ) {
      // This is the main step we read from...
      // Look up the main step to write to.
      // This is the output mapping definition with "main path" enabled.
      //
      for ( MappingIODefinition definition : outputMappings ) {
        if ( definition.isMainDataPath() || Utils.isEmpty( definition.getOutputStepname() ) ) {
          // This is the definition to use...
          //
          mappingOutputDefinition = definition;
        }
      }
    } else {
      // Is there an output mapping definition for this step?
      // If so, we can look up the Mapping output step to see what has changed.
      //

      for ( MappingIODefinition definition : outputMappings ) {
        if ( nextStep.getName().equals( definition.getOutputStepname() )
          || definition.isMainDataPath() || Utils.isEmpty( definition.getOutputStepname() ) ) {
          mappingOutputDefinition = definition;
        }
      }
    }

    if ( mappingOutputDefinition == null ) {
      throw new HopStepException( BaseMessages.getString(
        PKG, "MappingMeta.Exception.UnableToFindMappingDefinition" ) );
    }

    // OK, now find the mapping output step in the mapping...
    // This method in TransMeta takes into account a number of things, such as
    // the step not specified, etc.
    // The method never returns null but throws an exception.
    //
    StepMeta mappingOutputStep =
      mappingTransMeta.findMappingOutputStep( mappingOutputDefinition.getInputStepname() );

    // We know it's a mapping output step...
    MappingOutputMeta mappingOutputMeta = (MappingOutputMeta) mappingOutputStep.getStepMetaInterface();

    // Change a few columns.
    mappingOutputMeta.setOutputValueRenames( mappingOutputDefinition.getValueRenames() );

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
    String[] infoSteps = getStepIOMeta().getInfoStepnames();
    // Return null instead of empty array to preserve existing behavior
    return infoSteps.length == 0 ? null : infoSteps;
  }

  public String[] getTargetSteps() {

    List<String> targetSteps = new ArrayList<String>();
    // The infosteps are those steps that are specified in the input mappings
    for ( MappingIODefinition definition : outputMappings ) {
      if ( !definition.isMainDataPath() && !Utils.isEmpty( definition.getOutputStepname() ) ) {
        targetSteps.add( definition.getOutputStepname() );
      }
    }
    if ( targetSteps.isEmpty() ) {
      return null;
    }

    return targetSteps.toArray( new String[targetSteps.size()] );
  }


  @Deprecated
  public static final synchronized TransMeta loadMappingMeta( MappingMeta mappingMeta,
                                                              VariableSpace space ) throws HopException {
    return loadMappingMeta( mappingMeta, null, space );
  }

  public void check( List<CheckResultInterface> remarks, TransMeta transMeta, StepMeta stepMeta,
    RowMetaInterface prev, String[] input, String[] output, RowMetaInterface info, VariableSpace space,
    IMetaStore metaStore ) {
    CheckResult cr;
    if ( prev == null || prev.size() == 0 ) {
      cr =
        new CheckResult( CheckResultInterface.TYPE_RESULT_WARNING, BaseMessages.getString(
          PKG, "MappingMeta.CheckResult.NotReceivingAnyFields" ), stepMeta );
      remarks.add( cr );
    } else {
      cr =
        new CheckResult( CheckResultInterface.TYPE_RESULT_OK, BaseMessages.getString(
          PKG, "MappingMeta.CheckResult.StepReceivingFields", prev.size() + "" ), stepMeta );
      remarks.add( cr );
    }

    // See if we have input streams leading to this step!
    if ( input.length > 0 ) {
      cr =
        new CheckResult( CheckResultInterface.TYPE_RESULT_OK, BaseMessages.getString(
          PKG, "MappingMeta.CheckResult.StepReceivingFieldsFromOtherSteps" ), stepMeta );
      remarks.add( cr );
    } else {
      cr =
        new CheckResult( CheckResultInterface.TYPE_RESULT_ERROR, BaseMessages.getString(
          PKG, "MappingMeta.CheckResult.NoInputReceived" ), stepMeta );
      remarks.add( cr );
    }

    /*
     * TODO re-enable validation code for mappings...
     *
     * // Change the names of the fields if this is required by the mapping. for (int i=0;i<inputField.length;i++) { if
     * (inputField[i]!=null && inputField[i].length()>0) { if (inputMapping[i]!=null && inputMapping[i].length()>0) { if
     * (!inputField[i].equals(inputMapping[i])) // rename these! { int idx = prev.indexOfValue(inputField[i]); if
     * (idx<0) { cr = new CheckResult(CheckResultInterface.TYPE_RESULT_ERROR, BaseMessages.getString(PKG,
     * "MappingMeta.CheckResult.MappingTargetFieldNotPresent",inputField[i]), stepinfo); remarks.add(cr); } } } else {
     * cr = new CheckResult(CheckResultInterface.TYPE_RESULT_ERROR, BaseMessages.getString(PKG,
     * "MappingMeta.CheckResult.MappingTargetFieldNotSepecified" ,i+"",inputField[i]), stepinfo);
     * remarks.add(cr); } } else { cr = new CheckResult(CheckResultInterface.TYPE_RESULT_ERROR,
     * BaseMessages.getString(PKG, "MappingMeta.CheckResult.InputFieldNotSpecified",i+""), stepinfo); remarks.add(cr); }
     * }
     *
     * // Then check the fields that get added to the row. //
     *
     * Repository repository = Repository.getCurrentRepository(); TransMeta mappingTransMeta = null; try {
     * mappingTransMeta = loadMappingMeta(fileName, transName, directoryPath, repository); } catch(HopException e) {
     * cr = new CheckResult(CheckResultInterface.TYPE_RESULT_OK, BaseMessages.getString(PKG,
     * "MappingMeta.CheckResult.UnableToLoadMappingTransformation" )+":"+Const.getStackTracker(e), stepinfo);
     * remarks.add(cr); }
     *
     * if (mappingTransMeta!=null) { cr = new CheckResult(CheckResultInterface.TYPE_RESULT_OK,
     * BaseMessages.getString(PKG, "MappingMeta.CheckResult.MappingTransformationSpecified"), stepinfo);
     * remarks.add(cr);
     *
     * StepMeta stepMeta = mappingTransMeta.getMappingOutputStep();
     *
     * if (stepMeta!=null) { // See which fields are coming out of the mapping output step of the sub-transformation //
     * For these fields we check the existance // RowMetaInterface fields = null; try { fields =
     * mappingTransMeta.getStepFields(stepMeta);
     *
     * boolean allOK = true;
     *
     * // Check the fields... for (int i=0;i<outputMapping.length;i++) { ValueMetaInterface v =
     * fields.searchValueMeta(outputMapping[i]); if (v==null) // Not found! { cr = new
     * CheckResult(CheckResultInterface.TYPE_RESULT_ERROR, BaseMessages.getString(PKG,
     * "MappingMeta.CheckResult.MappingOutFieldSpecifiedCouldNotFound" )+outputMapping[i], stepinfo); remarks.add(cr);
     * allOK=false; } }
     *
     * if (allOK) { cr = new CheckResult(CheckResultInterface.TYPE_RESULT_OK, BaseMessages.getString(PKG,
     * "MappingMeta.CheckResult.AllOutputMappingFieldCouldBeFound"), stepinfo); remarks.add(cr); } }
     * catch(HopStepException e) { cr = new CheckResult(CheckResultInterface.TYPE_RESULT_ERROR,
     * BaseMessages.getString(PKG, "MappingMeta.CheckResult.UnableToGetStepOutputFields" )+stepMeta.getName()+"]",
     * stepinfo); remarks.add(cr); } } else { cr = new CheckResult(CheckResultInterface.TYPE_RESULT_ERROR,
     * BaseMessages.getString(PKG, "MappingMeta.CheckResult.NoMappingOutputStepSpecified"), stepinfo); remarks.add(cr);
     * } } else { cr = new CheckResult(CheckResultInterface.TYPE_RESULT_ERROR, BaseMessages.getString(PKG,
     * "MappingMeta.CheckResult.NoMappingSpecified"), stepinfo); remarks.add(cr); }
     */
  }

  public StepInterface getStep( StepMeta stepMeta, StepDataInterface stepDataInterface, int cnr, TransMeta tr,
    Trans trans ) {
    return new Mapping( stepMeta, stepDataInterface, cnr, tr, trans );
  }

  public StepDataInterface getStepData() {
    return new MappingData();
  }

  @Override
  public List<MappingIODefinition> getInputMappings() {
    return inputMappings;
  }

  /**
   * @param inputMappings
   *          the inputMappings to set
   */
  public void setInputMappings( List<MappingIODefinition> inputMappings ) {
    this.inputMappings = inputMappings;
    resetStepIoMeta();
  }

  @Override
  public List<MappingIODefinition> getOutputMappings() {
    return outputMappings;
  }

  /**
   * @param outputMappings
   *          the outputMappings to set
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
      // TODO Create a dynamic StepIOMeta so that we can more easily manipulate the info streams?
      ioMeta = new StepIOMeta( true, true, true, false, true, false );
      for ( MappingIODefinition def : inputMappings ) {
        if ( isInfoMapping( def ) ) {
          Stream stream =
            new Stream( StreamType.INFO, def.getInputStep(), BaseMessages.getString(
              PKG, "MappingMeta.InfoStream.Description" ), StreamIcon.INFO, null );
          ioMeta.addStream( stream );
        }
      }
      setStepIOMeta( ioMeta );
    }
    return ioMeta;
  }

  private boolean isInfoMapping( MappingIODefinition def ) {
    return !def.isMainDataPath() && !Utils.isEmpty( def.getInputStepname() );
  }

  public boolean excludeFromRowLayoutVerification() {
    return true;
  }

  @Override
  public void searchInfoAndTargetSteps( List<StepMeta> steps ) {
    // Assign all StepMeta references for Input Mappings that are INFO inputs
    for ( MappingIODefinition def : inputMappings ) {
      if ( isInfoMapping( def ) ) {
        def.setInputStep( StepMeta.findStep( steps, def.getInputStepname() ) );
      }
    }
  }

  public TransformationType[] getSupportedTransformationTypes() {
    return new TransformationType[] { TransformationType.Normal, };
  }

  /**
   * @return the allowingMultipleInputs
   */
  public boolean isAllowingMultipleInputs() {
    return allowingMultipleInputs;
  }

  /**
   * @param allowingMultipleInputs
   *          the allowingMultipleInputs to set
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
   * @param allowingMultipleOutputs
   *          the allowingMultipleOutputs to set
   */
  public void setAllowingMultipleOutputs( boolean allowingMultipleOutputs ) {
    this.allowingMultipleOutputs = allowingMultipleOutputs;
  }

  /**
   * @return The objects referenced in the step, like a mapping, a transformation, a job, ...
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

}
