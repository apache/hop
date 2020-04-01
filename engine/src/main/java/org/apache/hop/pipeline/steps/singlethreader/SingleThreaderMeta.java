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

package org.apache.hop.pipeline.steps.singlethreader;

import org.apache.commons.lang.StringUtils;
import org.apache.hop.core.CheckResult;
import org.apache.hop.core.CheckResultInterface;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.exception.HopStepException;
import org.apache.hop.core.exception.HopXMLException;
import org.apache.hop.core.row.RowMetaInterface;
import org.apache.hop.core.util.Utils;
import org.apache.hop.core.variables.VariableSpace;
import org.apache.hop.core.xml.XMLHandler;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.metastore.api.IMetaStore;
import org.apache.hop.pipeline.Pipeline;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.resource.ResourceEntry;
import org.apache.hop.resource.ResourceEntry.ResourceType;
import org.apache.hop.resource.ResourceReference;
import org.apache.hop.pipeline.ISubPipelineAwareMeta;
import org.apache.hop.pipeline.StepWithMappingMeta;
import org.apache.hop.pipeline.PipelineMeta.PipelineType;
import org.apache.hop.pipeline.step.StepDataInterface;
import org.apache.hop.pipeline.step.StepInterface;
import org.apache.hop.pipeline.step.StepMeta;
import org.apache.hop.pipeline.step.StepMetaInterface;
import org.w3c.dom.Node;

import java.util.ArrayList;
import java.util.List;

/**
 * Meta-data for the Mapping step: contains name of the (sub-) pipeline to execute
 *
 * @author Matt
 * @since 22-nov-2005
 */

public class SingleThreaderMeta extends StepWithMappingMeta implements StepMetaInterface, ISubPipelineAwareMeta {

  private static Class<?> PKG = SingleThreaderMeta.class; // for i18n purposes, needed by Translator!!

  private String batchSize;
  private String batchTime;

  private String injectStep;
  private String retrieveStep;

  private boolean passingAllParameters;

  private String[] parameters;
  private String[] parameterValues;

  private IMetaStore metaStore;

  public SingleThreaderMeta() {
    super(); // allocate BaseStepMeta

    setDefault();
  }

  public void loadXML( Node stepnode, IMetaStore metaStore ) throws HopXMLException {
    try {
      fileName = XMLHandler.getTagValue( stepnode, "filename" );

      batchSize = XMLHandler.getTagValue( stepnode, "batch_size" );
      batchTime = XMLHandler.getTagValue( stepnode, "batch_time" );
      injectStep = XMLHandler.getTagValue( stepnode, "inject_step" );
      retrieveStep = XMLHandler.getTagValue( stepnode, "retrieve_step" );

      Node parametersNode = XMLHandler.getSubNode( stepnode, "parameters" );

      String passAll = XMLHandler.getTagValue( parametersNode, "pass_all_parameters" );
      passingAllParameters = Utils.isEmpty( passAll ) || "Y".equalsIgnoreCase( passAll );

      int nrParameters = XMLHandler.countNodes( parametersNode, "parameter" );

      allocate( nrParameters );

      for ( int i = 0; i < nrParameters; i++ ) {
        Node knode = XMLHandler.getSubNodeByNr( parametersNode, "parameter", i );

        parameters[ i ] = XMLHandler.getTagValue( knode, "name" );
        parameterValues[ i ] = XMLHandler.getTagValue( knode, "value" );
      }
    } catch ( Exception e ) {
      throw new HopXMLException( BaseMessages.getString(
        PKG, "SingleThreaderMeta.Exception.ErrorLoadingPipelineStepFromXML" ), e );
    }
  }

  public void allocate( int nrParameters ) {
    parameters = new String[ nrParameters ];
    parameterValues = new String[ nrParameters ];
  }

  public Object clone() {
    SingleThreaderMeta retval = (SingleThreaderMeta) super.clone();
    int nrParameters = parameters.length;
    retval.allocate( nrParameters );
    System.arraycopy( parameters, 0, retval.parameters, 0, nrParameters );
    System.arraycopy( parameterValues, 0, retval.parameterValues, 0, nrParameters );

    return retval;
  }

  public String getXML() {
    StringBuilder retval = new StringBuilder( 300 );

    retval.append( "    " ).append( XMLHandler.addTagValue( "filename", fileName ) );

    retval.append( "    " ).append( XMLHandler.addTagValue( "batch_size", batchSize ) );
    retval.append( "    " ).append( XMLHandler.addTagValue( "batch_time", batchTime ) );
    retval.append( "    " ).append( XMLHandler.addTagValue( "inject_step", injectStep ) );
    retval.append( "    " ).append( XMLHandler.addTagValue( "retrieve_step", retrieveStep ) );

    if ( parameters != null ) {
      retval.append( "      " ).append( XMLHandler.openTag( "parameters" ) );

      retval.append( "        " ).append( XMLHandler.addTagValue( "pass_all_parameters", passingAllParameters ) );

      for ( int i = 0; i < parameters.length; i++ ) {
        // This is a better way of making the XML file than the arguments.
        retval.append( "            " ).append( XMLHandler.openTag( "parameter" ) );

        retval.append( "            " ).append( XMLHandler.addTagValue( "name", parameters[ i ] ) );
        retval.append( "            " ).append( XMLHandler.addTagValue( "value", parameterValues[ i ] ) );

        retval.append( "            " ).append( XMLHandler.closeTag( "parameter" ) );
      }
      retval.append( "      " ).append( XMLHandler.closeTag( "parameters" ) );
    }
    return retval.toString();
  }

  public void setDefault() {
    batchSize = "100";
    batchTime = "";

    passingAllParameters = true;

    parameters = new String[ 0 ];
    parameterValues = new String[ 0 ];
  }

  public void getFields( RowMetaInterface row, String origin, RowMetaInterface[] info, StepMeta nextStep,
                         VariableSpace space, IMetaStore metaStore ) throws HopStepException {

    // First load some interesting data...
    //
    // Then see which fields get added to the row.
    //
    PipelineMeta mappingPipelineMeta = null;
    try {
      mappingPipelineMeta = loadSingleThreadedPipelineMeta( this, space );
    } catch ( HopException e ) {
      throw new HopStepException( BaseMessages.getString(
        PKG, "SingleThreaderMeta.Exception.UnableToLoadMappingPipeline" ), e );
    }

    row.clear();

    // Let's keep it simple!
    //
    if ( !Utils.isEmpty( space.environmentSubstitute( retrieveStep ) ) ) {
      RowMetaInterface stepFields = mappingPipelineMeta.getStepFields( retrieveStep );
      row.addRowMeta( stepFields );
    }
  }


  public static final synchronized PipelineMeta loadSingleThreadedPipelineMeta( SingleThreaderMeta mappingMeta,
                                                                                VariableSpace space ) throws HopException {
    return loadMappingMeta( mappingMeta, null, space );
  }

  public static final synchronized PipelineMeta loadSingleThreadedPipelineMeta( SingleThreaderMeta mappingMeta,
                                                                                VariableSpace space, boolean passingAllParameters ) throws HopException {
    return loadMappingMeta( mappingMeta, null, space, passingAllParameters );
  }

  public void check( List<CheckResultInterface> remarks, PipelineMeta pipelineMeta, StepMeta stepMeta,
                     RowMetaInterface prev, String[] input, String[] output, RowMetaInterface info, VariableSpace space,
                     IMetaStore metaStore ) {
    CheckResult cr;
    if ( prev == null || prev.size() == 0 ) {
      cr =
        new CheckResult( CheckResultInterface.TYPE_RESULT_WARNING, BaseMessages.getString(
          PKG, "SingleThreaderMeta.CheckResult.NotReceivingAnyFields" ), stepMeta );
      remarks.add( cr );
    } else {
      cr =
        new CheckResult( CheckResultInterface.TYPE_RESULT_OK, BaseMessages.getString(
          PKG, "SingleThreaderMeta.CheckResult.StepReceivingFields", prev.size() + "" ), stepMeta );
      remarks.add( cr );
    }

    // See if we have input streams leading to this step!
    if ( input.length > 0 ) {
      cr =
        new CheckResult( CheckResultInterface.TYPE_RESULT_OK, BaseMessages.getString(
          PKG, "SingleThreaderMeta.CheckResult.StepReceivingFieldsFromOtherSteps" ), stepMeta );
      remarks.add( cr );
    } else {
      cr =
        new CheckResult( CheckResultInterface.TYPE_RESULT_ERROR, BaseMessages.getString(
          PKG, "SingleThreaderMeta.CheckResult.NoInputReceived" ), stepMeta );
      remarks.add( cr );
    }

  }

  public StepInterface getStep( StepMeta stepMeta, StepDataInterface stepDataInterface, int cnr, PipelineMeta tr,
                                Pipeline pipeline ) {
    return new SingleThreader( stepMeta, stepDataInterface, cnr, tr, pipeline );
  }

  public StepDataInterface getStepData() {
    return new SingleThreaderData();
  }

  @Override
  public List<ResourceReference> getResourceDependencies( PipelineMeta pipelineMeta, StepMeta stepInfo ) {
    List<ResourceReference> references = new ArrayList<ResourceReference>( 5 );
    String realFilename = pipelineMeta.environmentSubstitute( fileName );
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

  public PipelineType[] getSupportedPipelineTypes() {
    return new PipelineType[] { PipelineType.Normal, };
  }

  /**
   * @return the batchSize
   */
  public String getBatchSize() {
    return batchSize;
  }

  /**
   * @param batchSize the batchSize to set
   */
  public void setBatchSize( String batchSize ) {
    this.batchSize = batchSize;
  }

  /**
   * @return the injectStep
   */
  public String getInjectStep() {
    return injectStep;
  }

  /**
   * @param injectStep the injectStep to set
   */
  public void setInjectStep( String injectStep ) {
    this.injectStep = injectStep;
  }

  /**
   * @return the retrieveStep
   */
  public String getRetrieveStep() {
    return retrieveStep;
  }

  /**
   * @param retrieveStep the retrieveStep to set
   */
  public void setRetrieveStep( String retrieveStep ) {
    this.retrieveStep = retrieveStep;
  }

  /**
   * @return the passingAllParameters
   */
  public boolean isPassingAllParameters() {
    return passingAllParameters;
  }

  /**
   * @param passingAllParameters the passingAllParameters to set
   */
  public void setPassingAllParameters( boolean passingAllParameters ) {
    this.passingAllParameters = passingAllParameters;
  }

  /**
   * @return the parameters
   */
  public String[] getParameters() {
    return parameters;
  }

  /**
   * @param parameters the parameters to set
   */
  public void setParameters( String[] parameters ) {
    this.parameters = parameters;
  }

  /**
   * @return the parameterValues
   */
  public String[] getParameterValues() {
    return parameterValues;
  }

  /**
   * @param parameterValues the parameterValues to set
   */
  public void setParameterValues( String[] parameterValues ) {
    this.parameterValues = parameterValues;
  }

  /**
   * @return the batchTime
   */
  public String getBatchTime() {
    return batchTime;
  }

  /**
   * @param batchTime the batchTime to set
   */
  public void setBatchTime( String batchTime ) {
    this.batchTime = batchTime;
  }

  /**
   * @return The objects referenced in the step, like a mapping, a pipeline, a job, ...
   */
  public String[] getReferencedObjectDescriptions() {
    return new String[] { BaseMessages.getString( PKG, "SingleThreaderMeta.ReferencedObject.Description" ), };
  }

  private boolean isPipelineDefined() {
    return StringUtils.isNotEmpty( fileName );
  }

  public boolean[] isReferencedObjectEnabled() {
    return new boolean[] { isPipelineDefined(), };
  }

  /**
   * Load the referenced object
   *
   * @param index the object index to load
   * @param space the variable space to use
   * @return the referenced object once loaded
   * @throws HopException
   */
  @Deprecated
  public Object loadReferencedObject( int index, VariableSpace space ) throws HopException {
    return loadSingleThreadedPipelineMeta( this, space );
  }

  public Object loadReferencedObject( int index, IMetaStore metaStore, VariableSpace space ) throws HopException {
    return loadMappingMeta( this, metaStore, space );
  }

  @Override
  public boolean supportsErrorHandling() {
    return true;
  }

  @Override
  public boolean excludeFromCopyDistributeVerification() {
    return true;
  }

  public void setMetaStore( IMetaStore metaStore ) {
    this.metaStore = metaStore;
  }

  public IMetaStore getMetaStore() {
    return metaStore;
  }
}
