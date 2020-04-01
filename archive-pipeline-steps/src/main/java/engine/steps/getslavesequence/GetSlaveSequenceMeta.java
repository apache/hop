/*! ******************************************************************************
 *
 * Pentaho Data Integration
 *
 * Copyright (C) 2002-2017 by Hitachi Vantara : http://www.pentaho.com
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

package org.apache.hop.pipeline.steps.getslavesequence;

import org.apache.hop.core.CheckResult;
import org.apache.hop.core.CheckResultInterface;
import org.apache.hop.core.exception.HopStepException;
import org.apache.hop.core.exception.HopXMLException;
import org.apache.hop.core.row.RowMetaInterface;
import org.apache.hop.core.row.ValueMetaInterface;
import org.apache.hop.core.row.value.ValueMetaInteger;
import org.apache.hop.core.variables.VariableSpace;
import org.apache.hop.core.xml.XMLHandler;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.metastore.api.IMetaStore;
import org.apache.hop.pipeline.Pipeline;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.step.BaseStepMeta;
import org.apache.hop.pipeline.step.StepDataInterface;
import org.apache.hop.pipeline.step.StepInterface;
import org.apache.hop.pipeline.step.StepMeta;
import org.apache.hop.pipeline.step.StepMetaInterface;
import org.w3c.dom.Node;

import java.util.List;

/**
 * Meta data for the Add Sequence step.
 * <p>
 * Created on 13-may-2003
 */
public class GetSlaveSequenceMeta extends BaseStepMeta implements StepMetaInterface {
  private static Class<?> PKG = GetSlaveSequenceMeta.class; // for i18n purposes, needed by Translator!!

  private String valuename;
  private String slaveServerName;
  private String sequenceName;
  private String increment;

  @Override
  public void loadXML( Node stepnode, IMetaStore metaStore ) throws HopXMLException {
    readData( stepnode, metaStore );
  }

  @Override
  public Object clone() {
    Object retval = super.clone();
    return retval;
  }

  private void readData( Node stepnode, IMetaStore metaStore ) throws HopXMLException {
    try {
      valuename = XMLHandler.getTagValue( stepnode, "valuename" );
      slaveServerName = XMLHandler.getTagValue( stepnode, "slave" );
      sequenceName = XMLHandler.getTagValue( stepnode, "seqname" );
      increment = XMLHandler.getTagValue( stepnode, "increment" );
    } catch ( Exception e ) {
      throw new HopXMLException(
        BaseMessages.getString( PKG, "GetSequenceMeta.Exception.ErrorLoadingStepInfo" ), e );
    }
  }

  @Override
  public void setDefault() {
    valuename = "id";
    slaveServerName = "slave server name";
    sequenceName = "Slave Sequence Name -- To be configured";
    increment = "10000";
  }

  @Override
  public void getFields( RowMetaInterface row, String name, RowMetaInterface[] info, StepMeta nextStep,
                         VariableSpace space, IMetaStore metaStore ) throws HopStepException {
    ValueMetaInterface v = new ValueMetaInteger( valuename );
    v.setOrigin( name );
    row.addValueMeta( v );
  }

  @Override
  public String getXML() {
    StringBuilder retval = new StringBuilder( 300 );

    retval.append( "      " ).append( XMLHandler.addTagValue( "valuename", valuename ) );
    retval.append( "      " ).append( XMLHandler.addTagValue( "slave", slaveServerName ) );
    retval.append( "      " ).append( XMLHandler.addTagValue( "seqname", sequenceName ) );
    retval.append( "      " ).append( XMLHandler.addTagValue( "increment", increment ) );

    return retval.toString();
  }

  @Override
  public void check( List<CheckResultInterface> remarks, PipelineMeta pipelineMeta, StepMeta stepMeta,
                     RowMetaInterface prev, String[] input, String[] output, RowMetaInterface info, VariableSpace space,
                     IMetaStore metaStore ) {
    CheckResult cr;

    if ( input.length > 0 ) {
      cr =
        new CheckResult( CheckResultInterface.TYPE_RESULT_OK, BaseMessages.getString(
          PKG, "GetSequenceMeta.CheckResult.StepIsReceving.Title" ), stepMeta );
      remarks.add( cr );
    } else {
      cr =
        new CheckResult( CheckResultInterface.TYPE_RESULT_ERROR, BaseMessages.getString(
          PKG, "GetSequenceMeta.CheckResult.NoInputReceived.Title" ), stepMeta );
      remarks.add( cr );
    }
  }

  @Override
  public StepInterface getStep( StepMeta stepMeta, StepDataInterface stepDataInterface, int cnr,
                                PipelineMeta pipelineMeta, Pipeline pipeline ) {
    return new GetSlaveSequence( stepMeta, stepDataInterface, cnr, pipelineMeta, pipeline );
  }

  @Override
  public StepDataInterface getStepData() {
    return new GetSlaveSequenceData();
  }

  /**
   * @return the valuename
   */
  public String getValuename() {
    return valuename;
  }

  /**
   * @param valuename the valuename to set
   */
  public void setValuename( String valuename ) {
    this.valuename = valuename;
  }

  /**
   * @return the slaveServerName
   */
  public String getSlaveServerName() {
    return slaveServerName;
  }

  /**
   * @param slaveServerName the slaveServerName to set
   */
  public void setSlaveServerName( String slaveServerName ) {
    this.slaveServerName = slaveServerName;
  }

  /**
   * @return the sequenceName
   */
  public String getSequenceName() {
    return sequenceName;
  }

  /**
   * @param sequenceName the sequenceName to set
   */
  public void setSequenceName( String sequenceName ) {
    this.sequenceName = sequenceName;
  }

  /**
   * @return the increment
   */
  public String getIncrement() {
    return increment;
  }

  /**
   * @param increment the increment to set
   */
  public void setIncrement( String increment ) {
    this.increment = increment;
  }

}
