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

package org.apache.hop.pipeline.steps.prioritizestreams;

import org.apache.hop.core.CheckResult;
import org.apache.hop.core.CheckResultInterface;
import org.apache.hop.core.Const;
import org.apache.hop.core.exception.HopStepException;
import org.apache.hop.core.exception.HopXMLException;
import org.apache.hop.core.row.RowMetaInterface;
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

/*
 * Created on 30-06-2008
 *
 */

public class PrioritizeStreamsMeta extends BaseStepMeta implements StepMetaInterface {
  private static Class<?> PKG = PrioritizeStreamsMeta.class; // for i18n purposes, needed by Translator!!

  /**
   * by which steps to display?
   */
  private String[] stepName;

  public PrioritizeStreamsMeta() {
    super(); // allocate BaseStepMeta
  }

  public void loadXML( Node stepnode, IMetaStore metaStore ) throws HopXMLException {
    readData( stepnode, metaStore );
  }

  public Object clone() {
    PrioritizeStreamsMeta retval = (PrioritizeStreamsMeta) super.clone();

    int nrfields = stepName.length;

    retval.allocate( nrfields );
    System.arraycopy( stepName, 0, retval.stepName, 0, nrfields );
    return retval;
  }

  public void allocate( int nrfields ) {
    stepName = new String[ nrfields ];
  }

  /**
   * @return Returns the stepName.
   */
  public String[] getStepName() {
    return stepName;
  }

  /**
   * @param stepName The stepName to set.
   */
  public void setStepName( String[] stepName ) {
    this.stepName = stepName;
  }

  public void getFields( RowMetaInterface rowMeta, String origin, RowMetaInterface[] info, StepMeta nextStep,
                         VariableSpace space, IMetaStore metaStore ) throws HopStepException {
    // Default: nothing changes to rowMeta
  }

  private void readData( Node stepnode, IMetaStore metaStore ) throws HopXMLException {
    try {
      Node steps = XMLHandler.getSubNode( stepnode, "steps" );
      int nrsteps = XMLHandler.countNodes( steps, "step" );

      allocate( nrsteps );

      for ( int i = 0; i < nrsteps; i++ ) {
        Node fnode = XMLHandler.getSubNodeByNr( steps, "step", i );
        stepName[ i ] = XMLHandler.getTagValue( fnode, "name" );
      }
    } catch ( Exception e ) {
      throw new HopXMLException( "Unable to load step info from XML", e );
    }
  }

  public String getXML() {
    StringBuilder retval = new StringBuilder();

    retval.append( "    <steps>" + Const.CR );
    for ( int i = 0; i < stepName.length; i++ ) {
      retval.append( "      <step>" + Const.CR );
      retval.append( "        " + XMLHandler.addTagValue( "name", stepName[ i ] ) );
      retval.append( "        </step>" + Const.CR );
    }
    retval.append( "      </steps>" + Const.CR );

    return retval.toString();
  }

  public void setDefault() {
    int nrsteps = 0;

    allocate( nrsteps );

    for ( int i = 0; i < nrsteps; i++ ) {
      stepName[ i ] = "step" + i;
    }
  }

  public void check( List<CheckResultInterface> remarks, PipelineMeta pipelineMeta, StepMeta stepMeta,
                     RowMetaInterface prev, String[] input, String[] output, RowMetaInterface info, VariableSpace space,
                     IMetaStore metaStore ) {
    CheckResult cr;

    if ( prev == null || prev.size() == 0 ) {
      cr =
        new CheckResult( CheckResult.TYPE_RESULT_WARNING, BaseMessages.getString(
          PKG, "PrioritizeStreamsMeta.CheckResult.NotReceivingFields" ), stepMeta );
      remarks.add( cr );
    } else {
      if ( stepName.length > 0 ) {
        cr =
          new CheckResult( CheckResult.TYPE_RESULT_OK, BaseMessages.getString(
            PKG, "PrioritizeStreamsMeta.CheckResult.AllStepsFound" ), stepMeta );
        remarks.add( cr );
      } else {
        cr =
          new CheckResult( CheckResult.TYPE_RESULT_WARNING, BaseMessages.getString(
            PKG, "PrioritizeStreamsMeta.CheckResult.NoStepsEntered" ), stepMeta );
        remarks.add( cr );
      }

    }

    // See if we have input streams leading to this step!
    if ( input.length > 0 ) {
      cr =
        new CheckResult( CheckResult.TYPE_RESULT_OK, BaseMessages.getString(
          PKG, "PrioritizeStreamsMeta.CheckResult.StepRecevingData2" ), stepMeta );
      remarks.add( cr );
    } else {
      cr =
        new CheckResult( CheckResult.TYPE_RESULT_ERROR, BaseMessages.getString(
          PKG, "PrioritizeStreamsMeta.CheckResult.NoInputReceivedFromOtherSteps" ), stepMeta );
      remarks.add( cr );
    }
  }

  public StepInterface getStep( StepMeta stepMeta, StepDataInterface stepDataInterface, int cnr, PipelineMeta tr,
                                Pipeline pipeline ) {
    return new PrioritizeStreams( stepMeta, stepDataInterface, cnr, tr, pipeline );
  }

  public StepDataInterface getStepData() {
    return new PrioritizeStreamsData();
  }

}
