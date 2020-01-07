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

package org.apache.hop.trans.steps.blockuntilstepsfinish;

import java.util.List;

import org.apache.hop.core.CheckResult;
import org.apache.hop.core.CheckResultInterface;
import org.apache.hop.core.Const;
import org.apache.hop.core.database.DatabaseMeta;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.exception.HopStepException;
import org.apache.hop.core.exception.HopXMLException;
import org.apache.hop.core.row.RowMetaInterface;
import org.apache.hop.core.variables.VariableSpace;
import org.apache.hop.core.xml.XMLHandler;
import org.apache.hop.i18n.BaseMessages;

import org.apache.hop.trans.Trans;
import org.apache.hop.trans.TransMeta;
import org.apache.hop.trans.TransMeta.TransformationType;
import org.apache.hop.trans.step.BaseStepMeta;
import org.apache.hop.trans.step.StepDataInterface;
import org.apache.hop.trans.step.StepInterface;
import org.apache.hop.trans.step.StepMeta;
import org.apache.hop.trans.step.StepMetaInterface;
import org.apache.hop.metastore.api.IMetaStore;
import org.w3c.dom.Node;

/*
 * Created on 30-06-2008
 *
 */

public class BlockUntilStepsFinishMeta extends BaseStepMeta implements StepMetaInterface {
  private static Class<?> PKG = BlockUntilStepsFinishMeta.class; // for i18n purposes, needed by Translator2!!

  /** by which steps to display? */
  private String[] stepName;
  private String[] stepCopyNr;

  public BlockUntilStepsFinishMeta() {
    super(); // allocate BaseStepMeta
  }

  public void loadXML( Node stepnode, IMetaStore metaStore ) throws HopXMLException {
    readData( stepnode );
  }

  public Object clone() {
    BlockUntilStepsFinishMeta retval = (BlockUntilStepsFinishMeta) super.clone();

    int nrfields = stepName.length;

    retval.allocate( nrfields );
    System.arraycopy( stepName, 0, retval.stepName, 0, nrfields );
    System.arraycopy( stepCopyNr, 0, retval.stepCopyNr, 0, nrfields );
    return retval;
  }

  public void allocate( int nrfields ) {
    stepName = new String[nrfields];
    stepCopyNr = new String[nrfields];
  }

  /**
   * @return Returns the stepName.
   */
  public String[] getStepName() {
    return stepName;
  }

  /**
   * @return Returns the stepCopyNr.
   */
  public String[] getStepCopyNr() {
    return stepCopyNr;
  }

  /**
   * @param stepName
   *          The stepName to set.
   */
  public void setStepName( String[] stepName ) {
    this.stepName = stepName;
  }

  /**
   * @param stepCopyNr
   *          The stepCopyNr to set.
   */
  public void setStepCopyNr( String[] stepCopyNr ) {
    this.stepCopyNr = stepCopyNr;
  }

  public void getFields( RowMetaInterface r, String name, RowMetaInterface[] info, StepMeta nextStep,
    VariableSpace space, IMetaStore metaStore ) throws HopStepException {

  }

  private void readData( Node stepnode ) throws HopXMLException {
    try {
      Node steps = XMLHandler.getSubNode( stepnode, "steps" );
      int nrsteps = XMLHandler.countNodes( steps, "step" );

      allocate( nrsteps );

      for ( int i = 0; i < nrsteps; i++ ) {
        Node fnode = XMLHandler.getSubNodeByNr( steps, "step", i );
        stepName[i] = XMLHandler.getTagValue( fnode, "name" );
        stepCopyNr[i] = XMLHandler.getTagValue( fnode, "CopyNr" );
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
      retval.append( "        " + XMLHandler.addTagValue( "name", stepName[i] ) );
      retval.append( "        " + XMLHandler.addTagValue( "CopyNr", stepCopyNr[i] ) );

      retval.append( "        </step>" + Const.CR );
    }
    retval.append( "      </steps>" + Const.CR );

    return retval.toString();
  }

  public void setDefault() {
    int nrsteps = 0;

    allocate( nrsteps );

    for ( int i = 0; i < nrsteps; i++ ) {
      stepName[i] = "step" + i;
      stepCopyNr[i] = "CopyNr" + i;
    }
  }

  public void check( List<CheckResultInterface> remarks, TransMeta transMeta, StepMeta stepMeta,
    RowMetaInterface prev, String[] input, String[] output, RowMetaInterface info, VariableSpace space,
    IMetaStore metaStore ) {
    CheckResult cr;

    if ( prev == null || prev.size() == 0 ) {
      cr =
        new CheckResult( CheckResult.TYPE_RESULT_WARNING, BaseMessages.getString(
          PKG, "BlockUntilStepsFinishMeta.CheckResult.NotReceivingFields" ), stepMeta );
    } else {
      if ( stepName.length > 0 ) {
        cr =
          new CheckResult( CheckResult.TYPE_RESULT_OK, BaseMessages.getString(
            PKG, "BlockUntilStepsFinishMeta.CheckResult.AllStepsFound" ), stepMeta );
      } else {
        cr =
          new CheckResult( CheckResult.TYPE_RESULT_WARNING, BaseMessages.getString(
            PKG, "BlockUntilStepsFinishMeta.CheckResult.NoStepsEntered" ), stepMeta );
      }

    }
    remarks.add( cr );

    // See if we have input streams leading to this step!
    if ( input.length > 0 ) {
      cr =
        new CheckResult( CheckResult.TYPE_RESULT_OK, BaseMessages.getString(
          PKG, "BlockUntilStepsFinishMeta.CheckResult.StepRecevingData2" ), stepMeta );
    } else {
      cr =
        new CheckResult( CheckResult.TYPE_RESULT_ERROR, BaseMessages.getString(
          PKG, "BlockUntilStepsFinishMeta.CheckResult.NoInputReceivedFromOtherSteps" ), stepMeta );
    }
    remarks.add( cr );

  }

  public StepInterface getStep( StepMeta stepMeta, StepDataInterface stepDataInterface, int cnr, TransMeta tr,
    Trans trans ) {
    return new BlockUntilStepsFinish( stepMeta, stepDataInterface, cnr, tr, trans );
  }

  public StepDataInterface getStepData() {
    return new BlockUntilStepsFinishData();
  }

  public TransformationType[] getSupportedTransformationTypes() {
    return new TransformationType[] { TransformationType.Normal, };
  }
}
