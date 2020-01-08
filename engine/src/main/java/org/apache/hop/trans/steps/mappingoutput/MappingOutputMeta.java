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

package org.apache.hop.trans.steps.mappingoutput;

import org.apache.hop.core.CheckResult;
import org.apache.hop.core.CheckResultInterface;
import org.apache.hop.core.exception.HopStepException;
import org.apache.hop.core.row.RowMetaInterface;
import org.apache.hop.core.row.ValueMetaInterface;
import org.apache.hop.core.variables.VariableSpace;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.metastore.api.IMetaStore;
import org.apache.hop.trans.Trans;
import org.apache.hop.trans.TransMeta;
import org.apache.hop.trans.step.BaseStepMeta;
import org.apache.hop.trans.step.StepDataInterface;
import org.apache.hop.trans.step.StepInterface;
import org.apache.hop.trans.step.StepMeta;
import org.apache.hop.trans.step.StepMetaInterface;
import org.apache.hop.trans.steps.mapping.MappingValueRename;

import java.util.ArrayList;
import java.util.List;

/*
 * Created on 02-jun-2003
 *
 */

public class MappingOutputMeta extends BaseStepMeta implements StepMetaInterface {
  private static Class<?> PKG = MappingOutputMeta.class; // for i18n purposes, needed by Translator2!!

  private volatile List<MappingValueRename> inputValueRenames;
  private volatile List<MappingValueRename> outputValueRenames;

  public MappingOutputMeta() {
    super(); // allocate BaseStepMeta
    inputValueRenames = new ArrayList<MappingValueRename>();
    inputValueRenames = new ArrayList<MappingValueRename>();
  }

  public Object clone() {
    MappingOutputMeta retval = (MappingOutputMeta) super.clone();

    return retval;
  }

  public void allocate( int nrfields ) {
  }

  public void setDefault() {
    int nrfields = 0;

    allocate( nrfields );
  }

  public void getFields( RowMetaInterface r, String name, RowMetaInterface[] info, StepMeta nextStep,
                         VariableSpace space, IMetaStore metaStore ) throws HopStepException {
    // It's best that this method doesn't change anything by itself.
    // Eventually it's the Mapping step that's going to tell this step how to behave meta-data wise.
    // It is the mapping step that tells the mapping output step what fields to rename.
    //
    if ( inputValueRenames != null ) {
      for ( MappingValueRename valueRename : inputValueRenames ) {
        ValueMetaInterface valueMeta = r.searchValueMeta( valueRename.getTargetValueName() );
        if ( valueMeta != null ) {
          valueMeta.setName( valueRename.getSourceValueName() );
        }
      }
    }

    // This is the optionally entered stuff in the output tab of the mapping dialog.
    //
    if ( outputValueRenames != null ) {
      for ( MappingValueRename valueRename : outputValueRenames ) {
        int valueMetaRenameIndex = r.indexOfValue( valueRename.getSourceValueName() );
        if ( valueMetaRenameIndex >= 0 ) {
          ValueMetaInterface valueMetaRename = r.getValueMeta( valueMetaRenameIndex ).clone();
          valueMetaRename.setName( valueRename.getTargetValueName() );
          // must maintain the same columns order. Noticed when implementing the Mapping step in AEL (BACKLOG-23372)
          r.removeValueMeta( valueMetaRenameIndex );
          r.addValueMeta( valueMetaRenameIndex, valueMetaRename );
        }
      }
    }
  }

  public void check( List<CheckResultInterface> remarks, TransMeta transMeta, StepMeta stepMeta,
                     RowMetaInterface prev, String[] input, String[] output, RowMetaInterface info, VariableSpace space,
                     IMetaStore metaStore ) {
    CheckResult cr;
    if ( prev == null || prev.size() == 0 ) {
      cr =
        new CheckResult( CheckResultInterface.TYPE_RESULT_WARNING, BaseMessages.getString(
          PKG, "MappingOutputMeta.CheckResult.NotReceivingFields" ), stepMeta );
      remarks.add( cr );
    } else {
      cr =
        new CheckResult( CheckResultInterface.TYPE_RESULT_OK, BaseMessages.getString(
          PKG, "MappingOutputMeta.CheckResult.StepReceivingDatasOK", prev.size() + "" ), stepMeta );
      remarks.add( cr );
    }

    // See if we have input streams leading to this step!
    if ( input.length > 0 ) {
      cr =
        new CheckResult( CheckResultInterface.TYPE_RESULT_OK, BaseMessages.getString(
          PKG, "MappingOutputMeta.CheckResult.StepReceivingInfoFromOtherSteps" ), stepMeta );
      remarks.add( cr );
    } else {
      cr =
        new CheckResult( CheckResultInterface.TYPE_RESULT_ERROR, BaseMessages.getString(
          PKG, "MappingOutputMeta.CheckResult.NoInputReceived" ), stepMeta );
      remarks.add( cr );
    }
  }

  public StepInterface getStep( StepMeta stepMeta, StepDataInterface stepDataInterface, int cnr, TransMeta tr,
                                Trans trans ) {
    return new MappingOutput( stepMeta, stepDataInterface, cnr, tr, trans );
  }

  public StepDataInterface getStepData() {
    return new MappingOutputData();
  }

  /**
   * @return the inputValueRenames
   */
  public List<MappingValueRename> getInputValueRenames() {
    return inputValueRenames;
  }

  /**
   * @param inputValueRenames the inputValueRenames to set
   */
  public void setInputValueRenames( List<MappingValueRename> inputValueRenames ) {
    this.inputValueRenames = inputValueRenames;
  }

  /**
   * @return the outputValueRenames
   */
  public List<MappingValueRename> getOutputValueRenames() {
    return outputValueRenames;
  }

  /**
   * @param outputValueRenames the outputValueRenames to set
   */
  public void setOutputValueRenames( List<MappingValueRename> outputValueRenames ) {
    this.outputValueRenames = outputValueRenames;
  }
}
