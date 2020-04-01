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

package org.apache.hop.pipeline.steps.clonerow;

import org.apache.hop.core.CheckResult;
import org.apache.hop.core.CheckResultInterface;
import org.apache.hop.core.annotations.Step;
import org.apache.hop.core.exception.HopStepException;
import org.apache.hop.core.exception.HopXMLException;
import org.apache.hop.core.row.RowMetaInterface;
import org.apache.hop.core.row.ValueMetaInterface;
import org.apache.hop.core.row.value.ValueMetaBoolean;
import org.apache.hop.core.row.value.ValueMetaInteger;
import org.apache.hop.core.util.Utils;
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
 * Created on 27-06-2008
 *
 */
@Step( 
  id = "CloneRow", 
  name = "CloneRow.Name",  
  description = "CloneRow.Description",
  image = "CloneRow.svg",
  categoryDescription = "i18n:org.apache.hop.pipeline.step:BaseStep.Category.Utility",
  i18nPackageName = "org.apache.hop.pipeline.steps.clonerow"
)
public class CloneRowMeta extends BaseStepMeta implements StepMetaInterface {
  private static Class<?> PKG = CloneRowMeta.class; // for i18n purposes, needed by Translator!!

  /**
   * nr of clone rows
   */
  private String nrclones;

  /**
   * Flag: add clone flag
   */

  private boolean addcloneflag;

  /**
   * clone flag field
   */
  private String cloneflagfield;

  private boolean nrcloneinfield;

  private String nrclonefield;

  private boolean addclonenum;
  private String clonenumfield;

  public CloneRowMeta() {
    super(); // allocate BaseStepMeta
  }

  @Override
  public String getXML() {
    StringBuilder retval = new StringBuilder();
    retval.append( "    " + XMLHandler.addTagValue( "nrclones", nrclones ) );
    retval.append( "    " + XMLHandler.addTagValue( "addcloneflag", addcloneflag ) );
    retval.append( "    " + XMLHandler.addTagValue( "cloneflagfield", cloneflagfield ) );
    retval.append( "    " + XMLHandler.addTagValue( "nrcloneinfield", nrcloneinfield ) );
    retval.append( "    " + XMLHandler.addTagValue( "nrclonefield", nrclonefield ) );

    retval.append( "    " + XMLHandler.addTagValue( "addclonenum", addclonenum ) );
    retval.append( "    " + XMLHandler.addTagValue( "clonenumfield", clonenumfield ) );

    return retval.toString();
  }

  @Override
  public void loadXML( Node stepnode, IMetaStore metaStore ) throws HopXMLException {
    readData( stepnode );
  }

  public Object clone() {
    Object retval = super.clone();
    return retval;
  }

  public String getNrClones() {
    return nrclones;
  }

  public void setNrClones( String nrclones ) {
    this.nrclones = nrclones;
  }

  public boolean isAddCloneFlag() {
    return addcloneflag;
  }

  public void setAddCloneFlag( boolean addcloneflag ) {
    this.addcloneflag = addcloneflag;
  }

  public boolean isNrCloneInField() {
    return nrcloneinfield;
  }

  public void setNrCloneInField( boolean nrcloneinfield ) {
    this.nrcloneinfield = nrcloneinfield;
  }

  public boolean isAddCloneNum() {
    return addclonenum;
  }

  public void setAddCloneNum( boolean addclonenum ) {
    this.addclonenum = addclonenum;
  }

  public String getCloneNumField() {
    return clonenumfield;
  }

  public void setCloneNumField( String clonenumfield ) {
    this.clonenumfield = clonenumfield;
  }

  public String getNrCloneField() {
    return nrclonefield;
  }

  public void setNrCloneField( String nrclonefield ) {
    this.nrclonefield = nrclonefield;
  }

  public String getCloneFlagField() {
    return cloneflagfield;
  }

  public void setCloneFlagField( String cloneflagfield ) {
    this.cloneflagfield = cloneflagfield;
  }

  private void readData( Node stepnode ) throws HopXMLException {
    try {
      nrclones = XMLHandler.getTagValue( stepnode, "nrclones" );
      addcloneflag = "Y".equalsIgnoreCase( XMLHandler.getTagValue( stepnode, "addcloneflag" ) );
      cloneflagfield = XMLHandler.getTagValue( stepnode, "cloneflagfield" );
      nrcloneinfield = "Y".equalsIgnoreCase( XMLHandler.getTagValue( stepnode, "nrcloneinfield" ) );
      nrclonefield = XMLHandler.getTagValue( stepnode, "nrclonefield" );
      addclonenum = "Y".equalsIgnoreCase( XMLHandler.getTagValue( stepnode, "addclonenum" ) );
      clonenumfield = XMLHandler.getTagValue( stepnode, "clonenumfield" );

    } catch ( Exception e ) {
      throw new HopXMLException(
        BaseMessages.getString( PKG, "CloneRowMeta.Exception.UnableToReadStepInfo" ), e );
    }
  }

  @Override
  public void setDefault() {
    nrclones = "0";
    cloneflagfield = null;
    nrclonefield = null;
    nrcloneinfield = false;
    addcloneflag = false;
    addclonenum = false;
    clonenumfield = null;
  }

  @Override
  public void getFields( RowMetaInterface rowMeta, String origin, RowMetaInterface[] info, StepMeta nextStep,
                         VariableSpace space, IMetaStore metaStore ) throws HopStepException {
    // Output field (boolean) ?
    if ( addcloneflag ) {
      String realfieldValue = space.environmentSubstitute( cloneflagfield );
      if ( !Utils.isEmpty( realfieldValue ) ) {
        ValueMetaInterface v = new ValueMetaBoolean( realfieldValue );
        v.setOrigin( origin );
        rowMeta.addValueMeta( v );
      }
    }
    // Output clone row number
    if ( addclonenum ) {
      String realfieldValue = space.environmentSubstitute( clonenumfield );
      if ( !Utils.isEmpty( realfieldValue ) ) {
        ValueMetaInterface v = new ValueMetaInteger( realfieldValue );
        v.setOrigin( origin );
        rowMeta.addValueMeta( v );
      }
    }
  }

  public void check( List<CheckResultInterface> remarks, PipelineMeta pipelineMeta, StepMeta stepMeta,
                     RowMetaInterface prev, String[] input, String[] output, RowMetaInterface info, VariableSpace space,
                     IMetaStore metaStore ) {
    CheckResult cr;
    String error_message = "";

    if ( Utils.isEmpty( nrclones ) ) {
      error_message = BaseMessages.getString( PKG, "CloneRowMeta.CheckResult.NrClonesdMissing" );
      cr = new CheckResult( CheckResultInterface.TYPE_RESULT_ERROR, error_message, stepMeta );
    } else {
      error_message = BaseMessages.getString( PKG, "CloneRowMeta.CheckResult.NrClonesOK" );
      cr = new CheckResult( CheckResultInterface.TYPE_RESULT_OK, error_message, stepMeta );
    }
    remarks.add( cr );

    if ( addcloneflag ) {
      if ( Utils.isEmpty( cloneflagfield ) ) {
        error_message = BaseMessages.getString( PKG, "CloneRowMeta.CheckResult.CloneFlagFieldMissing" );
        cr = new CheckResult( CheckResultInterface.TYPE_RESULT_ERROR, error_message, stepMeta );
      } else {
        error_message = BaseMessages.getString( PKG, "CloneRowMeta.CheckResult.CloneFlagFieldOk" );
        cr = new CheckResult( CheckResultInterface.TYPE_RESULT_OK, error_message, stepMeta );
      }
      remarks.add( cr );
    }
    if ( addclonenum ) {
      if ( Utils.isEmpty( clonenumfield ) ) {
        error_message = BaseMessages.getString( PKG, "CloneRowMeta.CheckResult.CloneNumFieldMissing" );
        cr = new CheckResult( CheckResultInterface.TYPE_RESULT_ERROR, error_message, stepMeta );
      } else {
        error_message = BaseMessages.getString( PKG, "CloneRowMeta.CheckResult.CloneNumFieldOk" );
        cr = new CheckResult( CheckResultInterface.TYPE_RESULT_OK, error_message, stepMeta );
      }
      remarks.add( cr );
    }
    if ( nrcloneinfield ) {
      if ( Utils.isEmpty( nrclonefield ) ) {
        error_message = BaseMessages.getString( PKG, "CloneRowMeta.CheckResult.NrCloneFieldMissing" );
        cr = new CheckResult( CheckResult.TYPE_RESULT_ERROR, error_message, stepMeta );
      } else {
        error_message = BaseMessages.getString( PKG, "CloneRowMeta.CheckResult.NrCloneFieldOk" );
        cr = new CheckResult( CheckResult.TYPE_RESULT_OK, error_message, stepMeta );
      }
      remarks.add( cr );
    }

    if ( prev == null || prev.size() == 0 ) {
      cr =
        new CheckResult( CheckResultInterface.TYPE_RESULT_WARNING, BaseMessages.getString(
          PKG, "CloneRowMeta.CheckResult.NotReceivingFields" ), stepMeta );
    } else {
      cr =
        new CheckResult( CheckResultInterface.TYPE_RESULT_OK, BaseMessages.getString(
          PKG, "CloneRowMeta.CheckResult.StepRecevingData", prev.size() + "" ), stepMeta );
    }
    remarks.add( cr );

    // See if we have input streams leading to this step!
    if ( input.length > 0 ) {
      cr =
        new CheckResult( CheckResultInterface.TYPE_RESULT_OK, BaseMessages.getString(
          PKG, "CloneRowMeta.CheckResult.StepRecevingData2" ), stepMeta );
    } else {
      cr =
        new CheckResult( CheckResultInterface.TYPE_RESULT_ERROR, BaseMessages.getString(
          PKG, "CloneRowMeta.CheckResult.NoInputReceivedFromOtherSteps" ), stepMeta );
    }
    remarks.add( cr );
  }

  @Override
  public StepInterface getStep( StepMeta stepMeta, StepDataInterface stepDataInterface, int cnr, PipelineMeta tr,
                                Pipeline pipeline ) {
    return new CloneRow( stepMeta, stepDataInterface, cnr, tr, pipeline );
  }

  @Override
  public StepDataInterface getStepData() {
    return new CloneRowData();
  }

}
