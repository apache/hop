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

package org.apache.hop.trans.steps.fieldschangesequence;

import org.apache.hop.core.CheckResult;
import org.apache.hop.core.CheckResultInterface;
import org.apache.hop.core.Const;
import org.apache.hop.core.exception.HopXMLException;
import org.apache.hop.core.row.RowMetaInterface;
import org.apache.hop.core.row.ValueMetaInterface;
import org.apache.hop.core.row.value.ValueMetaInteger;
import org.apache.hop.core.util.Utils;
import org.apache.hop.core.variables.VariableSpace;
import org.apache.hop.core.xml.XMLHandler;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.metastore.api.IMetaStore;
import org.apache.hop.trans.Trans;
import org.apache.hop.trans.TransMeta;
import org.apache.hop.trans.step.BaseStepMeta;
import org.apache.hop.trans.step.StepDataInterface;
import org.apache.hop.trans.step.StepInterface;
import org.apache.hop.trans.step.StepMeta;
import org.apache.hop.trans.step.StepMetaInterface;
import org.w3c.dom.Node;

import java.util.List;

/*
 * Created on 30-06-2008
 *
 */

public class FieldsChangeSequenceMeta extends BaseStepMeta implements StepMetaInterface {
  private static Class<?> PKG = FieldsChangeSequenceMeta.class; // for i18n purposes, needed by Translator2!!

  /**
   * by which fields to display?
   */
  private String[] fieldName;

  private String resultfieldName;

  private String start;

  private String increment;

  public FieldsChangeSequenceMeta() {
    super(); // allocate BaseStepMeta
  }

  public String getStart() {
    return start;
  }

  /**
   * @return Returns the resultfieldName.
   */
  public String getResultFieldName() {
    return resultfieldName;
  }

  /**
   * @param resultfieldName The resultfieldName to set.
   */
  public void setResultFieldName( String resultfieldName ) {
    this.resultfieldName = resultfieldName;
  }

  @Override
  public void loadXML( Node stepnode, IMetaStore metaStore ) throws HopXMLException {
    readData( stepnode );
  }

  @Override
  public Object clone() {
    FieldsChangeSequenceMeta retval = (FieldsChangeSequenceMeta) super.clone();

    int nrfields = fieldName.length;

    retval.allocate( nrfields );

    System.arraycopy( fieldName, 0, retval.fieldName, 0, nrfields );
    return retval;
  }

  public void allocate( int nrfields ) {
    fieldName = new String[ nrfields ];
  }

  /**
   * @return Returns the fieldName.
   */
  public String[] getFieldName() {
    return fieldName;
  }

  /**
   * @param fieldName The fieldName to set.
   */
  public void setFieldName( String[] fieldName ) {
    this.fieldName = fieldName;
  }

  public void setStart( String start ) {
    this.start = start;
  }

  public void setIncrement( String increment ) {
    this.increment = increment;
  }

  public String getIncrement() {
    return increment;
  }

  private void readData( Node stepnode ) throws HopXMLException {
    try {
      start = XMLHandler.getTagValue( stepnode, "start" );
      increment = XMLHandler.getTagValue( stepnode, "increment" );
      resultfieldName = XMLHandler.getTagValue( stepnode, "resultfieldName" );

      Node fields = XMLHandler.getSubNode( stepnode, "fields" );
      int nrfields = XMLHandler.countNodes( fields, "field" );

      allocate( nrfields );

      for ( int i = 0; i < nrfields; i++ ) {
        Node fnode = XMLHandler.getSubNodeByNr( fields, "field", i );
        fieldName[ i ] = XMLHandler.getTagValue( fnode, "name" );
      }
    } catch ( Exception e ) {
      throw new HopXMLException( "Unable to load step info from XML", e );
    }
  }

  @Override
  public String getXML() {
    StringBuilder retval = new StringBuilder();
    retval.append( "      " + XMLHandler.addTagValue( "start", start ) );
    retval.append( "      " + XMLHandler.addTagValue( "increment", increment ) );
    retval.append( "      " + XMLHandler.addTagValue( "resultfieldName", resultfieldName ) );

    retval.append( "    <fields>" + Const.CR );
    for ( int i = 0; i < fieldName.length; i++ ) {
      retval.append( "      <field>" + Const.CR );
      retval.append( "        " + XMLHandler.addTagValue( "name", fieldName[ i ] ) );
      retval.append( "        </field>" + Const.CR );
    }
    retval.append( "      </fields>" + Const.CR );

    return retval.toString();
  }

  @Override
  public void setDefault() {
    resultfieldName = null;
    start = "1";
    increment = "1";
    int nrfields = 0;

    allocate( nrfields );

    for ( int i = 0; i < nrfields; i++ ) {
      fieldName[ i ] = "field" + i;
    }
  }

  @Override
  public void getFields( RowMetaInterface r, String name, RowMetaInterface[] info, StepMeta nextStep,
                         VariableSpace space, IMetaStore metaStore ) {
    if ( !Utils.isEmpty( resultfieldName ) ) {
      ValueMetaInterface v = new ValueMetaInteger( resultfieldName );
      v.setLength( ValueMetaInterface.DEFAULT_INTEGER_LENGTH, 0 );
      v.setOrigin( name );
      r.addValueMeta( v );
    }
  }

  @Override
  public void check( List<CheckResultInterface> remarks, TransMeta transMeta, StepMeta stepMeta,
                     RowMetaInterface prev, String[] input, String[] output, RowMetaInterface info, VariableSpace space,
                     IMetaStore metaStore ) {
    CheckResult cr;
    String error_message = "";

    if ( Utils.isEmpty( resultfieldName ) ) {
      error_message = BaseMessages.getString( PKG, "FieldsChangeSequenceMeta.CheckResult.ResultFieldMissing" );
      cr = new CheckResult( CheckResult.TYPE_RESULT_ERROR, error_message, stepMeta );
    } else {
      error_message = BaseMessages.getString( PKG, "FieldsChangeSequenceMeta.CheckResult.ResultFieldOK" );
      cr = new CheckResult( CheckResult.TYPE_RESULT_OK, error_message, stepMeta );
    }
    remarks.add( cr );

    if ( prev == null || prev.size() == 0 ) {
      cr =
        new CheckResult( CheckResult.TYPE_RESULT_WARNING, BaseMessages.getString(
          PKG, "FieldsChangeSequenceMeta.CheckResult.NotReceivingFields" ), stepMeta );
      remarks.add( cr );
    } else {
      cr =
        new CheckResult( CheckResult.TYPE_RESULT_OK, BaseMessages.getString(
          PKG, "FieldsChangeSequenceMeta.CheckResult.StepRecevingData", prev.size() + "" ), stepMeta );
      remarks.add( cr );

      boolean error_found = false;
      error_message = "";

      // Starting from selected fields in ...
      for ( int i = 0; i < fieldName.length; i++ ) {
        int idx = prev.indexOfValue( fieldName[ i ] );
        if ( idx < 0 ) {
          error_message += "\t\t" + fieldName[ i ] + Const.CR;
          error_found = true;
        }
      }
      if ( error_found ) {
        error_message =
          BaseMessages.getString( PKG, "FieldsChangeSequenceMeta.CheckResult.FieldsFound", error_message );

        cr = new CheckResult( CheckResult.TYPE_RESULT_ERROR, error_message, stepMeta );
        remarks.add( cr );
      } else {
        if ( fieldName.length > 0 ) {
          cr =
            new CheckResult( CheckResult.TYPE_RESULT_OK, BaseMessages.getString(
              PKG, "FieldsChangeSequenceMeta.CheckResult.AllFieldsFound" ), stepMeta );
          remarks.add( cr );
        } else {
          cr =
            new CheckResult( CheckResult.TYPE_RESULT_WARNING, BaseMessages.getString(
              PKG, "FieldsChangeSequenceMeta.CheckResult.NoFieldsEntered" ), stepMeta );
          remarks.add( cr );
        }
      }

    }

    // See if we have input streams leading to this step!
    if ( input.length > 0 ) {
      cr =
        new CheckResult( CheckResult.TYPE_RESULT_OK, BaseMessages.getString(
          PKG, "FieldsChangeSequenceMeta.CheckResult.StepRecevingData2" ), stepMeta );
      remarks.add( cr );
    } else {
      cr =
        new CheckResult( CheckResult.TYPE_RESULT_ERROR, BaseMessages.getString(
          PKG, "FieldsChangeSequenceMeta.CheckResult.NoInputReceivedFromOtherSteps" ), stepMeta );
      remarks.add( cr );
    }
  }

  @Override
  public StepInterface getStep( StepMeta stepMeta, StepDataInterface stepDataInterface, int cnr,
                                TransMeta transMeta, Trans trans ) {
    return new FieldsChangeSequence( stepMeta, stepDataInterface, cnr, transMeta, trans );
  }

  @Override
  public StepDataInterface getStepData() {
    return new FieldsChangeSequenceData();
  }

  @Override
  public boolean supportsErrorHandling() {
    return true;
  }

}
