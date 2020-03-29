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

package org.apache.hop.trans.steps.concatfields;

import org.apache.hop.core.CheckResult;
import org.apache.hop.core.CheckResultInterface;
import org.apache.hop.core.Const;
import org.apache.hop.core.annotations.Step;
import org.apache.hop.core.exception.HopStepException;
import org.apache.hop.core.exception.HopValueException;
import org.apache.hop.core.exception.HopXMLException;
import org.apache.hop.core.row.RowMetaInterface;
import org.apache.hop.core.row.ValueMetaInterface;
import org.apache.hop.core.row.value.ValueMetaString;
import org.apache.hop.core.util.Utils;
import org.apache.hop.core.variables.VariableSpace;
import org.apache.hop.core.xml.XMLHandler;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.metastore.api.IMetaStore;
import org.apache.hop.trans.Trans;
import org.apache.hop.trans.TransMeta;
import org.apache.hop.trans.step.StepDataInterface;
import org.apache.hop.trans.step.StepInterface;
import org.apache.hop.trans.step.StepMeta;
import org.apache.hop.trans.step.StepMetaInjectionInterface;
import org.apache.hop.trans.step.StepMetaInterface;
import org.apache.hop.trans.steps.textfileoutput.TextFileField;
import org.apache.hop.trans.steps.textfileoutput.TextFileOutputMeta;
import org.w3c.dom.Node;

import java.util.List;

/*
 * ConcatFieldsMeta
 * @author jb
 * @since 2012-08-31
 *
 */
@Step(
  id = "ConcatFields",
  name = "ConcatFields.Name",
  description = "ConcatFields.Description",
  categoryDescription = "BaseStep.Category.Transform",
  i18nPackageName = "org.apache.hop.trans.step"
)
public class ConcatFieldsMeta extends TextFileOutputMeta implements StepMetaInterface {

  private static final Class<?> PKG = ConcatFieldsMeta.class; // for i18n purposes, needed by Translator2!!

  private static final String ConcatFieldsNodeNameSpace = "ConcatFields";

  private String targetFieldName; // the target field name
  private int targetFieldLength; // the length of the string field
  private boolean removeSelectedFields; // remove the selected fields in the output stream

  public String getTargetFieldName() {
    return targetFieldName;
  }

  public void setTargetFieldName( String targetField ) {
    this.targetFieldName = targetField;
  }

  public int getTargetFieldLength() {
    return targetFieldLength;
  }

  public void setTargetFieldLength( int targetFieldLength ) {
    this.targetFieldLength = targetFieldLength;
  }

  public boolean isRemoveSelectedFields() {
    return removeSelectedFields;
  }

  public void setRemoveSelectedFields( boolean removeSelectedFields ) {
    this.removeSelectedFields = removeSelectedFields;
  }

  public ConcatFieldsMeta() {
    super(); // allocate TextFileOutputMeta
  }

  @Override
  public void setDefault() {
    super.setDefault();
    // overwrite header
    super.setHeaderEnabled( false );
    // set default for new properties specific to the concat fields
    targetFieldName = "";
    targetFieldLength = 0;
    removeSelectedFields = false;
  }

  public void getFieldsModifyInput( RowMetaInterface row, String name, RowMetaInterface[] info, StepMeta nextStep,
                                    VariableSpace space, IMetaStore metaStore )
    throws HopStepException {
    // the field precisions and lengths are altered! see TextFileOutputMeta.getFields().
    super.getFields( row, name, info, nextStep, space, metaStore );
  }

  @Override
  public void getFields( RowMetaInterface row, String name, RowMetaInterface[] info, StepMeta nextStep,
                         VariableSpace space, IMetaStore metaStore ) throws HopStepException {
    // do not call the super class from TextFileOutputMeta since it modifies the source meta data
    // see getFieldsModifyInput() instead

    // remove selected fields from the stream when true
    if ( removeSelectedFields ) {
      if ( getOutputFields().length > 0 ) {
        for ( int i = 0; i < getOutputFields().length; i++ ) {
          TextFileField field = getOutputFields()[ i ];
          try {
            row.removeValueMeta( field.getName() );
          } catch ( HopValueException e ) {
            // just ignore exceptions since missing fields are handled in the ConcatFields class
          }
        }
      } else { // no output fields selected, take them all, remove them all
        row.clear();
      }
    }

    // Check Target Field Name
    if ( Utils.isEmpty( targetFieldName ) ) {
      throw new HopStepException( BaseMessages.getString(
        PKG, "ConcatFieldsMeta.CheckResult.TargetFieldNameMissing" ) );
    }
    // add targetFieldName
    ValueMetaInterface vValue = new ValueMetaString( targetFieldName );
    vValue.setLength( targetFieldLength, 0 );
    vValue.setOrigin( name );
    if ( !Utils.isEmpty( getEncoding() ) ) {
      vValue.setStringEncoding( getEncoding() );
    }
    row.addValueMeta( vValue );
  }

  @Override
  public void loadXML( Node stepnode, IMetaStore metaStore ) throws HopXMLException {
    super.loadXML( stepnode, metaStore );
    targetFieldName = XMLHandler.getTagValue( stepnode, ConcatFieldsNodeNameSpace, "targetFieldName" );
    targetFieldLength =
      Const.toInt( XMLHandler.getTagValue( stepnode, ConcatFieldsNodeNameSpace, "targetFieldLength" ), 0 );
    removeSelectedFields =
      "Y"
        .equalsIgnoreCase( XMLHandler
          .getTagValue( stepnode, ConcatFieldsNodeNameSpace, "removeSelectedFields" ) );
  }

  @Override
  public String getXML() {
    String retval = super.getXML();
    retval = retval + "    <" + ConcatFieldsNodeNameSpace + ">" + Const.CR;
    retval = retval + XMLHandler.addTagValue( "targetFieldName", targetFieldName );
    retval = retval + XMLHandler.addTagValue( "targetFieldLength", targetFieldLength );
    retval = retval + XMLHandler.addTagValue( "removeSelectedFields", removeSelectedFields );
    retval = retval + "    </" + ConcatFieldsNodeNameSpace + ">" + Const.CR;
    return retval;
  }

  @Override
  public void check( List<CheckResultInterface> remarks, TransMeta transMeta, StepMeta stepMeta,
                     RowMetaInterface prev, String[] input, String[] output, RowMetaInterface info, VariableSpace space,
                     IMetaStore metaStore ) {
    CheckResult cr;

    // Check Target Field Name
    if ( Utils.isEmpty( targetFieldName ) ) {
      cr =
        new CheckResult( CheckResultInterface.TYPE_RESULT_ERROR, BaseMessages.getString(
          PKG, "ConcatFieldsMeta.CheckResult.TargetFieldNameMissing" ), stepMeta );
      remarks.add( cr );
    }

    // Check Target Field Length when Fast Data Dump
    if ( targetFieldLength <= 0 && isFastDump() ) {
      cr =
        new CheckResult( CheckResultInterface.TYPE_RESULT_WARNING, BaseMessages.getString(
          PKG, "ConcatFieldsMeta.CheckResult.TargetFieldLengthMissingFastDataDump" ), stepMeta );
      remarks.add( cr );
    }

    // Check output fields
    if ( prev != null && prev.size() > 0 ) {
      cr =
        new CheckResult( CheckResultInterface.TYPE_RESULT_OK, BaseMessages.getString(
          PKG, "ConcatFieldsMeta.CheckResult.FieldsReceived", "" + prev.size() ), stepMeta );
      remarks.add( cr );

      String error_message = "";
      boolean error_found = false;

      // Starting from selected fields in ...
      for ( int i = 0; i < getOutputFields().length; i++ ) {
        int idx = prev.indexOfValue( getOutputFields()[ i ].getName() );
        if ( idx < 0 ) {
          error_message += "\t\t" + getOutputFields()[ i ].getName() + Const.CR;
          error_found = true;
        }
      }
      if ( error_found ) {
        error_message = BaseMessages.getString( PKG, "ConcatFieldsMeta.CheckResult.FieldsNotFound", error_message );
        cr = new CheckResult( CheckResultInterface.TYPE_RESULT_ERROR, error_message, stepMeta );
        remarks.add( cr );
      } else {
        cr =
          new CheckResult( CheckResultInterface.TYPE_RESULT_OK, BaseMessages.getString(
            PKG, "ConcatFieldsMeta.CheckResult.AllFieldsFound" ), stepMeta );
        remarks.add( cr );
      }
    }

  }

  @Override
  public StepInterface getStep( StepMeta stepMeta, StepDataInterface stepDataInterface, int cnr,
                                TransMeta transMeta, Trans trans ) {
    return new ConcatFields( stepMeta, stepDataInterface, cnr, transMeta, trans );
  }

  @Override
  public StepDataInterface getStepData() {
    return new ConcatFieldsData();
  }

  @Override
  public StepMetaInjectionInterface getStepMetaInjectionInterface() {
    return new ConcatFieldsMetaInjection( this );
  }

}
