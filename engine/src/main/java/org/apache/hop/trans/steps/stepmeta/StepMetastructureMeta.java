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

package org.apache.hop.trans.steps.stepmeta;

import java.util.List;

import org.apache.hop.core.CheckResult;
import org.apache.hop.core.CheckResultInterface;
import org.apache.hop.core.database.DatabaseMeta;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.exception.HopStepException;
import org.apache.hop.core.exception.HopXMLException;
import org.apache.hop.core.row.RowMetaInterface;
import org.apache.hop.core.row.ValueMetaInterface;
import org.apache.hop.core.row.value.ValueMetaInteger;
import org.apache.hop.core.row.value.ValueMetaString;
import org.apache.hop.core.variables.VariableSpace;
import org.apache.hop.core.xml.XMLHandler;
import org.apache.hop.i18n.BaseMessages;

import org.apache.hop.trans.Trans;
import org.apache.hop.trans.TransMeta;
import org.apache.hop.trans.step.BaseStepMeta;
import org.apache.hop.trans.step.StepDataInterface;
import org.apache.hop.trans.step.StepInterface;
import org.apache.hop.trans.step.StepMeta;
import org.apache.hop.trans.step.StepMetaInterface;
import org.apache.hop.metastore.api.IMetaStore;
import org.w3c.dom.Node;

public class StepMetastructureMeta extends BaseStepMeta implements StepMetaInterface {

  private static Class<?> PKG = StepMetastructureMeta.class; // for i18n purposes, needed by Translator2!!

  private String fieldName;
  private String comments;
  private String typeName;
  private String positionName;
  private String lengthName;
  private String precisionName;
  private String originName;

  private boolean outputRowcount;
  private String rowcountField;

  @Override
  public void loadXML( Node stepnode, IMetaStore metaStore ) throws HopXMLException {
    readData( stepnode );
  }

  @Override
  public Object clone() {
    Object retval = super.clone();
    return retval;
  }

  @Override
  public String getXML() {
    StringBuilder retval = new StringBuilder( 500 );

    retval.append( "      " ).append( XMLHandler.addTagValue( "outputRowcount", outputRowcount ) );
    retval.append( "    " ).append( XMLHandler.addTagValue( "rowcountField", rowcountField ) );

    return retval.toString();
  }

  private void readData( Node stepnode ) throws HopXMLException {
    try {
      outputRowcount = "Y".equalsIgnoreCase( XMLHandler.getTagValue( stepnode, "outputRowcount" ) );
      rowcountField = XMLHandler.getTagValue( stepnode, "rowcountField" );
    } catch ( Exception e ) {
      throw new HopXMLException( "Unable to load step info from XML", e );
    }
  }

  @Override
  public StepInterface getStep( StepMeta stepMeta, StepDataInterface stepDataInterface, int cnr, TransMeta tr,
    Trans trans ) {
    return new StepMetastructure( stepMeta, stepDataInterface, cnr, tr, trans );
  }

  @Override
  public StepDataInterface getStepData() {
    return new StepMetastructureData();
  }

  @Override
  public void check( List<CheckResultInterface> remarks, TransMeta transMeta, StepMeta stepMeta,
    RowMetaInterface prev, String[] input, String[] output, RowMetaInterface info, VariableSpace space,
    IMetaStore metaStore ) {
    CheckResult cr;

    cr = new CheckResult( CheckResultInterface.TYPE_RESULT_OK, "Not implemented", stepMeta );
    remarks.add( cr );

  }

  @Override
  public void getFields( RowMetaInterface r, String name, RowMetaInterface[] info, StepMeta nextStep,
    VariableSpace space, IMetaStore metaStore ) throws HopStepException {
    // we create a new output row structure - clear r
    r.clear();

    this.setDefault();
    // create the new fields
    // Position
    ValueMetaInterface positionFieldValue = new ValueMetaInteger( positionName );
    positionFieldValue.setOrigin( name );
    r.addValueMeta( positionFieldValue );
    // field name
    ValueMetaInterface nameFieldValue = new ValueMetaString( fieldName );
    nameFieldValue.setOrigin( name );
    r.addValueMeta( nameFieldValue );
    // comments
    ValueMetaInterface commentsFieldValue = new ValueMetaString( comments );
    nameFieldValue.setOrigin( name );
    r.addValueMeta( commentsFieldValue );
    // Type
    ValueMetaInterface typeFieldValue = new ValueMetaString( typeName );
    typeFieldValue.setOrigin( name );
    r.addValueMeta( typeFieldValue );
    // Length
    ValueMetaInterface lengthFieldValue = new ValueMetaInteger( lengthName );
    lengthFieldValue.setOrigin( name );
    r.addValueMeta( lengthFieldValue );
    // Precision
    ValueMetaInterface precisionFieldValue = new ValueMetaInteger( precisionName );
    precisionFieldValue.setOrigin( name );
    r.addValueMeta( precisionFieldValue );
    // Origin
    ValueMetaInterface originFieldValue = new ValueMetaString( originName );
    originFieldValue.setOrigin( name );
    r.addValueMeta( originFieldValue );

    if ( isOutputRowcount() ) {
      // RowCount
      ValueMetaInterface v = new ValueMetaInteger( this.getRowcountField() );
      v.setOrigin( name );
      r.addValueMeta( v );
    }

  }

  @Override
  public void setDefault() {
    positionName = BaseMessages.getString( PKG, "StepMetastructureMeta.PositionName" );
    fieldName = BaseMessages.getString( PKG, "StepMetastructureMeta.FieldName" );
    comments = BaseMessages.getString( PKG, "StepMetastructureMeta.Comments" );
    typeName = BaseMessages.getString( PKG, "StepMetastructureMeta.TypeName" );
    lengthName = BaseMessages.getString( PKG, "StepMetastructureMeta.LengthName" );
    precisionName = BaseMessages.getString( PKG, "StepMetastructureMeta.PrecisionName" );
    originName = BaseMessages.getString( PKG, "StepMetastructureMeta.OriginName" );

  }

  public boolean isOutputRowcount() {
    return outputRowcount;
  }

  public void setOutputRowcount( boolean outputRowcount ) {
    this.outputRowcount = outputRowcount;
  }

  public String getRowcountField() {
    return rowcountField;
  }

  public void setRowcountField( String rowcountField ) {
    this.rowcountField = rowcountField;
  }

}
