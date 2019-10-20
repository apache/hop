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

package org.apache.hop.trans.steps.creditcardvalidator;

import java.util.List;

import org.apache.hop.core.CheckResult;
import org.apache.hop.core.CheckResultInterface;
import org.apache.hop.core.database.DatabaseMeta;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.exception.HopStepException;
import org.apache.hop.core.exception.HopXMLException;
import org.apache.hop.core.row.RowMetaInterface;
import org.apache.hop.core.row.ValueMetaInterface;
import org.apache.hop.core.row.value.ValueMetaBoolean;
import org.apache.hop.core.row.value.ValueMetaString;
import org.apache.hop.core.util.Utils;
import org.apache.hop.core.variables.VariableSpace;
import org.apache.hop.core.xml.XMLHandler;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.repository.ObjectId;
import org.apache.hop.repository.Repository;
import org.apache.hop.trans.Trans;
import org.apache.hop.trans.TransMeta;
import org.apache.hop.trans.step.BaseStepMeta;
import org.apache.hop.trans.step.StepDataInterface;
import org.apache.hop.trans.step.StepInterface;
import org.apache.hop.trans.step.StepMeta;
import org.apache.hop.trans.step.StepMetaInterface;
import org.apache.hop.metastore.api.IMetaStore;
import org.w3c.dom.Node;

/*
 * Created on 03-Juin-2008
 *
 */

public class CreditCardValidatorMeta extends BaseStepMeta implements StepMetaInterface {
  private static Class<?> PKG = CreditCardValidatorMeta.class; // for i18n purposes, needed by Translator2!!

  /** dynamic field */
  private String fieldname;

  private String cardtype;

  private String notvalidmsg;

  /** function result: new value name */
  private String resultfieldname;

  private boolean onlydigits;

  public CreditCardValidatorMeta() {
    super(); // allocate BaseStepMeta
  }

  /**
   * @return Returns the fieldname.
   */
  public String getDynamicField() {
    return this.fieldname;
  }

  /**
   * @param fieldname
   *          The fieldname to set.
   */
  public void setDynamicField( String fieldname ) {
    this.fieldname = fieldname;
  }

  /**
   * @return Returns the resultName.
   */
  public String getResultFieldName() {
    return resultfieldname;
  }

  public void setOnlyDigits( boolean onlydigits ) {
    this.onlydigits = onlydigits;
  }

  public boolean isOnlyDigits() {
    return this.onlydigits;
  }

  /**
   * @param resultfieldname
   *          The resultfieldname to set.
   */
  public void setResultFieldName( String resultfieldname ) {
    this.resultfieldname = resultfieldname;
  }

  /**
   * @param cardtype
   *          The cardtype to set.
   */
  public void setCardType( String cardtype ) {
    this.cardtype = cardtype;
  }

  /**
   * @return Returns the cardtype.
   */
  public String getCardType() {
    return cardtype;
  }

  /**
   * @param notvalidmsg
   *          The notvalidmsg to set.
   */
  public void setNotValidMsg( String notvalidmsg ) {
    this.notvalidmsg = notvalidmsg;
  }

  /**
   * @return Returns the notvalidmsg.
   */
  public String getNotValidMsg() {
    return notvalidmsg;
  }

  public void loadXML( Node stepnode, List<DatabaseMeta> databases, IMetaStore metaStore ) throws HopXMLException {
    readData( stepnode );
  }

  public Object clone() {
    CreditCardValidatorMeta retval = (CreditCardValidatorMeta) super.clone();

    return retval;
  }

  public void setDefault() {
    resultfieldname = "result";
    onlydigits = false;
    cardtype = "card type";
    notvalidmsg = "not valid message";
  }

  public void getFields( RowMetaInterface inputRowMeta, String name, RowMetaInterface[] info, StepMeta nextStep,
    VariableSpace space, Repository repository, IMetaStore metaStore ) throws HopStepException {
    String realresultfieldname = space.environmentSubstitute( resultfieldname );
    if ( !Utils.isEmpty( realresultfieldname ) ) {
      ValueMetaInterface v = new ValueMetaBoolean( realresultfieldname );
      v.setOrigin( name );
      inputRowMeta.addValueMeta( v );
    }
    String realcardtype = space.environmentSubstitute( cardtype );
    if ( !Utils.isEmpty( realcardtype ) ) {
      ValueMetaInterface v = new ValueMetaString( realcardtype );
      v.setOrigin( name );
      inputRowMeta.addValueMeta( v );
    }
    String realnotvalidmsg = space.environmentSubstitute( notvalidmsg );
    if ( !Utils.isEmpty( notvalidmsg ) ) {
      ValueMetaInterface v = new ValueMetaString( realnotvalidmsg );
      v.setOrigin( name );
      inputRowMeta.addValueMeta( v );
    }
  }

  public String getXML() {
    StringBuilder retval = new StringBuilder();

    retval.append( "    " ).append( XMLHandler.addTagValue( "fieldname", fieldname ) );
    retval.append( "    " ).append( XMLHandler.addTagValue( "resultfieldname", resultfieldname ) );
    retval.append( "    " ).append( XMLHandler.addTagValue( "cardtype", cardtype ) );
    retval.append( "    " ).append( XMLHandler.addTagValue( "onlydigits", onlydigits ) );
    retval.append( "    " ).append( XMLHandler.addTagValue( "notvalidmsg", notvalidmsg ) );

    return retval.toString();
  }

  private void readData( Node stepnode ) throws HopXMLException {
    try {
      fieldname = XMLHandler.getTagValue( stepnode, "fieldname" );
      resultfieldname = XMLHandler.getTagValue( stepnode, "resultfieldname" );
      cardtype = XMLHandler.getTagValue( stepnode, "cardtype" );
      notvalidmsg = XMLHandler.getTagValue( stepnode, "notvalidmsg" );
      onlydigits = "Y".equalsIgnoreCase( XMLHandler.getTagValue( stepnode, "onlydigits" ) );

    } catch ( Exception e ) {
      throw new HopXMLException( BaseMessages.getString(
        PKG, "CreditCardValidatorMeta.Exception.UnableToReadStepInfo" ), e );
    }
  }

  public void readRep( Repository rep, IMetaStore metaStore, ObjectId id_step, List<DatabaseMeta> databases ) throws HopException {
    try {
      fieldname = rep.getStepAttributeString( id_step, "fieldname" );
      resultfieldname = rep.getStepAttributeString( id_step, "resultfieldname" );
      cardtype = rep.getStepAttributeString( id_step, "cardtype" );
      notvalidmsg = rep.getStepAttributeString( id_step, "notvalidmsg" );
      onlydigits = rep.getStepAttributeBoolean( id_step, "onlydigits" );

    } catch ( Exception e ) {
      throw new HopException( BaseMessages.getString(
        PKG, "CreditCardValidatorMeta.Exception.UnexpectedErrorReadingStepInfo" ), e );
    }
  }

  public void saveRep( Repository rep, IMetaStore metaStore, ObjectId id_transformation, ObjectId id_step ) throws HopException {
    try {
      rep.saveStepAttribute( id_transformation, id_step, "fieldname", fieldname );
      rep.saveStepAttribute( id_transformation, id_step, "resultfieldname", resultfieldname );
      rep.saveStepAttribute( id_transformation, id_step, "cardtype", cardtype );
      rep.saveStepAttribute( id_transformation, id_step, "notvalidmsg", notvalidmsg );
      rep.saveStepAttribute( id_transformation, id_step, "onlydigits", onlydigits );

    } catch ( Exception e ) {
      throw new HopException( BaseMessages.getString(
        PKG, "CreditCardValidatorMeta.Exception.UnableToSaveStepInfo" )
        + id_step, e );
    }
  }

  public void check( List<CheckResultInterface> remarks, TransMeta transMeta, StepMeta stepMeta,
    RowMetaInterface prev, String[] input, String[] output, RowMetaInterface info, VariableSpace space,
    Repository repository, IMetaStore metaStore ) {
    CheckResult cr;
    String error_message = "";

    String realresultfieldname = transMeta.environmentSubstitute( resultfieldname );
    if ( Utils.isEmpty( realresultfieldname ) ) {
      error_message = BaseMessages.getString( PKG, "CreditCardValidatorMeta.CheckResult.ResultFieldMissing" );
      cr = new CheckResult( CheckResult.TYPE_RESULT_ERROR, error_message, stepMeta );
      remarks.add( cr );
    } else {
      error_message = BaseMessages.getString( PKG, "CreditCardValidatorMeta.CheckResult.ResultFieldOK" );
      cr = new CheckResult( CheckResult.TYPE_RESULT_OK, error_message, stepMeta );
      remarks.add( cr );
    }
    if ( Utils.isEmpty( fieldname ) ) {
      error_message = BaseMessages.getString( PKG, "CreditCardValidatorMeta.CheckResult.CardFieldMissing" );
      cr = new CheckResult( CheckResult.TYPE_RESULT_ERROR, error_message, stepMeta );
      remarks.add( cr );
    } else {
      error_message = BaseMessages.getString( PKG, "CreditCardValidatorMeta.CheckResult.CardFieldOK" );
      cr = new CheckResult( CheckResult.TYPE_RESULT_OK, error_message, stepMeta );
      remarks.add( cr );
    }
    // See if we have input streams leading to this step!
    if ( input.length > 0 ) {
      cr =
        new CheckResult( CheckResult.TYPE_RESULT_OK, BaseMessages.getString(
          PKG, "CreditCardValidatorMeta.CheckResult.ReceivingInfoFromOtherSteps" ), stepMeta );
      remarks.add( cr );
    } else {
      cr =
        new CheckResult( CheckResult.TYPE_RESULT_ERROR, BaseMessages.getString(
          PKG, "CreditCardValidatorMeta.CheckResult.NoInpuReceived" ), stepMeta );
      remarks.add( cr );
    }

  }

  public StepInterface getStep( StepMeta stepMeta, StepDataInterface stepDataInterface, int cnr,
    TransMeta transMeta, Trans trans ) {
    return new CreditCardValidator( stepMeta, stepDataInterface, cnr, transMeta, trans );
  }

  public StepDataInterface getStepData() {
    return new CreditCardValidatorData();
  }

  public boolean supportsErrorHandling() {
    return true;
  }

}
