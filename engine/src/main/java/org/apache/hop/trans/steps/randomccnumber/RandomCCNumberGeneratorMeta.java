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

package org.apache.hop.trans.steps.randomccnumber;

import java.util.List;

import org.apache.hop.core.CheckResult;
import org.apache.hop.core.CheckResultInterface;
import org.apache.hop.core.Const;
import org.apache.hop.core.util.Utils;
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
import org.apache.hop.trans.step.StepIOMeta;
import org.apache.hop.trans.step.StepIOMetaInterface;
import org.apache.hop.trans.step.StepInterface;
import org.apache.hop.trans.step.StepMeta;
import org.apache.hop.trans.step.StepMetaInterface;
import org.apache.hop.metastore.api.IMetaStore;
import org.w3c.dom.Node;

/**
 * Generate random credit card number.
 *
 * @author Samatar
 * @since 01-4-2010
 */
public class RandomCCNumberGeneratorMeta extends BaseStepMeta implements StepMetaInterface {
  private static Class<?> PKG = RandomCCNumberGeneratorMeta.class; // for i18n purposes, needed by Translator2!!

  private String[] fieldCCType;
  private String[] fieldCCLength;
  private String[] fieldCCSize;

  private String cardNumberFieldName;
  private String cardLengthFieldName;
  private String cardTypeFieldName;

  public RandomCCNumberGeneratorMeta() {
    super(); // allocate BaseStepMeta
  }

  /**
   * @return Returns the fieldCCType.
   */
  public String[] getFieldCCType() {
    return fieldCCType;
  }

  /**
   * @return Returns the cardNumberFieldName.
   */
  public String getCardNumberFieldName() {
    return cardNumberFieldName;
  }

  /**
   * @param cardNumberFieldName
   *          The cardNumberFieldName to set.
   */
  public void setCardNumberFieldName( String cardNumberFieldName ) {
    this.cardNumberFieldName = cardNumberFieldName;
  }

  /**
   * @return Returns the cardLengthFieldName.
   */
  public String getCardLengthFieldName() {
    return cardLengthFieldName;
  }

  /**
   * @param cardLengthFieldName
   *          The cardLengthFieldName to set.
   */
  public void setCardLengthFieldName( String cardLengthFieldName ) {
    this.cardLengthFieldName = cardLengthFieldName;
  }

  /**
   * @return Returns the cardTypeFieldName.
   */
  public String getCardTypeFieldName() {
    return cardTypeFieldName;
  }

  /**
   * @param cardTypeFieldName
   *          The cardTypeFieldName to set.
   */
  public void setCardTypeFieldName( String cardTypeFieldName ) {
    this.cardTypeFieldName = cardTypeFieldName;
  }

  /**
   * @param fieldName
   *          The fieldCCType to set.
   */
  public void setFieldCCType( String[] fieldName ) {
    this.fieldCCType = fieldName;
  }

  /**
   * @return Returns the fieldType.
   */
  public String[] getFieldCCLength() {
    return fieldCCLength;
  }

  /**
   * @return Returns the fieldCCSize.
   */
  public String[] getFieldCCSize() {
    return fieldCCSize;
  }

  public void setFieldCCSize( String[] ccSize ) {
    this.fieldCCSize = ccSize;
  }

  /**
   * @deprecated Use setFieldCCLength instead
   */
  @Deprecated
  public void setFieldType( String[] fieldType ) {
    this.fieldCCLength = fieldType;
  }

  public void setFieldCCLength( String[] ccLength ) {
    this.fieldCCLength = ccLength;
  }

  public void loadXML( Node stepnode, IMetaStore metaStore ) throws HopXMLException {
    readData( stepnode );
  }

  public void allocate( int count ) {
    fieldCCType = new String[count];
    fieldCCLength = new String[count];
    fieldCCSize = new String[count];
  }

  public Object clone() {
    RandomCCNumberGeneratorMeta retval = (RandomCCNumberGeneratorMeta) super.clone();

    int count = fieldCCType.length;

    retval.allocate( count );
    System.arraycopy( fieldCCType, 0, retval.fieldCCType, 0, count );
    System.arraycopy( fieldCCLength, 0, retval.fieldCCLength, 0, count );
    System.arraycopy( fieldCCSize, 0, retval.fieldCCSize, 0, count );

    return retval;
  }

  private void readData( Node stepnode ) throws HopXMLException {
    try {
      Node fields = XMLHandler.getSubNode( stepnode, "fields" );
      int count = XMLHandler.countNodes( fields, "field" );

      allocate( count );

      for ( int i = 0; i < count; i++ ) {
        Node fnode = XMLHandler.getSubNodeByNr( fields, "field", i );

        fieldCCType[i] = XMLHandler.getTagValue( fnode, "cctype" );
        fieldCCLength[i] = XMLHandler.getTagValue( fnode, "cclen" );
        fieldCCSize[i] = XMLHandler.getTagValue( fnode, "ccsize" );
      }

      cardNumberFieldName = XMLHandler.getTagValue( stepnode, "cardNumberFieldName" );
      cardLengthFieldName = XMLHandler.getTagValue( stepnode, "cardLengthFieldName" );
      cardTypeFieldName = XMLHandler.getTagValue( stepnode, "cardTypeFieldName" );
    } catch ( Exception e ) {
      throw new HopXMLException( "Unable to read step information from XML", e );
    }
  }

  public void setDefault() {
    int count = 0;

    allocate( count );

    for ( int i = 0; i < count; i++ ) {
      fieldCCType[i] = "field" + i;
      fieldCCLength[i] = "";
      fieldCCSize[i] = "";
    }
    cardNumberFieldName = BaseMessages.getString( PKG, "RandomCCNumberGeneratorMeta.CardNumberField" );
    cardLengthFieldName = BaseMessages.getString( PKG, "RandomCCNumberGeneratorMeta.CardLengthField" );
    cardTypeFieldName = BaseMessages.getString( PKG, "RandomCCNumberGeneratorMeta.CardTypeField" );
  }

  public void getFields( RowMetaInterface row, String name, RowMetaInterface[] info, StepMeta nextStep,
    VariableSpace space, IMetaStore metaStore ) throws HopStepException {

    ValueMetaInterface v = new ValueMetaString( cardNumberFieldName );
    v.setOrigin( name );
    row.addValueMeta( v );

    if ( !Utils.isEmpty( getCardTypeFieldName() ) ) {
      v = new ValueMetaString( cardTypeFieldName );
      v.setOrigin( name );
      row.addValueMeta( v );
    }

    if ( !Utils.isEmpty( getCardLengthFieldName() ) ) {
      v = new ValueMetaInteger( cardLengthFieldName );
      v.setLength( ValueMetaInterface.DEFAULT_INTEGER_LENGTH, 0 );
      v.setOrigin( name );
      row.addValueMeta( v );
    }

  }

  public String getXML() {
    StringBuilder retval = new StringBuilder( 200 );

    retval.append( "    <fields>" ).append( Const.CR );

    for ( int i = 0; i < fieldCCType.length; i++ ) {
      retval.append( "      <field>" ).append( Const.CR );
      retval.append( "        " ).append( XMLHandler.addTagValue( "cctype", fieldCCType[i] ) );
      retval.append( "        " ).append( XMLHandler.addTagValue( "cclen", fieldCCLength[i] ) );
      retval.append( "        " ).append( XMLHandler.addTagValue( "ccsize", fieldCCSize[i] ) );
      retval.append( "      </field>" ).append( Const.CR );
    }
    retval.append( "    </fields>" + Const.CR );

    retval.append( "    " + XMLHandler.addTagValue( "cardNumberFieldName", cardNumberFieldName ) );
    retval.append( "    " + XMLHandler.addTagValue( "cardLengthFieldName", cardLengthFieldName ) );
    retval.append( "    " + XMLHandler.addTagValue( "cardTypeFieldName", cardTypeFieldName ) );
    return retval.toString();
  }

  public void check( List<CheckResultInterface> remarks, TransMeta transMeta, StepMeta stepMeta,
    RowMetaInterface prev, String[] input, String[] output, RowMetaInterface info, VariableSpace space,
    IMetaStore metaStore ) {
    // See if we have input streams leading to this step!
    int nrRemarks = remarks.size();
    for ( int i = 0; i < fieldCCType.length; i++ ) {
      int len = Const.toInt( transMeta.environmentSubstitute( getFieldCCLength()[i] ), -1 );
      if ( len < 0 ) {
        CheckResult cr =
          new CheckResult( CheckResult.TYPE_RESULT_ERROR, BaseMessages.getString(
            PKG, "RandomCCNumberGeneratorMeta.CheckResult.WrongLen", String.valueOf( i ) ), stepMeta );
        remarks.add( cr );
      }
      int size = Const.toInt( transMeta.environmentSubstitute( getFieldCCSize()[i] ), -1 );
      if ( size < 0 ) {
        CheckResult cr =
          new CheckResult( CheckResult.TYPE_RESULT_ERROR, BaseMessages.getString(
            PKG, "RandomCCNumberGeneratorMeta.CheckResult.WrongSize", String.valueOf( i ) ), stepMeta );
        remarks.add( cr );
      }
    }
    if ( remarks.size() == nrRemarks ) {
      CheckResult cr =
        new CheckResult( CheckResult.TYPE_RESULT_OK, BaseMessages.getString(
          PKG, "RandomCCNumberGeneratorMeta.CheckResult.AllTypesSpecified" ), stepMeta );
      remarks.add( cr );
    }

    if ( Utils.isEmpty( getCardNumberFieldName() ) ) {
      CheckResult cr =
        new CheckResult( CheckResult.TYPE_RESULT_ERROR, BaseMessages.getString(
          PKG, "RandomCCNumberGeneratorMeta.CheckResult.CardNumberFieldMissing" ), stepMeta );
      remarks.add( cr );
    }
  }

  public StepInterface getStep( StepMeta stepMeta, StepDataInterface stepDataInterface, int cnr,
    TransMeta transMeta, Trans trans ) {
    return new RandomCCNumberGenerator( stepMeta, stepDataInterface, cnr, transMeta, trans );
  }

  public StepDataInterface getStepData() {
    return new RandomCCNumberGeneratorData();
  }

  /**
   * Returns the Input/Output metadata for this step. The generator step only produces output, does not accept input!
   */
  public StepIOMetaInterface getStepIOMeta() {
    return new StepIOMeta( false, true, false, false, false, false );
  }
}
