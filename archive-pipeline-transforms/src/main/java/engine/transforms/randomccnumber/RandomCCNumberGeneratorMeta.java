/*! ******************************************************************************
 *
 * Hop : The Hop Orchestration Platform
 *
 * http://www.project-hop.org
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

package org.apache.hop.pipeline.transforms.randomccnumber;

import org.apache.hop.core.CheckResult;
import org.apache.hop.core.CheckResultInterface;
import org.apache.hop.core.Const;
import org.apache.hop.core.exception.HopTransformException;
import org.apache.hop.core.exception.HopXmlException;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.core.row.value.ValueMetaInteger;
import org.apache.hop.core.row.value.ValueMetaString;
import org.apache.hop.core.util.Utils;
import org.apache.hop.core.variables.iVariables;
import org.apache.hop.core.xml.XmlHandler;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.metastore.api.IMetaStore;
import org.apache.hop.pipeline.Pipeline;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.BaseTransformMeta;
import org.apache.hop.pipeline.transform.ITransformData;
import org.apache.hop.pipeline.transform.TransformIOMeta;
import org.apache.hop.pipeline.transform.TransformIOMetaInterface;
import org.apache.hop.pipeline.transform.ITransform;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.apache.hop.pipeline.transform.ITransform;
import org.w3c.dom.Node;

import java.util.List;

/**
 * Generate random credit card number.
 *
 * @author Samatar
 * @since 01-4-2010
 */
public class RandomCCNumberGeneratorMeta extends BaseTransformMeta implements ITransform {
  private static Class<?> PKG = RandomCCNumberGeneratorMeta.class; // for i18n purposes, needed by Translator!!

  private String[] fieldCCType;
  private String[] fieldCCLength;
  private String[] fieldCCSize;

  private String cardNumberFieldName;
  private String cardLengthFieldName;
  private String cardTypeFieldName;

  public RandomCCNumberGeneratorMeta() {
    super(); // allocate BaseTransformMeta
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
   * @param cardNumberFieldName The cardNumberFieldName to set.
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
   * @param cardLengthFieldName The cardLengthFieldName to set.
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
   * @param cardTypeFieldName The cardTypeFieldName to set.
   */
  public void setCardTypeFieldName( String cardTypeFieldName ) {
    this.cardTypeFieldName = cardTypeFieldName;
  }

  /**
   * @param fieldName The fieldCCType to set.
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

  public void loadXml( Node transformNode, IMetaStore metaStore ) throws HopXmlException {
    readData( transformNode );
  }

  public void allocate( int count ) {
    fieldCCType = new String[ count ];
    fieldCCLength = new String[ count ];
    fieldCCSize = new String[ count ];
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

  private void readData( Node transformNode ) throws HopXmlException {
    try {
      Node fields = XmlHandler.getSubNode( transformNode, "fields" );
      int count = XmlHandler.countNodes( fields, "field" );

      allocate( count );

      for ( int i = 0; i < count; i++ ) {
        Node fnode = XmlHandler.getSubNodeByNr( fields, "field", i );

        fieldCCType[ i ] = XmlHandler.getTagValue( fnode, "cctype" );
        fieldCCLength[ i ] = XmlHandler.getTagValue( fnode, "cclen" );
        fieldCCSize[ i ] = XmlHandler.getTagValue( fnode, "ccsize" );
      }

      cardNumberFieldName = XmlHandler.getTagValue( transformNode, "cardNumberFieldName" );
      cardLengthFieldName = XmlHandler.getTagValue( transformNode, "cardLengthFieldName" );
      cardTypeFieldName = XmlHandler.getTagValue( transformNode, "cardTypeFieldName" );
    } catch ( Exception e ) {
      throw new HopXmlException( "Unable to read transform information from XML", e );
    }
  }

  public void setDefault() {
    int count = 0;

    allocate( count );

    for ( int i = 0; i < count; i++ ) {
      fieldCCType[ i ] = "field" + i;
      fieldCCLength[ i ] = "";
      fieldCCSize[ i ] = "";
    }
    cardNumberFieldName = BaseMessages.getString( PKG, "RandomCCNumberGeneratorMeta.CardNumberField" );
    cardLengthFieldName = BaseMessages.getString( PKG, "RandomCCNumberGeneratorMeta.CardLengthField" );
    cardTypeFieldName = BaseMessages.getString( PKG, "RandomCCNumberGeneratorMeta.CardTypeField" );
  }

  public void getFields( IRowMeta row, String name, IRowMeta[] info, TransformMeta nextTransform,
                         iVariables variables, IMetaStore metaStore ) throws HopTransformException {

    IValueMeta v = new ValueMetaString( cardNumberFieldName );
    v.setOrigin( name );
    row.addValueMeta( v );

    if ( !Utils.isEmpty( getCardTypeFieldName() ) ) {
      v = new ValueMetaString( cardTypeFieldName );
      v.setOrigin( name );
      row.addValueMeta( v );
    }

    if ( !Utils.isEmpty( getCardLengthFieldName() ) ) {
      v = new ValueMetaInteger( cardLengthFieldName );
      v.setLength( IValueMeta.DEFAULT_INTEGER_LENGTH, 0 );
      v.setOrigin( name );
      row.addValueMeta( v );
    }

  }

  public String getXml() {
    StringBuilder retval = new StringBuilder( 200 );

    retval.append( "    <fields>" ).append( Const.CR );

    for ( int i = 0; i < fieldCCType.length; i++ ) {
      retval.append( "      <field>" ).append( Const.CR );
      retval.append( "        " ).append( XmlHandler.addTagValue( "cctype", fieldCCType[ i ] ) );
      retval.append( "        " ).append( XmlHandler.addTagValue( "cclen", fieldCCLength[ i ] ) );
      retval.append( "        " ).append( XmlHandler.addTagValue( "ccsize", fieldCCSize[ i ] ) );
      retval.append( "      </field>" ).append( Const.CR );
    }
    retval.append( "    </fields>" + Const.CR );

    retval.append( "    " + XmlHandler.addTagValue( "cardNumberFieldName", cardNumberFieldName ) );
    retval.append( "    " + XmlHandler.addTagValue( "cardLengthFieldName", cardLengthFieldName ) );
    retval.append( "    " + XmlHandler.addTagValue( "cardTypeFieldName", cardTypeFieldName ) );
    return retval.toString();
  }

  public void check( List<CheckResultInterface> remarks, PipelineMeta pipelineMeta, TransformMeta transformMeta,
                     IRowMeta prev, String[] input, String[] output, IRowMeta info, iVariables variables,
                     IMetaStore metaStore ) {
    // See if we have input streams leading to this transform!
    int nrRemarks = remarks.size();
    for ( int i = 0; i < fieldCCType.length; i++ ) {
      int len = Const.toInt( pipelineMeta.environmentSubstitute( getFieldCCLength()[ i ] ), -1 );
      if ( len < 0 ) {
        CheckResult cr =
          new CheckResult( CheckResult.TYPE_RESULT_ERROR, BaseMessages.getString(
            PKG, "RandomCCNumberGeneratorMeta.CheckResult.WrongLen", String.valueOf( i ) ), transformMeta );
        remarks.add( cr );
      }
      int size = Const.toInt( pipelineMeta.environmentSubstitute( getFieldCCSize()[ i ] ), -1 );
      if ( size < 0 ) {
        CheckResult cr =
          new CheckResult( CheckResult.TYPE_RESULT_ERROR, BaseMessages.getString(
            PKG, "RandomCCNumberGeneratorMeta.CheckResult.WrongSize", String.valueOf( i ) ), transformMeta );
        remarks.add( cr );
      }
    }
    if ( remarks.size() == nrRemarks ) {
      CheckResult cr =
        new CheckResult( CheckResult.TYPE_RESULT_OK, BaseMessages.getString(
          PKG, "RandomCCNumberGeneratorMeta.CheckResult.AllTypesSpecified" ), transformMeta );
      remarks.add( cr );
    }

    if ( Utils.isEmpty( getCardNumberFieldName() ) ) {
      CheckResult cr =
        new CheckResult( CheckResult.TYPE_RESULT_ERROR, BaseMessages.getString(
          PKG, "RandomCCNumberGeneratorMeta.CheckResult.CardNumberFieldMissing" ), transformMeta );
      remarks.add( cr );
    }
  }

  public ITransform getTransform( TransformMeta transformMeta, ITransformData data, int cnr,
                                PipelineMeta pipelineMeta, Pipeline pipeline ) {
    return new RandomCCNumberGenerator( transformMeta, this, data, cnr, pipelineMeta, pipeline );
  }

  public ITransformData getTransformData() {
    return new RandomCCNumberGeneratorData();
  }

  /**
   * Returns the Input/Output metadata for this transform. The generator transform only produces output, does not accept input!
   */
  public TransformIOMetaInterface getTransformIOMeta() {
    return new TransformIOMeta( false, true, false, false, false, false );
  }
}
