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

package org.apache.hop.trans.steps.denormaliser;

import org.apache.hop.core.CheckResult;
import org.apache.hop.core.CheckResultInterface;
import org.apache.hop.core.Const;
import org.apache.hop.core.annotations.Step;
import org.apache.hop.core.exception.HopStepException;
import org.apache.hop.core.exception.HopXMLException;
import org.apache.hop.core.row.RowMetaInterface;
import org.apache.hop.core.row.ValueMetaInterface;
import org.apache.hop.core.row.value.ValueMetaFactory;
import org.apache.hop.core.variables.VariableSpace;
import org.apache.hop.core.xml.XMLHandler;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.metastore.api.IMetaStore;
import org.apache.hop.trans.Trans;
import org.apache.hop.trans.TransMeta;
import org.apache.hop.trans.step.*;
import org.w3c.dom.Node;

import java.util.List;

/**
 * The Denormaliser transformation step meta-data
 *
 * @author Matt
 * @since 17-jan-2006
 */

@Step(
        id = "Denormaliser",
        image = "ui/images/UNP.svg",
        i18nPackageName = "org.apache.hop.trans.steps.denormaliser",
        name = "BaseStep.TypeLongDesc.RowDenormaliser",
        description = "i18n:org.apache.hop.trans.step:BaseStep.TypeLongDesc.RowDenormaliser",
        categoryDescription = "i18n:org.apache.hop.trans.step:BaseStep.Category.Transform"
)
public class DenormaliserMeta extends BaseStepMeta implements StepMetaInterface {
  private static Class<?> PKG = DenormaliserMeta.class; // for i18n purposes, needed by Translator2!!

  /**
   * Fields to group over
   */
  private String[] groupField;

  /**
   * The key field
   */
  private String keyField;

  /**
   * The fields to unpivot
   */
  private DenormaliserTargetField[] denormaliserTargetField;

  public DenormaliserMeta() {
    super(); // allocate BaseStepMeta
  }

  /**
   * @return Returns the keyField.
   */
  public String getKeyField() {
    return keyField;
  }

  /**
   * @param keyField The keyField to set.
   */
  public void setKeyField( String keyField ) {
    this.keyField = keyField;
  }

  /**
   * @return Returns the groupField.
   */
  public String[] getGroupField() {
    return groupField;
  }

  /**
   * @param groupField The groupField to set.
   */
  public void setGroupField( String[] groupField ) {
    this.groupField = groupField;
  }

  public String[] getDenormaliserTargetFields() {
    String[] fields = new String[ denormaliserTargetField.length ];
    for ( int i = 0; i < fields.length; i++ ) {
      fields[ i ] = denormaliserTargetField[ i ].getTargetName();
    }

    return fields;
  }

  public DenormaliserTargetField searchTargetField( String targetName ) {
    for ( int i = 0; i < denormaliserTargetField.length; i++ ) {
      DenormaliserTargetField field = denormaliserTargetField[ i ];
      if ( field.getTargetName().equalsIgnoreCase( targetName ) ) {
        return field;
      }
    }
    return null;
  }

  /**
   * @return Returns the pivotField.
   */
  public DenormaliserTargetField[] getDenormaliserTargetField() {
    return denormaliserTargetField;
  }

  /**
   * @param pivotField The pivotField to set.
   */
  public void setDenormaliserTargetField( DenormaliserTargetField[] pivotField ) {
    this.denormaliserTargetField = pivotField;
  }

  public void loadXML( Node stepnode, IMetaStore metaStore ) throws HopXMLException {
    readData( stepnode );
  }

  public void allocate( int sizegroup, int nrfields ) {
    groupField = new String[ sizegroup ];
    denormaliserTargetField = new DenormaliserTargetField[ nrfields ];
  }

  public Object clone() {
    Object retval = super.clone();
    return retval;
  }

  public void setDefault() {
    int sizegroup = 0;
    int nrfields = 0;

    allocate( sizegroup, nrfields );
  }

  @Override
  public void getFields( RowMetaInterface row, String name, RowMetaInterface[] info, StepMeta nextStep,
                         VariableSpace space, IMetaStore metaStore ) throws HopStepException {

    // Remove the key value (there will be different entries for each output row)
    //
    if ( keyField != null && keyField.length() > 0 ) {
      int idx = row.indexOfValue( keyField );
      if ( idx < 0 ) {
        throw new HopStepException( BaseMessages.getString(
          PKG, "DenormaliserMeta.Exception.UnableToLocateKeyField", keyField ) );
      }
      row.removeValueMeta( idx );
    } else {
      throw new HopStepException( BaseMessages.getString( PKG, "DenormaliserMeta.Exception.RequiredKeyField" ) );
    }

    // Remove all field value(s) (there will be different entries for each output row)
    //
    for ( int i = 0; i < denormaliserTargetField.length; i++ ) {
      String fieldname = denormaliserTargetField[ i ].getFieldName();
      if ( fieldname != null && fieldname.length() > 0 ) {
        int idx = row.indexOfValue( fieldname );
        if ( idx >= 0 ) {
          row.removeValueMeta( idx );
        }
      } else {
        throw new HopStepException( BaseMessages.getString(
          PKG, "DenormaliserMeta.Exception.RequiredTargetFieldName", ( i + 1 ) + "" ) );
      }
    }

    // Re-add the target fields
    for ( int i = 0; i < denormaliserTargetField.length; i++ ) {
      DenormaliserTargetField field = denormaliserTargetField[ i ];
      try {
        ValueMetaInterface target =
          ValueMetaFactory.createValueMeta( field.getTargetName(), field.getTargetType() );
        target.setLength( field.getTargetLength(), field.getTargetPrecision() );
        target.setOrigin( name );
        row.addValueMeta( target );
      } catch ( Exception e ) {
        throw new HopStepException( e );
      }
    }
  }

  private void readData( Node stepnode ) throws HopXMLException {
    try {
      keyField = XMLHandler.getTagValue( stepnode, "key_field" );

      Node groupn = XMLHandler.getSubNode( stepnode, "group" );
      Node fields = XMLHandler.getSubNode( stepnode, "fields" );

      int sizegroup = XMLHandler.countNodes( groupn, "field" );
      int nrfields = XMLHandler.countNodes( fields, "field" );

      allocate( sizegroup, nrfields );

      for ( int i = 0; i < sizegroup; i++ ) {
        Node fnode = XMLHandler.getSubNodeByNr( groupn, "field", i );
        groupField[ i ] = XMLHandler.getTagValue( fnode, "name" );
      }

      for ( int i = 0; i < nrfields; i++ ) {
        Node fnode = XMLHandler.getSubNodeByNr( fields, "field", i );
        denormaliserTargetField[ i ] = new DenormaliserTargetField();
        denormaliserTargetField[ i ].setFieldName( XMLHandler.getTagValue( fnode, "field_name" ) );
        denormaliserTargetField[ i ].setKeyValue( XMLHandler.getTagValue( fnode, "key_value" ) );
        denormaliserTargetField[ i ].setTargetName( XMLHandler.getTagValue( fnode, "target_name" ) );
        denormaliserTargetField[ i ].setTargetType( XMLHandler.getTagValue( fnode, "target_type" ) );
        denormaliserTargetField[ i ].setTargetFormat( XMLHandler.getTagValue( fnode, "target_format" ) );
        denormaliserTargetField[ i ].setTargetLength( Const.toInt(
          XMLHandler.getTagValue( fnode, "target_length" ), -1 ) );
        denormaliserTargetField[ i ].setTargetPrecision( Const.toInt( XMLHandler.getTagValue(
          fnode, "target_precision" ), -1 ) );
        denormaliserTargetField[ i ]
          .setTargetDecimalSymbol( XMLHandler.getTagValue( fnode, "target_decimal_symbol" ) );
        denormaliserTargetField[ i ].setTargetGroupingSymbol( XMLHandler.getTagValue(
          fnode, "target_grouping_symbol" ) );
        denormaliserTargetField[ i ].setTargetCurrencySymbol( XMLHandler.getTagValue(
          fnode, "target_currency_symbol" ) );
        denormaliserTargetField[ i ].setTargetNullString( XMLHandler.getTagValue( fnode, "target_null_string" ) );
        denormaliserTargetField[ i ].setTargetAggregationType( XMLHandler.getTagValue(
          fnode, "target_aggregation_type" ) );
      }
    } catch ( Exception e ) {
      throw new HopXMLException( BaseMessages.getString(
        PKG, "DenormaliserMeta.Exception.UnableToLoadStepInfoFromXML" ), e );
    }
  }

  public String getXML() {
    StringBuilder retval = new StringBuilder();

    retval.append( "      " + XMLHandler.addTagValue( "key_field", keyField ) );

    retval.append( "      <group>" + Const.CR );
    for ( int i = 0; i < groupField.length; i++ ) {
      retval.append( "        <field>" + Const.CR );
      retval.append( "          " + XMLHandler.addTagValue( "name", groupField[ i ] ) );
      retval.append( "          </field>" + Const.CR );
    }
    retval.append( "        </group>" + Const.CR );

    retval.append( "      <fields>" + Const.CR );
    for ( int i = 0; i < denormaliserTargetField.length; i++ ) {
      DenormaliserTargetField field = denormaliserTargetField[ i ];

      retval.append( "        <field>" + Const.CR );
      retval.append( "          " + XMLHandler.addTagValue( "field_name", field.getFieldName() ) );
      retval.append( "          " + XMLHandler.addTagValue( "key_value", field.getKeyValue() ) );
      retval.append( "          " + XMLHandler.addTagValue( "target_name", field.getTargetName() ) );
      retval.append( "          " + XMLHandler.addTagValue( "target_type", field.getTargetTypeDesc() ) );
      retval.append( "          " + XMLHandler.addTagValue( "target_format", field.getTargetFormat() ) );
      retval.append( "          " + XMLHandler.addTagValue( "target_length", field.getTargetLength() ) );
      retval.append( "          " + XMLHandler.addTagValue( "target_precision", field.getTargetPrecision() ) );
      retval.append( "          "
        + XMLHandler.addTagValue( "target_decimal_symbol", field.getTargetDecimalSymbol() ) );
      retval.append( "          "
        + XMLHandler.addTagValue( "target_grouping_symbol", field.getTargetGroupingSymbol() ) );
      retval.append( "          "
        + XMLHandler.addTagValue( "target_currency_symbol", field.getTargetCurrencySymbol() ) );
      retval.append( "          " + XMLHandler.addTagValue( "target_null_string", field.getTargetNullString() ) );
      retval.append( "          "
        + XMLHandler.addTagValue( "target_aggregation_type", field.getTargetAggregationTypeDesc() ) );
      retval.append( "          </field>" + Const.CR );
    }
    retval.append( "        </fields>" + Const.CR );

    return retval.toString();
  }

  public void check( List<CheckResultInterface> remarks, TransMeta transMeta, StepMeta stepMeta,
                     RowMetaInterface prev, String[] input, String[] output, RowMetaInterface info, VariableSpace space,
                     IMetaStore metaStore ) {
    CheckResult cr;

    if ( input.length > 0 ) {
      cr =
        new CheckResult( CheckResult.TYPE_RESULT_OK, BaseMessages.getString(
          PKG, "DenormaliserMeta.CheckResult.ReceivingInfoFromOtherSteps" ), stepMeta );
      remarks.add( cr );
    } else {
      cr =
        new CheckResult( CheckResult.TYPE_RESULT_ERROR, BaseMessages.getString(
          PKG, "DenormaliserMeta.CheckResult.NoInputReceived" ), stepMeta );
      remarks.add( cr );
    }
  }

  public StepInterface getStep( StepMeta stepMeta, StepDataInterface stepDataInterface, int cnr,
                                TransMeta transMeta, Trans trans ) {
    return new Denormaliser( stepMeta, stepDataInterface, cnr, transMeta, trans );
  }

  public StepDataInterface getStepData() {
    return new DenormaliserData();
  }

  @Override
  public StepMetaInjectionInterface getStepMetaInjectionInterface() {
    return new DenormaliserMetaInjection( this );
  }
}
