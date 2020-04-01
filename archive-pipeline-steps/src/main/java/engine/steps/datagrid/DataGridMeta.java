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

package org.apache.hop.pipeline.steps.datagrid;

import org.apache.hop.core.Const;
import org.apache.hop.core.exception.HopStepException;
import org.apache.hop.core.exception.HopXMLException;
import org.apache.hop.core.row.RowMetaInterface;
import org.apache.hop.core.row.ValueMetaInterface;
import org.apache.hop.core.row.value.ValueMetaFactory;
import org.apache.hop.core.util.Utils;
import org.apache.hop.core.variables.VariableSpace;
import org.apache.hop.core.xml.XMLHandler;
import org.apache.hop.metastore.api.IMetaStore;
import org.apache.hop.pipeline.Pipeline;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.step.BaseStepMeta;
import org.apache.hop.pipeline.step.StepDataInterface;
import org.apache.hop.pipeline.step.StepInterface;
import org.apache.hop.pipeline.step.StepMeta;
import org.apache.hop.pipeline.step.StepMetaInjectionInterface;
import org.apache.hop.pipeline.step.StepMetaInterface;
import org.w3c.dom.Node;

import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.List;

public class DataGridMeta extends BaseStepMeta implements StepMetaInterface {
  private String[] currency;
  private String[] decimal;
  private String[] group;

  private String[] fieldName;
  private String[] fieldType;
  private String[] fieldFormat;

  private int[] fieldLength;
  private int[] fieldPrecision;
  /**
   * Flag : set empty string
   **/
  private boolean[] setEmptyString;

  private List<List<String>> dataLines;

  public DataGridMeta() {
    super(); // allocate BaseStepMeta
  }

  /**
   * @return the setEmptyString
   */
  public boolean[] isSetEmptyString() {
    return setEmptyString;
  }

  /**
   * @param setEmptyString the setEmptyString to set
   */
  public void setEmptyString( boolean[] setEmptyString ) {
    this.setEmptyString = setEmptyString;
  }

  /**
   * @return Returns the currency.
   */
  public String[] getCurrency() {
    return currency;
  }

  /**
   * @param currency The currency to set.
   */
  public void setCurrency( String[] currency ) {
    this.currency = currency;
  }

  /**
   * @return Returns the decimal.
   */
  public String[] getDecimal() {
    return decimal;
  }

  /**
   * @param decimal The decimal to set.
   */
  public void setDecimal( String[] decimal ) {
    this.decimal = decimal;
  }

  /**
   * @return Returns the fieldFormat.
   */
  public String[] getFieldFormat() {
    return fieldFormat;
  }

  /**
   * @param fieldFormat The fieldFormat to set.
   */
  public void setFieldFormat( String[] fieldFormat ) {
    this.fieldFormat = fieldFormat;
  }

  /**
   * @return Returns the fieldLength.
   */
  public int[] getFieldLength() {
    return fieldLength;
  }

  /**
   * @param fieldLength The fieldLength to set.
   */
  public void setFieldLength( int[] fieldLength ) {
    this.fieldLength = fieldLength;
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

  /**
   * @return Returns the fieldPrecision.
   */
  public int[] getFieldPrecision() {
    return fieldPrecision;
  }

  /**
   * @param fieldPrecision The fieldPrecision to set.
   */
  public void setFieldPrecision( int[] fieldPrecision ) {
    this.fieldPrecision = fieldPrecision;
  }

  /**
   * @return Returns the fieldType.
   */
  public String[] getFieldType() {
    return fieldType;
  }

  /**
   * @param fieldType The fieldType to set.
   */
  public void setFieldType( String[] fieldType ) {
    this.fieldType = fieldType;
  }

  /**
   * @return Returns the group.
   */
  public String[] getGroup() {
    return group;
  }

  /**
   * @param group The group to set.
   */
  public void setGroup( String[] group ) {
    this.group = group;
  }

  public List<List<String>> getDataLines() {
    return dataLines;
  }

  public void setDataLines( List<List<String>> dataLines ) {
    this.dataLines = dataLines;
  }

  @Override
  public void loadXML( Node stepnode, IMetaStore metaStore ) throws HopXMLException {
    readData( stepnode );
  }

  public void allocate( int nrfields ) {
    fieldName = new String[ nrfields ];
    fieldType = new String[ nrfields ];
    fieldFormat = new String[ nrfields ];
    fieldLength = new int[ nrfields ];
    fieldPrecision = new int[ nrfields ];
    currency = new String[ nrfields ];
    decimal = new String[ nrfields ];
    group = new String[ nrfields ];
    setEmptyString = new boolean[ nrfields ];
  }

  @Override
  public Object clone() {
    DataGridMeta retval = (DataGridMeta) super.clone();

    int nrfields = fieldName.length;

    retval.allocate( nrfields );

    System.arraycopy( fieldName, 0, retval.fieldName, 0, nrfields );
    System.arraycopy( fieldType, 0, retval.fieldType, 0, nrfields );
    System.arraycopy( fieldFormat, 0, retval.fieldFormat, 0, nrfields );
    System.arraycopy( currency, 0, retval.currency, 0, nrfields );
    System.arraycopy( decimal, 0, retval.decimal, 0, nrfields );
    System.arraycopy( group, 0, retval.group, 0, nrfields );
    System.arraycopy( fieldLength, 0, retval.fieldLength, 0, nrfields );
    System.arraycopy( fieldPrecision, 0, retval.fieldPrecision, 0, nrfields );
    System.arraycopy( setEmptyString, 0, retval.setEmptyString, 0, nrfields );

    if ( dataLines != null ) {
      retval.setDataLines( new ArrayList<List<String>>() );
      for ( List<String> line : dataLines ) {
        List<String> newLine = new ArrayList<>();
        newLine.addAll( line );
        retval.getDataLines().add( newLine );
      }
    }
    return retval;
  }

  private void readData( Node stepnode ) throws HopXMLException {
    try {
      Node fields = XMLHandler.getSubNode( stepnode, "fields" );
      int nrfields = XMLHandler.countNodes( fields, "field" );

      allocate( nrfields );

      String slength, sprecision;

      for ( int i = 0; i < nrfields; i++ ) {
        Node fnode = XMLHandler.getSubNodeByNr( fields, "field", i );

        fieldName[ i ] = XMLHandler.getTagValue( fnode, "name" );
        fieldType[ i ] = XMLHandler.getTagValue( fnode, "type" );
        fieldFormat[ i ] = XMLHandler.getTagValue( fnode, "format" );
        currency[ i ] = XMLHandler.getTagValue( fnode, "currency" );
        decimal[ i ] = XMLHandler.getTagValue( fnode, "decimal" );
        group[ i ] = XMLHandler.getTagValue( fnode, "group" );
        slength = XMLHandler.getTagValue( fnode, "length" );
        sprecision = XMLHandler.getTagValue( fnode, "precision" );

        fieldLength[ i ] = Const.toInt( slength, -1 );
        fieldPrecision[ i ] = Const.toInt( sprecision, -1 );
        String emptyString = XMLHandler.getTagValue( fnode, "set_empty_string" );
        setEmptyString[ i ] = !Utils.isEmpty( emptyString ) && "Y".equalsIgnoreCase( emptyString );
      }

      Node datanode = XMLHandler.getSubNode( stepnode, "data" );
      // NodeList childNodes = datanode.getChildNodes();
      dataLines = new ArrayList<List<String>>();

      Node lineNode = datanode.getFirstChild();
      while ( lineNode != null ) {
        if ( "line".equals( lineNode.getNodeName() ) ) {
          List<String> line = new ArrayList<>();
          Node itemNode = lineNode.getFirstChild();
          while ( itemNode != null ) {
            if ( "item".equals( itemNode.getNodeName() ) ) {
              String itemNodeValue = XMLHandler.getNodeValue( itemNode );
              line.add( itemNodeValue );
            }
            itemNode = itemNode.getNextSibling();
          }
          /*
           * for (int f=0;f<nrfields;f++) { Node itemNode = XMLHandler.getSubNodeByNr(lineNode, "item", f); String item
           * = XMLHandler.getNodeValue(itemNode); line.add(item); }
           */
          dataLines.add( line );

        }

        lineNode = lineNode.getNextSibling();
      }
    } catch ( Exception e ) {
      throw new HopXMLException( "Unable to load step info from XML", e );
    }
  }

  @Override
  public void setDefault() {
    int i, nrfields = 0;

    allocate( nrfields );

    DecimalFormat decimalFormat = new DecimalFormat();

    for ( i = 0; i < nrfields; i++ ) {
      fieldName[ i ] = "field" + i;
      fieldType[ i ] = "Number";
      fieldFormat[ i ] = "\u00A40,000,000.00;\u00A4-0,000,000.00";
      fieldLength[ i ] = 9;
      fieldPrecision[ i ] = 2;
      currency[ i ] = decimalFormat.getDecimalFormatSymbols().getCurrencySymbol();
      decimal[ i ] = new String( new char[] { decimalFormat.getDecimalFormatSymbols().getDecimalSeparator() } );
      group[ i ] = new String( new char[] { decimalFormat.getDecimalFormatSymbols().getGroupingSeparator() } );
      setEmptyString[ i ] = false;
    }

    dataLines = new ArrayList<List<String>>();
  }

  @Override
  public void getFields( RowMetaInterface rowMeta, String name, RowMetaInterface[] info, StepMeta nextStep,
                         VariableSpace space, IMetaStore metaStore ) throws HopStepException {
    for ( int i = 0; i < fieldName.length; i++ ) {
      try {
        if ( !Utils.isEmpty( fieldName[ i ] ) ) {
          int type = ValueMetaFactory.getIdForValueMeta( fieldType[ i ] );
          if ( type == ValueMetaInterface.TYPE_NONE ) {
            type = ValueMetaInterface.TYPE_STRING;
          }
          ValueMetaInterface v = ValueMetaFactory.createValueMeta( fieldName[ i ], type );
          v.setLength( fieldLength[ i ] );
          v.setPrecision( fieldPrecision[ i ] );
          v.setOrigin( name );
          v.setConversionMask( fieldFormat[ i ] );
          v.setCurrencySymbol( currency[ i ] );
          v.setGroupingSymbol( group[ i ] );
          v.setDecimalSymbol( decimal[ i ] );

          rowMeta.addValueMeta( v );
        }
      } catch ( Exception e ) {
        throw new HopStepException( "Unable to create value of type " + fieldType[ i ], e );
      }
    }
  }

  @Override
  public String getXML() {
    StringBuilder retval = new StringBuilder( 300 );

    retval.append( "    <fields>" ).append( Const.CR );
    for ( int i = 0; i < fieldName.length; i++ ) {
      if ( fieldName[ i ] != null && fieldName[ i ].length() != 0 ) {
        retval.append( "      <field>" ).append( Const.CR );
        retval.append( "        " ).append( XMLHandler.addTagValue( "name", fieldName[ i ] ) );
        retval.append( "        " ).append( XMLHandler.addTagValue( "type", fieldType[ i ] ) );
        retval.append( "        " ).append( XMLHandler.addTagValue( "format", fieldFormat[ i ] ) );
        retval.append( "        " ).append( XMLHandler.addTagValue( "currency", currency[ i ] ) );
        retval.append( "        " ).append( XMLHandler.addTagValue( "decimal", decimal[ i ] ) );
        retval.append( "        " ).append( XMLHandler.addTagValue( "group", group[ i ] ) );
        retval.append( "        " ).append( XMLHandler.addTagValue( "length", fieldLength[ i ] ) );
        retval.append( "        " ).append( XMLHandler.addTagValue( "precision", fieldPrecision[ i ] ) );
        retval.append( "        " ).append( XMLHandler.addTagValue( "set_empty_string", setEmptyString[ i ] ) );
        retval.append( "      </field>" ).append( Const.CR );
      }
    }
    retval.append( "    </fields>" ).append( Const.CR );

    retval.append( "    <data>" ).append( Const.CR );
    for ( List<String> line : dataLines ) {
      retval.append( "      <line> " );
      for ( String item : line ) {
        retval.append( XMLHandler.addTagValue( "item", item, false ) );
      }
      retval.append( " </line>" ).append( Const.CR );
    }
    retval.append( "    </data>" ).append( Const.CR );

    return retval.toString();
  }

  @Override
  public StepInterface getStep( StepMeta stepMeta, StepDataInterface stepDataInterface, int cnr,
                                PipelineMeta pipelineMeta, Pipeline pipeline ) {
    return new DataGrid( stepMeta, stepDataInterface, cnr, pipelineMeta, pipeline );
  }

  @Override
  public StepDataInterface getStepData() {
    return new DataGridData();
  }

  @Override
  public StepMetaInjectionInterface getStepMetaInjectionInterface() {
    return new DataGridMetaInjection( this );
  }

}
