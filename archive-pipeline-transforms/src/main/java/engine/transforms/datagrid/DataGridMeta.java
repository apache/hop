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

package org.apache.hop.pipeline.transforms.datagrid;

import org.apache.hop.core.Const;
import org.apache.hop.core.exception.HopTransformException;
import org.apache.hop.core.exception.HopXMLException;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.core.row.value.ValueMetaFactory;
import org.apache.hop.core.util.Utils;
import org.apache.hop.core.variables.iVariables;
import org.apache.hop.core.xml.XMLHandler;
import org.apache.hop.metastore.api.IMetaStore;
import org.apache.hop.pipeline.Pipeline;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.BaseTransformMeta;
import org.apache.hop.pipeline.transform.ITransformData;
import org.apache.hop.pipeline.transform.ITransform;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.apache.hop.pipeline.transform.TransformMetaInjectionInterface;
import org.apache.hop.pipeline.transform.ITransform;
import org.w3c.dom.Node;

import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.List;

public class DataGridMeta extends BaseTransformMeta implements ITransform {
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
    super(); // allocate BaseTransformMeta
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
  public void loadXML( Node transformNode, IMetaStore metaStore ) throws HopXMLException {
    readData( transformNode );
  }

  public void allocate( int nrFields ) {
    fieldName = new String[ nrFields ];
    fieldType = new String[ nrFields ];
    fieldFormat = new String[ nrFields ];
    fieldLength = new int[ nrFields ];
    fieldPrecision = new int[ nrFields ];
    currency = new String[ nrFields ];
    decimal = new String[ nrFields ];
    group = new String[ nrFields ];
    setEmptyString = new boolean[ nrFields ];
  }

  @Override
  public Object clone() {
    DataGridMeta retval = (DataGridMeta) super.clone();

    int nrFields = fieldName.length;

    retval.allocate( nrFields );

    System.arraycopy( fieldName, 0, retval.fieldName, 0, nrFields );
    System.arraycopy( fieldType, 0, retval.fieldType, 0, nrFields );
    System.arraycopy( fieldFormat, 0, retval.fieldFormat, 0, nrFields );
    System.arraycopy( currency, 0, retval.currency, 0, nrFields );
    System.arraycopy( decimal, 0, retval.decimal, 0, nrFields );
    System.arraycopy( group, 0, retval.group, 0, nrFields );
    System.arraycopy( fieldLength, 0, retval.fieldLength, 0, nrFields );
    System.arraycopy( fieldPrecision, 0, retval.fieldPrecision, 0, nrFields );
    System.arraycopy( setEmptyString, 0, retval.setEmptyString, 0, nrFields );

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

  private void readData( Node transformNode ) throws HopXMLException {
    try {
      Node fields = XMLHandler.getSubNode( transformNode, "fields" );
      int nrFields = XMLHandler.countNodes( fields, "field" );

      allocate( nrFields );

      String slength, sprecision;

      for ( int i = 0; i < nrFields; i++ ) {
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

      Node datanode = XMLHandler.getSubNode( transformNode, "data" );
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
           * for (int f=0;f<nrFields;f++) { Node itemNode = XMLHandler.getSubNodeByNr(lineNode, "item", f); String item
           * = XMLHandler.getNodeValue(itemNode); line.add(item); }
           */
          dataLines.add( line );

        }

        lineNode = lineNode.getNextSibling();
      }
    } catch ( Exception e ) {
      throw new HopXMLException( "Unable to load transform info from XML", e );
    }
  }

  @Override
  public void setDefault() {
    int i, nrFields = 0;

    allocate( nrFields );

    DecimalFormat decimalFormat = new DecimalFormat();

    for ( i = 0; i < nrFields; i++ ) {
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
  public void getFields( IRowMeta rowMeta, String name, IRowMeta[] info, TransformMeta nextTransform,
                         iVariables variables, IMetaStore metaStore ) throws HopTransformException {
    for ( int i = 0; i < fieldName.length; i++ ) {
      try {
        if ( !Utils.isEmpty( fieldName[ i ] ) ) {
          int type = ValueMetaFactory.getIdForValueMeta( fieldType[ i ] );
          if ( type == IValueMeta.TYPE_NONE ) {
            type = IValueMeta.TYPE_STRING;
          }
          IValueMeta v = ValueMetaFactory.createValueMeta( fieldName[ i ], type );
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
        throw new HopTransformException( "Unable to create value of type " + fieldType[ i ], e );
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
  public ITransform getTransform( TransformMeta transformMeta, ITransformData data, int cnr,
                                PipelineMeta pipelineMeta, Pipeline pipeline ) {
    return new DataGrid( transformMeta, this, data, cnr, pipelineMeta, pipeline );
  }

  @Override
  public ITransformData getTransformData() {
    return new DataGridData();
  }

  @Override
  public TransformMetaInjectionInterface getTransformMetaInjectionInterface() {
    return new DataGridMetaInjection( this );
  }

}
