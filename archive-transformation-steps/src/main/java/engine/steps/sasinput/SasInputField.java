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

package org.apache.hop.trans.steps.sasinput;

import org.apache.hop.core.Const;
import org.apache.hop.core.exception.HopXMLException;
import org.apache.hop.core.row.value.ValueMetaFactory;
import org.apache.hop.core.row.value.ValueMetaString;
import org.apache.hop.core.xml.XMLHandler;
import org.apache.hop.core.xml.XMLInterface;
import org.w3c.dom.Node;

/**
 * This defines a selected list of fields from the input files including
 *
 * @author matt
 * @since 10-OCT-2011
 */
public class SasInputField implements XMLInterface, Cloneable {
  private String name;
  private String rename;
  private int type;
  private int length;
  private int precision;
  private String conversionMask;
  private String decimalSymbol;
  private String groupingSymbol;
  private int trimType;

  /**
   * @param name
   * @param rename
   * @param type
   * @param conversionMask
   * @param decimalSymbol
   * @param groupingSymbol
   * @param trimType
   */
  public SasInputField( String name, String rename, int type, String conversionMask, String decimalSymbol,
                        String groupingSymbol, int trimType ) {
    this.name = name;
    this.rename = rename;
    this.type = type;
    this.conversionMask = conversionMask;
    this.decimalSymbol = decimalSymbol;
    this.groupingSymbol = groupingSymbol;
    this.trimType = trimType;
  }

  public SasInputField() {
  }

  @Override
  protected SasInputField clone() {
    try {
      return (SasInputField) super.clone();
    } catch ( CloneNotSupportedException e ) {
      throw new RuntimeException( e );
    }
  }

  @Override
  public String getXML() {
    StringBuilder retval = new StringBuilder();

    retval.append( "    " + XMLHandler.addTagValue( "name", name ) );
    retval.append( "    " + XMLHandler.addTagValue( "rename", rename ) );
    retval.append( "    " + XMLHandler.addTagValue( "type", ValueMetaFactory.getValueMetaName( type ) ) );
    retval.append( "    " + XMLHandler.addTagValue( "length", length ) );
    retval.append( "    " + XMLHandler.addTagValue( "precision", precision ) );
    retval.append( "    " + XMLHandler.addTagValue( "conversion_mask", conversionMask ) );
    retval.append( "    " + XMLHandler.addTagValue( "decimal", decimalSymbol ) );
    retval.append( "    " + XMLHandler.addTagValue( "grouping", groupingSymbol ) );
    retval.append( "    " + XMLHandler.addTagValue( "trim_type", ValueMetaString.getTrimTypeCode( trimType ) ) );

    return retval.toString();
  }

  public SasInputField( Node node ) throws HopXMLException {
    name = XMLHandler.getTagValue( node, "name" );
    rename = XMLHandler.getTagValue( node, "rename" );
    type = ValueMetaFactory.getIdForValueMeta( XMLHandler.getTagValue( node, "type" ) );
    length = Const.toInt( XMLHandler.getTagValue( node, "length" ), -1 );
    precision = Const.toInt( XMLHandler.getTagValue( node, "precision" ), -1 );
    conversionMask = XMLHandler.getTagValue( node, "conversion_mask" );
    decimalSymbol = XMLHandler.getTagValue( node, "decimal" );
    groupingSymbol = XMLHandler.getTagValue( node, "grouping" );
    trimType = ValueMetaString.getTrimTypeByCode( XMLHandler.getTagValue( node, "trim_type" ) );
  }

  /**
   * @return the name
   */
  public String getName() {
    return name;
  }

  /**
   * @param name the name to set
   */
  public void setName( String name ) {
    this.name = name;
  }

  /**
   * @return the rename
   */
  public String getRename() {
    return rename;
  }

  /**
   * @param rename the rename to set
   */
  public void setRename( String rename ) {
    this.rename = rename;
  }

  /**
   * @return the type
   */
  public int getType() {
    return type;
  }

  /**
   * @param type the type to set
   */
  public void setType( int type ) {
    this.type = type;
  }

  /**
   * @return the conversionMask
   */
  public String getConversionMask() {
    return conversionMask;
  }

  /**
   * @param conversionMask the conversionMask to set
   */
  public void setConversionMask( String conversionMask ) {
    this.conversionMask = conversionMask;
  }

  /**
   * @return the decimalSymbol
   */
  public String getDecimalSymbol() {
    return decimalSymbol;
  }

  /**
   * @param decimalSymbol the decimalSymbol to set
   */
  public void setDecimalSymbol( String decimalSymbol ) {
    this.decimalSymbol = decimalSymbol;
  }

  /**
   * @return the groupingSymbol
   */
  public String getGroupingSymbol() {
    return groupingSymbol;
  }

  /**
   * @param groupingSymbol the groupingSymbol to set
   */
  public void setGroupingSymbol( String groupingSymbol ) {
    this.groupingSymbol = groupingSymbol;
  }

  /**
   * @return the trimType
   */
  public int getTrimType() {
    return trimType;
  }

  /**
   * @param trimType the trimType to set
   */
  public void setTrimType( int trimType ) {
    this.trimType = trimType;
  }

  /**
   * @return the precision
   */
  public int getPrecision() {
    return precision;
  }

  /**
   * @param precision the precision to set
   */
  public void setPrecision( int precision ) {
    this.precision = precision;
  }

  /**
   * @return the length
   */
  public int getLength() {
    return length;
  }

  /**
   * @param length the length to set
   */
  public void setLength( int length ) {
    this.length = length;
  }

  public String getTrimTypeDesc() {
    return ValueMetaString.getTrimTypeDesc( trimType );
  }
}
