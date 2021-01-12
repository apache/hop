/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hop.pipeline.transforms.xml.getxmldata;


import org.apache.hop.core.Const;
import org.apache.hop.core.exception.HopValueException;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.core.row.value.ValueMetaBase;
import org.apache.hop.core.xml.XmlHandler;
import org.apache.hop.i18n.BaseMessages;
import org.w3c.dom.Node;

/**
 * Describes an XML field and the position in an XML field.
 * 
 * @author Samatar, Brahim
 * @since 20-06-2007
 */
public class GetXmlDataField implements Cloneable {
  private static final Class<?> PKG = GetXmlDataMeta.class; // For Translator

  public static final int RESULT_TYPE_VALUE_OF = 0;
  public static final int RESULT_TYPE_TYPE_SINGLE_NODE = 1;

  public static final String[] ResultTypeCode = { "valueof", "singlenode" };

  public static final String[] ResultTypeDesc = { BaseMessages.getString( PKG, "GetXMLDataField.ResultType.ValueOf" ),
    BaseMessages.getString( PKG, "GetXMLDataField.ResultType.SingleNode" ) };

  public static final int TYPE_TRIM_NONE = 0;
  public static final int TYPE_TRIM_LEFT = 1;
  public static final int TYPE_TRIM_RIGHT = 2;
  public static final int TYPE_TRIM_BOTH = 3;

  public static final int ELEMENT_TYPE_NODE = 0;
  public static final int ELEMENT_TYPE_ATTRIBUT = 1;

  public static final String[] trimTypeCode = { "none", "left", "right", "both" };

  public static final String[] trimTypeDesc = { BaseMessages.getString( PKG, "GetXMLDataField.TrimType.None" ),
    BaseMessages.getString( PKG, "GetXMLDataField.TrimType.Left" ),
    BaseMessages.getString( PKG, "GetXMLDataField.TrimType.Right" ),
    BaseMessages.getString( PKG, "GetXMLDataField.TrimType.Both" ) };

  // //////////////////////////////////////////////////////////////
  //
  // Conversion to be done to go from "attribute" to "attribute"
  // - The output is written as "attribut" but both "attribut" and
  // "attribute" are accepted as input.
  // - When v3.1 is being deprecated all supported versions will
  // support "attribut" and "attribute". Then output "attribute"
  // as all version support it.
  // - In a distant future remove "attribut" all together in v5 or so.
  //
  // TODO Sven Boden
  //
  // //////////////////////////////////////////////////////////////
  public static final String[] ElementTypeCode = { "node", "attribute" };

  public static final String[] ElementOldTypeCode = { "node", "attribut" };

  public static final String[] ElementTypeDesc = { BaseMessages.getString( PKG, "GetXMLDataField.ElementType.Node" ),
    BaseMessages.getString( PKG, "GetXMLDataField.ElementType.Attribute" ) };

  private String name;
  private String xpath;
  private String resolvedXpath;

  private int type;
  private int length;
  private String format;
  private int trimtype;
  private int elementtype;
  private int resulttype;
  private int precision;
  private String currencySymbol;
  private String decimalSymbol;
  private String groupSymbol;
  private boolean repeat;

  public GetXmlDataField(String fieldname ) {
    this.name = fieldname;
    this.xpath = "";
    this.length = -1;
    this.type = IValueMeta.TYPE_STRING;
    this.format = "";
    this.trimtype = TYPE_TRIM_NONE;
    this.elementtype = ELEMENT_TYPE_NODE;
    this.resulttype = RESULT_TYPE_VALUE_OF;
    this.groupSymbol = "";
    this.decimalSymbol = "";
    this.currencySymbol = "";
    this.precision = -1;
    this.repeat = false;
  }

  public GetXmlDataField() {
    this( "" );
  }

  public String getXml() {
    StringBuffer xml = new StringBuffer( 400 );

    xml.append( "      <field>" ).append( Const.CR );
    xml.append( "        " ).append( XmlHandler.addTagValue( "name", getName() ) );
    xml.append( "        " ).append( XmlHandler.addTagValue( "xpath", getXPath() ) );
    xml.append( "        " ).append( XmlHandler.addTagValue( "element_type", getElementTypeCode() ) );
    xml.append( "        " ).append( XmlHandler.addTagValue( "result_type", getResultTypeCode() ) );
    xml.append( "        " ).append( XmlHandler.addTagValue( "type", getTypeDesc() ) );
    xml.append( "        " ).append( XmlHandler.addTagValue( "format", getFormat() ) );
    xml.append( "        " ).append( XmlHandler.addTagValue( "currency", getCurrencySymbol() ) );
    xml.append( "        " ).append( XmlHandler.addTagValue( "decimal", getDecimalSymbol() ) );
    xml.append( "        " ).append( XmlHandler.addTagValue( "group", getGroupSymbol() ) );
    xml.append( "        " ).append( XmlHandler.addTagValue( "length", getLength() ) );
    xml.append( "        " ).append( XmlHandler.addTagValue( "precision", getPrecision() ) );
    xml.append( "        " ).append( XmlHandler.addTagValue( "trim_type", getTrimTypeCode() ) );
    xml.append( "        " ).append( XmlHandler.addTagValue( "repeat", isRepeated() ) );

    xml.append( "      </field>" ).append( Const.CR );

    return xml.toString();
  }

  public GetXmlDataField(Node fnode ) throws HopValueException {
    setName( XmlHandler.getTagValue( fnode, "name" ) );
    setXPath( XmlHandler.getTagValue( fnode, "xpath" ) );
    setElementType( getElementTypeByCode( XmlHandler.getTagValue( fnode, "element_type" ) ) );
    setResultType( getResultTypeByCode( XmlHandler.getTagValue( fnode, "result_type" ) ) );
    setType( ValueMetaBase.getType( XmlHandler.getTagValue( fnode, "type" ) ) );
    setFormat( XmlHandler.getTagValue( fnode, "format" ) );
    setCurrencySymbol( XmlHandler.getTagValue( fnode, "currency" ) );
    setDecimalSymbol( XmlHandler.getTagValue( fnode, "decimal" ) );
    setGroupSymbol( XmlHandler.getTagValue( fnode, "group" ) );
    setLength( Const.toInt( XmlHandler.getTagValue( fnode, "length" ), -1 ) );
    setPrecision( Const.toInt( XmlHandler.getTagValue( fnode, "precision" ), -1 ) );
    setTrimType( getTrimTypeByCode( XmlHandler.getTagValue( fnode, "trim_type" ) ) );
    setRepeated( !"N".equalsIgnoreCase( XmlHandler.getTagValue( fnode, "repeat" ) ) );
  }

  public static final int getTrimTypeByCode( String tt ) {
    if ( tt == null ) {
      return 0;
    }

    for ( int i = 0; i < trimTypeCode.length; i++ ) {
      if ( trimTypeCode[i].equalsIgnoreCase( tt ) ) {
        return i;
      }
    }
    return 0;
  }

  public static final int getElementTypeByCode( String tt ) {
    if ( tt == null ) {
      return 0;
    }

    // / Code to be removed later on as explained in the top of
    // this file.
    // //////////////////////////////////////////////////////////////
    for ( int i = 0; i < ElementOldTypeCode.length; i++ ) {
      if ( ElementOldTypeCode[i].equalsIgnoreCase( tt ) ) {
        return i;
      }
    }
    // //////////////////////////////////////////////////////////////

    for ( int i = 0; i < ElementTypeCode.length; i++ ) {
      if ( ElementTypeCode[i].equalsIgnoreCase( tt ) ) {
        return i;
      }
    }

    return 0;
  }

  public static final int getTrimTypeByDesc( String tt ) {
    if ( tt == null ) {
      return 0;
    }

    for ( int i = 0; i < trimTypeDesc.length; i++ ) {
      if ( trimTypeDesc[i].equalsIgnoreCase( tt ) ) {
        return i;
      }
    }
    return 0;
  }

  public static final int getElementTypeByDesc( String tt ) {
    if ( tt == null ) {
      return 0;
    }

    for ( int i = 0; i < ElementTypeDesc.length; i++ ) {
      if ( ElementTypeDesc[i].equalsIgnoreCase( tt ) ) {
        return i;
      }
    }
    return 0;
  }

  public static final String getTrimTypeCode( int i ) {
    if ( i < 0 || i >= trimTypeCode.length ) {
      return trimTypeCode[0];
    }
    return trimTypeCode[i];
  }

  public static final String getElementTypeCode( int i ) {
    // To be changed to the new code once all are converted
    if ( i < 0 || i >= ElementOldTypeCode.length ) {
      return ElementOldTypeCode[0];
    }
    return ElementOldTypeCode[i];
  }

  public static final String getTrimTypeDesc( int i ) {
    if ( i < 0 || i >= trimTypeDesc.length ) {
      return trimTypeDesc[0];
    }
    return trimTypeDesc[i];
  }

  public static final String getElementTypeDesc( int i ) {
    if ( i < 0 || i >= ElementTypeDesc.length ) {
      return ElementTypeDesc[0];
    }
    return ElementTypeDesc[i];
  }

  public Object clone() {
    try {
      GetXmlDataField retval = (GetXmlDataField) super.clone();

      return retval;
    } catch ( CloneNotSupportedException e ) {
      return null;
    }
  }

  public int getLength() {
    return length;
  }

  public void setLength( int length ) {
    this.length = length;
  }

  public String getName() {
    return name;
  }

  public String getXPath() {
    return xpath;
  }

  protected String getResolvedXPath() {
    return resolvedXpath;
  }

  public void setXPath( String fieldxpath ) {
    this.xpath = fieldxpath;
  }

  protected void setResolvedXPath( String resolvedXpath ) {
    this.resolvedXpath = resolvedXpath;
  }

  public void setName( String fieldname ) {
    this.name = fieldname;
  }

  public int getType() {
    return type;
  }

  public String getTypeDesc() {
    return ValueMetaBase.getTypeDesc( type );
  }

  public void setType( int type ) {
    this.type = type;
  }

  public String getFormat() {
    return format;
  }

  public void setFormat( String format ) {
    this.format = format;
  }

  public int getTrimType() {
    return trimtype;
  }

  public int getElementType() {
    return elementtype;
  }

  public String getTrimTypeCode() {
    return getTrimTypeCode( trimtype );
  }

  public String getElementTypeCode() {
    return getElementTypeCode( elementtype );
  }

  public String getTrimTypeDesc() {
    return getTrimTypeDesc( trimtype );
  }

  public String getElementTypeDesc() {
    return getElementTypeDesc( elementtype );
  }

  public void setTrimType( int trimtype ) {
    this.trimtype = trimtype;
  }

  public void setElementType( int elementType ) {
    this.elementtype = elementType;
  }

  public String getGroupSymbol() {
    return groupSymbol;
  }

  public void setGroupSymbol( String group_symbol ) {
    this.groupSymbol = group_symbol;
  }

  public String getDecimalSymbol() {
    return decimalSymbol;
  }

  public void setDecimalSymbol( String decimal_symbol ) {
    this.decimalSymbol = decimal_symbol;
  }

  public String getCurrencySymbol() {
    return currencySymbol;
  }

  public void setCurrencySymbol( String currency_symbol ) {
    this.currencySymbol = currency_symbol;
  }

  public int getPrecision() {
    return precision;
  }

  public void setPrecision( int precision ) {
    this.precision = precision;
  }

  public boolean isRepeated() {
    return repeat;
  }

  public void setRepeated( boolean repeat ) {
    this.repeat = repeat;
  }

  public void flipRepeated() {
    repeat = !repeat;
  }

  public static final int getResultTypeByDesc( String tt ) {
    if ( tt == null ) {
      return 0;
    }

    for ( int i = 0; i < ResultTypeDesc.length; i++ ) {
      if ( ResultTypeDesc[i].equalsIgnoreCase( tt ) ) {
        return i;
      }
    }
    return 0;
  }

  public String getResultTypeDesc() {
    return getResultTypeDesc( resulttype );
  }

  public static final String getResultTypeDesc( int i ) {
    if ( i < 0 || i >= ResultTypeDesc.length ) {
      return ResultTypeDesc[0];
    }
    return ResultTypeDesc[i];
  }

  public int getResultType() {
    return resulttype;
  }

  public void setResultType( int resulttype ) {
    this.resulttype = resulttype;
  }

  public static final int getResultTypeByCode( String tt ) {
    if ( tt == null ) {
      return 0;
    }

    for ( int i = 0; i < ResultTypeCode.length; i++ ) {
      if ( ResultTypeCode[i].equalsIgnoreCase( tt ) ) {
        return i;
      }
    }

    return 0;
  }

  public static final String getResultTypeCode( int i ) {
    if ( i < 0 || i >= ResultTypeCode.length ) {
      return ResultTypeCode[0];
    }
    return ResultTypeCode[i];
  }

  public String getResultTypeCode() {
    return getResultTypeCode( resulttype );
  }
}
