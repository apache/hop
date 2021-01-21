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

package org.apache.hop.pipeline.transforms.salesforceinput;

import org.apache.hop.core.Const;
import org.apache.hop.core.exception.HopTransformException;
import org.apache.hop.core.injection.Injection;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.core.row.value.ValueMetaFactory;
import org.apache.hop.core.xml.XmlHandler;
import org.apache.hop.i18n.BaseMessages;
import org.w3c.dom.Node;

/**
 * Describes an SalesforceInput field
 *
 * @author Samatar Hassan
 * @since 10-06-2007
 */
public class SalesforceInputField implements Cloneable {
  private static Class<?> PKG = SalesforceInputMeta.class; // For Translator

  public static final int TYPE_TRIM_NONE = 0;
  public static final int TYPE_TRIM_LEFT = 1;
  public static final int TYPE_TRIM_RIGHT = 2;
  public static final int TYPE_TRIM_BOTH = 3;

  public static final String[] trimTypeCode = { "none", "left", "right", "both" };

  public static final String[] trimTypeDesc = {
    BaseMessages.getString( PKG, "SalesforceInputField.TrimType.None" ),
    BaseMessages.getString( PKG, "SalesforceInputField.TrimType.Left" ),
    BaseMessages.getString( PKG, "SalesforceInputField.TrimType.Right" ),
    BaseMessages.getString( PKG, "SalesforceInputField.TrimType.Both" ) };

  @Injection( name = "NAME", group = "FIELDS" )
  private String name;

  @Injection( name = "FIELD", group = "FIELDS" )
  private String field;

  private int type;

  @Injection( name = "LENGTH", group = "FIELDS" )
  private int length;

  @Injection( name = "FORMAT", group = "FIELDS" )
  private String format;

  private int trimtype;

  @Injection( name = "PRECISION", group = "FIELDS" )
  private int precision;

  @Injection( name = "CURRENCY", group = "FIELDS" )
  private String currencySymbol;

  @Injection( name = "DECIMAL", group = "FIELDS" )
  private String decimalSymbol;

  @Injection( name = "GROUP", group = "FIELDS" )
  private String groupSymbol;

  @Injection( name = "REPEAT", group = "FIELDS" )
  private boolean repeat;

  @Injection( name = "ISIDLOOKUP", group = "FIELDS" )
  private boolean idlookup;

  private String[] samples;

  public SalesforceInputField( String fieldname ) {
    this.name = fieldname;
    this.field = "";
    this.length = -1;
    this.type = IValueMeta.TYPE_STRING;
    this.format = "";
    this.trimtype = TYPE_TRIM_NONE;
    this.groupSymbol = "";
    this.decimalSymbol = "";
    this.currencySymbol = "";
    this.precision = -1;
    this.repeat = false;
    this.idlookup = false;
  }

  public SalesforceInputField() {
    this( "" );
  }

  public SalesforceInputField( Node fnode ) throws HopTransformException {
    readData( fnode );
  }

  public String getXml() {
    StringBuilder retval = new StringBuilder();

    retval.append( "      " ).append( XmlHandler.openTag( "field" ) ).append( Const.CR );
    retval.append( "        " ).append( XmlHandler.addTagValue( "name", getName() ) );
    retval.append( "        " ).append( XmlHandler.addTagValue( "field", getField() ) );
    retval.append( "        " ).append( XmlHandler.addTagValue( "idlookup", isIdLookup() ) );
    retval.append( "        " ).append( XmlHandler.addTagValue( "type", getTypeDesc() ) );
    retval.append( "        " ).append( XmlHandler.addTagValue( "format", getFormat() ) );
    retval.append( "        " ).append( XmlHandler.addTagValue( "currency", getCurrencySymbol() ) );
    retval.append( "        " ).append( XmlHandler.addTagValue( "decimal", getDecimalSymbol() ) );
    retval.append( "        " ).append( XmlHandler.addTagValue( "group", getGroupSymbol() ) );
    retval.append( "        " ).append( XmlHandler.addTagValue( "length", getLength() ) );
    retval.append( "        " ).append( XmlHandler.addTagValue( "precision", getPrecision() ) );
    retval.append( "        " ).append( XmlHandler.addTagValue( "trim_type", getTrimTypeCode() ) );
    retval.append( "        " ).append( XmlHandler.addTagValue( "repeat", isRepeated() ) );
    retval.append( "      " ).append( XmlHandler.closeTag( "field" ) ).append( Const.CR );
    return retval.toString();
  }

  public void readData( Node fnode ) {
    setName( XmlHandler.getTagValue( fnode, "name" ) );
    setField( XmlHandler.getTagValue( fnode, "field" ) );
    setIdLookup( "Y".equalsIgnoreCase( XmlHandler.getTagValue( fnode, "idlookup" ) ) );
    setType( ValueMetaFactory.getIdForValueMeta( XmlHandler.getTagValue( fnode, "type" ) ) );
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

  public static final int getTrimTypeByDesc( String tt ) {
    if ( tt == null ) {
      return 0;
    }

    for ( int i = 0; i < trimTypeDesc.length; i++ ) {
      if ( trimTypeDesc[i].equalsIgnoreCase( tt ) ) {
        return i;
      }
    }
    return SalesforceInputField.getTrimTypeByCode( tt );
  }

  public static final String getTrimTypeCode( int i ) {
    if ( i < 0 || i >= trimTypeCode.length ) {
      return trimTypeCode[0];
    }
    return trimTypeCode[i];
  }

  public static final String getTrimTypeDesc( int i ) {
    if ( i < 0 || i >= trimTypeDesc.length ) {
      return trimTypeDesc[0];
    }
    return trimTypeDesc[i];
  }

  public Object clone() {
    try {
      return (SalesforceInputField) super.clone();
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

  public String getField() {
    return field;
  }

  public void setField( String fieldvalue ) {
    this.field = fieldvalue;
  }

  public void setName( String fieldname ) {
    this.name = fieldname;
  }

  public int getType() {
    return type;
  }

  public String getTypeDesc() {
    return ValueMetaFactory.getValueMetaName( type );
  }

  public void setType( int type ) {
    this.type = type;
  }

  @Injection( name = "TYPE", group = "FIELDS" )
  public void setType( String typeDesc ) {
    this.type = ValueMetaFactory.getIdForValueMeta( typeDesc );
  }

  public String getFormat() {
    return format;
  }

  public void setFormat( String format ) {
    this.format = format;
  }

  public void setSamples( String[] samples ) {
    this.samples = samples;
  }

  public String[] getSamples() {
    return samples;
  }

  public int getTrimType() {
    return trimtype;
  }

  public String getTrimTypeCode() {
    return getTrimTypeCode( trimtype );
  }

  public String getTrimTypeDesc() {
    return getTrimTypeDesc( trimtype );
  }

  public void setTrimType( int trimtype ) {
    this.trimtype = trimtype;
  }

  @Injection( name = "TRIM_TYPE", group = "FIELDS" )
  public void setTrimTypeDesc( String trimTypeDesc ) {
    this.trimtype = SalesforceInputField.getTrimTypeByDesc( trimTypeDesc );
  }

  public String getGroupSymbol() {
    return groupSymbol;
  }

  public void setGroupSymbol( String groupSymbol ) {
    this.groupSymbol = groupSymbol;
  }

  public String getDecimalSymbol() {
    return decimalSymbol;
  }

  public void setDecimalSymbol( String decimalSymbol ) {
    this.decimalSymbol = decimalSymbol;
  }

  public String getCurrencySymbol() {
    return currencySymbol;
  }

  public void setCurrencySymbol( String currencySymbol ) {
    this.currencySymbol = currencySymbol;
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

  public boolean isIdLookup() {
    return idlookup;
  }

  public void setIdLookup( boolean idlookup ) {
    this.idlookup = idlookup;
  }

  public void flipRepeated() {
    repeat = !repeat;
  }

}
