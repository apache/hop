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

package org.apache.hop.pipeline.transforms.yamlinput;

import org.apache.hop.core.Const;
import org.apache.hop.core.exception.HopValueException;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.core.row.value.ValueMetaFactory;
import org.apache.hop.core.xml.XmlHandler;
import org.apache.hop.i18n.BaseMessages;
import org.w3c.dom.Node;

/**
 * Read YAML files, parse them and convert them to rows and writes these to one or more output streams.
 *
 * @author Samatar
 * @since 20-06-2007
 */
public class YamlInputField implements Cloneable {
  private static final Class<?> PKG = YamlInputMeta.class; // For Translator

  public static final int TYPE_TRIM_NONE = 0;
  public static final int TYPE_TRIM_LEFT = 1;
  public static final int TYPE_TRIM_RIGHT = 2;
  public static final int TYPE_TRIM_BOTH = 3;

  public static final String[] trimTypeCode = { "none", "left", "right", "both" };

  public static final String[] trimTypeDesc = {
    BaseMessages.getString( PKG, "YamlInputField.TrimType.None" ),
    BaseMessages.getString( PKG, "YamlInputField.TrimType.Left" ),
    BaseMessages.getString( PKG, "YamlInputField.TrimType.Right" ),
    BaseMessages.getString( PKG, "YamlInputField.TrimType.Both" ) };

  private String name;
  private String path;
  private int type;
  private int length;
  private String format;
  private int trimtype;
  private int precision;
  private String currencySymbol;
  private String decimalSymbol;
  private String groupSymbol;

  public YamlInputField( String fieldname ) {
    this.name = fieldname;
    this.path = "";
    this.length = -1;
    this.type = IValueMeta.TYPE_STRING;
    this.format = "";
    this.trimtype = TYPE_TRIM_NONE;
    this.groupSymbol = "";
    this.decimalSymbol = "";
    this.currencySymbol = "";
    this.precision = -1;
  }

  public YamlInputField() {
    this( "" );
  }

  public String getXml() {
    StringBuilder retval = new StringBuilder( 400 );

    retval.append( "      <field>" ).append( Const.CR );
    retval.append( "        " ).append( XmlHandler.addTagValue( "name", getName() ) );
    retval.append( "        " ).append( XmlHandler.addTagValue( "path", getPath() ) );
    retval.append( "        " ).append( XmlHandler.addTagValue( "type", getTypeDesc() ) );
    retval.append( "        " ).append( XmlHandler.addTagValue( "format", getFormat() ) );
    retval.append( "        " ).append( XmlHandler.addTagValue( "currency", getCurrencySymbol() ) );
    retval.append( "        " ).append( XmlHandler.addTagValue( "decimal", getDecimalSymbol() ) );
    retval.append( "        " ).append( XmlHandler.addTagValue( "group", getGroupSymbol() ) );
    retval.append( "        " ).append( XmlHandler.addTagValue( "length", getLength() ) );
    retval.append( "        " ).append( XmlHandler.addTagValue( "precision", getPrecision() ) );
    retval.append( "        " ).append( XmlHandler.addTagValue( "trim_type", getTrimTypeCode() ) );

    retval.append( "      </field>" ).append( Const.CR );

    return retval.toString();
  }

  public YamlInputField( Node fnode ) throws HopValueException {
    setName( XmlHandler.getTagValue( fnode, "name" ) );
    setPath( XmlHandler.getTagValue( fnode, "path" ) );
    setType( ValueMetaFactory.getIdForValueMeta( XmlHandler.getTagValue( fnode, "type" ) ) );
    setFormat( XmlHandler.getTagValue( fnode, "format" ) );
    setCurrencySymbol( XmlHandler.getTagValue( fnode, "currency" ) );
    setDecimalSymbol( XmlHandler.getTagValue( fnode, "decimal" ) );
    setGroupSymbol( XmlHandler.getTagValue( fnode, "group" ) );
    setLength( Const.toInt( XmlHandler.getTagValue( fnode, "length" ), -1 ) );
    setPrecision( Const.toInt( XmlHandler.getTagValue( fnode, "precision" ), -1 ) );
    setTrimType( getTrimTypeByCode( XmlHandler.getTagValue( fnode, "trim_type" ) ) );
  }

  public static final int getTrimTypeByCode( String tt ) {
    if ( tt == null ) {
      return 0;
    }

    for ( int i = 0; i < trimTypeCode.length; i++ ) {
      if ( trimTypeCode[ i ].equalsIgnoreCase( tt ) ) {
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
      if ( trimTypeDesc[ i ].equalsIgnoreCase( tt ) ) {
        return i;
      }
    }
    return 0;
  }

  public static final String getTrimTypeCode( int i ) {
    if ( i < 0 || i >= trimTypeCode.length ) {
      return trimTypeCode[ 0 ];
    }
    return trimTypeCode[ i ];
  }

  public static final String getTrimTypeDesc( int i ) {
    if ( i < 0 || i >= trimTypeDesc.length ) {
      return trimTypeDesc[ 0 ];
    }
    return trimTypeDesc[ i ];
  }

  public Object clone() {
    try {
      YamlInputField retval = (YamlInputField) super.clone();

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

  public String getPath() {
    return path;
  }

  public void setPath( String fieldpath ) {
    this.path = fieldpath;
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

  public String getFormat() {
    return format;
  }

  public void setFormat( String format ) {
    this.format = format;
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

}
