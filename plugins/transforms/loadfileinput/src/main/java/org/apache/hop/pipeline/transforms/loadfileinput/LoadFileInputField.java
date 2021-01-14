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

package org.apache.hop.pipeline.transforms.loadfileinput;

import org.apache.hop.core.Const;
import org.apache.hop.core.exception.HopValueException;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.core.row.value.ValueMetaFactory;
import org.apache.hop.core.xml.XmlHandler;
import org.apache.hop.i18n.BaseMessages;
import org.w3c.dom.Node;

/**
 * Describes a field
 *
 * @author Samatar
 * @since 20-06-2007
 */
public class LoadFileInputField implements Cloneable {
  private static final Class<?> PKG = LoadFileInputMeta.class; // For Translator

  public static final int TYPE_TRIM_NONE = 0;
  public static final int TYPE_TRIM_LEFT = 1;
  public static final int TYPE_TRIM_RIGHT = 2;
  public static final int TYPE_TRIM_BOTH = 3;

  public static final String[] trimTypeCode = { "none", "left", "right", "both" };

  public static final String[] trimTypeDesc = {
    BaseMessages.getString( PKG, "LoadFileInputField.TrimType.None" ),
    BaseMessages.getString( PKG, "LoadFileInputField.TrimType.Left" ),
    BaseMessages.getString( PKG, "LoadFileInputField.TrimType.Right" ),
    BaseMessages.getString( PKG, "LoadFileInputField.TrimType.Both" ) };

  public static final int ELEMENT_TYPE_FILECONTENT = 0;
  public static final int ELEMENT_TYPE_FILESIZE = 1;

  public static final String[] ElementTypeCode = { "content", "size" };

  public static final String[] ElementTypeDesc = {
    BaseMessages.getString( PKG, "LoadFileInputField.ElementType.FileContent" ),
    BaseMessages.getString( PKG, "LoadFileInputField.ElementType.FileSize" ), };

  private String name;
  private int type;
  private int length;
  private String format;
  private int trimtype;
  private int elementtype;
  private int precision;
  private String currencySymbol;
  private String decimalSymbol;
  private String groupSymbol;
  private boolean repeat;

  public LoadFileInputField( String fieldname ) {
    this.name = fieldname;
    this.elementtype = ELEMENT_TYPE_FILECONTENT;
    this.length = -1;
    this.type = IValueMeta.TYPE_STRING;
    this.format = "";
    this.trimtype = TYPE_TRIM_NONE;
    this.groupSymbol = "";
    this.decimalSymbol = "";
    this.currencySymbol = "";
    this.precision = -1;
    this.repeat = false;
  }

  public LoadFileInputField() {
    this( "" );
  }

  public String getXml() {
    String retval = "";

    retval += "      <field>" + Const.CR;
    retval += "        " + XmlHandler.addTagValue( "name", getName() );
    retval += "        " + XmlHandler.addTagValue( "element_type", getElementTypeCode() );
    retval += "        " + XmlHandler.addTagValue( "type", getTypeDesc() );
    retval += "        " + XmlHandler.addTagValue( "format", getFormat() );
    retval += "        " + XmlHandler.addTagValue( "currency", getCurrencySymbol() );
    retval += "        " + XmlHandler.addTagValue( "decimal", getDecimalSymbol() );
    retval += "        " + XmlHandler.addTagValue( "group", getGroupSymbol() );
    retval += "        " + XmlHandler.addTagValue( "length", getLength() );
    retval += "        " + XmlHandler.addTagValue( "precision", getPrecision() );
    retval += "        " + XmlHandler.addTagValue( "trim_type", getTrimTypeCode() );
    retval += "        " + XmlHandler.addTagValue( "repeat", isRepeated() );

    retval += "        </field>" + Const.CR;

    return retval;
  }

  public LoadFileInputField( Node fnode ) throws HopValueException {
    setName( XmlHandler.getTagValue( fnode, "name" ) );
    setElementType( getElementTypeByCode( XmlHandler.getTagValue( fnode, "element_type" ) ) );
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
      if ( trimTypeCode[ i ].equalsIgnoreCase( tt ) ) {
        return i;
      }
    }
    return 0;
  }

  public static final int getElementTypeByCode( String tt ) {
    if ( tt == null ) {
      return 0;
    }

    for ( int i = 0; i < ElementTypeCode.length; i++ ) {
      if ( ElementTypeCode[ i ].equalsIgnoreCase( tt ) ) {
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

  public static final int getElementTypeByDesc( String tt ) {
    if ( tt == null ) {
      return 0;
    }

    for ( int i = 0; i < ElementTypeDesc.length; i++ ) {
      if ( ElementTypeDesc[ i ].equalsIgnoreCase( tt ) ) {
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

  public static final String getElementTypeCode( int i ) {
    if ( i < 0 || i >= ElementTypeCode.length ) {
      return ElementTypeCode[ 0 ];
    }
    return ElementTypeCode[ i ];
  }

  public static final String getTrimTypeDesc( int i ) {
    if ( i < 0 || i >= trimTypeDesc.length ) {
      return trimTypeDesc[ 0 ];
    }
    return trimTypeDesc[ i ];
  }

  public static final String getElementTypeDesc( int i ) {
    if ( i < 0 || i >= ElementTypeDesc.length ) {
      return ElementTypeDesc[ 0 ];
    }
    return ElementTypeDesc[ i ];
  }

  public Object clone() {
    try {
      LoadFileInputField retval = (LoadFileInputField) super.clone();

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

  public String getFieldPositionsCode() {
    String enc = "";

    return enc;
  }

  public void guess() {
  }

}
