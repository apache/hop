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

package org.apache.hop.core.row;

import org.apache.hop.core.Const;
import org.apache.hop.core.exception.HopValueException;
import org.apache.hop.core.row.value.ValueMetaBase;
import org.apache.hop.core.row.value.ValueMetaBigNumber;
import org.apache.hop.core.row.value.ValueMetaBinary;
import org.apache.hop.core.row.value.ValueMetaBoolean;
import org.apache.hop.core.row.value.ValueMetaDate;
import org.apache.hop.core.row.value.ValueMetaFactory;
import org.apache.hop.core.row.value.ValueMetaInteger;
import org.apache.hop.core.row.value.ValueMetaNumber;
import org.apache.hop.core.row.value.ValueMetaSerializable;
import org.apache.hop.core.row.value.ValueMetaString;
import org.apache.hop.core.xml.XmlHandler;
import org.w3c.dom.Node;

import java.math.BigDecimal;
import java.util.Date;

public class ValueMetaAndData {
  public static final String XML_TAG = "value";

  private IValueMeta valueMeta;
  private Object valueData;

  public ValueMetaAndData() {
  }

  /**
   * @param valueMeta
   * @param valueData
   */
  public ValueMetaAndData( IValueMeta valueMeta, Object valueData ) {
    this.valueMeta = valueMeta;
    this.valueData = valueData;
  }

  public ValueMetaAndData( String valueName, Object valueData ) throws HopValueException {
    this.valueData = valueData;
    if ( valueData instanceof String ) {
      this.valueMeta = new ValueMetaString( valueName );
    } else if ( valueData instanceof Double ) {
      this.valueMeta = new ValueMetaNumber( valueName );
    } else if ( valueData instanceof Long ) {
      this.valueMeta = new ValueMetaInteger( valueName );
    } else if ( valueData instanceof Date ) {
      this.valueMeta = new ValueMetaDate( valueName );
    } else if ( valueData instanceof BigDecimal ) {
      this.valueMeta = new ValueMetaBigNumber( valueName );
    } else if ( valueData instanceof Boolean ) {
      this.valueMeta = new ValueMetaBoolean( valueName );
    } else if ( valueData instanceof byte[] ) {
      this.valueMeta = new ValueMetaBinary( valueName );
    } else {
      this.valueMeta = new ValueMetaSerializable( valueName );
    }
  }

  @Override
  public Object clone() {
    ValueMetaAndData vmad = new ValueMetaAndData();
    try {
      vmad.valueData = valueMeta.cloneValueData( valueData );
    } catch ( HopValueException e ) {
      vmad.valueData = null; // TODO: should we really do this? Is it safe?
    }
    vmad.valueMeta = valueMeta.clone();

    return vmad;
  }

  @Override
  public String toString() {
    try {
      return valueMeta.getString( valueData );
    } catch ( HopValueException e ) {
      return "<![" + e.getMessage() + "]!>";
    }
  }

  /**
   * Produce the XML representation of this value.
   *
   * @return a String containing the XML to represent this Value.
   * @throws HopValueException in case there is a data conversion error, only throws in case of lazy conversion
   */
  public String getXml() throws HopValueException {
    IValueMeta meta = valueMeta.clone();
    meta.setDecimalSymbol( "." );
    meta.setGroupingSymbol( null );
    meta.setCurrencySymbol( null );

    StringBuilder retval = new StringBuilder( 128 );
    retval.append( "<" + XML_TAG + ">" );
    retval.append( XmlHandler.addTagValue( "name", meta.getName(), false ) );
    retval.append( XmlHandler.addTagValue( "type", meta.getTypeDesc(), false ) );
    try {
      retval.append( XmlHandler.addTagValue( "text", meta.getCompatibleString( valueData ), false ) );
    } catch ( HopValueException e ) {
      // LogWriter.getInstance().logError(toString(), Const.getStackTracker(e));
      retval.append( XmlHandler.addTagValue( "text", "", false ) );
    }
    retval.append( XmlHandler.addTagValue( "length", meta.getLength(), false ) );
    retval.append( XmlHandler.addTagValue( "precision", meta.getPrecision(), false ) );
    retval.append( XmlHandler.addTagValue( "isnull", meta.isNull( valueData ), false ) );
    retval.append( XmlHandler.addTagValue( "mask", meta.getConversionMask(), false ) );
    retval.append( "</" + XML_TAG + ">" );

    return retval.toString();
  }

  /**
   * Construct a new Value and read the data from XML
   *
   * @param valnode The XML Node to read from.
   */
  public ValueMetaAndData( Node valnode ) {
    this();
    loadXml( valnode );
  }

  /**
   * Read the data for this Value from an XML Node
   *
   * @param valnode The XML Node to read from
   * @return true if all went well, false if something went wrong.
   */
  public boolean loadXml( Node valnode ) {
    valueMeta = null;

    try {
      String valname = XmlHandler.getTagValue( valnode, "name" );
      int valtype = ValueMetaBase.getType( XmlHandler.getTagValue( valnode, "type" ) );
      String text = XmlHandler.getTagValue( valnode, "text" );
      boolean isnull = "Y".equalsIgnoreCase( XmlHandler.getTagValue( valnode, "isnull" ) );
      int len = Const.toInt( XmlHandler.getTagValue( valnode, "length" ), -1 );
      int prec = Const.toInt( XmlHandler.getTagValue( valnode, "precision" ), -1 );
      String mask = XmlHandler.getTagValue( valnode, "mask" );

      valueMeta = ValueMetaFactory.createValueMeta( valname, valtype, len, prec );
      valueData = text;

      if ( mask != null ) {
        valueMeta.setConversionMask( mask );
      }

      if ( valtype != IValueMeta.TYPE_STRING ) {
        IValueMeta originMeta = new ValueMetaString( valname );
        if ( valueMeta.isNumeric() ) {
          originMeta.setDecimalSymbol( "." );
          originMeta.setGroupingSymbol( null );
          originMeta.setCurrencySymbol( null );
        }
        if ( valtype == IValueMeta.TYPE_DATE ) {
          originMeta.setConversionMask( ValueMetaBase.COMPATIBLE_DATE_FORMAT_PATTERN );
        }
        valueData = Const.trim( text );
        valueData = valueMeta.convertData( originMeta, valueData );
      }

      if ( isnull ) {
        valueData = null;
      }
    } catch ( Exception e ) {
      valueData = null;
      return false;
    }

    return true;
  }

  public String toStringMeta() {
    return valueMeta.toStringMeta();
  }

  /**
   * @return the valueData
   */
  public Object getValueData() {
    return valueData;
  }

  /**
   * @param valueData the valueData to set
   */
  public void setValueData( Object valueData ) {
    this.valueData = valueData;
  }

  /**
   * @return the valueMeta
   */
  public IValueMeta getValueMeta() {
    return valueMeta;
  }

  /**
   * @param valueMeta the valueMeta to set
   */
  public void setValueMeta( IValueMeta valueMeta ) {
    this.valueMeta = valueMeta;
  }

}
