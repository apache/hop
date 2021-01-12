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
package org.apache.hop.pipeline.transforms.userdefinedjavaclass;

import org.apache.hop.core.exception.HopValueException;
import org.apache.hop.core.logging.LogChannel;
import org.apache.hop.core.logging.ILogChannel;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.core.row.value.ValueMetaInternetAddress;
import org.apache.hop.core.row.value.ValueMetaTimestamp;
import org.apache.hop.i18n.BaseMessages;

import java.io.Serializable;
import java.math.BigDecimal;
import java.net.InetAddress;
import java.sql.Timestamp;
import java.util.Date;
import java.util.regex.Pattern;

public class FieldHelper {
  private static final Class<?> PKG = FieldHelper.class; // For Translator

  private int index = -1;
  private IValueMeta meta;

  public FieldHelper( IRowMeta rowMeta, String fieldName ) {
    this.meta = rowMeta.searchValueMeta( fieldName );
    this.index = rowMeta.indexOfValue( fieldName );
    if ( this.index == -1 ) {
      throw new IllegalArgumentException( String.format(
        "FieldHelper could not be initialized. The field named '%s' not found in RowMeta: %s", fieldName,
        rowMeta.toStringMeta() ) );
    }
  }

  public Object getObject( Object[] dataRow ) {
    return dataRow[ index ];
  }

  @Deprecated
  public BigDecimal getBigNumber( Object[] dataRow ) throws HopValueException {
    return getBigDecimal( dataRow );
  }

  public BigDecimal getBigDecimal( Object[] dataRow ) throws HopValueException {
    return meta.getBigNumber( dataRow[ index ] );
  }

  public byte[] getBinary( Object[] dataRow ) throws HopValueException {
    return meta.getBinary( dataRow[ index ] );
  }

  public Boolean getBoolean( Object[] dataRow ) throws HopValueException {
    return meta.getBoolean( dataRow[ index ] );
  }

  public Date getDate( Object[] dataRow ) throws HopValueException {
    return meta.getDate( dataRow[ index ] );
  }

  @Deprecated
  public Long getInteger( Object[] dataRow ) throws HopValueException {
    return getLong( dataRow );
  }

  public Long getLong( Object[] dataRow ) throws HopValueException {
    return meta.getInteger( dataRow[ index ] );
  }

  @Deprecated
  public Double getNumber( Object[] dataRow ) throws HopValueException {
    return getDouble( dataRow );
  }

  public Double getDouble( Object[] dataRow ) throws HopValueException {
    return meta.getNumber( dataRow[ index ] );
  }

  public Timestamp getTimestamp( Object[] dataRow ) throws HopValueException {
    return ( (ValueMetaTimestamp) meta ).getTimestamp( dataRow[ index ] );
  }

  public InetAddress getInetAddress( Object[] dataRow ) throws HopValueException {
    return ( (ValueMetaInternetAddress) meta ).getInternetAddress( dataRow[ index ] );
  }

  public Serializable getSerializable( Object[] dataRow ) throws HopValueException {
    return (Serializable) dataRow[ index ];
  }

  public String getString( Object[] dataRow ) throws HopValueException {
    return meta.getString( dataRow[ index ] );
  }

  public IValueMeta getValueMeta() {
    return meta;
  }

  public int indexOfValue() {
    return index;
  }

  public void setValue( Object[] dataRow, Object value ) {
    dataRow[ index ] = value;
  }

  public void setValue( Object[] dataRow, byte[] value ) {
    dataRow[ index ] = value;
  }

  private static final Pattern validJavaIdentifier = Pattern.compile( "^[\\w&&\\D]\\w*" );

  public static String getAccessor( boolean isIn, String fieldName ) {
    StringBuilder sb = new StringBuilder( "get(Fields." );
    sb.append( isIn ? "In" : "Out" );
    sb.append( String.format( ", \"%s\")", fieldName.replace( "\\", "\\\\" ).replace( "\"", "\\\"" ) ) );
    return sb.toString();
  }

  public static String getGetSignature( String accessor, IValueMeta v ) {
    StringBuilder sb = new StringBuilder();

    switch ( v.getType() ) {
      case IValueMeta.TYPE_BIGNUMBER:
        sb.append( "BigDecimal " );
        break;
      case IValueMeta.TYPE_BINARY:
        sb.append( "byte[] " );
        break;
      case IValueMeta.TYPE_BOOLEAN:
        sb.append( "Boolean " );
        break;
      case IValueMeta.TYPE_DATE:
        sb.append( "Date " );
        break;
      case IValueMeta.TYPE_INTEGER:
        sb.append( "Long " );
        break;
      case IValueMeta.TYPE_NUMBER:
        sb.append( "Double " );
        break;
      case IValueMeta.TYPE_STRING:
        sb.append( "String " );
        break;
      case IValueMeta.TYPE_INET:
        sb.append( "InetAddress " );
        break;
      case IValueMeta.TYPE_TIMESTAMP:
        sb.append( "Timestamp " );
        break;
      case IValueMeta.TYPE_SERIALIZABLE:
      default:
        sb.append( "Object " );
        break;
    }

    if ( validJavaIdentifier.matcher( v.getName() ).matches() ) {
      sb.append( v.getName() );
    } else {
      sb.append( "value" );
    }
    String name = getNativeDataTypeSimpleName( v );
    sb
      .append( " = " ).append( accessor ).append( ".get" ).append( "-".equals( name ) ? "Object" : name )
      .append( "(r);" );

    return sb.toString();
  }

  public static String getNativeDataTypeSimpleName( IValueMeta v ) {
    try {
      return v.getType() != IValueMeta.TYPE_BINARY ? v.getNativeDataTypeClass().getSimpleName() : "Binary";
    } catch ( HopValueException e ) {
      ILogChannel log = new LogChannel( v );
      log.logDebug( BaseMessages.getString( PKG, "FieldHelper.Log.UnknownNativeDataTypeSimpleName" ) );
      return "Object";
    }
  }
}
