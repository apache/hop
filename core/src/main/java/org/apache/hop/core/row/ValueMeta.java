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

package org.apache.hop.core.row;

import java.io.DataInputStream;
import java.util.Locale;
import java.util.TimeZone;

import org.apache.hop.core.Const;
import org.apache.hop.core.exception.HopEOFException;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.exception.HopFileException;
import org.apache.hop.core.exception.HopPluginException;
import org.apache.hop.core.exception.HopValueException;
import org.apache.hop.core.row.value.ValueMetaBase;
import org.apache.hop.core.row.value.ValueMetaFactory;
import org.apache.hop.i18n.BaseMessages;
import org.w3c.dom.Node;

/**
 * Convenience class for backward compatibility.
 *
 *
 */
@Deprecated
public class ValueMeta extends ValueMetaBase {
  private static Class<?> PKG = Const.class;

  private ValueMetaInterface nativeType; // Used only for getNativeDataTypeClass(), not a "deep" clone of this object

  public ValueMeta() {
    this( null, ValueMetaInterface.TYPE_NONE, -1, -1 );
  }

  @Deprecated
  public ValueMeta( String name ) {
    this( name, ValueMetaInterface.TYPE_NONE, -1, -1 );
  }

  public ValueMeta( String name, int type ) {
    this( name, type, -1, -1 );
  }

  @Deprecated
  public ValueMeta( String name, int type, int storageType ) {
    this( name, type, -1, -1 );
    setStorageType( storageType );
  }

  public ValueMeta( String name, int type, int length, int precision ) {
    this.name = name;
    this.type = type;
    this.length = length;
    this.precision = precision;
    this.storageType = STORAGE_TYPE_NORMAL;
    this.sortedDescending = false;
    this.outputPaddingEnabled = false;
    this.decimalSymbol = "" + Const.DEFAULT_DECIMAL_SEPARATOR;
    this.groupingSymbol = "" + Const.DEFAULT_GROUPING_SEPARATOR;
    this.dateFormatLocale = Locale.getDefault();
    this.dateFormatTimeZone = TimeZone.getDefault();
    this.identicalFormat = true;
    this.bigNumberFormatting = true;
    this.lenientStringToNumber =
      convertStringToBoolean( Const.NVL( System.getProperty(
        Const.HOP_LENIENT_STRING_TO_NUMBER_CONVERSION, "N" ), "N" ) );

    super.determineSingleByteEncoding();
    setDefaultConversionMask();
  }

  /**
   * @param inputStream
   * @throws HopFileException
   * @throws HopEOFException
   * @deprecated
   */
  @Deprecated
  public ValueMeta( DataInputStream inputStream ) throws HopFileException, HopEOFException {
    super( inputStream );
  }

  /**
   * @param node
   * @throws HopException
   * @deprecated
   */
  @Deprecated
  public ValueMeta( Node node ) throws HopException {
    super( node );
  }

  /**
   * @deprecated
   */
  @Override
  @Deprecated
  public void setType( int type ) {
    super.setType( type );
  }

  @Override
  public Class<?> getNativeDataTypeClass() throws HopValueException {
    if ( nativeType == null ) {
      try {
        nativeType = ValueMetaFactory.createValueMeta( getType() );
      } catch ( HopPluginException e ) {
        throw new HopValueException( e );
      }
    }
    return nativeType.getNativeDataTypeClass();
  }
}
