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

package org.apache.hop.repository.kdr.delegates;

import org.apache.hop.core.RowMetaAndData;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.row.ValueMetaAndData;
import org.apache.hop.core.row.ValueMetaInterface;
import org.apache.hop.core.row.value.ValueMetaFactory;
import org.apache.hop.core.row.value.ValueMetaString;
import org.apache.hop.repository.ObjectId;
import org.apache.hop.repository.kdr.HopDatabaseRepository;

public class HopDatabaseRepositoryValueDelegate extends HopDatabaseRepositoryBaseDelegate {

  // private static Class<?> PKG = ValueMetaAndData.class; // for i18n purposes, needed by Translator2!!

  public HopDatabaseRepositoryValueDelegate( HopDatabaseRepository repository ) {
    super( repository );
  }

  public RowMetaAndData getValue( ObjectId id_value ) throws HopException {
    return repository.connectionDelegate.getOneRow(
      quoteTable( HopDatabaseRepository.TABLE_R_VALUE ),
      quote( HopDatabaseRepository.FIELD_VALUE_ID_VALUE ), id_value );
  }

  public ValueMetaAndData loadValueMetaAndData( ObjectId id_value ) throws HopException {
    ValueMetaAndData valueMetaAndData = new ValueMetaAndData();
    try {
      RowMetaAndData r = getValue( id_value );
      if ( r != null ) {
        String name = r.getString( HopDatabaseRepository.FIELD_VALUE_NAME, null );
        int valtype = ValueMetaFactory.getIdForValueMeta(
          r.getString( HopDatabaseRepository.FIELD_VALUE_VALUE_TYPE, null ) );
        boolean isNull = r.getBoolean( HopDatabaseRepository.FIELD_VALUE_IS_NULL, false );
        ValueMetaInterface v = ValueMetaFactory.createValueMeta( name, valtype );
        valueMetaAndData.setValueMeta( v );

        if ( isNull ) {
          valueMetaAndData.setValueData( null );
        } else {
          ValueMetaInterface stringValueMeta = new ValueMetaString( name );
          ValueMetaInterface valueMeta = valueMetaAndData.getValueMeta();
          stringValueMeta.setConversionMetadata( valueMeta );

          valueMeta.setDecimalSymbol( ValueMetaAndData.VALUE_REPOSITORY_DECIMAL_SYMBOL );
          valueMeta.setGroupingSymbol( ValueMetaAndData.VALUE_REPOSITORY_GROUPING_SYMBOL );

          switch ( valueMeta.getType() ) {
            case ValueMetaInterface.TYPE_NUMBER:
              valueMeta.setConversionMask( ValueMetaAndData.VALUE_REPOSITORY_NUMBER_CONVERSION_MASK );
              break;
            case ValueMetaInterface.TYPE_INTEGER:
              valueMeta.setConversionMask( ValueMetaAndData.VALUE_REPOSITORY_INTEGER_CONVERSION_MASK );
              break;
            default:
              break;
          }

          String string = r.getString( "VALUE_STR", null );
          valueMetaAndData.setValueData( stringValueMeta.convertDataUsingConversionMetaData( string ) );

          // OK, now comes the dirty part...
          // We want the defaults back on there...
          //
          valueMeta = ValueMetaFactory.createValueMeta( name, valueMeta.getType() );
        }
      }

      return valueMetaAndData;
    } catch ( HopException dbe ) {
      throw new HopException( "Unable to load Value from repository with id_value=" + id_value, dbe );
    }
  }

}
