/*! ******************************************************************************
 *
 * Hop : The Hop Orchestration Platform
 *
 * Copyright (C) 2002-2018 by Hitachi Vantara : http://www.pentaho.com
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

package org.apache.hop.core.row.value;

import org.apache.hop.core.Const;
import org.apache.hop.core.database.IDatabase;
import org.apache.hop.core.database.DatabaseMeta;
import org.apache.hop.core.exception.HopDatabaseException;
import org.apache.hop.core.exception.HopEofException;
import org.apache.hop.core.exception.HopFileException;
import org.apache.hop.core.exception.HopValueException;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.core.row.value.timestamp.SimpleTimestampFormat;
import org.apache.hop.core.util.Utils;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.math.BigDecimal;
import java.net.SocketTimeoutException;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Locale;

public class ValueMetaTimestamp extends ValueMetaDate {

  public ValueMetaTimestamp() {
    this( null );
  }

  public ValueMetaTimestamp( String name ) {
    super( name, IValueMeta.TYPE_TIMESTAMP );
  }

  @Override
  public boolean isDate() {
    return true;
  }

  @Override
  public Date getDate( Object object ) throws HopValueException {
    Timestamp timestamp = getTimestamp( object );
    if ( timestamp == null ) {
      return null;
    }
    return timestamp;
  }

  @Override
  public Long getInteger( Object object ) throws HopValueException {
    Timestamp timestamp = getTimestamp( object );
    if ( timestamp == null ) {
      return null;
    }

    return timestamp.getTime();
  }

  @Override
  public Double getNumber( Object object ) throws HopValueException {
    Timestamp timestamp = getTimestamp( object );
    if ( timestamp == null ) {
      return null;
    }    
    
    return Double.valueOf( timestamp.getTime() );   
  }

  @Override
  public BigDecimal getBigNumber( Object object ) throws HopValueException {
    Timestamp timestamp = getTimestamp( object );
    if ( timestamp == null ) {
      return null;
    }
    BigDecimal nanos =
      BigDecimal.valueOf( timestamp.getTime() ).multiply( BigDecimal.valueOf( 1000000000L ) ).add(
        BigDecimal.valueOf( timestamp.getNanos() ) );
    return nanos;
  }

  @Override
  public Boolean getBoolean( Object object ) throws HopValueException {
    throw new HopValueException( toStringMeta() + ": it's not possible to convert from Timestamp to Boolean" );
  }

  @Override
  public String getString( Object object ) throws HopValueException {
    return convertTimestampToString( getTimestamp( object ) );
  }

  public Timestamp getTimestamp( Object object ) throws HopValueException {
    if ( object == null ) {
      return null;
    }
    switch ( type ) {
      case TYPE_TIMESTAMP:
        switch ( storageType ) {
          case STORAGE_TYPE_NORMAL:
            return (Timestamp) object;
          case STORAGE_TYPE_BINARY_STRING:
            return (Timestamp) convertBinaryStringToNativeType( (byte[]) object );
          case STORAGE_TYPE_INDEXED:
            return (Timestamp) index[ ( (Integer) object ).intValue() ];
          default:
            throw new HopValueException( toString() + " : Unknown storage type " + storageType + " specified." );
        }
      case TYPE_STRING:
        switch ( storageType ) {
          case STORAGE_TYPE_NORMAL:
            return convertStringToTimestamp( (String) object );
          case STORAGE_TYPE_BINARY_STRING:
            return convertStringToTimestamp( (String) convertBinaryStringToNativeType( (byte[]) object ) );
          case STORAGE_TYPE_INDEXED:
            return convertStringToTimestamp( (String) index[ ( (Integer) object ).intValue() ] );
          default:
            throw new HopValueException( toString() + " : Unknown storage type " + storageType + " specified." );
        }
      case TYPE_NUMBER:
        switch ( storageType ) {
          case STORAGE_TYPE_NORMAL:
            return convertNumberToTimestamp( (Double) object );
          case STORAGE_TYPE_BINARY_STRING:
            return convertNumberToTimestamp( (Double) convertBinaryStringToNativeType( (byte[]) object ) );
          case STORAGE_TYPE_INDEXED:
            return convertNumberToTimestamp( (Double) index[ ( (Integer) object ).intValue() ] );
          default:
            throw new HopValueException( toString() + " : Unknown storage type " + storageType + " specified." );
        }
      case TYPE_INTEGER:
        switch ( storageType ) {
          case STORAGE_TYPE_NORMAL:
            return convertIntegerToTimestamp( (Long) object );
          case STORAGE_TYPE_BINARY_STRING:
            return convertIntegerToTimestamp( (Long) convertBinaryStringToNativeType( (byte[]) object ) );
          case STORAGE_TYPE_INDEXED:
            return convertIntegerToTimestamp( (Long) index[ ( (Integer) object ).intValue() ] );
          default:
            throw new HopValueException( toString() + " : Unknown storage type " + storageType + " specified." );
        }
      case TYPE_BIGNUMBER:
        switch ( storageType ) {
          case STORAGE_TYPE_NORMAL:
            return convertBigNumberToTimestamp( (BigDecimal) object );
          case STORAGE_TYPE_BINARY_STRING:
            return convertBigNumberToTimestamp( (BigDecimal) convertBinaryStringToNativeType( (byte[]) object ) );
          case STORAGE_TYPE_INDEXED:
            return convertBigNumberToTimestamp( (BigDecimal) index[ ( (Integer) object ).intValue() ] );
          default:
            throw new HopValueException( toString() + " : Unknown storage type " + storageType + " specified." );
        }
      case TYPE_BOOLEAN:
        throw new HopValueException( toString() + " : I don't know how to convert a boolean to a timestamp." );
      case TYPE_BINARY:
        throw new HopValueException( toString() + " : I don't know how to convert a binary value to timestamp." );
      case TYPE_SERIALIZABLE:
        throw new HopValueException( toString()
          + " : I don't know how to convert a serializable value to timestamp." );

      default:
        throw new HopValueException( toString() + " : Unknown type " + type + " specified." );
    }
  }

  public int compare( Object data1, Object data2 ) throws HopValueException {
    Timestamp timestamp1 = getTimestamp( data1 );
    Timestamp timestamp2 = getTimestamp( data2 );
    int cmp = 0;
    if ( timestamp1 == null ) {
      if ( timestamp2 == null ) {
        cmp = 0;
      } else {
        cmp = -1;
      }
    } else if ( timestamp2 == null ) {
      cmp = 1;
    } else {
      cmp = timestamp1.compareTo( timestamp2 );
    }
    if ( isSortedDescending() ) {
      return -cmp;
    } else {
      return cmp;
    }
  }

  protected Timestamp convertBigNumberToTimestamp( BigDecimal bd ) {
    if ( bd == null ) {
      return null;
    }
    return convertIntegerToTimestamp( bd.longValue() );
  }

  protected Timestamp convertNumberToTimestamp( Double d ) {
    if ( d == null ) {
      return null;
    }
    long nanos = d.longValue();

    return convertIntegerToTimestamp( nanos );
  }

  protected Timestamp convertIntegerToTimestamp( Long nanos ) {
    if ( nanos == null ) {
      return null;
    }

    long msSinceEpoch = nanos / 1000000;
    int leftNanos = (int) ( nanos - ( msSinceEpoch * 1000000 ) );
    Timestamp timestamp = new Timestamp( msSinceEpoch );
    timestamp.setNanos( leftNanos );

    return timestamp;
  }

  protected synchronized Timestamp convertStringToTimestamp( String string ) throws HopValueException {
    // See if trimming needs to be performed before conversion
    //
    string = Const.trimToType( string, getTrimType() );

    if ( Utils.isEmpty( string ) ) {
      return null;
    }
    Timestamp returnValue;
    try {
      returnValue = Timestamp.valueOf( string );
    } catch ( IllegalArgumentException e ) {
      try {
        returnValue = (Timestamp) getDateFormat().parse( string );
      } catch ( ParseException ex ) {
        throw new HopValueException( toString() + " : couldn't convert string [" + string
          + "] to a timestamp, expecting format [yyyy-mm-dd hh:mm:ss.ffffff]", e );
      }
    }
    return returnValue;
  }

  protected synchronized String convertTimestampToString( Timestamp timestamp ) throws HopValueException {

    if ( timestamp == null ) {
      return null;
    }

    return getDateFormat().format( timestamp );
  }

  @Override
  public Object convertDataFromString( String pol, IValueMeta convertMeta, String nullIf, String ifNull,
                                       int trimType ) throws HopValueException {
    // null handling and conversion of value to null
    //
    String nullValue = nullIf;
    if ( nullValue == null ) {
      switch ( convertMeta.getType() ) {
        case IValueMeta.TYPE_BOOLEAN:
          nullValue = Const.NULL_BOOLEAN;
          break;
        case IValueMeta.TYPE_STRING:
          nullValue = Const.NULL_STRING;
          break;
        case IValueMeta.TYPE_BIGNUMBER:
          nullValue = Const.NULL_BIGNUMBER;
          break;
        case IValueMeta.TYPE_NUMBER:
          nullValue = Const.NULL_NUMBER;
          break;
        case IValueMeta.TYPE_INTEGER:
          nullValue = Const.NULL_INTEGER;
          break;
        case IValueMeta.TYPE_DATE:
          nullValue = Const.NULL_DATE;
          break;
        case IValueMeta.TYPE_BINARY:
          nullValue = Const.NULL_BINARY;
          break;
        default:
          nullValue = Const.NULL_NONE;
          break;
      }
    }

    // See if we need to convert a null value into a String
    // For example, we might want to convert null into "Empty".
    //
    if ( !Utils.isEmpty( ifNull ) ) {
      // Note that you can't pull the pad method up here as a nullComp variable
      // because you could get an NPE since you haven't checked isEmpty(pol)
      // yet!
      if ( Utils.isEmpty( pol )
        || pol.equalsIgnoreCase( Const.rightPad( new StringBuilder( nullValue ), pol.length() ) ) ) {
        pol = ifNull;
      }
    }

    // See if the polled value is empty
    // In that case, we have a null value on our hands...
    //
    if ( Utils.isEmpty( pol ) ) {
      return null;
    } else {
      // if the null_value is specified, we try to match with that.
      //
      if ( !Utils.isEmpty( nullValue ) ) {
        if ( nullValue.length() <= pol.length() ) {
          // If the polled value is equal to the spaces right-padded null_value,
          // we have a match
          //
          if ( pol.equalsIgnoreCase( Const.rightPad( new StringBuilder( nullValue ), pol.length() ) ) ) {
            return null;
          }
        }
      } else {
        // Verify if there are only spaces in the polled value...
        // We consider that empty as well...
        //
        if ( Const.onlySpaces( pol ) ) {
          return null;
        }
      }
    }

    // Trimming
    StringBuilder strpol;
    switch ( trimType ) {
      case IValueMeta.TRIM_TYPE_LEFT:
        strpol = new StringBuilder( pol );
        while ( strpol.length() > 0 && strpol.charAt( 0 ) == ' ' ) {
          strpol.deleteCharAt( 0 );
        }
        pol = strpol.toString();

        break;
      case IValueMeta.TRIM_TYPE_RIGHT:
        strpol = new StringBuilder( pol );
        while ( strpol.length() > 0 && strpol.charAt( strpol.length() - 1 ) == ' ' ) {
          strpol.deleteCharAt( strpol.length() - 1 );
        }
        pol = strpol.toString();
        break;
      case IValueMeta.TRIM_TYPE_BOTH:
        strpol = new StringBuilder( pol );
        while ( strpol.length() > 0 && strpol.charAt( 0 ) == ' ' ) {
          strpol.deleteCharAt( 0 );
        }
        while ( strpol.length() > 0 && strpol.charAt( strpol.length() - 1 ) == ' ' ) {
          strpol.deleteCharAt( strpol.length() - 1 );
        }
        pol = strpol.toString();
        break;
      default:
        break;
    }

    // On with the regular program...
    // Simply call the ValueMeta routines to do the conversion
    // We need to do some effort here: copy all
    //
    return convertData( convertMeta, pol );
  }

  public Timestamp convertDateToTimestamp( Date date ) throws HopValueException {
    if ( date == null ) {
      return null;
    }
    Timestamp result = null;
    if ( date instanceof Timestamp ) {
      result = (Timestamp) date;
    } else {
      result = new Timestamp( date.getTime() );
    }
    return result;
  }

  /**
   * Convert the specified data to the data type specified in this object.
   *
   * @param meta2 the metadata of the object to be converted
   * @param data2 the data of the object to be converted
   * @return the object in the data type of this value metadata object
   * @throws HopValueException in case there is a data conversion error
   */
  @Override
  public Object convertData( IValueMeta meta2, Object data2 ) throws HopValueException {
    switch ( meta2.getType() ) {
      case TYPE_TIMESTAMP:
        return ( (ValueMetaTimestamp) meta2 ).getTimestamp( data2 );
      case TYPE_STRING:
        return convertStringToTimestamp( meta2.getString( data2 ) );
      case TYPE_INTEGER:
        return convertIntegerToTimestamp( meta2.getInteger( data2 ) );
      case TYPE_NUMBER:
        return convertNumberToTimestamp( meta2.getNumber( data2 ) );
      case TYPE_DATE:
        return convertDateToTimestamp( meta2.getDate( data2 ) );
      case TYPE_BIGNUMBER:
        return convertBigNumberToTimestamp( meta2.getBigNumber( data2 ) );
      default:
        throw new HopValueException( meta2.toStringMeta() + " : can't be converted to a timestamp" );
    }
  }

  @Override
  public Object cloneValueData( Object object ) throws HopValueException {
    Timestamp timestamp = getTimestamp( object );
    if ( timestamp == null ) {
      return null;
    }

    Timestamp clone = new Timestamp( timestamp.getTime() );
    clone.setNanos( timestamp.getNanos() );
    return clone;
  }

  @Override
  public IValueMeta getMetadataPreview( DatabaseMeta databaseMeta, ResultSet rs )
    throws HopDatabaseException {

    try {
      if ( java.sql.Types.TIMESTAMP == rs.getInt( "COLUMN_TYPE" ) ) {
        IValueMeta vmi = super.getMetadataPreview( databaseMeta, rs );
        IValueMeta valueMeta;
        if ( databaseMeta.supportsTimestampDataType() ) {
          valueMeta = new ValueMetaTimestamp( name );
        } else {
          valueMeta = new ValueMetaDate( name );
        }
        valueMeta.setLength( vmi.getLength() );
        valueMeta.setOriginalColumnType( vmi.getOriginalColumnType() );
        valueMeta.setOriginalColumnTypeName( vmi.getOriginalColumnTypeName() );
        valueMeta.setOriginalNullable( vmi.getOriginalNullable() );
        valueMeta.setOriginalPrecision( vmi.getOriginalPrecision() );
        valueMeta.setOriginalScale( vmi.getOriginalScale() );
        valueMeta.setOriginalSigned( vmi.getOriginalSigned() );
        return valueMeta;
      }
    } catch ( SQLException e ) {
      throw new HopDatabaseException( e );
    }
    return null;
  }

  @Override
  public IValueMeta getValueFromSqlType(DatabaseMeta databaseMeta, String name, ResultSetMetaData rm,
                                        int index, boolean ignoreLength, boolean lazyConversion ) throws HopDatabaseException {

    try {
      int type = rm.getColumnType( index );
      if ( type == java.sql.Types.TIMESTAMP ) {
        int length = rm.getScale( index );
        IValueMeta valueMeta;
        if ( databaseMeta.supportsTimestampDataType() ) {
          valueMeta = new ValueMetaTimestamp( name );
        } else {
          valueMeta = new ValueMetaDate( name );
        }
        valueMeta.setLength( length );

        // Also get original column details, comment, etc.
        //
        getOriginalColumnMetadata( valueMeta, rm, index, ignoreLength );

        return valueMeta;
      }

      return null;
    } catch ( Exception e ) {
      throw new HopDatabaseException( "Error evaluating timestamp value metadata", e );
    }
  }

  @Override
  public Object getValueFromResultSet( IDatabase iDatabase, ResultSet resultSet, int index )
    throws HopDatabaseException {

    try {

      return resultSet.getTimestamp( index + 1 );

    } catch ( Exception e ) {
      throw new HopDatabaseException(
        toStringMeta() + " : Unable to get timestamp from resultset at index " + index, e );
    }

  }

  @Override
  public void setPreparedStatementValue( DatabaseMeta databaseMeta, PreparedStatement preparedStatement, int index,
                                         Object data ) throws HopDatabaseException {

    try {
      if ( data != null ) {
        preparedStatement.setTimestamp( index, getTimestamp( data ) );
      } else {
        preparedStatement.setNull( index, java.sql.Types.TIMESTAMP );
      }
    } catch ( Exception e ) {
      throw new HopDatabaseException( toStringMeta() + " : Unable to set value on prepared statement on index "
        + index, e );
    }

  }

  @Override
  public Object convertDataUsingConversionMetaData( Object data2 ) throws HopValueException {
    if ( conversionMetadata == null ) {
      throw new HopValueException(
        "API coding error: please specify the conversion metadata before attempting to convert value " + name );
    }

    return super.convertDataUsingConversionMetaData( data2 );
  }

  @Override
  public byte[] getBinaryString( Object object ) throws HopValueException {

    if ( object == null ) {
      return null;
    }

    if ( isStorageBinaryString() && identicalFormat ) {
      return (byte[]) object; // shortcut it directly for better performance.
    }

    switch ( storageType ) {
      case STORAGE_TYPE_NORMAL:
        return convertStringToBinaryString( getString( object ) );
      case STORAGE_TYPE_BINARY_STRING:
        return convertStringToBinaryString( (String) convertBinaryStringToNativeType( (byte[]) object ) );
      case STORAGE_TYPE_INDEXED:
        return convertStringToBinaryString( getString( index[ ( (Integer) object ).intValue() ] ) );
      default:
        throw new HopValueException( toString() + " : Unknown storage type " + storageType + " specified." );
    }

  }

  @Override
  public void writeData( DataOutputStream outputStream, Object object ) throws HopFileException {
    try {
      // Is the value NULL?
      outputStream.writeBoolean( object == null );

      if ( object != null ) {
        switch ( storageType ) {
          case STORAGE_TYPE_NORMAL:
            // Handle Content -- only when not NULL
            Timestamp timestamp = convertDateToTimestamp( (Date) object );

            outputStream.writeLong( timestamp.getTime() );
            outputStream.writeInt( timestamp.getNanos() );
            break;

          case STORAGE_TYPE_BINARY_STRING:
            // Handle binary string content -- only when not NULL
            // In this case, we opt not to convert anything at all for speed.
            // That way, we can save on CPU power.
            // Since the streams can be compressed, volume shouldn't be an issue
            // at all.
            //
            writeBinaryString( outputStream, (byte[]) object );
            break;

          case STORAGE_TYPE_INDEXED:
            writeInteger( outputStream, (Integer) object ); // just an index
            break;

          default:
            throw new HopFileException( toString() + " : Unknown storage type " + getStorageType() );
        }
      }
    } catch ( ClassCastException e ) {
      throw new RuntimeException( toString() + " : There was a data type error: the data type of "
        + object.getClass().getName() + " object [" + object + "] does not correspond to value meta ["
        + toStringMeta() + "]" );
    } catch ( IOException e ) {
      throw new HopFileException( toString() + " : Unable to write value timestamp data to output stream", e );
    } catch ( HopValueException e ) {
      throw new RuntimeException( toString() + " : There was a data type error: the data type of "
        + object.getClass().getName() + " object [" + object + "] does not correspond to value meta ["
        + toStringMeta() + "]" );
    }
  }

  @Override
  public Object readData( DataInputStream inputStream ) throws HopFileException, HopEofException,
    SocketTimeoutException {
    try {
      // Is the value NULL?
      if ( inputStream.readBoolean() ) {
        return null; // done
      }

      switch ( storageType ) {
        case STORAGE_TYPE_NORMAL:
          // Handle Content -- only when not NULL
          long time = inputStream.readLong();
          int nanos = inputStream.readInt();
          Timestamp timestamp = new Timestamp( time );
          timestamp.setNanos( nanos );
          return timestamp;

        case STORAGE_TYPE_BINARY_STRING:
          return readBinaryString( inputStream );

        case STORAGE_TYPE_INDEXED:
          return readSmallInteger( inputStream ); // just an index: 4-bytes should be enough.

        default:
          throw new HopFileException( toString() + " : Unknown storage type " + getStorageType() );
      }
    } catch ( EOFException e ) {
      throw new HopEofException( e );
    } catch ( SocketTimeoutException e ) {
      throw e;
    } catch ( IOException e ) {
      throw new HopFileException( toString() + " : Unable to read value timestamp data from input stream", e );
    }
  }

  @Override
  public synchronized SimpleDateFormat getDateFormat() {
    return getDateFormat( getType() );
  }

  private synchronized SimpleDateFormat getDateFormat( int valueMetaType ) {
    if ( conversionMetadata != null ) {
      return new SimpleTimestampFormat( conversionMetadata.getDateFormat().toPattern() );
    }

    if ( dateFormat == null || dateFormatChanged ) {
      // This may not become static as the class is not thread-safe!
      dateFormat = new SimpleTimestampFormat( new SimpleDateFormat().toPattern() );

      String mask = getMask( valueMetaType );

      // Do we have a locale?
      //
      if ( dateFormatLocale == null || dateFormatLocale.equals( Locale.getDefault() ) ) {
        dateFormat = new SimpleTimestampFormat( mask );
      } else {
        dateFormat = new SimpleTimestampFormat( mask, dateFormatLocale );
      }

      // Do we have a time zone?
      //
      if ( dateFormatTimeZone != null ) {
        dateFormat.setTimeZone( dateFormatTimeZone );
      }

      // Set the conversion leniency as well
      //
      dateFormat.setLenient( dateFormatLenient );

      dateFormatChanged = false;
    }

    return dateFormat;
  }

  @Override
  public String getFormatMask() {
    return getTimestampFormatMask();
  }

  @Override
  public Class<?> getNativeDataTypeClass() throws HopValueException {
    return Timestamp.class;
  }
}
