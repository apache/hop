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

package org.apache.hop.core;

import org.apache.hop.core.exception.HopPluginException;
import org.apache.hop.core.exception.HopValueException;
import org.apache.hop.core.injection.InjectionTypeConverter;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.core.row.RowDataUtil;
import org.apache.hop.core.row.RowMeta;
import org.apache.hop.core.row.value.ValueMetaFactory;
import org.apache.hop.core.row.value.ValueMetaNone;

import java.math.BigDecimal;
import java.util.Date;

public class RowMetaAndData implements Cloneable {
  private IRowMeta rowMeta;

  private Object[] data;

  public RowMetaAndData() {
    clear();
  }

  /**
   * @param rowMeta
   * @param data
   */
  public RowMetaAndData( IRowMeta rowMeta, Object... data ) {
    this.rowMeta = rowMeta;
    this.data = data;
  }

  @Override
  public RowMetaAndData clone() {
    RowMetaAndData c = new RowMetaAndData();
    c.rowMeta = rowMeta.clone();
    try {
      c.data = rowMeta.cloneRow( data );
    } catch ( HopValueException e ) {
      throw new RuntimeException( "Problem with clone row detected in RowMetaAndData", e );
    }

    return c;
  }

  @Override
  public String toString() {
    try {
      return rowMeta.getString( data );
    } catch ( HopValueException e ) {
      return rowMeta.toString() + ", error presenting data: " + e.toString();
    }
  }

  /**
   * @return the data
   */
  public Object[] getData() {
    return data;
  }

  /**
   * @param data the data to set
   */
  public void setData( Object[] data ) {
    this.data = data;
  }

  /**
   * @return the rowMeta
   */
  public IRowMeta getRowMeta() {
    return rowMeta;
  }

  /**
   * @param rowMeta the rowMeta to set
   */
  public void setRowMeta( IRowMeta rowMeta ) {
    this.rowMeta = rowMeta;
  }

  @Override
  public int hashCode() {
    try {
      return rowMeta.hashCode( data );
    } catch ( HopValueException e ) {
      throw new RuntimeException(
        "Row metadata and data: unable to calculate hashcode because of a data conversion problem", e );
    }
  }

  @Override
  public boolean equals( Object obj ) {
    try {
      return rowMeta.compare( data, ( (RowMetaAndData) obj ).getData() ) == 0;
    } catch ( HopValueException e ) {
      throw new RuntimeException(
        "Row metadata and data: unable to compare rows because of a data conversion problem", e );
    }
  }

  public void addValue( IValueMeta valueMeta, Object valueData ) {
    data = RowDataUtil.addValueData( data, rowMeta.size(), valueData );
    rowMeta.addValueMeta( valueMeta );
  }

  public void addValue( String valueName, int valueType, Object valueData ) {
    IValueMeta v;
    try {
      v = ValueMetaFactory.createValueMeta( valueName, valueType );
    } catch ( HopPluginException e ) {
      v = new ValueMetaNone( valueName );
    }
    addValue( v, valueData );
  }

  public void clear() {
    rowMeta = new RowMeta();
    data = new Object[] {};
  }

  public long getInteger( String valueName, long def ) throws HopValueException {
    int idx = rowMeta.indexOfValue( valueName );
    if ( idx < 0 ) {
      throw new HopValueException( "Unknown column '" + valueName + "'" );
    }
    return getInteger( idx, def );
  }

  public long getInteger( int index, long def ) throws HopValueException {
    Long number = rowMeta.getInteger( data, index );
    if ( number == null ) {
      return def;
    }
    return number.longValue();
  }

  public Long getInteger( String valueName ) throws HopValueException {
    int idx = rowMeta.indexOfValue( valueName );
    if ( idx < 0 ) {
      throw new HopValueException( "Unknown column '" + valueName + "'" );
    }
    return rowMeta.getInteger( data, idx );
  }

  public Long getInteger( int index ) throws HopValueException {
    return rowMeta.getInteger( data, index );
  }

  public double getNumber( String valueName, double def ) throws HopValueException {
    int idx = rowMeta.indexOfValue( valueName );
    if ( idx < 0 ) {
      throw new HopValueException( "Unknown column '" + valueName + "'" );
    }
    return getNumber( idx, def );
  }

  public double getNumber( int index, double def ) throws HopValueException {
    Double number = rowMeta.getNumber( data, index );
    if ( number == null ) {
      return def;
    }
    return number.doubleValue();
  }

  public Date getDate( String valueName, Date def ) throws HopValueException {
    int idx = rowMeta.indexOfValue( valueName );
    if ( idx < 0 ) {
      throw new HopValueException( "Unknown column '" + valueName + "'" );
    }
    return getDate( idx, def );
  }

  public Date getDate( int index, Date def ) throws HopValueException {
    Date date = rowMeta.getDate( data, index );
    if ( date == null ) {
      return def;
    }
    return date;
  }

  public BigDecimal getBigNumber( String valueName, BigDecimal def ) throws HopValueException {
    int idx = rowMeta.indexOfValue( valueName );
    if ( idx < 0 ) {
      throw new HopValueException( "Unknown column '" + valueName + "'" );
    }
    return getBigNumber( idx, def );
  }

  public BigDecimal getBigNumber( int index, BigDecimal def ) throws HopValueException {
    BigDecimal number = rowMeta.getBigNumber( data, index );
    if ( number == null ) {
      return def;
    }
    return number;
  }

  public boolean getBoolean( String valueName, boolean def ) throws HopValueException {
    int idx = rowMeta.indexOfValue( valueName );
    if ( idx < 0 ) {
      throw new HopValueException( "Unknown column '" + valueName + "'" );
    }
    return getBoolean( idx, def );
  }

  public boolean getBoolean( int index, boolean def ) throws HopValueException {
    Boolean b = rowMeta.getBoolean( data, index );
    if ( b == null ) {
      return def;
    }
    return b.booleanValue();
  }

  public String getString( String valueName, String def ) throws HopValueException {
    int idx = rowMeta.indexOfValue( valueName );
    if ( idx < 0 ) {
      throw new HopValueException( "Unknown column '" + valueName + "'" );
    }
    return getString( idx, def );
  }

  public String getString( int index, String def ) throws HopValueException {
    String string = rowMeta.getString( data, index );
    if ( string == null ) {
      return def;
    }
    return string;
  }

  public byte[] getBinary( String valueName, byte[] def ) throws HopValueException {
    int idx = rowMeta.indexOfValue( valueName );
    if ( idx < 0 ) {
      throw new HopValueException( "Unknown column '" + valueName + "'" );
    }
    return getBinary( idx, def );
  }

  public byte[] getBinary( int index, byte[] def ) throws HopValueException {
    byte[] bin = rowMeta.getBinary( data, index );
    if ( bin == null ) {
      return def;
    }
    return bin;
  }

  public int compare( RowMetaAndData compare, int[] is, boolean[] bs ) throws HopValueException {
    return rowMeta.compare( data, compare.getData(), is );
  }

  public boolean isNumeric( int index ) {
    return rowMeta.getValueMeta( index ).isNumeric();
  }

  public int size() {
    return rowMeta.size();
  }

  public IValueMeta getValueMeta( int index ) {
    return rowMeta.getValueMeta( index );
  }

  public boolean isEmptyValue( String valueName ) throws HopValueException {
    int idx = rowMeta.indexOfValue( valueName );
    if ( idx < 0 ) {
      throw new HopValueException( "Unknown column '" + valueName + "'" );
    }

    IValueMeta metaType = rowMeta.getValueMeta( idx );
    // find by source value type
    switch ( metaType.getType() ) {
      case IValueMeta.TYPE_STRING:
        return rowMeta.getString( data, idx ) == null;
      case IValueMeta.TYPE_BOOLEAN:
        return rowMeta.getBoolean( data, idx ) == null;
      case IValueMeta.TYPE_INTEGER:
        return rowMeta.getInteger( data, idx ) == null;
      case IValueMeta.TYPE_NUMBER:
        return rowMeta.getNumber( data, idx ) == null;
      case IValueMeta.TYPE_BIGNUMBER:
        return rowMeta.getBigNumber( data, idx ) == null;
      case IValueMeta.TYPE_BINARY:
        return rowMeta.getBinary( data, idx ) == null;
      case IValueMeta.TYPE_DATE:
      case IValueMeta.TYPE_TIMESTAMP:
        return rowMeta.getDate( data, idx ) == null;
      case IValueMeta.TYPE_INET:
        return rowMeta.getString( data, idx ) == null;
    }
    throw new HopValueException( "Unknown source type: " + metaType.getTypeDesc() );
  }

  /**
   * Converts string value into specified type. Used for constant injection.
   */
  public static Object getStringAsJavaType( String vs, Class<?> destinationType, InjectionTypeConverter converter )
    throws HopValueException {
    if ( String.class.isAssignableFrom( destinationType ) ) {
      return converter.string2string( vs );
    } else if ( int.class.isAssignableFrom( destinationType ) ) {
      return converter.string2intPrimitive( vs );
    } else if ( Integer.class.isAssignableFrom( destinationType ) ) {
      return converter.string2integer( vs );
    } else if ( long.class.isAssignableFrom( destinationType ) ) {
      return converter.string2longPrimitive( vs );
    } else if ( Long.class.isAssignableFrom( destinationType ) ) {
      return converter.string2long( vs );
    } else if ( boolean.class.isAssignableFrom( destinationType ) ) {
      return converter.string2booleanPrimitive( vs );
    } else if ( Boolean.class.isAssignableFrom( destinationType ) ) {
      return converter.string2boolean( vs );
    } else if ( destinationType.isEnum() ) {
      return converter.string2enum( destinationType, vs );
    } else {
      throw new RuntimeException( "Wrong value conversion to " + destinationType );
    }
  }

  /**
   * Returns value as specified java type using converter. Used for metadata injection.
   */
  public Object getAsJavaType( String valueName, Class<?> destinationType, InjectionTypeConverter converter )
    throws HopValueException {
    int idx = rowMeta.indexOfValue( valueName );
    if ( idx < 0 ) {
      throw new HopValueException( "Unknown column '" + valueName + "'" );
    }

    IValueMeta metaType = rowMeta.getValueMeta( idx );
    // find by source value type
    switch ( metaType.getType() ) {
      case IValueMeta.TYPE_STRING:
        String vs = rowMeta.getString( data, idx );
        return getStringAsJavaType( vs, destinationType, converter );
      case IValueMeta.TYPE_BOOLEAN:
        Boolean vb = rowMeta.getBoolean( data, idx );
        if ( String.class.isAssignableFrom( destinationType ) ) {
          return converter.boolean2string( vb );
        } else if ( int.class.isAssignableFrom( destinationType ) ) {
          return converter.boolean2intPrimitive( vb );
        } else if ( Integer.class.isAssignableFrom( destinationType ) ) {
          return converter.boolean2integer( vb );
        } else if ( long.class.isAssignableFrom( destinationType ) ) {
          return converter.boolean2longPrimitive( vb );
        } else if ( Long.class.isAssignableFrom( destinationType ) ) {
          return converter.boolean2long( vb );
        } else if ( boolean.class.isAssignableFrom( destinationType ) ) {
          return converter.boolean2booleanPrimitive( vb );
        } else if ( Boolean.class.isAssignableFrom( destinationType ) ) {
          return converter.boolean2boolean( vb );
        } else if ( destinationType.isEnum() ) {
          return converter.boolean2enum( destinationType, vb );
        } else {
          throw new RuntimeException( "Wrong value conversion to " + destinationType );
        }
      case IValueMeta.TYPE_INTEGER:
        Long vi = rowMeta.getInteger( data, idx );
        if ( String.class.isAssignableFrom( destinationType ) ) {
          return converter.integer2string( vi );
        } else if ( int.class.isAssignableFrom( destinationType ) ) {
          return converter.integer2intPrimitive( vi );
        } else if ( Integer.class.isAssignableFrom( destinationType ) ) {
          return converter.integer2integer( vi );
        } else if ( long.class.isAssignableFrom( destinationType ) ) {
          return converter.integer2longPrimitive( vi );
        } else if ( Long.class.isAssignableFrom( destinationType ) ) {
          return converter.integer2long( vi );
        } else if ( boolean.class.isAssignableFrom( destinationType ) ) {
          return converter.integer2booleanPrimitive( vi );
        } else if ( Boolean.class.isAssignableFrom( destinationType ) ) {
          return converter.integer2boolean( vi );
        } else if ( destinationType.isEnum() ) {
          return converter.integer2enum( destinationType, vi );
        } else {
          throw new RuntimeException( "Wrong value conversion to " + destinationType );
        }
      case IValueMeta.TYPE_NUMBER:
        Double vn = rowMeta.getNumber( data, idx );
        if ( String.class.isAssignableFrom( destinationType ) ) {
          return converter.number2string( vn );
        } else if ( int.class.isAssignableFrom( destinationType ) ) {
          return converter.number2intPrimitive( vn );
        } else if ( Integer.class.isAssignableFrom( destinationType ) ) {
          return converter.number2integer( vn );
        } else if ( long.class.isAssignableFrom( destinationType ) ) {
          return converter.number2longPrimitive( vn );
        } else if ( Long.class.isAssignableFrom( destinationType ) ) {
          return converter.number2long( vn );
        } else if ( boolean.class.isAssignableFrom( destinationType ) ) {
          return converter.number2booleanPrimitive( vn );
        } else if ( Boolean.class.isAssignableFrom( destinationType ) ) {
          return converter.number2boolean( vn );
        } else if ( destinationType.isEnum() ) {
          return converter.number2enum( destinationType, vn );
        } else {
          throw new RuntimeException( "Wrong value conversion to " + destinationType );
        }
    }

    throw new HopValueException( "Unknown conversion from " + metaType.getTypeDesc() + " into " + destinationType );
  }

  public void removeValue( String valueName ) throws HopValueException {
    int index = rowMeta.indexOfValue( valueName );
    if ( index < 0 ) {
      throw new HopValueException( "Unable to find '" + valueName + "' in the row" );
    }
    removeValue( index );
  }

  public void removeValue( int index ) {
    rowMeta.removeValueMeta( index );
    data = RowDataUtil.removeItem( data, index );
  }

  public void mergeRowMetaAndData( RowMetaAndData rowMetaAndData, String originTransformName ) {
    int originalMetaSize = rowMeta.size();
    rowMeta.mergeRowMeta( rowMetaAndData.getRowMeta(), originTransformName );
    data = RowDataUtil.addRowData( data, originalMetaSize, rowMetaAndData.getData() );
  }
}
