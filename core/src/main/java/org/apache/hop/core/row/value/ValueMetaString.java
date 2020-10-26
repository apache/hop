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

import org.apache.hop.core.exception.HopValueException;
import org.apache.hop.core.row.IValueMeta;

import java.util.Comparator;

public class ValueMetaString extends ValueMetaBase implements IValueMeta {

  public ValueMetaString() {
    this( null );
  }

  public ValueMetaString( String name ) {
    super( name, IValueMeta.TYPE_STRING );
  }

  public ValueMetaString( String name, Comparator<Object> comparator ) {
    super( name, IValueMeta.TYPE_STRING, comparator );
  }

  public ValueMetaString( String name, int length, int precision ) {
    super( name, IValueMeta.TYPE_STRING, length, precision );
  }

  @Override
  public Object getNativeDataType( Object object ) throws HopValueException {
    return getString( object );
  }

  @Override
  public Class<?> getNativeDataTypeClass() throws HopValueException {
    return String.class;
  }
}
