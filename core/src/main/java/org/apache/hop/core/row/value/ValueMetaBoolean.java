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

package org.apache.hop.core.row.value;

import org.apache.hop.core.exception.HopValueException;
import org.apache.hop.core.row.IValueMeta;

@ValueMetaPlugin(
  id = "4",
  name = "Boolean",
  description = "Boolean",
  image = "images/boolean.svg"
)
public class ValueMetaBoolean extends ValueMetaBase implements IValueMeta {

  public ValueMetaBoolean() {
    this( null );
  }

  public ValueMetaBoolean( String name ) {
    super( name, IValueMeta.TYPE_BOOLEAN );
  }

  public ValueMetaBoolean( String name, int length, int precision ) {
    super( name, IValueMeta.TYPE_BOOLEAN, length, precision );
  }

  @Override
  public Object getNativeDataType( Object object ) throws HopValueException {
    return getBoolean( object );
  }

  @Override
  public Class<?> getNativeDataTypeClass() throws HopValueException {
    return Boolean.class;
  }
}
