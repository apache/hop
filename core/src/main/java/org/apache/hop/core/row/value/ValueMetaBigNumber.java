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

import java.math.BigDecimal;

@ValueMetaPlugin(
  id = "6",
  name = "BigNumber",
  description = "BigNumber",
  image = "images/number.svg"
)
public class ValueMetaBigNumber extends ValueMetaBase implements IValueMeta {

  public ValueMetaBigNumber() {
    this( null );
  }

  public ValueMetaBigNumber( String name ) {
    super( name, IValueMeta.TYPE_BIGNUMBER );
  }

  public ValueMetaBigNumber( String name, int length, int precision ) {
    super( name, IValueMeta.TYPE_BIGNUMBER, length, precision );
  }

  @Override
  public Object getNativeDataType( Object object ) throws HopValueException {
    return getBigNumber( object );
  }

  @Override
  public Class<?> getNativeDataTypeClass() throws HopValueException {
    return BigDecimal.class;
  }

  @Override
  public String getFormatMask() {
    return getBigNumberFormatMask();
  }

}
