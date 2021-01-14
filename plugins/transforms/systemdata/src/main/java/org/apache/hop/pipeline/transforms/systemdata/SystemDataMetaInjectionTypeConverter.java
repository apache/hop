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

package org.apache.hop.pipeline.transforms.systemdata;

import org.apache.hop.core.exception.HopValueException;
import org.apache.hop.core.injection.InjectionTypeConverter;

public class SystemDataMetaInjectionTypeConverter extends InjectionTypeConverter {

  @Override
  public Enum<?> string2enum( Class<?> enumClass, String v ) throws HopValueException {
    // For SystemDataMeta, enum should be a SystemDataTypes
    SystemDataTypes type = SystemDataTypes.getTypeFromString( v );
    if ( !SystemDataTypes.TYPE_SYSTEM_INFO_NONE.toString().equals( v ) && type == SystemDataTypes.TYPE_SYSTEM_INFO_NONE ) {
      // Throw exception to let user know entered string was not valid SystemDataType
      throw new HopValueException( "Unknown value '" + v + "' for enum " + enumClass );
    } else {
      return type;
    }
  }
}
