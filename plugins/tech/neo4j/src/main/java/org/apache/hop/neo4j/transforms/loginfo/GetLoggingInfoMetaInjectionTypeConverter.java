/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

/*! ******************************************************************************
 *
 * Pentaho Data Integration
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

package org.apache.hop.neo4j.transforms.loginfo;

import org.apache.hop.core.exception.HopValueException;
import org.apache.hop.core.injection.InjectionTypeConverter;

public class GetLoggingInfoMetaInjectionTypeConverter extends InjectionTypeConverter {

  @Override
  public Enum<?> string2enum(Class<?> enumClass, String v) throws HopValueException {
    // For GetLoggingInfoMeta, enum should be a GetLoggingInfoTypes
    GetLoggingInfoTypes type = GetLoggingInfoTypes.getTypeFromString(v);
    if (!GetLoggingInfoTypes.TYPE_SYSTEM_INFO_NONE.toString().equals(v)
        && type == GetLoggingInfoTypes.TYPE_SYSTEM_INFO_NONE) {
      // Throw exception to let user know entered string was not valid SystemDataType
      throw new HopValueException("Unknown value '" + v + "' for enum " + enumClass);
    } else {
      return type;
    }
  }
}
