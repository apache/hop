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

package org.apache.hop.core.injection;

import static org.junit.jupiter.api.Assertions.assertEquals;

import org.apache.hop.core.HopClientEnvironment;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.exception.HopValueException;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.core.row.value.ValueMetaFactory;
import org.apache.hop.junit.rules.RestoreHopEnvironmentExtension;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

@ExtendWith(RestoreHopEnvironmentExtension.class)
class DataTypeConverterTest {

  @BeforeAll
  static void initHop() throws HopException {
    HopClientEnvironment.init();
  }

  @Test
  void string2intPrimitiveMapsValueMetaNameToPluginId() throws HopValueException {
    DataTypeConverter c = new DataTypeConverter();
    int stringId = ValueMetaFactory.getIdForValueMeta("String");
    assertEquals(stringId, c.string2intPrimitive("String"));
    assertEquals(stringId, c.string2intPrimitive("string"));
  }

  @Test
  void unknownTypeNameReturnsTypeNone() throws HopValueException {
    DataTypeConverter c = new DataTypeConverter();
    assertEquals(IValueMeta.TYPE_NONE, c.string2intPrimitive("__not_a_registered_type__"));
  }
}
