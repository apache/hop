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

package org.apache.hop.pipeline.transforms.formula.util;

import static org.junit.jupiter.api.Assertions.assertEquals;

import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.junit.rules.RestoreHopEngineEnvironmentExtension;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

/** Unit test for {@link StringToTypeConverter} */
@ExtendWith(RestoreHopEngineEnvironmentExtension.class)
class StringToTypeConverterTest {

  private StringToTypeConverter converter;

  @BeforeEach
  void setUp() {
    converter = new StringToTypeConverter();
  }

  @Test
  void string2intPrimitiveMapsKnownTypes() throws Exception {
    assertEquals(IValueMeta.TYPE_STRING, converter.string2intPrimitive("String"));
    assertEquals(IValueMeta.TYPE_NUMBER, converter.string2intPrimitive("Number"));
    assertEquals(IValueMeta.TYPE_INTEGER, converter.string2intPrimitive("Integer"));
    assertEquals(IValueMeta.TYPE_BIGNUMBER, converter.string2intPrimitive("BigNumber"));
    assertEquals(IValueMeta.TYPE_BOOLEAN, converter.string2intPrimitive("Boolean"));
    assertEquals(IValueMeta.TYPE_DATE, converter.string2intPrimitive("Date"));
  }

  @Test
  void string2intPrimitiveIsCaseInsensitive() throws Exception {
    assertEquals(IValueMeta.TYPE_STRING, converter.string2intPrimitive("string"));
    assertEquals(IValueMeta.TYPE_NUMBER, converter.string2intPrimitive("NUMBER"));
    assertEquals(IValueMeta.TYPE_INTEGER, converter.string2intPrimitive("InTeGeR"));
  }

  @Test
  void string2intPrimitiveReturnsNoneForUnknownType() throws Exception {
    assertEquals(IValueMeta.TYPE_NONE, converter.string2intPrimitive("NotARealValueMetaType"));
    assertEquals(IValueMeta.TYPE_NONE, converter.string2intPrimitive(null));
  }
}
