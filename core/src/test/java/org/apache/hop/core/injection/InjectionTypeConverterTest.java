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

import static org.junit.jupiter.api.Assertions.assertThrows;

import org.apache.hop.core.exception.HopRuntimeException;
import org.junit.jupiter.api.Test;

class InjectionTypeConverterTest {

  @Test
  void defaultImplementationRejectsConversions() {
    InjectionTypeConverter c = new InjectionTypeConverter();
    assertThrows(HopRuntimeException.class, () -> c.string2string("x"));
    assertThrows(HopRuntimeException.class, () -> c.string2intPrimitive("1"));
    assertThrows(HopRuntimeException.class, () -> c.string2integer("1"));
    assertThrows(HopRuntimeException.class, () -> c.string2booleanPrimitive("Y"));
    assertThrows(HopRuntimeException.class, () -> c.string2enum(SampleEnum.class, "A"));
    assertThrows(HopRuntimeException.class, () -> c.boolean2string(true));
    assertThrows(HopRuntimeException.class, () -> c.integer2string(1L));
    assertThrows(HopRuntimeException.class, () -> c.number2string(1.0));
  }

  private enum SampleEnum {
    A
  }
}
