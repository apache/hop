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

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class NullNumberConverterTest {

  private NullNumberConverter c;

  @BeforeEach
  void setUp() {
    c = new NullNumberConverter();
  }

  @Test
  void blankStringYieldsMinusOneForIntPrimitive() {
    assertEquals(-1, c.string2intPrimitive(null));
    assertEquals(-1, c.string2intPrimitive(""));
    assertEquals(-1, c.string2intPrimitive("   "));
    assertEquals(5, c.string2intPrimitive("5"));
  }

  @Test
  void nullBoxedNumbersYieldMinusOneForIntPrimitive() {
    assertEquals(-1, c.integer2intPrimitive(null));
    assertEquals(-1, c.number2intPrimitive(null));
    assertEquals(3, c.integer2intPrimitive(3L));
    assertEquals(2, c.number2intPrimitive(1.7));
  }
}
