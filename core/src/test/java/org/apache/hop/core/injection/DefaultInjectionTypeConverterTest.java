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
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.apache.hop.core.exception.HopValueException;
import org.apache.hop.metadata.api.IEnumHasCodeAndDescription;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class DefaultInjectionTypeConverterTest {

  private DefaultInjectionTypeConverter c;

  @BeforeEach
  void setUp() {
    c = new DefaultInjectionTypeConverter();
  }

  @Test
  void stringConversions() throws HopValueException {
    assertEquals("hi", c.string2string("hi"));
    assertEquals(42, c.string2intPrimitive("42"));
    assertNull(c.string2integer(null));
    assertEquals(7, c.string2integer("7"));
    assertEquals(9L, c.string2longPrimitive("9"));
    assertNull(c.string2long(null));
    assertEquals(3L, c.string2long("3"));
  }

  @Test
  void stringToBooleanVariants() throws HopValueException {
    assertTrue(c.string2booleanPrimitive("Y"));
    assertTrue(c.string2booleanPrimitive("yes"));
    assertTrue(c.string2booleanPrimitive("True"));
    assertTrue(c.string2booleanPrimitive("1"));
    assertFalse(c.string2booleanPrimitive("N"));
    assertFalse(c.string2booleanPrimitive("0"));
    assertNull(c.string2boolean(null));
    assertFalse(c.string2boolean("no"));
  }

  @Test
  void stringToEnumByNameCodeAndDescriptionStyle() throws HopValueException {
    assertEquals(Plain.A, c.string2enum(Plain.class, "A"));
    assertNull(c.string2enum(Plain.class, null));
    assertEquals(Coded.ONE, c.string2enum(Coded.class, "ONE"));
    assertEquals(Coded.ONE, c.string2enum(Coded.class, "cd1"));
    assertEquals(Coded.TWO, c.string2enum(Coded.class, "CD2"));
    HopValueException ex =
        assertThrows(HopValueException.class, () -> c.string2enum(Plain.class, "Z"));
    assertTrue(ex.getMessage().contains("Unknown value"));
  }

  @Test
  void booleanConversions() throws HopValueException {
    assertNull(c.boolean2string(null));
    assertEquals("Y", c.boolean2string(true));
    assertEquals("N", c.boolean2string(false));
    assertEquals(1, c.boolean2intPrimitive(true));
    assertEquals(0, c.boolean2intPrimitive(false));
    assertEquals(0, c.boolean2intPrimitive(null));
    assertNull(c.boolean2integer(null));
    assertEquals(1L, c.boolean2longPrimitive(true));
    assertNull(c.boolean2long(null));
    assertTrue(c.boolean2booleanPrimitive(true));
    assertFalse(c.boolean2booleanPrimitive(false));
    assertTrue(c.boolean2boolean(true));
    assertNull(c.boolean2boolean(null));
  }

  @Test
  void integerConversions() throws HopValueException {
    assertNull(c.integer2string(null));
    assertEquals("100", c.integer2string(100L));
    assertEquals(5, c.integer2intPrimitive(5L));
    assertNull(c.integer2integer(null));
    assertEquals(9, c.integer2integer(9L));
    assertEquals(12L, c.integer2longPrimitive(12L));
    assertNull(c.integer2long(null));
    assertFalse(c.integer2booleanPrimitive(0L));
    assertTrue(c.integer2booleanPrimitive(2L));
    assertNull(c.integer2boolean(null));
  }

  @Test
  void numberConversions() throws HopValueException {
    assertNull(c.number2string(null));
    assertEquals("3.25", c.number2string(3.25));
    assertEquals(2, c.number2intPrimitive(1.6));
    assertEquals(0, c.number2intPrimitive(0.4));
    assertNull(c.number2integer(null));
    assertEquals(4L, c.number2longPrimitive(3.7));
    assertNull(c.number2long(null));
    assertFalse(c.number2booleanPrimitive(0.0));
    assertTrue(c.number2booleanPrimitive(1.2));
    assertNull(c.number2boolean(null));
  }

  private enum Plain {
    A,
    B
  }

  private enum Coded implements IEnumHasCodeAndDescription {
    ONE("cd1", "First"),
    TWO("CD2", "Second");

    private final String code;
    private final String description;

    Coded(String code, String description) {
      this.code = code;
      this.description = description;
    }

    @Override
    public String getCode() {
      return code;
    }

    @Override
    public String getDescription() {
      return description;
    }
  }
}
