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

package org.apache.hop.core.util;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.math.BigDecimal;
import java.util.Date;
import java.util.Objects;
import org.apache.hop.core.row.IValueMeta;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.mozilla.javascript.Context;
import org.mozilla.javascript.NativeJavaObject;
import org.mozilla.javascript.Scriptable;
import org.mozilla.javascript.ScriptableObject;
import org.mozilla.javascript.Undefined;

/** A set of tests for {@linkplain JavaScriptUtils} class. */
class JavaScriptUtilsTest {

  private static final String UNDEFINED = Undefined.class.getName();
  private static final String JAVA_OBJECT = NativeJavaObject.class.getName();
  private static final String NATIVE_NUMBER = "org.mozilla.javascript.NativeNumber";

  private static Context ctx;
  private static ScriptableObject scope;

  @BeforeAll
  static void setUp() {
    ctx = Context.enter();
    scope = ctx.initStandardObjects();
  }

  @AfterAll
  static void tearDown() {
    scope = null;
    ctx = null;
    Context.exit();
  }

  private static Scriptable getIntValue() {
    return Context.toObject(1L, scope);
  }

  private static Scriptable getDoubleValue() {
    return Context.toObject(1.0, scope);
  }

  // jsToNumber tests

  @Test
  void jsToNumber_Undefined() {
    assertNull(JavaScriptUtils.jsToNumber(null, UNDEFINED));
  }

  @Test
  void jsToNumber_NativeJavaObject_Double() {
    Scriptable value = getDoubleValue();
    Number number = JavaScriptUtils.jsToNumber(value, JAVA_OBJECT);
    Assertions.assertNotNull(number);
    assertEquals(1.0, number.doubleValue(), 1e-6);
  }

  @Test
  void jsToNumber_NativeJavaObject_Int() {
    Scriptable value = getIntValue();
    Number number = JavaScriptUtils.jsToNumber(value, JAVA_OBJECT);
    Assertions.assertNotNull(number);
    assertEquals(1.0, number.doubleValue(), 1e-6);
  }

  @Test
  void jsToNumber_NativeNumber() {
    Scriptable value = Context.toObject(1.0, scope);
    Number number = JavaScriptUtils.jsToNumber(value, NATIVE_NUMBER);
    Assertions.assertNotNull(number);
    assertEquals(1.0, number.doubleValue(), 1e-6);
  }

  @Test
  void jsToNumber_JavaNumber() {
    Number number = JavaScriptUtils.jsToNumber(1.0, Double.class.getName());
    Assertions.assertNotNull(number);
    assertEquals(1.0, number.doubleValue(), 1e-6);
  }

  // jsToInteger tests

  @Test
  void jsToInteger_Undefined() {
    assertNull(JavaScriptUtils.jsToInteger(null, Undefined.class));
  }

  @Test
  void jsToInteger_NaturalNumbers() {
    Number[] naturalNumbers = new Number[] {(byte) 1, (short) 1, 1, (long) 1};

    for (Number number : naturalNumbers) {
      assertEquals(Long.valueOf(1), JavaScriptUtils.jsToInteger(number, number.getClass()));
    }
  }

  @Test
  void jsToInteger_String() {
    assertEquals(Long.valueOf(1), JavaScriptUtils.jsToInteger("1", String.class));
  }

  @Test
  void jsToInteger_String_Unparseable() {
    assertThrows(NumberFormatException.class, () -> JavaScriptUtils.jsToInteger("q", String.class));
  }

  @Test
  void jsToInteger_Double() {
    assertEquals(Long.valueOf(1), JavaScriptUtils.jsToInteger(1.0, Double.class));
  }

  @Test
  void jsToInteger_NativeJavaObject_Int() {
    Scriptable value = getIntValue();
    assertEquals(Long.valueOf(1), JavaScriptUtils.jsToInteger(value, NativeJavaObject.class));
  }

  @Test
  void jsToInteger_NativeJavaObject_Double() {
    Scriptable value = getDoubleValue();
    assertEquals(Long.valueOf(1), JavaScriptUtils.jsToInteger(value, NativeJavaObject.class));
  }

  @Test
  void jsToInteger_Other_Int() {
    assertEquals(Long.valueOf(1), JavaScriptUtils.jsToInteger(1, getClass()));
  }

  @Test
  void jsToInteger_Other_String() {
    Class<? extends JavaScriptUtilsTest> aClass = getClass();
    assertThrows(NumberFormatException.class, () -> JavaScriptUtils.jsToInteger("qwerty", aClass));
  }

  // jsToString tests

  @Test
  void jsToString_Undefined() {
    assertEquals("null", JavaScriptUtils.jsToString(null, UNDEFINED));
  }

  @Test
  void jsToString_NativeJavaObject_Int() {
    assertEquals("1", JavaScriptUtils.jsToString(getIntValue(), JAVA_OBJECT));
  }

  @Test
  void jsToString_NativeJavaObject_Double() {
    //  return "1.0" in previous release with org.apache.hop.compatibility.Value
    assertEquals("1", JavaScriptUtils.jsToString(getDoubleValue(), JAVA_OBJECT));

    Scriptable value = Context.toObject(1.23, scope);
    assertEquals("1.23", JavaScriptUtils.jsToString(value, JAVA_OBJECT));
  }

  @Test
  void jsToString_String() {
    assertEquals("qwerty", JavaScriptUtils.jsToString("qwerty", String.class.getName()));
  }

  // jsToDate tests

  @Test
  void jsToDate_Undefined() throws Exception {
    assertNull(JavaScriptUtils.jsToDate(null, UNDEFINED));
  }

  @Test
  void jsToDate_NativeDate() throws Exception {
    Date date = new Date(1);
    Scriptable value = ctx.newObject(scope, "Date", new Object[] {date.getTime()});
    assertEquals(date, JavaScriptUtils.jsToDate(value, "org.mozilla.javascript.NativeDate"));
  }

  @Test
  void jsToDate_NativeJavaObject() throws Exception {
    Scriptable value = Context.toObject(new Date(1), scope);
    assertEquals(new Date(1), JavaScriptUtils.jsToDate(value, JAVA_OBJECT));
  }

  @Test
  void jsToDate_Double() throws Exception {
    assertEquals(new Date(1), JavaScriptUtils.jsToDate(1.0, Double.class.getName()));
  }

  @Test
  void jsToDate_String() throws Exception {
    assertEquals(new Date(1), JavaScriptUtils.jsToDate("1.0", String.class.getName()));
  }

  @Test
  void jsToDate_String_Unparseable() {
    String name = String.class.getName();
    assertThrows(NumberFormatException.class, () -> JavaScriptUtils.jsToDate("qwerty", name));
  }

  // jsToBigNumber tests

  @Test
  void jsToBigNumber_Undefined() {
    assertNull(JavaScriptUtils.jsToBigNumber(null, UNDEFINED));
  }

  @Test
  void jsToBigNumber_NativeNumber() {
    Scriptable value = Context.toObject(1.0, scope);
    BigDecimal number = JavaScriptUtils.jsToBigNumber(value, NATIVE_NUMBER);
    Assertions.assertNotNull(number);
    assertEquals(1.0, number.doubleValue(), 1e-6);
  }

  @Test
  void jsToBigNumber_NativeJavaObject_Int() {
    assertEquals(
        1.0,
        Objects.requireNonNull(JavaScriptUtils.jsToBigNumber(getIntValue(), JAVA_OBJECT))
            .doubleValue(),
        1e-6);
  }

  @Test
  void jsToBigNumber_NativeJavaObject_Double() {
    assertEquals(
        1.0,
        Objects.requireNonNull(JavaScriptUtils.jsToBigNumber(getDoubleValue(), JAVA_OBJECT))
            .doubleValue(),
        1e-6);
  }

  @Test
  void jsToBigNumber_NativeJavaObject_BigDecimal() {
    Scriptable object = Context.toObject(BigDecimal.ONE, scope);
    assertEquals(
        1.0,
        Objects.requireNonNull(JavaScriptUtils.jsToBigNumber(object, JAVA_OBJECT)).doubleValue(),
        1e-6);
  }

  @Test
  void jsToBigNumber_NaturalNumbers() {
    Number[] naturalNumbers = new Number[] {(byte) 1, (short) 1, 1, (long) 1};

    for (Number number : naturalNumbers) {
      assertEquals(
          1.0,
          Objects.requireNonNull(JavaScriptUtils.jsToBigNumber(number, number.getClass().getName()))
              .doubleValue(),
          1e-6);
    }
  }

  @Test
  void jsToBigNumber_Double() {
    assertEquals(
        1.0,
        Objects.requireNonNull(JavaScriptUtils.jsToBigNumber(1.0, Double.class.getName()))
            .doubleValue(),
        1e-6);
  }

  @Test
  void jsToBigNumber_String() {
    assertEquals(
        1.0,
        Objects.requireNonNull(JavaScriptUtils.jsToBigNumber("1", String.class.getName()))
            .doubleValue(),
        1e-6);
  }

  @Test
  void jsToBigNumber_UnknownClass() {
    assertThrows(RuntimeException.class, () -> JavaScriptUtils.jsToBigNumber("1", "qwerty"));
  }

  // convertFromJs tests

  @Test
  void convertFromJs_TypeNone() {
    assertThrows(
        RuntimeException.class,
        () -> JavaScriptUtils.convertFromJs(null, IValueMeta.TYPE_NONE, "qwerty"));
  }

  @Test
  void convertFromJs_TypeBoolean() throws Exception {
    Object o = new Object();
    Object o2 = JavaScriptUtils.convertFromJs(o, IValueMeta.TYPE_BOOLEAN, "qwerty");
    assertEquals(o, o2);
  }

  @Test
  void convertFromJs_TypeBinary() throws Exception {
    byte[] bytes = new byte[] {0, 1};
    Object converted = JavaScriptUtils.convertFromJs(bytes, IValueMeta.TYPE_BINARY, "qwerty");
    assertInstanceOf(byte[].class, converted);
    assertArrayEquals(bytes, (byte[]) converted);
  }
}
