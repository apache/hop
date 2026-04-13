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

package org.apache.hop.pipeline.transforms.javascript;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Date;
import org.apache.hop.core.Const;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mozilla.javascript.Context;
import org.mozilla.javascript.EvaluatorException;
import org.mozilla.javascript.Scriptable;

/**
 * Exercises {@link ScriptValuesAddedFunctions} through the same Rhino entry points the JavaScript
 * transform uses ({@link Context#toString}, {@link Context#toNumber}, {@link Context#jsToJava}).
 */
class ScriptValuesAddedFunctionsRhinoIntegrationTest {

  private Context cx;
  private Scriptable scope;

  @BeforeEach
  void enterRhino() {
    cx = Context.enter();
    scope = cx.initStandardObjects();
  }

  @AfterEach
  void exitRhino() {
    Context.exit();
  }

  private Object[] args(Object... o) {
    return o;
  }

  @Test
  void getDigitsOnly_stripsNonDigits() {
    assertEquals("123", ScriptValuesAddedFunctions.getDigitsOnly(cx, scope, args("a1b2c3"), null));
  }

  @Test
  void getDigitsOnly_requiresOneArgument() {
    assertThrows(
        EvaluatorException.class,
        () -> ScriptValuesAddedFunctions.getDigitsOnly(cx, scope, args(), null));
  }

  @Test
  void luhnCheck_validAndInvalid() {
    assertTrue(ScriptValuesAddedFunctions.LuhnCheck(cx, scope, args("79927398713"), null));
    assertTrue(ScriptValuesAddedFunctions.LuhnCheck(cx, scope, args("0"), null));
    assertFalse(ScriptValuesAddedFunctions.LuhnCheck(cx, scope, args("79927398712"), null));
    assertFalse(ScriptValuesAddedFunctions.LuhnCheck(cx, scope, args("not-a-number"), null));
  }

  @Test
  void indexOf_twoAndThreeArguments() {
    assertEquals(2, ScriptValuesAddedFunctions.indexOf(cx, scope, args("hello", "l"), null));
    assertEquals(3, ScriptValuesAddedFunctions.indexOf(cx, scope, args("hello", "l", 3), null));
  }

  @Test
  void trim_ltrim_rtrim() {
    assertEquals("x", ScriptValuesAddedFunctions.trim(cx, scope, args("  x  "), null));
    assertEquals("x  ", ScriptValuesAddedFunctions.ltrim(cx, scope, args("  x  "), null));
    assertEquals("  x", ScriptValuesAddedFunctions.rtrim(cx, scope, args("  x  "), null));
  }

  @Test
  void substr_twoAndThreeArguments() {
    assertEquals("bcd", ScriptValuesAddedFunctions.substr(cx, scope, args("abcd", 1), null));
    assertEquals("bc", ScriptValuesAddedFunctions.substr(cx, scope, args("abcd", 1, 2), null));
  }

  @Test
  void fillString_lpad_rpad() {
    assertEquals("xxx", ScriptValuesAddedFunctions.fillString(cx, scope, args("x", 3), null));
    assertEquals("00ab", ScriptValuesAddedFunctions.lpad(cx, scope, args("ab", "0", 4), null));
    assertEquals("ab00", ScriptValuesAddedFunctions.rpad(cx, scope, args("ab", "0", 4), null));
  }

  @Test
  void upper_lower() {
    assertEquals("AB", ScriptValuesAddedFunctions.upper(cx, scope, args("aB"), null));
    assertEquals("ab", ScriptValuesAddedFunctions.lower(cx, scope, args("Ab"), null));
  }

  @Test
  void escapeAndUnescapeHelpers_delegateToConst() {
    String raw = "<tag>&\"'";
    assertEquals(
        Const.escapeXml(raw), ScriptValuesAddedFunctions.escapeXml(cx, scope, args(raw), null));
    assertEquals(
        Const.escapeHtml(raw), ScriptValuesAddedFunctions.escapeHtml(cx, scope, args(raw), null));
    String escaped = Const.escapeHtml(raw);
    assertEquals(
        Const.unEscapeHtml(escaped),
        ScriptValuesAddedFunctions.unEscapeHtml(cx, scope, args(escaped), null));
    String xmlEsc = Const.escapeXml(raw);
    assertEquals(
        Const.unEscapeXml(xmlEsc),
        ScriptValuesAddedFunctions.unEscapeXml(cx, scope, args(xmlEsc), null));
    assertEquals(
        Const.escapeSql("O'Brien"),
        ScriptValuesAddedFunctions.escapeSql(cx, scope, args("O'Brien"), null));
    assertEquals(
        Const.protectXmlCdata("]]>"),
        ScriptValuesAddedFunctions.protectXmlCdata(cx, scope, args("]]>"), null));
    assertEquals(
        Const.removeDigits("a1b2"),
        ScriptValuesAddedFunctions.removeDigits(cx, scope, args("a1b2"), null));
  }

  @Test
  void initCap_removeCRLF_getOcuranceString() {
    assertNotNull(ScriptValuesAddedFunctions.initCap(cx, scope, args("hello world"), null));
    assertEquals("ab", ScriptValuesAddedFunctions.removeCRLF(cx, scope, args("a\r\nb"), null));
    assertEquals(
        Const.getOcuranceString("banana", "a"),
        ScriptValuesAddedFunctions.getOcuranceString(cx, scope, args("banana", "a"), null));
  }

  @Test
  void trunc_floorsNumber() {
    Object out = ScriptValuesAddedFunctions.trunc(cx, scope, args(-3.7), null);
    assertInstanceOf(Double.class, out);
    assertEquals(-4.0, (Double) out, 1e-9);
  }

  @Test
  void abs_ceil_floor() {
    assertEquals(2.5, (Double) ScriptValuesAddedFunctions.abs(cx, scope, args(-2.5), null), 1e-9);
    assertEquals(3.0, (Double) ScriptValuesAddedFunctions.ceil(cx, scope, args(2.1), null), 1e-9);
    assertEquals(2.0, (Double) ScriptValuesAddedFunctions.floor(cx, scope, args(2.9), null), 1e-9);
  }

  @Test
  void num2str_and_str2num_roundTripInteger() {
    String s = ScriptValuesAddedFunctions.num2str(cx, scope, args(42.0), null);
    assertNotNull(s);
    Object n = ScriptValuesAddedFunctions.str2num(cx, scope, args(s), null);
    assertInstanceOf(Double.class, n);
    assertEquals(42.0, (Double) n, 1e-9);
  }

  @Test
  void isNum_isDate() {
    assertEquals(Boolean.TRUE, ScriptValuesAddedFunctions.isNum(cx, scope, args(1.0), null));
    assertEquals(Boolean.FALSE, ScriptValuesAddedFunctions.isNum(cx, scope, args("x"), null));
    Object jsDate = Context.javaToJS(new Date(), scope);
    assertEquals(Boolean.TRUE, ScriptValuesAddedFunctions.isDate(cx, scope, args(jsDate), null));
    assertEquals(Boolean.FALSE, ScriptValuesAddedFunctions.isDate(cx, scope, args("nope"), null));
  }

  @Test
  void decode_and_replace() {
    assertEquals(
        "yes", ScriptValuesAddedFunctions.decode(cx, scope, args("k1", "k1", "yes", "no"), null));
    assertEquals("d", ScriptValuesAddedFunctions.decode(cx, scope, args("a", "b", "c", "d"), null));
    assertEquals(
        "xby", ScriptValuesAddedFunctions.replace(cx, scope, args("aby", "ab", "xb"), null));
  }

  @Test
  void isEmpty_and_isMailValid() {
    assertEquals(Boolean.TRUE, ScriptValuesAddedFunctions.isEmpty(cx, scope, args(""), null));
    assertEquals(Boolean.FALSE, ScriptValuesAddedFunctions.isMailValid(cx, scope, args(""), null));
    assertEquals(
        Boolean.FALSE,
        ScriptValuesAddedFunctions.isMailValid(cx, scope, args("not-an-email"), null));
    assertEquals(
        Boolean.TRUE, ScriptValuesAddedFunctions.isMailValid(cx, scope, args("a@b.co"), null));
  }

  @Test
  void isCodepage_utf8_ascii() {
    Object ok = ScriptValuesAddedFunctions.isCodepage(cx, scope, args("abc", "UTF-8"), null);
    assertEquals(Boolean.TRUE, ok);
  }

  @Test
  void year_month_fromDate() {
    Date d = new Date(124, 5, 15);
    Object y = ScriptValuesAddedFunctions.year(cx, scope, args(Context.javaToJS(d, scope)), null);
    Object m = ScriptValuesAddedFunctions.month(cx, scope, args(Context.javaToJS(d, scope)), null);
    assertInstanceOf(Double.class, y);
    assertInstanceOf(Double.class, m);
    assertEquals(2024.0, (Double) y, 1e-9);
    // Calendar.MONTH is 0-based (June -> 5), matching ScriptValuesAddedFunctions.month().
    assertEquals(5.0, (Double) m, 1e-9);
  }

  @Test
  void getDayNumber_weekMonth() {
    Date d = new Date(124, 5, 15);
    Object js = Context.javaToJS(d, scope);
    assertNotNull(ScriptValuesAddedFunctions.getDayNumber(cx, scope, args(js, "m"), null));
    assertNotNull(ScriptValuesAddedFunctions.getDayNumber(cx, scope, args(js, "w"), null));
  }
}
