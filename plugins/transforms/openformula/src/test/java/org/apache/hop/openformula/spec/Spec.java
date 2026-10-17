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
 */

package org.apache.hop.openformula.spec;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.math.BigDecimal;
import java.util.Calendar;
import java.util.Date;
import org.apache.hop.openformula.DefaultFormulaContext;
import org.apache.hop.openformula.Formula;
import org.apache.hop.openformula.FormulaContext;
import org.apache.hop.openformula.error.FormulaErrorValue;
import org.apache.hop.openformula.typing.ArrayCallback;

/** Evaluates a formula with literals. Callers cite the OpenFormula section they follow. */
public final class Spec {
  private Spec() {}

  public static Object value(String formula) throws Exception {
    return value(formula, new DefaultFormulaContext());
  }

  public static Object value(String formula, FormulaContext context) throws Exception {
    Formula compiled = new Formula(formula);
    compiled.initialize(context);
    return compiled.evaluate();
  }

  public static void number(String formula, String expected) throws Exception {
    Object actual = value(formula);
    assertInstanceOf(Number.class, actual, formula);
    assertEquals(
        0,
        new BigDecimal(expected).compareTo(new BigDecimal(actual.toString())),
        formula + " -> " + actual);
  }

  public static void approx(String formula, double expected) throws Exception {
    Object actual = value(formula);
    assertInstanceOf(Number.class, actual, formula);
    assertEquals(expected, ((Number) actual).doubleValue(), 1e-9, formula);
  }

  public static void text(String formula, String expected) throws Exception {
    assertEquals(expected, value(formula), formula);
  }

  public static void logical(String formula, boolean expected) throws Exception {
    assertEquals(expected, value(formula), formula);
  }

  public static void error(String formula, int errorCode) throws Exception {
    Object actual = value(formula);
    assertInstanceOf(FormulaErrorValue.class, actual, formula);
    assertEquals(errorCode, ((FormulaErrorValue) actual).getErrorCode(), formula);
  }

  public static void date(String formula, int year, int month, int day) throws Exception {
    Object actual = value(formula);
    assertInstanceOf(Date.class, actual, formula);
    Calendar calendar = Calendar.getInstance();
    calendar.setTime((Date) actual);
    assertEquals(year, calendar.get(Calendar.YEAR), formula);
    assertEquals(month, calendar.get(Calendar.MONTH) + 1, formula);
    assertEquals(day, calendar.get(Calendar.DAY_OF_MONTH), formula);
  }

  public static Object cell(String formula, int row, int column) throws Exception {
    Object actual = value(formula);
    assertInstanceOf(ArrayCallback.class, actual, formula);
    return ((ArrayCallback) actual).getValue(row, column);
  }

  public static void isDate(String formula) throws Exception {
    assertInstanceOf(Date.class, value(formula), formula);
  }

  public static void isNull(String formula) throws Exception {
    assertNull(value(formula), formula);
  }

  public static void textContains(String formula, String fragment) throws Exception {
    Object actual = value(formula);
    assertInstanceOf(String.class, actual, formula);
    assertTrue(((String) actual).contains(fragment), formula + " -> " + actual);
  }
}
