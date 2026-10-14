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

package org.apache.hop.openformula.common;

import java.math.BigDecimal;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Date;
import java.util.GregorianCalendar;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.TimeZone;
import org.apache.hop.openformula.EvaluationException;
import org.apache.hop.openformula.FormulaContext;
import org.apache.hop.openformula.LocalizationContext;
import org.apache.hop.openformula.error.FormulaErrorValue;
import org.apache.hop.openformula.function.DefaultFunctionRegistry;
import org.apache.hop.openformula.function.FunctionRegistry;
import org.apache.hop.openformula.typing.DataType;
import org.apache.hop.openformula.typing.DefaultTypeRegistry;
import org.apache.hop.openformula.typing.StaticArrayCallback;
import org.apache.hop.openformula.typing.TypeRegistry;

/** Standard test dataset context for the OpenFormula engine. */
public class TestFormulaContext implements FormulaContext, LocalizationContext {
  private final Object[][] data;
  private final TypeRegistry typeRegistry;
  private final FunctionRegistry functionRegistry;
  private final Map<String, Object> extraReferences = new HashMap<>();

  public static Date createDate(int year, int month, int day) {
    Calendar cal = GregorianCalendar.getInstance(TimeZone.getTimeZone("GMT+01:00"), Locale.US);
    cal.clear();
    cal.set(year, month, day, 0, 0, 0);
    cal.set(Calendar.MILLISECOND, 0);
    return new java.sql.Date(cal.getTimeInMillis());
  }

  public static Date createDateTime(
      int year, int month, int day, int hour, int min, int sec, int milli) {
    Calendar cal = GregorianCalendar.getInstance(TimeZone.getTimeZone("GMT+01:00"), Locale.US);
    cal.clear();
    cal.set(year, month, day, hour, min, sec);
    cal.set(Calendar.MILLISECOND, milli);
    return cal.getTime();
  }

  public static Date createDate1(
      int year, int month, int day, int hour, int min, int sec, int milli) {
    return createDateTime(year, month, day, hour, min, sec, milli);
  }

  public static final Object[][] DEFAULT_TEST_DATA =
      new Object[][] {
        {null, null}, // 0
        {null, null}, // 1
        {null, null}, // 2
        {"7", null}, // 3
        {new BigDecimal(2), new BigDecimal(4)}, // 4
        {new BigDecimal(3), new BigDecimal(5)}, // 5
        {Boolean.TRUE, new BigDecimal(7)}, // 6
        {"Hello", createDate(2005, Calendar.JANUARY, 31)}, // 7
        {null, createDateTime(2006, Calendar.JANUARY, 31, 0, 0, 0, 0)}, // 8
        {FormulaErrorValue.ERROR_ARITHMETIC_VALUE, createDateTime(0, 0, 0, 2, 0, 0, 0)}, // 9
        {new BigDecimal(0), createDateTime(0, 0, 0, 23, 0, 0, 0)}, // 10
        {new BigDecimal(3), new BigDecimal(5)}, // 11
        {new BigDecimal(4), new BigDecimal(6)}, // 12
        {null, null}, // 13
        {new BigDecimal(1), new BigDecimal(4)}, // 14
        {new BigDecimal(2), new BigDecimal(3)}, // 15
        {new BigDecimal(3), new BigDecimal(2)}, // 16
        {new BigDecimal(4), new BigDecimal(1)}, // 17
        {
          new Object[] {new BigDecimal(1), new BigDecimal(2), new BigDecimal(3)},
          Arrays.asList(new BigDecimal(1), new BigDecimal(2), new BigDecimal(3))
        }, // 18
        {
          new Object[] {new Object[0]},
          Arrays.asList(List.of(), new BigDecimal(42), new BigDecimal(43))
        }, // 19
      };
  public static final Object[][] testCaseDataset = DEFAULT_TEST_DATA;

  public TestFormulaContext() {
    this(DEFAULT_TEST_DATA);
  }

  public TestFormulaContext(Object[][] data) {
    this.data = data;
    this.typeRegistry = new DefaultTypeRegistry(Locale.US, TimeZone.getTimeZone("GMT+01:00"));
    this.functionRegistry = DefaultFunctionRegistry.getInstance();
  }

  public void defineReference(String name, Object value) {
    extraReferences.put(name, value);
  }

  @Override
  public Object resolveReference(Object name) throws EvaluationException {
    if (name == null) return null;
    String ref = name.toString();
    if (extraReferences.containsKey(ref)) {
      return extraReferences.get(ref);
    }
    if (ref.startsWith(".")) {
      String[] split = ref.split(":");
      String colName = split[0].substring(1, 2);
      int col = colName.equalsIgnoreCase("B") ? 0 : (colName.equalsIgnoreCase("C") ? 1 : -1);
      int firstRow = Integer.parseInt(split[0].substring(2));
      if (split.length == 2) {
        int secondRow = Integer.parseInt(split[1].substring(2));
        int count = secondRow - firstRow + 1;
        Object[][] slice = new Object[count][1];
        for (int r = 0; r < count; r++) {
          slice[r][0] = data[firstRow + r][col];
        }
        return new StaticArrayCallback(slice);
      } else {
        return data[firstRow][col];
      }
    }
    throw new EvaluationException(FormulaErrorValue.ERROR_REFERENCE_NOT_RESOLVABLE_VALUE);
  }

  @Override
  public DataType resolveReferenceType(Object name) {
    try {
      Object val = resolveReference(name);
      return typeRegistry.guessType(val);
    } catch (EvaluationException e) {
      return DataType.ERROR;
    }
  }

  @Override
  public Locale getLocale() {
    return Locale.US;
  }

  @Override
  public TimeZone getTimeZone() {
    return TimeZone.getTimeZone("GMT+01:00");
  }

  @Override
  public LocalizationContext getLocalizationContext() {
    return this;
  }

  @Override
  public TypeRegistry getTypeRegistry() {
    return typeRegistry;
  }

  @Override
  public FunctionRegistry getFunctionRegistry() {
    return functionRegistry;
  }

  @Override
  public Date getCurrentDate() {
    Calendar cal = new GregorianCalendar(TimeZone.getTimeZone("GMT+01:00"), Locale.US);
    cal.clear();
    cal.set(2011, Calendar.APRIL, 7, 15, 0, 0);
    return cal.getTime();
  }
}
