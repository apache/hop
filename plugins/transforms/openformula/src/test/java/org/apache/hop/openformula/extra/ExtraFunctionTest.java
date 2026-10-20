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

package org.apache.hop.openformula.extra;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.math.BigDecimal;
import java.util.Calendar;
import java.util.Date;
import java.util.Set;
import org.apache.hop.openformula.spec.Spec;
import org.apache.hop.openformula.typing.ArrayCallback;
import org.junit.jupiter.api.Test;

/**
 * Functions the engine registers that are not in OpenDocument v1.2 Part 2. Each case follows that
 * function's description in the engine.
 */
public class ExtraFunctionTest {
  public static final Set<String> EXTRAS =
      Set.of(
          "ARRAYCONCATENATE",
          "ARRAYCONTAINS",
          "ARRAYLEFT",
          "ARRAYMID",
          "ARRAYRIGHT",
          "BEGINSWITH",
          "CONTAINS",
          "CSVARRAY",
          "CSVTEXT",
          "DATETIMEVALUE",
          "ENDSWITH",
          "EQUALS",
          "ERROR",
          "HASCHANGED",
          "IN",
          "LIKE",
          "MESSAGE",
          "MONTHEND",
          "NORMALIZEARRAY",
          "NULL",
          "PARSEDATE",
          "PREVWEEKDAY",
          "SEQUENCEQUOTER",
          "STRINGCOUNT",
          "SUMA",
          "URLBUILDER",
          "URLENCODE",
          "YESTERDAY");

  @Test
  void stringPredicates() throws Exception {
    Spec.logical("BEGINSWITH(\"Hello\";\"He\")", true);
    Spec.logical("CONTAINS(\"Hello\";\"ell\")", true);
    Spec.logical("ENDSWITH(\"Hello\";\"lo\")", true);
    Spec.logical("EQUALS(\"a\";\"a\")", true);
    Spec.logical("EQUALS(\"a\";\"b\")", false);
    Spec.logical("IN(2;1;2;3)", true);
    Spec.logical("IN(9;1;2;3)", false);
    Spec.logical("LIKE(\"Hello\";\"H%\")", true);
  }

  @Test
  void textHelpers() throws Exception {
    Spec.text("MESSAGE(\"n={0}\";2)", "n=2");
    Spec.number("STRINGCOUNT(\"abab\";\"ab\")", "2");
    Spec.textContains("URLENCODE(\"a b\")", "a");
    Object encoded = Spec.value("URLENCODE(\"a b\")");
    assertNotEquals("a b", encoded);
    Spec.textContains("URLBUILDER(\"https://example.test\";\"q\";\"ab\")", "example.test");
    Spec.textContains("URLBUILDER(\"https://example.test\";\"q\";\"ab\")", "q");
  }

  @Test
  void arraysAndNull() throws Exception {
    Spec.logical("ARRAYCONTAINS({1;2;3};2)", true);
    assertNumber(Spec.cell("ARRAYLEFT({1;2;3};2)", 0, 0), "1");
    assertNumber(Spec.cell("ARRAYLEFT({1;2;3};2)", 0, 1), "2");
    assertNumber(Spec.cell("ARRAYRIGHT({1;2;3};1)", 0, 0), "3");
    assertNumber(Spec.cell("ARRAYMID({1;2;3};2;1)", 0, 0), "2");
    assertNumber(Spec.cell("ARRAYCONCATENATE({1};{2})", 0, 0), "1");
    assertNumber(Spec.cell("ARRAYCONCATENATE({1};{2})", 0, 1), "2");
    Object flat = Spec.value("NORMALIZEARRAY({1;2|3;4})");
    assertInstanceOf(ArrayCallback.class, flat);
    ArrayCallback array = (ArrayCallback) flat;
    assertEquals(4, array.getRowCount() * array.getColumnCount());
    assertEquals("a", Spec.cell("CSVARRAY(\"a,b\")", 0, 0));
    Spec.textContains("CSVTEXT({1;2})", "1");
    Object quoted = Spec.cell("SEQUENCEQUOTER({1})", 0, 0);
    assertTrue(String.valueOf(quoted).contains("1"));
    // NULL is the null literal, so it is written without parentheses.
    Spec.isNull("NULL");
  }

  @Test
  void datesErrorsAndSums() throws Exception {
    Spec.date("PARSEDATE(\"2020-01-15\";\"yyyy-MM-dd\")", 2020, 1, 15);
    // The description returns a date/time serial, which is a number.
    Object earlier = Spec.value("DATETIMEVALUE(\"2020-01-15 03:04:05\")");
    Object later = Spec.value("DATETIMEVALUE(\"2020-01-16 03:04:05\")");
    assertInstanceOf(Number.class, earlier);
    assertInstanceOf(Number.class, later);
    assertTrue(((Number) earlier).doubleValue() < ((Number) later).doubleValue());
    Spec.date("MONTHEND(DATE(2020;1;15))", 2020, 1, 31);
    Spec.number("SUMA(1;2;3)", "6");
    Spec.error("ERROR(522)", 522);
    // A literal has not changed.
    Spec.logical("HASCHANGED(1)", false);

    Object yesterday = Spec.value("YESTERDAY()");
    assertInstanceOf(Date.class, yesterday);
    Calendar expected = Calendar.getInstance();
    expected.add(Calendar.DAY_OF_MONTH, -1);
    Calendar actual = Calendar.getInstance();
    actual.setTime((Date) yesterday);
    assertEquals(expected.get(Calendar.YEAR), actual.get(Calendar.YEAR));
    assertEquals(expected.get(Calendar.DAY_OF_YEAR), actual.get(Calendar.DAY_OF_YEAR));

    Object previous = Spec.value("PREVWEEKDAY(DATE(2020;1;6))");
    assertInstanceOf(Date.class, previous);
    Calendar day = Calendar.getInstance();
    day.setTime((Date) previous);
    int dow = day.get(Calendar.DAY_OF_WEEK);
    assertTrue(dow != Calendar.SATURDAY && dow != Calendar.SUNDAY);
    assertTrue(day.get(Calendar.DAY_OF_MONTH) < 6);
  }

  private static void assertNumber(Object actual, String expected) {
    assertEquals(0, new BigDecimal(expected).compareTo(new BigDecimal(actual.toString())));
  }
}
