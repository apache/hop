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

package org.apache.hop.openformula.typing;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import java.util.Calendar;
import java.util.Date;
import java.util.GregorianCalendar;
import org.apache.hop.openformula.EvaluationException;
import org.apache.hop.openformula.FormulaContext;
import org.apache.hop.openformula.common.TestFormulaContext;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class TypeRegisteryTest {
  private FormulaContext context;

  @BeforeEach
  public void setUp() throws Exception {
    context = new TestFormulaContext(TestFormulaContext.testCaseDataset);
  }

  @Test
  public void testZeroDateConvertion() throws EvaluationException {
    final Calendar cal =
        new GregorianCalendar(
            context.getLocalizationContext().getTimeZone(),
            context.getLocalizationContext().getLocale());
    cal.setTimeInMillis(0);

    final Date d = cal.getTime();

    final TypeRegistry typeRegistry = context.getTypeRegistry();
    final Number n = typeRegistry.convertToNumber(DataType.DATETIME, d);
    assertNotNull(n, "The date has not been converted to a number");

    final Date d1 = typeRegistry.convertToDate(DataType.DATETIME, n);
    assertNotNull(d1, "The number has not been converted to a date");
    assertEquals(d.getTime(), d1.getTime(), "dates are different: " + d1 + " vs. " + d);
  }

  @Test
  public void testNowDateConvertion() throws Exception {
    final Calendar cal =
        new GregorianCalendar(
            context.getLocalizationContext().getTimeZone(),
            context.getLocalizationContext().getLocale());

    final Date d = cal.getTime();
    final Number n = context.getTypeRegistry().convertToNumber(DataType.DATETIME, d);
    assertNotNull(n, "The date has not been converted to a number");
    final Date d1 = context.getTypeRegistry().convertToDate(DataType.NUMBER, n);
    assertNotNull(d1, "The number has not been converted to a date");

    assertEquals(d.getTime(), d1.getTime(), "dates are different");
  }

  @Test
  public void testStringDateConversion() throws EvaluationException {
    final Date d = TestFormulaContext.createDate1(2004, GregorianCalendar.JANUARY, 1, 0, 0, 0, 0);
    final TypeRegistry typeRegistry = context.getTypeRegistry();
    final Number n = typeRegistry.convertToNumber(DataType.DATETIME, d);
    final Date d1 = typeRegistry.convertToDate(DataType.TEXT, "2004-01-01");

    assertEquals(d.getTime(), d1.getTime(), "dates are different");
  }

  @Test
  public void testStringNumberConversion() throws EvaluationException {
    final Number d = Double.valueOf(2000.5);
    final TypeRegistry typeRegistry = context.getTypeRegistry();
    final Number n = typeRegistry.convertToNumber(DataType.NUMBER, d);
    final Number d1 = typeRegistry.convertToNumber(DataType.TEXT, "2000.5");

    assertEquals(d.doubleValue(), d1.doubleValue(), 0.0, "numbers are different");
  }
}
