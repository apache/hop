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

package org.apache.hop.openformula;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;

import java.math.BigDecimal;
import java.util.Calendar;
import java.util.Date;
import java.util.Locale;
import java.util.TimeZone;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.core.row.value.ValueMetaFactory;
import org.apache.hop.core.row.value.ValueMetaInteger;
import org.apache.hop.openformula.spec.Spec;
import org.apache.hop.openformula.util.DateUtil;
import org.junit.jupiter.api.Test;

/** Day arithmetic stays on the civil clock across a daylight-saving transition. */
class DateArithmeticTest {
  private static final TimeZone BRUSSELS = TimeZone.getTimeZone("Europe/Brussels");

  @Test
  void addingOneDayCrossesTheAutumnFallback() throws Exception {
    DefaultFormulaContext context = new DefaultFormulaContext(Locale.US, BRUSSELS);
    context.defineReference("d", DateUtil.createDate(2026, 10, 25, BRUSSELS));

    Object actual = Spec.value("[d]+1", context);

    assertInstanceOf(Date.class, actual);
    Calendar calendar = Calendar.getInstance(BRUSSELS);
    calendar.setTime((Date) actual);
    assertEquals(2026, calendar.get(Calendar.YEAR));
    assertEquals(Calendar.OCTOBER, calendar.get(Calendar.MONTH));
    assertEquals(26, calendar.get(Calendar.DAY_OF_MONTH));
    assertEquals(0, calendar.get(Calendar.HOUR_OF_DAY));
    assertEquals(0, calendar.get(Calendar.MINUTE));
  }

  @Test
  void subtractingAcrossTheSpringForwardIsOneIntegerDay() throws Exception {
    DefaultFormulaContext context = new DefaultFormulaContext(Locale.US, BRUSSELS);
    context.defineReference("start", DateUtil.createDate(2026, 3, 29, BRUSSELS));
    context.defineReference("end", DateUtil.createDate(2026, 3, 30, BRUSSELS));

    Object days = Spec.value("[end]-[start]", context);

    assertInstanceOf(BigDecimal.class, days);
    assertEquals(0, BigDecimal.ONE.compareTo((BigDecimal) days));
    IValueMeta target = new ValueMetaInteger("days");
    Object stored = target.convertData(ValueMetaFactory.guessValueMetaInterface(days), days);
    assertEquals(1L, stored);
  }

  @Test
  void daysKeepsThePdiOrderAcrossTheSpringForward() throws Exception {
    DefaultFormulaContext context = new DefaultFormulaContext(Locale.US, BRUSSELS);

    Object days = Spec.value("DAYS(DATE(2026;3;29);DATE(2026;3;30))", context);

    assertInstanceOf(Number.class, days);
    assertEquals(0, BigDecimal.ONE.compareTo(new BigDecimal(days.toString())));
  }
}
