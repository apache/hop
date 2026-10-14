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

import java.util.Calendar;
import java.util.Date;
import org.junit.jupiter.api.Test;

/** Part 2 section 6.10. Dates are mid-month so daylight-saving boundaries stay out of the cases. */
class DateTimeSpecTest {
  @Test
  void dateAndTimeFunctions() throws Exception {
    // 6.10.2 DATE builds a date from year, month, and day.
    Spec.date("DATE(2020;1;15)", 2020, 1, 15);
    // 6.10.4 DATEVALUE accepts ISO dates.
    Spec.date("DATEVALUE(\"2020-01-15\")", 2020, 1, 15);
    // 6.10.5 DAY, 6.10.13 MONTH, 6.10.23 YEAR extract those parts.
    Spec.number("DAY(DATE(2020;1;15))", "15");
    Spec.number("MONTH(DATE(2020;1;15))", "1");
    Spec.number("YEAR(DATE(2020;1;15))", "2020");
    // 6.10.6 DAYS is the second date minus the first.
    Spec.number("DAYS(DATE(2020;1;15);DATE(2020;1;10))", "5");
    // 6.10.3 DATEDIF format "d" counts days, "y" counts years.
    Spec.number("DATEDIF(DATE(2020;1;10);DATE(2020;1;15);\"d\")", "5");
    Spec.number("DATEDIF(DATE(2019;1;15);DATE(2021;1;15);\"y\")", "2");
    // 6.10.17 TIME returns a time serial: hours, minutes, and seconds as a fraction of a day.
    Spec.approx("TIME(13;2;3)", 13.0 / 24 + 2.0 / 1440 + 3.0 / 86400);
    // 6.10.10 HOUR, 6.10.12 MINUTE, 6.10.16 SECOND read that serial back.
    Spec.number("HOUR(TIME(13;2;3))", "13");
    Spec.number("MINUTE(TIME(13;2;3))", "2");
    Spec.number("SECOND(TIME(13;2;3))", "3");
    // 6.10.18 TIMEVALUE accepts ISO times.
    Spec.number("HOUR(TIMEVALUE(\"13:02:03\"))", "13");
    // 6.10.20 WEEKDAY type 1 numbers Sunday as 1. 2020-01-05 was a Sunday.
    Spec.number("WEEKDAY(DATE(2020;1;5);1)", "1");
    // 6.10.15 NOW and 6.10.19 TODAY return the current date.
    Spec.isDate("NOW()");
    Object today = Spec.value("TODAY()");
    assertInstanceOf(Date.class, today);
    Calendar now = Calendar.getInstance();
    Calendar got = Calendar.getInstance();
    got.setTime((Date) today);
    assertEquals(now.get(Calendar.YEAR), got.get(Calendar.YEAR));
    assertEquals(now.get(Calendar.DAY_OF_YEAR), got.get(Calendar.DAY_OF_YEAR));
  }
}
