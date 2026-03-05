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

package org.apache.hop.core.row;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.Calendar;
import java.util.TimeZone;
import org.apache.hop.core.exception.HopValueException;
import org.apache.hop.core.row.value.ValueMetaDate;
import org.apache.hop.junit.rules.RestoreHopEnvironmentExtension;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

@ExtendWith(RestoreHopEnvironmentExtension.class)
class ValueDateUtilTest {
  private TimeZone defTimeZone;
  private TimeZone defUserTimezone;

  @BeforeEach
  void setUp() {
    defUserTimezone = TimeZone.getTimeZone(System.getProperty("user.timezone"));
    defTimeZone = java.util.TimeZone.getDefault();
    System.setProperty("user.timezone", "UTC");
    TimeZone.setDefault(null);
  }

  @AfterEach
  void tearDown() {
    System.setProperty("user.timezone", defUserTimezone.getID());
    TimeZone.setDefault(defTimeZone);
  }

  @Test
  void shouldCalculateHourOfDayUsingValueMetasTimeZoneByDefault() throws HopValueException {
    Calendar date = Calendar.getInstance();
    date.setTimeInMillis(1454313600000L); // 2016-07-01 08:00:00 UTC
    IValueMeta valueMetaDate = new ValueMetaDate();
    valueMetaDate.setDateFormatTimeZone(TimeZone.getTimeZone("CET")); // UTC +1
    long offsetCET = (long) TimeZone.getTimeZone("CET").getRawOffset() / 3600000;

    Object hourOfDayCET = ValueDataUtil.hourOfDay(valueMetaDate, date.getTime());

    assertEquals(8L + offsetCET, hourOfDayCET);
  }

  @Test
  void shouldCalculateDateWorkingDiff_JAN() throws HopValueException {
    IValueMeta metaA = new ValueMetaDate();
    IValueMeta metaB = new ValueMetaDate();
    Calendar startDate = Calendar.getInstance();
    Calendar endDate = Calendar.getInstance();
    startDate.setTimeInMillis(1230768000000L); // 2009-01-01 00:00:00
    endDate.setTimeInMillis(1233360000000L); // 2009-01-31 00:00:00
    Object workingDayOfJAN =
        ValueDataUtil.dateWorkingDiff(metaA, endDate.getTime(), metaB, startDate.getTime());
    assertEquals(22L, workingDayOfJAN, "Working days count in JAN ");
  }

  @Test
  void shouldCalculateDateWorkingDiff_FEB() throws HopValueException {
    IValueMeta metaA = new ValueMetaDate();
    IValueMeta metaB = new ValueMetaDate();
    Calendar startDate = Calendar.getInstance();
    Calendar endDate = Calendar.getInstance();
    startDate.setTimeInMillis(1233446400000L); // 2009-02-01 00:00:00
    endDate.setTimeInMillis(1235779200000L); // 2009-02-28 00:00:00
    Object workingDayOfFEB =
        ValueDataUtil.dateWorkingDiff(metaA, endDate.getTime(), metaB, startDate.getTime());
    assertEquals(20L, workingDayOfFEB, "Working days count in FEB ");
  }

  @Test
  void shouldCalculateDateWorkingDiff_MAR() throws HopValueException {
    IValueMeta metaA = new ValueMetaDate();
    IValueMeta metaB = new ValueMetaDate();
    Calendar startDate = Calendar.getInstance();
    Calendar endDate = Calendar.getInstance();
    startDate.setTimeInMillis(1235865600000L); // 2009-03-01 00:00:00
    endDate.setTimeInMillis(1238457600000L); // 2009-03-31 00:00:00
    Object workingDayOfMAR =
        ValueDataUtil.dateWorkingDiff(metaA, endDate.getTime(), metaB, startDate.getTime());
    assertEquals(22L, workingDayOfMAR, "Working days count in MAR ");
  }

  @Test
  void shouldCalculateDateWorkingDiff_APR() throws HopValueException {
    IValueMeta metaA = new ValueMetaDate();
    IValueMeta metaB = new ValueMetaDate();
    Calendar startDate = Calendar.getInstance();
    Calendar endDate = Calendar.getInstance();
    startDate.setTimeInMillis(1238544000000L); // 2009-04-01 00:00:00
    endDate.setTimeInMillis(1241049600000L); // 2009-04-30 00:00:00
    Object workingDayOfAPR =
        ValueDataUtil.dateWorkingDiff(metaA, endDate.getTime(), metaB, startDate.getTime());
    assertEquals(22L, workingDayOfAPR, "Working days count in APR ");
  }

  @Test
  void shouldCalculateDateWorkingDiff_MAY() throws HopValueException {
    IValueMeta metaA = new ValueMetaDate();
    IValueMeta metaB = new ValueMetaDate();
    Calendar startDate = Calendar.getInstance();
    Calendar endDate = Calendar.getInstance();
    startDate.setTimeInMillis(1241136000000L); // 2009-05-01 00:00:00
    endDate.setTimeInMillis(1243728000000L); // 2009-05-31 00:00:00
    Object workingDayOfMAY =
        ValueDataUtil.dateWorkingDiff(metaA, endDate.getTime(), metaB, startDate.getTime());
    assertEquals(21L, workingDayOfMAY, "Working days count in MAY ");
  }

  @Test
  void shouldCalculateDateWorkingDiff_JUN() throws HopValueException {
    IValueMeta metaA = new ValueMetaDate();
    IValueMeta metaB = new ValueMetaDate();
    Calendar startDate = Calendar.getInstance();
    Calendar endDate = Calendar.getInstance();
    startDate.setTimeInMillis(1243814400000L); // 2009-06-01 00:00:00
    endDate.setTimeInMillis(1246320000000L); // 2009-06-30 00:00:00
    Object workingDayOfJUN =
        ValueDataUtil.dateWorkingDiff(metaA, endDate.getTime(), metaB, startDate.getTime());
    assertEquals(22L, workingDayOfJUN, "Working days count in JUN ");
  }

  @Test
  void shouldCalculateDateWorkingDiff_JUL() throws HopValueException {
    IValueMeta metaA = new ValueMetaDate();
    IValueMeta metaB = new ValueMetaDate();
    Calendar startDate = Calendar.getInstance();
    Calendar endDate = Calendar.getInstance();
    startDate.setTimeInMillis(1246406400000L); // 2009-07-01 00:00:00
    endDate.setTimeInMillis(1248998400000L); // 2009-07-31 00:00:00
    Object workingDayOfJUL =
        ValueDataUtil.dateWorkingDiff(metaA, endDate.getTime(), metaB, startDate.getTime());
    assertEquals(23L, workingDayOfJUL, "Working days count in JUL ");
  }

  @Test
  void shouldCalculateDateWorkingDiff_AUG() throws HopValueException {
    IValueMeta metaA = new ValueMetaDate();
    IValueMeta metaB = new ValueMetaDate();
    Calendar startDate = Calendar.getInstance();
    Calendar endDate = Calendar.getInstance();
    startDate.setTimeInMillis(1249084800000L); // 2009-08-01 00:00:00
    endDate.setTimeInMillis(1251676800000L); // 2009-08-31 00:00:00
    Object workingDayOfAUG =
        ValueDataUtil.dateWorkingDiff(metaA, endDate.getTime(), metaB, startDate.getTime());
    assertEquals(21L, workingDayOfAUG, "Working days count in AUG ");
  }

  @Test
  void shouldCalculateDateWorkingDiff_SEP() throws HopValueException {
    IValueMeta metaA = new ValueMetaDate();
    IValueMeta metaB = new ValueMetaDate();
    Calendar startDate = Calendar.getInstance();
    Calendar endDate = Calendar.getInstance();
    startDate.setTimeInMillis(1251763200000L); // 2009-09-01 00:00:00
    endDate.setTimeInMillis(1254268800000L); // 2009-09-30 00:00:00
    Object workingDayOfSEP =
        ValueDataUtil.dateWorkingDiff(metaA, endDate.getTime(), metaB, startDate.getTime());
    assertEquals(22L, workingDayOfSEP, "Working days count in SEP ");
  }

  @Test
  void shouldCalculateDateWorkingDiff_OCT() throws HopValueException {
    IValueMeta metaA = new ValueMetaDate();
    IValueMeta metaB = new ValueMetaDate();
    Calendar startDate = Calendar.getInstance();
    Calendar endDate = Calendar.getInstance();
    startDate.setTimeInMillis(1254355200000L); // 2009-10-01 00:00:00
    endDate.setTimeInMillis(1256947200000L); // 2009-10-31 00:00:00
    Object workingDayOfOCT =
        ValueDataUtil.dateWorkingDiff(metaA, endDate.getTime(), metaB, startDate.getTime());
    assertEquals(22L, workingDayOfOCT, "Working days count in OCT ");
  }

  @Test
  void shouldCalculateDateWorkingDiff_NOV() throws HopValueException {
    IValueMeta metaA = new ValueMetaDate();
    IValueMeta metaB = new ValueMetaDate();
    Calendar startDate = Calendar.getInstance();
    Calendar endDate = Calendar.getInstance();
    startDate.setTimeInMillis(1257033600000L); // 2009-11-01 00:00:00
    endDate.setTimeInMillis(1259539200000L); // 2009-11-30 00:00:00
    Object workingDayOfNOV =
        ValueDataUtil.dateWorkingDiff(metaA, endDate.getTime(), metaB, startDate.getTime());
    assertEquals(21L, workingDayOfNOV, "Working days count in NOV ");
  }

  @Test
  void shouldCalculateDateWorkingDiff_DEC() throws HopValueException {
    IValueMeta metaA = new ValueMetaDate();
    IValueMeta metaB = new ValueMetaDate();
    Calendar startDate = Calendar.getInstance();
    Calendar endDate = Calendar.getInstance();
    startDate.setTimeInMillis(1259625600000L); // 2009-12-01 00:00:00
    endDate.setTimeInMillis(1262217600000L); // 2009-12-31 00:00:00
    Object workingDayOfDEC =
        ValueDataUtil.dateWorkingDiff(metaA, endDate.getTime(), metaB, startDate.getTime());
    assertEquals(23L, workingDayOfDEC, "Working days count in DEC ");
  }
}
