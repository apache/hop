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

package org.apache.hop.pipeline.transforms.salesforcebulk;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

import java.util.Calendar;
import java.util.Date;
import java.util.TimeZone;
import org.apache.hop.core.row.value.ValueMetaDate;
import org.apache.hop.core.row.value.ValueMetaInteger;
import org.apache.hop.core.row.value.ValueMetaNumber;
import org.apache.hop.core.row.value.ValueMetaString;
import org.junit.jupiter.api.Test;

class SalesforceBulkValuesTest {

  @Test
  void writesNullAsEmptyOrNa() throws Exception {
    ValueMetaString meta = new ValueMetaString("Name");
    assertEquals("", SalesforceBulkValues.format(meta, null, false));
    assertEquals("#N/A", SalesforceBulkValues.format(meta, null, true));
    assertEquals("Ada", SalesforceBulkValues.format(meta, "Ada", true));
  }

  @Test
  void writesNumbersWithoutGrouping() throws Exception {
    ValueMetaNumber meta = new ValueMetaNumber("Amount");
    assertEquals("1234.5", SalesforceBulkValues.format(meta, 1234.5d, false));
    ValueMetaInteger integer = new ValueMetaInteger("Count");
    assertEquals("42", SalesforceBulkValues.format(integer, 42L, false));
  }

  @Test
  void writesDatesInUtc() throws Exception {
    Calendar calendar = Calendar.getInstance(TimeZone.getTimeZone("UTC"));
    calendar.clear();
    calendar.set(2024, Calendar.MARCH, 2, 4, 5, 6);
    calendar.set(Calendar.MILLISECOND, 7);
    Date date = calendar.getTime();
    ValueMetaDate when = new ValueMetaDate("When");
    when.setDateFormatTimeZone(TimeZone.getTimeZone("UTC"));
    assertEquals("2024-03-02T04:05:06.007Z", SalesforceBulkValues.format(when, date, false));
    assertEquals(
        date, SalesforceBulkValues.parse(new ValueMetaDate("When"), "2024-03-02T04:05:06.007Z"));
  }

  @Test
  void keepsLoadedCalendarDateIndependentOfJvmTimeZone() throws Exception {
    TimeZone original = TimeZone.getDefault();
    try {
      for (String zoneId : new String[] {"Europe/Brussels", "America/New_York"}) {
        TimeZone.setDefault(TimeZone.getTimeZone(zoneId));
        Calendar calendar = Calendar.getInstance(TimeZone.getDefault());
        calendar.clear();
        calendar.set(2026, Calendar.MARCH, 15, 0, 0, 0);
        calendar.set(Calendar.MILLISECOND, 0);
        Date date = calendar.getTime();
        ValueMetaDate meta = new ValueMetaDate("Closed");
        String formatted = SalesforceBulkValues.format(meta, date, false);
        assertEquals("2026-03-15T00:00:00.000Z", formatted, zoneId);
        assertEquals(date, SalesforceBulkValues.parse(meta, "2026-03-15"), zoneId);
        assertEquals(
            formatted,
            SalesforceBulkValues.format(
                meta, SalesforceBulkValues.parse(meta, "2026-03-15"), false),
            zoneId);
      }
    } finally {
      TimeZone.setDefault(original);
    }
  }

  @Test
  void parsesDateOnlyValuesInTheDefaultZone() throws Exception {
    TimeZone original = TimeZone.getDefault();
    try {
      for (String zoneId : new String[] {"Europe/Brussels", "America/New_York"}) {
        TimeZone.setDefault(TimeZone.getTimeZone(zoneId));
        ValueMetaDate meta = new ValueMetaDate("Closed");
        Date parsed = (Date) SalesforceBulkValues.parse(meta, "2026-03-15");
        Calendar calendar = Calendar.getInstance(TimeZone.getDefault());
        calendar.setTime(parsed);
        assertEquals(2026, calendar.get(Calendar.YEAR), zoneId);
        assertEquals(Calendar.MARCH, calendar.get(Calendar.MONTH), zoneId);
        assertEquals(15, calendar.get(Calendar.DAY_OF_MONTH), zoneId);
        assertEquals(0, calendar.get(Calendar.HOUR_OF_DAY), zoneId);
        assertEquals(
            "2026-03-15T00:00:00.000Z", SalesforceBulkValues.format(meta, parsed, false), zoneId);

        Calendar utc = Calendar.getInstance(TimeZone.getTimeZone("UTC"));
        utc.clear();
        utc.set(2026, Calendar.MARCH, 15, 12, 0, 0);
        utc.set(Calendar.MILLISECOND, 0);
        assertEquals(
            utc.getTime(), SalesforceBulkValues.parse(meta, "2026-03-15T12:00:00.000Z"), zoneId);
      }
    } finally {
      TimeZone.setDefault(original);
    }
  }

  @Test
  void readsAnEmptyQueryFieldAsNull() throws Exception {
    assertNull(SalesforceBulkValues.parse(new ValueMetaString("Name"), ""));
  }
}
