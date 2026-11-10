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
    assertEquals(
        "2024-03-02T04:05:06.007Z",
        SalesforceBulkValues.format(new ValueMetaDate("When"), date, false));
    assertEquals(
        date, SalesforceBulkValues.parse(new ValueMetaDate("When"), "2024-03-02T04:05:06.007Z"));
  }

  @Test
  void readsAnEmptyQueryFieldAsNull() throws Exception {
    assertNull(SalesforceBulkValues.parse(new ValueMetaString("Name"), ""));
  }
}
