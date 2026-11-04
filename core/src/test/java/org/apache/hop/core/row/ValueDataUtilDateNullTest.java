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
import static org.junit.jupiter.api.Assertions.assertNull;

import java.util.Calendar;
import java.util.Date;
import org.apache.hop.core.row.value.ValueMetaDate;
import org.apache.hop.core.row.value.ValueMetaInteger;
import org.junit.jupiter.api.Test;

/**
 * The Calculator's "Date A + B" calculations return null for a null date or amount, like the other
 * calculations. Issue #9259.
 */
class ValueDataUtilDateNullTest {

  private final IValueMeta date = new ValueMetaDate("a");
  private final IValueMeta amount = new ValueMetaInteger("b");
  private final Date someDate = new Date(1_700_000_000_000L);

  @Test
  void nullDateGivesNull() throws Exception {
    assertNull(ValueDataUtil.addDays(date, null, amount, 1L));
    assertNull(ValueDataUtil.addHours(date, null, amount, 1L));
    assertNull(ValueDataUtil.addMinutes(date, null, amount, 1L));
    assertNull(ValueDataUtil.addSeconds(date, null, amount, 1L));
    assertNull(ValueDataUtil.addMonths(date, null, amount, 1L));
  }

  @Test
  void nullAmountGivesNull() throws Exception {
    assertNull(ValueDataUtil.addDays(date, someDate, amount, null));
    assertNull(ValueDataUtil.addHours(date, someDate, amount, null));
    assertNull(ValueDataUtil.addMinutes(date, someDate, amount, null));
    assertNull(ValueDataUtil.addSeconds(date, someDate, amount, null));
    assertNull(ValueDataUtil.addMonths(date, someDate, amount, null));
  }

  @Test
  void valuesStillAdd() throws Exception {
    Calendar expected = Calendar.getInstance();
    expected.setTime(someDate);
    expected.add(Calendar.DAY_OF_YEAR, 2);

    assertEquals(expected.getTime(), ValueDataUtil.addDays(date, someDate, amount, 2L));
  }
}
