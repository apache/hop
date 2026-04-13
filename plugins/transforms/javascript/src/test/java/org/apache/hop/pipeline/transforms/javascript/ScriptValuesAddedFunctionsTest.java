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

package org.apache.hop.pipeline.transforms.javascript;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.Calendar;
import java.util.Date;
import org.apache.commons.lang3.time.DateFormatUtils;
import org.junit.jupiter.api.Test;
import org.mozilla.javascript.EvaluatorException;

class ScriptValuesAddedFunctionsTest {
  private static final String FORMATTER = "yyyy-MM-dd HH:mm:ss";

  @Test
  void testTruncDate() {
    LocalDateTime time = LocalDateTime.of(2025, 2, 15, 11, 11, 11, 123);
    Date dateBase = Date.from(time.atZone(ZoneId.systemDefault()).toInstant());

    Date rtn;
    Calendar c2 = Calendar.getInstance();
    rtn = ScriptValuesAddedFunctions.truncDate(dateBase, 5);
    c2.setTime(rtn);
    assertEquals(Calendar.JANUARY, c2.get(Calendar.MONTH));
    assertEquals("2025-01-01 00:00:00", DateFormatUtils.format(rtn, FORMATTER));

    rtn = ScriptValuesAddedFunctions.truncDate(dateBase, 4);
    c2.setTime(rtn);
    assertEquals(1, c2.get(Calendar.DAY_OF_MONTH));
    assertEquals("2025-02-01 00:00:00", DateFormatUtils.format(rtn, FORMATTER));

    rtn = ScriptValuesAddedFunctions.truncDate(dateBase, 3);
    c2.setTime(rtn);
    assertEquals(0, c2.get(Calendar.HOUR_OF_DAY));
    assertEquals("2025-02-15 00:00:00", DateFormatUtils.format(rtn, FORMATTER));

    rtn = ScriptValuesAddedFunctions.truncDate(dateBase, 2);
    c2.setTime(rtn);
    assertEquals(0, c2.get(Calendar.MINUTE));
    assertEquals("2025-02-15 11:00:00", DateFormatUtils.format(rtn, FORMATTER));

    rtn = ScriptValuesAddedFunctions.truncDate(dateBase, 1);
    c2.setTime(rtn);
    assertEquals(0, c2.get(Calendar.SECOND));
    assertEquals("2025-02-15 11:11:00", DateFormatUtils.format(rtn, FORMATTER));

    rtn = ScriptValuesAddedFunctions.truncDate(dateBase, 0);
    c2.setTime(rtn);
    assertEquals(0, c2.get(Calendar.MILLISECOND));
    assertEquals("2025-02-15 11:11:11", DateFormatUtils.format(rtn, FORMATTER));

    final Date truncatedToMs = rtn;
    assertThrows(
        EvaluatorException.class, () -> ScriptValuesAddedFunctions.truncDate(truncatedToMs, 6));
    assertThrows(
        EvaluatorException.class, () -> ScriptValuesAddedFunctions.truncDate(truncatedToMs, -7));
  }
}
