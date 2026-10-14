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

package org.apache.hop.openformula.function.datetime;

import java.util.Date;
import java.util.TimeZone;
import org.apache.hop.openformula.common.FormulaTestBase;
import org.junit.jupiter.api.Test;

public class TimeValueFunctionTest extends FormulaTestBase {

  /**
   * @author Marc Batchelor
   */
  @Override
  public Object[][] createDataTest() {
    // Account for TimeZone in TIMEVALUE -vs- TIME below...
    // Notes:
    // TIME() returns TimeZone offset values, and this will
    // change depending upon whether you're "in DST" and what
    // your timezone is. The below tests that TIMEVALUE(DATETIMEVALUE)
    // returns correct time-zone and DST offset values.
    TimeZone tz = getContext() != null ? getContext().getTimeZone() : TimeZone.getDefault();
    // 2015-09-08 (YYYY-MM-DD) in Americas/New York has a -4 hour offset because
    // because DST is observed.
    int hrOffset1 = tz.getOffset(new Date(115, 8, 8).getTime()) / 1000 / 60 / 60;
    int base1 = 17 + hrOffset1;
    int base2 = 9 + hrOffset1;
    String t1 = "TIME(" + Integer.toString(base1) + ";15;00)";
    String t2 = "TIME(" + Integer.toString(base2) + ";30;00)";
    // 2015-11-08 (YYYY-MM-DD) in Americas/New York has a -5 hour offset because
    // because DST is not in effect.
    int hrOffset2 = tz.getOffset(new Date(115, 11, 8).getTime()) / 1000 / 60 / 60;
    int base3 = 17 + hrOffset2;
    int base4 = 9 + hrOffset2;
    String t3 = "TIME(" + Integer.toString(base3) + ";15;00)";
    String t4 = "TIME(" + Integer.toString(base4) + ";30;00)";
    return new Object[][] {
      {"TIMEVALUE(DATETIMEVALUE(\"2015-09-08T17:15:00.00+0000\")) = " + t1, Boolean.TRUE},
      {"TIMEVALUE(DATETIMEVALUE(\"2015-09-08T09:30:00.00+0000\")) = " + t2, Boolean.TRUE},
      {"TIMEVALUE(DATETIMEVALUE(\"2015-11-08T17:15:00.00+0000\")) = " + t3, Boolean.TRUE},
      {"TIMEVALUE(DATETIMEVALUE(\"2015-11-08T09:30:00.00+0000\")) = " + t4, Boolean.TRUE},
    };
  }

  @Test
  public void testDefault() throws Exception {
    runDefaultTest();
  }
}
