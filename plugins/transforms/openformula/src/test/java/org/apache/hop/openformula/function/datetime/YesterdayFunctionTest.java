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

import static org.junit.jupiter.api.Assertions.*;

import java.util.Calendar;
import java.util.Date;
import java.util.GregorianCalendar;
import org.apache.hop.openformula.common.FormulaTestBase;
import org.junit.jupiter.api.Test;

public class YesterdayFunctionTest extends FormulaTestBase {
  @Override
  public Object[][] createDataTest() {
    return new Object[][] {
      {"YESTERDAY()", createYesterdaysDate()},
    };
  }

  private Date createYesterdaysDate() {
    final GregorianCalendar gcal = new GregorianCalendar(2011, Calendar.APRIL, 6, 0, 0, 0);
    gcal.setTimeZone(getContext().getLocalizationContext().getTimeZone());
    return gcal.getTime();
  }

  @Test
  public void testDefault() throws Exception {
    runDefaultTest();
  }
}
