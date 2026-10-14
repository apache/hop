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

import java.math.BigDecimal;
import org.apache.hop.openformula.common.FormulaTestBase;
import org.junit.jupiter.api.Test;

/**
 * @author Cedric Pronzato
 */
public class WeekDayFunctionTest extends FormulaTestBase {
  @Override
  public Object[][] createDataTest() {
    return new Object[][] {
      {"WEEKDAY(DATE(2006;5;21))", new BigDecimal(1)},
      {"WEEKDAY(DATE(2005;1;1))", new BigDecimal(7)},
      {"WEEKDAY(DATE(2005;1;1);1)", new BigDecimal(7)},
      {"WEEKDAY(DATE(2005;1;1);2)", new BigDecimal(6)},
      {"WEEKDAY(DATE(2005;1;1);3)", new BigDecimal(5)},
    };
  }

  private Number[][] createTypeDataTest() {
    return new Number[][] {
      {new BigDecimal(1), new BigDecimal(7), new BigDecimal(6)},
      {new BigDecimal(2), new BigDecimal(1), new BigDecimal(0)},
      {new BigDecimal(3), new BigDecimal(2), new BigDecimal(1)},
      {new BigDecimal(4), new BigDecimal(3), new BigDecimal(2)},
      {new BigDecimal(5), new BigDecimal(4), new BigDecimal(3)},
      {new BigDecimal(6), new BigDecimal(5), new BigDecimal(4)},
      {new BigDecimal(7), new BigDecimal(6), new BigDecimal(5)},
    };
  }

  @Test
  public void testAllTypes() {
    final DateTimeFunctions.WeekDayFunction function = new DateTimeFunctions.WeekDayFunction();
    final Number[][] dataTest = createTypeDataTest();
    for (int i = 0; i < dataTest.length; i++) {
      final Number[] objects = dataTest[i];
      final Number type1 = objects[0];
      final Number type2 = objects[1];
      final Number type3 = objects[2];
      assertEquals(
          type1.intValue(),
          function.convertType(type1.intValue(), 1),
          "Error with Type 1 conversion");
      assertEquals(
          type2.intValue(),
          function.convertType(type1.intValue(), 2),
          "Error with Type 2 conversion");
      assertEquals(
          type3.intValue(),
          function.convertType(type1.intValue(), 3),
          "Error with Type 3 conversion");
    }
  }

  @Test
  public void testDefault() throws Exception {
    runDefaultTest();
  }
}
