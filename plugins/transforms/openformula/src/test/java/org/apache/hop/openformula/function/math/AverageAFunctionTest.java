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

package org.apache.hop.openformula.function.math;

import static org.junit.jupiter.api.Assertions.*;

import java.math.BigDecimal;
import org.apache.hop.openformula.FormulaContext;
import org.apache.hop.openformula.common.FormulaTestBase;
import org.apache.hop.openformula.common.TestFormulaContext;
import org.apache.hop.openformula.error.FormulaErrorValue;
import org.apache.hop.openformula.typing.TypedValue;
import org.junit.jupiter.api.Test;

public class AverageAFunctionTest extends FormulaTestBase {
  private static final double DELTA = 0.0000001;
  private static final String FORMULA_NAME = "AVERAGEA";

  private FormulaContext context = new TestFormulaContext();

  /** A list of valid values to be given to the function and the (expected) resulting value */
  private static final Object[][] VALID_EXAMPLES = {
    // Only numbers
    {BigDecimal.valueOf(3), new Object[] {0, 1, 5, 6}},
    // Number as a string
    {BigDecimal.valueOf(3), new Object[] {0, 1, "5", 6}},
    // Logical values
    {BigDecimal.valueOf(3), new Object[] {false, true, 5, 6}},
    // Number as a string and Logical values
    {BigDecimal.valueOf(3), new Object[] {false, true, "5", 6}},
    // String not convertible to number
    {BigDecimal.valueOf(3), new Object[] {"xpto", 1, 5, 6}}
  };

  @Override
  public Object[][] createDataTest() {
    // Ignore this feature
    return null;
  }

  @Test
  public void testValidExamples() throws Exception {
    for (Object[] testValues : VALID_EXAMPLES) {
      BigDecimal expectedResult = (BigDecimal) testValues[0];
      Object[] parameters = (Object[]) testValues[1];

      TypedValue res = evaluateFormula(getFormulaText(FORMULA_NAME, parameters), context);
      assertNotNull(res);
      Object resValue = res.getValue();
      assertNotNull(resValue);
      assertTrue(resValue instanceof BigDecimal);
      assertEquals(expectedResult, resValue);
    }
  }

  @Test
  public void testStringParameters_Numbers() throws Exception {
    TypedValue res =
        evaluateFormula(getFormulaText(FORMULA_NAME, new Object[] {"100", "-1", "0"}), context);
    assertNotNull(res);
    Object resValue = res.getValue();
    assertNotNull(resValue);
    assertTrue(resValue instanceof BigDecimal);
    assertEquals(BigDecimal.valueOf(33), resValue);
  }

  @Test
  public void testStringParameters_NoNumbers() throws Exception {
    TypedValue res =
        evaluateFormula(
            getFormulaText(FORMULA_NAME, new Object[] {"abc", "def", "g", ""}), context);
    assertNotNull(res);
    Object resValue = res.getValue();
    assertNotNull(resValue);
    assertTrue(resValue instanceof BigDecimal);
    assertEquals(BigDecimal.ZERO, resValue);
  }

  @Test
  public void testStringParameters_Mix() throws Exception {
    TypedValue res =
        evaluateFormula(
            getFormulaText(
                FORMULA_NAME, new Object[] {"xpto", "100", "2.5", -1, "alfa", 0, "+200", "-1.5"}),
            context);
    assertNotNull(res);
    Object resValue = res.getValue();
    assertNotNull(resValue);
    assertTrue(resValue instanceof BigDecimal);
    assertEquals(
        ((0 + 100 + 2.5 + (-1) + 0 + (+200) + (-1.5)) / 8.0),
        ((BigDecimal) resValue).doubleValue(),
        DELTA);
  }

  @Test
  public void testLogicalParameters() throws Exception {
    TypedValue res =
        evaluateFormula(
            getFormulaText(
                FORMULA_NAME,
                new Object[] {Boolean.FALSE, Boolean.TRUE, Boolean.FALSE, Boolean.FALSE}),
            context);
    assertNotNull(res);
    Object resValue = res.getValue();
    assertNotNull(resValue);
    assertTrue(resValue instanceof BigDecimal);
    assertEquals(BigDecimal.valueOf((0 + 1 + 0 + 0) / 4.0), resValue);
  }

  @Test
  public void testZeroParameters() throws Exception {
    TypedValue res = evaluateFormula(getFormulaText(FORMULA_NAME, null), context);
    assertNotNull(res);
    Object resValue = res.getValue();
    assertNotNull(resValue);
    assertEquals(FormulaErrorValue.ERROR_ARGUMENTS_VALUE, resValue);
  }
}
