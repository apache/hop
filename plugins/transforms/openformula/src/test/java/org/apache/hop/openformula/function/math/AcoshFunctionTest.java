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

import java.math.BigDecimal;
import org.apache.hop.openformula.common.FormulaTestBase;
import org.apache.hop.openformula.error.FormulaErrorValue;
import org.junit.jupiter.api.Test;

public class AcoshFunctionTest extends FormulaTestBase {
  private static final BigDecimal[][] VALID_VALUES = {
    {BigDecimal.ONE, BigDecimal.ZERO},
    {new BigDecimal("1.1"), new BigDecimal("0.443568254385115")},
    {BigDecimal.TEN, new BigDecimal("2.99322284612638")},
    {new BigDecimal("10000000.0"), new BigDecimal("16.8112428315183")}
  };

  private static final BigDecimal[] INVALID_VALUES = {
    new BigDecimal("-123456789.0"),
    new BigDecimal("-1.2"),
    BigDecimal.ZERO,
    new BigDecimal("0.9999999999999")
  };

  @Test
  public void testDefault() throws Exception {
    runDefaultTest();
  }

  @Override
  public Object[][] createDataTest() {
    return new Object[][] {
      {"LEFT(ACOSH(10);10)", "2.99322284"},
    };
  }

  @Test
  public void testValuesInRange() throws Exception {
    for (BigDecimal[] testValues : VALID_VALUES) {
      performTest("ACOSH(" + testValues[0] + ")", testValues[1]);
    }
  }

  @Test
  public void testValuesNotInRange() throws Exception {
    for (BigDecimal testValue : INVALID_VALUES) {
      performTest("ACOSH(" + testValue + ")", FormulaErrorValue.ERROR_INVALID_ARGUMENT_VALUE);
    }
  }

  @Test
  public void testWrongParameterNumber_Zero() throws Exception {
    performTest("ACOSH()", FormulaErrorValue.ERROR_ARGUMENTS_VALUE);
  }

  @Test
  public void testWrongParameterNumber_Two() throws Exception {
    performTest("ACOSH(1; 2)", FormulaErrorValue.ERROR_ARGUMENTS_VALUE);
  }

  @Test
  public void testWrongParameterType() throws Exception {
    performTest("ACOSH(\"error\")", FormulaErrorValue.ERROR_INVALID_ARGUMENT_VALUE);
  }
}
