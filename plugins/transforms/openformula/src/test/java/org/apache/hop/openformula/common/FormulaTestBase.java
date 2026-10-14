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

package org.apache.hop.openformula.common;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Date;
import org.apache.hop.openformula.EvaluationException;
import org.apache.hop.openformula.Formula;
import org.apache.hop.openformula.FormulaContext;
import org.apache.hop.openformula.error.FormulaErrorValue;
import org.apache.hop.openformula.typing.ArrayCallback;
import org.apache.hop.openformula.typing.TypedValue;
import org.junit.jupiter.api.BeforeEach;

public abstract class FormulaTestBase {
  private static final String FORMULA_TEXT_PATTERN = "%s(%s)";
  protected FormulaContext context;

  @BeforeEach
  public void setUp() throws Exception {
    this.context = new TestFormulaContext();
  }

  public FormulaContext getContext() {
    return context;
  }

  public Object[][] createDataTest() {
    return new Object[0][0];
  }

  protected void runDefaultTest() throws Exception {
    Object[][] dataTest = createDataTest();
    if (dataTest != null) {
      runTest(dataTest);
    }
  }

  protected void runTest(Object[][] dataTest) throws Exception {
    for (Object[] row : dataTest) {
      String formula = (String) row[0];
      Object expected = row[1];
      performTest(formula, expected);
    }
  }

  protected void performTest(String formulaText, Object expected) throws Exception {
    performTest(formulaText, expected, getContext());
  }

  @SuppressWarnings({"unchecked", "rawtypes"})
  protected void performTest(String formulaText, Object expected, FormulaContext ctx)
      throws Exception {
    TypedValue res = evaluateFormula(formulaText, ctx);
    Object actual = res.getValue();

    if (expected instanceof FormulaErrorValue expErr) {
      assertTrue(
          actual instanceof FormulaErrorValue,
          "Expected error " + expErr + " for [" + formulaText + "], but got: " + actual);
      assertEquals(
          expErr.getErrorCode(),
          ((FormulaErrorValue) actual).getErrorCode(),
          "Error code mismatch for [" + formulaText + "]");
      return;
    }

    if (actual instanceof ArrayCallback ac && expected instanceof Object[] expArr) {
      Object[] actualArr = new Object[ac.getColumnCount()];
      for (int i = 0; i < ac.getColumnCount(); i++) actualArr[i] = ac.getValue(0, i);
      assertArrayEquals(expArr, actualArr);
      return;
    }

    if (expected instanceof Number expNum && actual instanceof Number actNum) {
      double diff = Math.abs(expNum.doubleValue() - actNum.doubleValue());
      assertTrue(
          diff < 0.0001,
          "Expected "
              + expected
              + " but got "
              + actual
              + " for ["
              + formulaText
              + "] diff: "
              + diff);
      return;
    }

    if (expected instanceof Date expDate && actual instanceof Date actDate) {
      assertEquals(expDate.getTime(), actDate.getTime(), "Date mismatch for [" + formulaText + "]");
      return;
    }

    if (expected instanceof Comparable expComp && actual instanceof Comparable actComp) {
      try {
        int cmp = expComp.compareTo(actComp);
        assertEquals(
            0,
            cmp,
            "Comparison mismatch for ["
                + formulaText
                + "]: expected "
                + expected
                + " but got "
                + actual);
        return;
      } catch (ClassCastException ignored) {
      }
    }

    assertEquals(expected, actual, "Failure on [" + formulaText + "]");
  }

  protected String getFormulaText(Object functionName, Object[] parameterValues) {
    return String.format(
        FORMULA_TEXT_PATTERN, functionName, getParametersAsText(parameterValues, ';'));
  }

  protected String getParametersAsText(Object[] parameterValues, char separatorToUse) {
    StringBuilder sb = new StringBuilder();
    if (parameterValues != null) {
      char separator = ' ';
      for (Object parameterValue : parameterValues) {
        sb.append(separator);
        separator = separatorToUse;
        if (parameterValue instanceof Object[]) {
          sb.append('{').append(getParametersAsText((Object[]) parameterValue, '|')).append('}');
        } else if (parameterValue instanceof Number) {
          sb.append(parameterValue);
        } else if (parameterValue instanceof Boolean) {
          sb.append(parameterValue.toString().toUpperCase()).append("()");
        } else if (parameterValue instanceof String) {
          sb.append('"').append(parameterValue).append('"');
        } else {
          sb.append(parameterValue);
        }
      }
    }
    return sb.toString();
  }

  protected TypedValue evaluateFormula(String formulaText, FormulaContext ctx) throws Exception {
    Formula formula = new Formula(formulaText);
    formula.initialize(ctx);
    try {
      return formula.evaluateTyped();
    } catch (EvaluationException e) {
      return TypedValue.ofError(e.getErrorValue());
    }
  }
}
