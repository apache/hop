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

package org.apache.hop.openformula.typing;

import static org.junit.jupiter.api.Assertions.*;

import org.apache.hop.openformula.EvaluationException;
import org.apache.hop.openformula.Formula;
import org.apache.hop.openformula.FormulaContext;
import org.apache.hop.openformula.common.TestFormulaContext;
import org.apache.hop.openformula.error.FormulaErrorValue;
import org.apache.hop.openformula.parser.ParseException;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class ArrayTest {
  private FormulaContext context;

  @BeforeEach
  public void setUp() throws Exception {
    context = new TestFormulaContext();
  }

  @Test
  public void testRowsInlineArrays() throws Exception {
    final Formula formula = new Formula("{3|2|1}");
    formula.initialize(context);
    final TypedValue evaluation = formula.evaluateTyped();
    assertNotNull(evaluation);
    assertEquals(DataType.ARRAY, evaluation.getType());

    final ArrayCallback table = (ArrayCallback) evaluation.getValue();
    assertEquals(1, table.getColumnCount());
    assertEquals(3, table.getRowCount());
  }

  @Test
  public void testColumnsInlineArrays() throws Exception {
    final Formula formula = new Formula("{3;2;1}");
    formula.initialize(context);
    final TypedValue evaluation = formula.evaluateTyped();
    assertNotNull(evaluation);
    assertEquals(DataType.ARRAY, evaluation.getType());

    final ArrayCallback table = (ArrayCallback) evaluation.getValue();
    assertEquals(3, table.getColumnCount());
    assertEquals(1, table.getRowCount());
  }

  @Test
  public void testInlineArrays() throws Exception {
    final Formula formula = new Formula("{3;2;1|2;4;6}");
    formula.initialize(context);

    final TypedValue evaluation = formula.evaluateTyped();
    assertNotNull(evaluation);
    assertEquals(DataType.ARRAY, evaluation.getType());

    final ArrayCallback table = (ArrayCallback) evaluation.getValue();
    assertEquals(3, table.getColumnCount());
    assertEquals(2, table.getRowCount());
  }

  @Test
  public void testInvalidInlineArrays() throws Exception {
    final Formula formula = new Formula("{3;2;1|2;6}");
    formula.initialize(context);
    final Object evaluate = formula.evaluate();
    assertEquals(FormulaErrorValue.ERROR_ILLEGAL_ARRAY_VALUE, evaluate);
  }

  @Test
  public void testInvalidInlineArrays2() throws EvaluationException, ParseException {
    final Formula formula = new Formula("{3;1|2;6;5;6}");
    formula.initialize(context);
    final Object evaluate = formula.evaluate();
    assertEquals(FormulaErrorValue.ERROR_ILLEGAL_ARRAY_VALUE, evaluate);
  }

  @Test
  public void testEmptyArray() throws EvaluationException, ParseException {
    final Formula formula = new Formula("{}");
    formula.initialize(context);
    final TypedValue evaluation = formula.evaluateTyped();
    assertNotNull(evaluation);
    assertEquals(DataType.ARRAY, evaluation.getType());

    final ArrayCallback table = (ArrayCallback) evaluation.getValue();
    assertEquals(0, table.getColumnCount());
    assertEquals(0, table.getRowCount());
  }
}
