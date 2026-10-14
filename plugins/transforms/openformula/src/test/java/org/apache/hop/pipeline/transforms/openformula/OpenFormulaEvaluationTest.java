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

package org.apache.hop.pipeline.transforms.openformula;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;

import java.net.URL;
import java.net.URLClassLoader;
import org.apache.hop.core.row.RowMeta;
import org.apache.hop.core.row.value.ValueMetaBoolean;
import org.apache.hop.core.row.value.ValueMetaInteger;
import org.apache.hop.core.row.value.ValueMetaNumber;
import org.apache.hop.openformula.Formula;
import org.apache.hop.openformula.error.FormulaErrorValue;
import org.junit.jupiter.api.Test;

class OpenFormulaEvaluationTest {

  @Test
  void arithmeticAndFieldRefs() throws Exception {
    Object result = evaluate(qtyPrice(), new Object[] {2L, 4.0d}, "[qty]*[price]");
    assertEquals(8.0d, ((Number) result).doubleValue(), 0.0001);
  }

  @Test
  void sampleBasicFormulas() throws Exception {
    RowMeta rowMeta = new RowMeta();
    rowMeta.addValueMeta(new ValueMetaInteger("one"));
    rowMeta.addValueMeta(new ValueMetaInteger("three"));
    rowMeta.addValueMeta(new ValueMetaInteger("ten"));
    Object[] row = new Object[] {1L, 3L, 10L};
    assertEquals(
        8.0d, ((Number) evaluate(rowMeta, row, "[one]+([ten]-[three])")).doubleValue(), 0.0001);
    assertEquals(
        30.0d, ((Number) evaluate(rowMeta, row, "[one]*[three]*[ten]")).doubleValue(), 0.0001);
  }

  @Test
  void parsesOperatorsWhenThreadContextClassLoaderIsIsolated() throws Exception {
    ClassLoader previous = Thread.currentThread().getContextClassLoader();
    try {
      Thread.currentThread().setContextClassLoader(new URLClassLoader(new URL[0], null));
      RowMeta rowMeta = new RowMeta();
      rowMeta.addValueMeta(new ValueMetaInteger("one"));
      rowMeta.addValueMeta(new ValueMetaInteger("three"));
      rowMeta.addValueMeta(new ValueMetaInteger("ten"));
      Object result = evaluate(rowMeta, new Object[] {1L, 3L, 10L}, "[one]+([ten]-[three])");
      assertEquals(8.0d, ((Number) result).doubleValue(), 0.0001);
    } finally {
      Thread.currentThread().setContextClassLoader(previous);
    }
  }

  @Test
  void openFormulaIfUsesSemicolon() throws Exception {
    RowMeta rowMeta = new RowMeta();
    rowMeta.addValueMeta(new ValueMetaBoolean("flag"));
    Object result = evaluate(rowMeta, new Object[] {Boolean.TRUE}, "IF([flag];\"Y\";\"N\")");
    assertEquals("Y", result);
  }

  @Test
  void missingFieldIsFormulaError() throws Exception {
    Object result = evaluate(qtyPrice(), new Object[] {1L, 1.0d}, "[missing]");
    assertInstanceOf(FormulaErrorValue.class, result);
  }

  @Test
  void compiledExpressionIsReusedAcrossRows() throws Exception {
    RowMeta rowMeta = qtyPrice();
    RowFormulaContext context = new RowFormulaContext(rowMeta);
    Formula formula = new Formula("[qty]*[price]");
    formula.initialize(context);
    context.setRowData(new Object[] {2L, 4.0d});
    assertEquals(8.0d, ((Number) formula.evaluate()).doubleValue(), 0.0001);
    context.setRowData(new Object[] {3L, 5.0d});
    assertEquals(15.0d, ((Number) formula.evaluate()).doubleValue(), 0.0001);
  }

  private static RowMeta qtyPrice() {
    RowMeta rowMeta = new RowMeta();
    rowMeta.addValueMeta(new ValueMetaInteger("qty"));
    rowMeta.addValueMeta(new ValueMetaNumber("price"));
    return rowMeta;
  }

  private static Object evaluate(RowMeta rowMeta, Object[] row, String formulaText)
      throws Exception {
    RowFormulaContext context = new RowFormulaContext(rowMeta);
    context.setRowData(row);
    Formula formula = new Formula(formulaText);
    formula.initialize(context);
    return formula.evaluate();
  }
}
