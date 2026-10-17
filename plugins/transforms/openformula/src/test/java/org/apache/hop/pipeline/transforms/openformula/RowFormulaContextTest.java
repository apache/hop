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
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.sql.Timestamp;
import java.util.Calendar;
import java.util.Date;
import org.apache.hop.core.row.RowMeta;
import org.apache.hop.core.row.value.ValueMetaInteger;
import org.apache.hop.core.row.value.ValueMetaNumber;
import org.apache.hop.core.row.value.ValueMetaString;
import org.apache.hop.core.row.value.ValueMetaTimestamp;
import org.apache.hop.openformula.EvaluationException;
import org.apache.hop.openformula.Formula;
import org.junit.jupiter.api.Test;

class RowFormulaContextTest {

  @Test
  void resolvesNumericAndStringFields() throws Exception {
    RowMeta rowMeta = new RowMeta();
    rowMeta.addValueMeta(new ValueMetaInteger("qty"));
    rowMeta.addValueMeta(new ValueMetaNumber("price"));
    rowMeta.addValueMeta(new ValueMetaString("name"));
    RowFormulaContext context = new RowFormulaContext(rowMeta);
    context.setRowData(new Object[] {3L, 1.5d, "box"});
    assertEquals(3L, context.resolveReference("qty"));
    assertEquals(1.5d, context.resolveReference("price"));
    assertEquals("box", context.resolveReference("name"));
  }

  @Test
  void timestampFieldSupportsDateArithmeticAndComparison() throws Exception {
    RowMeta rowMeta = new RowMeta();
    rowMeta.addValueMeta(new ValueMetaTimestamp("ts"));
    RowFormulaContext context = new RowFormulaContext(rowMeta);

    context.setRowData(new Object[] {Timestamp.valueOf("2025-06-15 00:00:00")});
    assertInstanceOf(Date.class, context.resolveReference("ts"));
    assertEquals(Boolean.TRUE, evaluate(context, "[ts]<DATE(2026;1;1)"));

    context.setRowData(new Object[] {Timestamp.valueOf("2026-06-15 00:00:00")});
    assertEquals(Boolean.FALSE, evaluate(context, "[ts]<DATE(2026;1;1)"));
    Object plusOne = evaluate(context, "[ts]+1");
    assertInstanceOf(Date.class, plusOne);
    Calendar calendar = Calendar.getInstance();
    calendar.setTime((Date) plusOne);
    assertEquals(2026, calendar.get(Calendar.YEAR));
    assertEquals(Calendar.JUNE, calendar.get(Calendar.MONTH));
    assertEquals(16, calendar.get(Calendar.DAY_OF_MONTH));
  }

  @Test
  void missingFieldThrows() {
    RowMeta rowMeta = new RowMeta();
    rowMeta.addValueMeta(new ValueMetaInteger("qty"));
    RowFormulaContext context = new RowFormulaContext(rowMeta);
    context.setRowData(new Object[] {1L});
    assertThrows(EvaluationException.class, () -> context.resolveReference("missing"));
  }

  private static Object evaluate(RowFormulaContext context, String formulaText) throws Exception {
    Formula formula = new Formula(formulaText);
    formula.initialize(context);
    return formula.evaluate();
  }
}
