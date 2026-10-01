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

package org.apache.hop.pipeline.transforms.formula.util;

import static org.apache.hop.pipeline.transforms.formula.util.FormulaFieldsExtractor.getFormulaFieldList;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.math.BigDecimal;
import java.sql.Timestamp;
import java.time.LocalDate;
import java.time.ZoneId;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import org.apache.hop.core.exception.HopValueException;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.core.row.RowMeta;
import org.apache.hop.core.row.value.ValueMetaBigNumber;
import org.apache.hop.core.row.value.ValueMetaBoolean;
import org.apache.hop.core.row.value.ValueMetaDate;
import org.apache.hop.core.row.value.ValueMetaInteger;
import org.apache.hop.core.row.value.ValueMetaNumber;
import org.apache.hop.core.row.value.ValueMetaString;
import org.apache.hop.core.row.value.ValueMetaTimestamp;
import org.apache.hop.core.variables.Variables;
import org.apache.hop.pipeline.transforms.formula.FormulaMetaFunction;
import org.apache.hop.pipeline.transforms.formula.FormulaPoi;
import org.apache.poi.ss.usermodel.CellType;
import org.apache.poi.ss.usermodel.CellValue;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

/** Unit tests for {@link FormulaParser} formula evaluation and field binding. */
class FormulaParserEvaluationTest {

  private FormulaPoi poi;

  @AfterEach
  void tearDown() throws Exception {
    if (poi != null) {
      poi.destroy();
      poi = null;
    }
  }

  @Test
  void numberFieldGreaterThanLiteral() throws Exception {
    CellValue result =
        evaluate(List.of(field(new ValueMetaNumber("amount"), 150.0)), "[amount] > 100", false);

    assertEquals(CellType.BOOLEAN, result.getCellType());
    assertTrue(result.getBooleanValue());
  }

  @Test
  void stringFieldEqualsLiteral() throws Exception {
    CellValue result =
        evaluate(
            List.of(field(new ValueMetaString("status"), "active")),
            "[status] = \"active\"",
            false);

    assertEquals(CellType.BOOLEAN, result.getCellType());
    assertTrue(result.getBooleanValue());
  }

  @Test
  void booleanFieldIsTrue() throws Exception {
    CellValue result =
        evaluate(
            List.of(field(new ValueMetaBoolean("flag"), Boolean.TRUE)), "[flag] = TRUE", false);

    assertEquals(CellType.BOOLEAN, result.getCellType());
    assertTrue(result.getBooleanValue());
  }

  @Test
  void bigNumberFieldGreaterThanLiteral() throws Exception {
    CellValue result =
        evaluate(
            List.of(field(new ValueMetaBigNumber("amount"), new BigDecimal("200.5"))),
            "[amount] > 100",
            false);

    assertEquals(CellType.BOOLEAN, result.getCellType());
    assertTrue(result.getBooleanValue());
  }

  @Test
  void nullFieldIsBlankWhenSetNaIsFalse() throws Exception {
    CellValue result =
        evaluate(
            List.of(field(new ValueMetaInteger("amount"), null)),
            "IF(ISBLANK([amount]), 1, 0)",
            false);

    assertEquals(CellType.NUMERIC, result.getCellType());
    assertEquals(1.0, result.getNumberValue());
  }

  @Test
  void nullFieldIsNaWhenSetNaIsTrue() throws Exception {
    CellValue result =
        evaluate(List.of(field(new ValueMetaInteger("amount"), null)), "ISNA([amount])", true);

    assertEquals(CellType.BOOLEAN, result.getCellType());
    assertTrue(result.getBooleanValue());
  }

  @Test
  void dateFieldOnTheExcelEpochIsEvaluated() throws Exception {
    // 1899-12-31 is the oldest representable date: it is Excel date serial number 0.
    CellValue result =
        evaluate(List.of(field(new ValueMetaDate("start"), date(1899, 12, 31))), "[start]", false);

    assertEquals(CellType.NUMERIC, result.getCellType());
    assertEquals(0.0, result.getNumberValue());
  }

  @Test
  void dateFieldBeforeTheExcelEpochIsRejected() {
    HopValueException e =
        assertThrows(
            HopValueException.class,
            () ->
                evaluate(
                    List.of(field(new ValueMetaDate("start"), date(1800, 1, 1))),
                    "IF(1=2, DATE(2000,1,1), [start])",
                    false));

    assertTrue(e.getMessage().contains("start"), e.getMessage());
    assertTrue(e.getMessage().contains("1899-12-31"), e.getMessage());
  }

  @Test
  void timestampFieldBeforeTheExcelEpochIsRejected() {
    Timestamp timestamp = new Timestamp(date(1750, 6, 15).getTime());

    HopValueException e =
        assertThrows(
            HopValueException.class,
            () ->
                evaluate(
                    List.of(field(new ValueMetaTimestamp("start"), timestamp)),
                    "[start] + 1",
                    false));

    assertTrue(e.getMessage().contains("start"), e.getMessage());
  }

  @Test
  void nullDateFieldIsNotRejected() throws Exception {
    CellValue result =
        evaluate(
            List.of(field(new ValueMetaDate("start"), null)), "IF(ISBLANK([start]), 1, 0)", false);

    assertEquals(CellType.NUMERIC, result.getCellType());
    assertEquals(1.0, result.getNumberValue());
  }

  @Test
  void twoIntegerFieldsAreSummed() throws Exception {
    CellValue result =
        evaluate(
            List.of(field(new ValueMetaInteger("a"), 10L), field(new ValueMetaInteger("b"), 20L)),
            "[a] + [b]",
            false);

    assertEquals(CellType.NUMERIC, result.getCellType());
    assertEquals(30.0, result.getNumberValue());
  }

  @Test
  void replaceMapRedirectsFormulaFieldToRealColumn() throws Exception {
    RowMeta rowMeta = new RowMeta();
    rowMeta.addValueMeta(new ValueMetaInteger("realAmount"));
    Object[] row = new Object[] {42L};

    HashMap<String, String> replaceMap = new HashMap<>();
    replaceMap.put("aliasAmount", "realAmount");

    String formula = "[aliasAmount] * 2";
    FormulaMetaFunction fn =
        new FormulaMetaFunction("result", formula, IValueMeta.TYPE_INTEGER, -1, -1, "", false);

    poi = new FormulaPoi(msg -> {});
    FormulaParser parser =
        new FormulaParser(
            fn, rowMeta, row, poi, new Variables(), replaceMap, getFormulaFieldList(formula));

    CellValue result = parser.getFormulaValue();
    assertEquals(CellType.NUMERIC, result.getCellType());
    assertEquals(84.0, result.getNumberValue());
  }

  @Test
  void variablesAreResolvedBeforeEvaluation() throws Exception {
    Variables variables = new Variables();
    variables.setVariable("THRESHOLD", "50");

    CellValue result =
        evaluate(
            variables,
            List.of(field(new ValueMetaInteger("amount"), 60L)),
            "[amount] > ${THRESHOLD}",
            false);

    assertEquals(CellType.BOOLEAN, result.getCellType());
    assertTrue(result.getBooleanValue());
  }

  private CellValue evaluate(List<FieldBinding> bindings, String formula, boolean setNa)
      throws Exception {
    return evaluate(new Variables(), bindings, formula, setNa);
  }

  private CellValue evaluate(
      Variables variables, List<FieldBinding> bindings, String formula, boolean setNa)
      throws Exception {
    RowMeta rowMeta = new RowMeta();
    Object[] row = new Object[bindings.size()];
    for (int i = 0; i < bindings.size(); i++) {
      FieldBinding binding = bindings.get(i);
      rowMeta.addValueMeta(binding.meta);
      row[i] = binding.value;
    }

    FormulaMetaFunction fn =
        new FormulaMetaFunction("result", formula, IValueMeta.TYPE_STRING, -1, -1, "", setNa);

    poi = new FormulaPoi(msg -> {});
    FormulaParser parser =
        new FormulaParser(
            fn,
            rowMeta,
            row,
            poi,
            variables,
            new HashMap<>(),
            getFormulaFieldList(variables.resolve(formula)));

    return parser.getFormulaValue();
  }

  private static FieldBinding field(IValueMeta meta, Object value) {
    return new FieldBinding(meta, value);
  }

  /** POI converts dates in the default time zone, so build them in the default zone as well. */
  private static Date date(int year, int month, int day) {
    return Date.from(
        LocalDate.of(year, month, day).atStartOfDay(ZoneId.systemDefault()).toInstant());
  }

  private record FieldBinding(IValueMeta meta, Object value) {}
}
