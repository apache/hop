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
package org.apache.hop.pipeline.transforms.formula.fast;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.math.BigDecimal;
import java.util.List;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.core.row.RowMeta;
import org.apache.hop.core.row.value.ValueMetaBigNumber;
import org.apache.hop.core.row.value.ValueMetaBoolean;
import org.apache.hop.core.row.value.ValueMetaDate;
import org.apache.hop.core.row.value.ValueMetaInteger;
import org.apache.hop.core.row.value.ValueMetaNumber;
import org.apache.hop.core.row.value.ValueMetaString;
import org.apache.hop.pipeline.transforms.formula.fast.FastFormulaCompiler.CompiledFormula;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/** Unit tests for {@link FastFormulaCompiler}: eligibility and plain-Java evaluation. */
class FastFormulaCompilerTest {

  private RowMeta rowMeta;

  @BeforeEach
  void setUp() {
    FastFormulaCompiler.clear();
    rowMeta = new RowMeta();
  }

  @AfterEach
  void tearDown() {
    FastFormulaCompiler.clear();
  }

  private void add(IValueMeta meta) {
    rowMeta.addValueMeta(meta);
  }

  private Object evaluate(String formula, List<String> fields, boolean setNa, Object[] row) {
    CompiledFormula compiled = FastFormulaCompiler.compile(formula, fields, rowMeta, setNa);
    assertTrue(compiled.fastPath(), "expected fast path for " + formula);
    return compiled.function().apply(row);
  }

  @Test
  void numericFieldsAreSummedAsDoubles() {
    add(new ValueMetaInteger("left"));
    add(new ValueMetaInteger("right"));
    Object result =
        evaluate("[left] + [right]", List.of("left", "right"), false, new Object[] {10L, 20L});
    assertEquals(30.0d, result);
  }

  @Test
  void bigNumberFieldIsComparedNumerically() {
    add(new ValueMetaBigNumber("qty"));
    Object result =
        evaluate("[qty] > 100", List.of("qty"), false, new Object[] {new BigDecimal("200.5")});
    assertEquals(Boolean.TRUE, result);
  }

  @Test
  void textFieldEqualityIsCaseInsensitive() {
    add(new ValueMetaString("label"));
    Object result =
        evaluate("[label] = \"Active\"", List.of("label"), false, new Object[] {"active"});
    assertEquals(Boolean.TRUE, result);
  }

  @Test
  void booleanFieldComparesToTrue() {
    add(new ValueMetaBoolean("on"));
    Object result = evaluate("[on] = TRUE", List.of("on"), false, new Object[] {Boolean.TRUE});
    assertEquals(Boolean.TRUE, result);
  }

  @Test
  void ifSelectsTheTakenBranch() {
    add(new ValueMetaInteger("score"));
    Object result =
        evaluate(
            "IF([score] >= 60, \"Pass\", \"Fail\")", List.of("score"), false, new Object[] {85L});
    assertEquals("Pass", result);
  }

  @Test
  void andOrNotAreSupported() {
    add(new ValueMetaInteger("a"));
    add(new ValueMetaInteger("b"));
    assertEquals(
        Boolean.TRUE,
        evaluate("AND([a] > 1, [b] > 1)", List.of("a", "b"), false, new Object[] {2L, 3L}));
    assertEquals(
        Boolean.FALSE,
        evaluate("AND([a] > 9, [b] > 1)", List.of("a", "b"), false, new Object[] {2L, 3L}));
    assertEquals(
        Boolean.TRUE,
        evaluate("OR([a] > 9, [b] > 1)", List.of("a", "b"), false, new Object[] {2L, 3L}));
    assertEquals(Boolean.FALSE, evaluate("NOT([a] > 1)", List.of("a"), false, new Object[] {2L}));
  }

  @Test
  void logicalOperatorsAreSupportedAsSymbols() {
    add(new ValueMetaInteger("a"));
    add(new ValueMetaInteger("b"));
    assertEquals(
        Boolean.TRUE,
        evaluate("[a] > 1 && [b] > 1", List.of("a", "b"), false, new Object[] {2L, 3L}));
    assertEquals(
        Boolean.TRUE,
        evaluate("[a] > 1 || [b] > 9", List.of("a", "b"), false, new Object[] {2L, 3L}));
    assertEquals(Boolean.TRUE, evaluate("![a] > 9", List.of("a"), false, new Object[] {2L}));
  }

  @Test
  void absLenAndTrimAreSupported() {
    add(new ValueMetaNumber("delta"));
    assertEquals(5.0d, evaluate("ABS([delta])", List.of("delta"), false, new Object[] {-5.0}));
    add(new ValueMetaString("name"));
    assertEquals(4.0d, evaluate("LEN([name])", List.of("name"), false, new Object[] {"Hop "}));
    assertEquals(
        "a b", evaluate("TRIM([name])", List.of("name"), false, new Object[] {"  a   b  "}));
  }

  @Test
  void isBlankTrueForNullWithoutNaOption() {
    add(new ValueMetaInteger("amount"));
    Object result = evaluate("ISBLANK([amount])", List.of("amount"), false, new Object[] {null});
    assertEquals(Boolean.TRUE, result);
  }

  @Test
  void isNaTrueForNullWithNaOption() {
    add(new ValueMetaInteger("amount"));
    Object result =
        evaluate("ISNA([amount])", List.of("amount"), false, new Object[] {FastFormulaCompiler.NA});
    assertEquals(Boolean.TRUE, result);
  }

  @Test
  void isNaFalseForBlankNull() {
    add(new ValueMetaInteger("amount"));
    Object result = evaluate("ISNA([amount])", List.of("amount"), false, new Object[] {null});
    assertEquals(Boolean.FALSE, result);
  }

  @Test
  void concatCoercesValuesToText() {
    add(new ValueMetaString("prefix"));
    add(new ValueMetaInteger("num"));
    Object result =
        evaluate("[prefix] & [num]", List.of("prefix", "num"), false, new Object[] {"id-", 7L});
    assertEquals("id-7", result);
  }

  @Test
  void everyComparisonOperatorIsSupported() {
    add(new ValueMetaInteger("a"));
    add(new ValueMetaInteger("b"));
    assertEquals(
        Boolean.TRUE, evaluate("[a] < [b]", List.of("a", "b"), false, new Object[] {1L, 2L}));
    assertEquals(
        Boolean.TRUE, evaluate("[a] <= [b]", List.of("a", "b"), false, new Object[] {2L, 2L}));
    assertEquals(
        Boolean.TRUE, evaluate("[a] >= [b]", List.of("a", "b"), false, new Object[] {2L, 2L}));
    assertEquals(
        Boolean.TRUE, evaluate("[a] <> [b]", List.of("a", "b"), false, new Object[] {1L, 2L}));
    assertEquals(
        Boolean.FALSE, evaluate("[a] <> [b]", List.of("a", "b"), false, new Object[] {2L, 2L}));
  }

  @Test
  void divisionByZeroThrowsDivZero() {
    add(new ValueMetaInteger("a"));
    add(new ValueMetaInteger("b"));
    CompiledFormula compiled =
        FastFormulaCompiler.compile("[a] / [b]", List.of("a", "b"), rowMeta, false);
    assertTrue(compiled.fastPath());
    ArithmeticException e =
        org.junit.jupiter.api.Assertions.assertThrows(
            ArithmeticException.class, () -> compiled.function().apply(new Object[] {1L, 0L}));
    assertTrue(e.getMessage().contains("#DIV/0!"), e.getMessage());
  }

  @Test
  void disabledFastPathReportsNotEligible() {
    add(new ValueMetaInteger("amount"));
    String previous = System.getProperty(FastFormulaCompiler.ENABLED_PROPERTY);
    try {
      System.setProperty(FastFormulaCompiler.ENABLED_PROPERTY, "false");
      FastFormulaCompiler.clear();
      CompiledFormula compiled =
          FastFormulaCompiler.compile("[amount] + 1", List.of("amount"), rowMeta, false);
      assertFalse(compiled.fastPath());
    } finally {
      if (previous == null) {
        System.clearProperty(FastFormulaCompiler.ENABLED_PROPERTY);
      } else {
        System.setProperty(FastFormulaCompiler.ENABLED_PROPERTY, previous);
      }
      FastFormulaCompiler.clear();
    }
  }

  @Test
  void dateFieldIsNotEligible() {
    add(new ValueMetaDate("start"));
    CompiledFormula compiled =
        FastFormulaCompiler.compile("[start]", List.of("start"), rowMeta, false);
    assertFalse(compiled.fastPath());
  }

  @Test
  void ternaryIsNotEligible() {
    add(new ValueMetaInteger("count"));
    CompiledFormula compiled =
        FastFormulaCompiler.compile("[count] != null ? 1 : 0", List.of("count"), rowMeta, false);
    assertFalse(compiled.fastPath());
  }

  @Test
  void methodCallIsNotEligible() {
    add(new ValueMetaString("code"));
    CompiledFormula compiled =
        FastFormulaCompiler.compile("[code].toLowerCase()", List.of("code"), rowMeta, false);
    assertFalse(compiled.fastPath());
  }

  @Test
  void unsupportedFunctionIsNotEligible() {
    add(new ValueMetaString("name"));
    CompiledFormula compiled =
        FastFormulaCompiler.compile("UPPER([name])", List.of("name"), rowMeta, false);
    assertFalse(compiled.fastPath());
  }

  @Test
  void identicalFormulasShareOneCacheEntry() {
    add(new ValueMetaInteger("amount"));
    FastFormulaCompiler.compile("[amount] + 1", List.of("amount"), rowMeta, false);
    FastFormulaCompiler.compile("[amount] + 1", List.of("amount"), rowMeta, false);
    assertEquals(1, FastFormulaCompiler.cacheSize());
  }

  @Test
  void cacheEntryCountsDistinctSchemas() {
    add(new ValueMetaInteger("amount"));
    FastFormulaCompiler.compile("[amount] + 1", List.of("amount"), rowMeta, false);
    RowMeta other = new RowMeta();
    other.addValueMeta(new ValueMetaString("amount"));
    FastFormulaCompiler.compile("[amount] + 1", List.of("amount"), other, false);
    assertEquals(2, FastFormulaCompiler.cacheSize());
  }

  @Test
  void cacheEntryCountsSetNaOptionSeparately() {
    add(new ValueMetaInteger("amount"));
    FastFormulaCompiler.compile("[amount] + 1", List.of("amount"), rowMeta, false);
    FastFormulaCompiler.compile("[amount] + 1", List.of("amount"), rowMeta, true);
    assertEquals(2, FastFormulaCompiler.cacheSize());
  }

  @Test
  void cacheEvictsLeastRecentlyUsedAboveMaxSize() {
    add(new ValueMetaInteger("amount"));
    int max = FastFormulaCompiler.DEFAULT_MAX_SIZE;
    for (int i = 0; i < max; i++) {
      FastFormulaCompiler.compile("[amount] + " + i, List.of("amount"), rowMeta, false);
    }
    assertEquals(max, FastFormulaCompiler.cacheSize());
    FastFormulaCompiler.compile("[amount] + " + max, List.of("amount"), rowMeta, false);
    assertTrue(
        FastFormulaCompiler.cacheSize() <= max,
        "cache should not grow past the configured max size");
  }
}
