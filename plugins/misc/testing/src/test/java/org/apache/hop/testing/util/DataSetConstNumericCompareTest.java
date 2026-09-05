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

package org.apache.hop.testing.util;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.math.BigDecimal;
import java.text.DecimalFormat;
import org.apache.hop.core.HopEnvironment;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.exception.HopValueException;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.core.row.value.ValueMetaBigNumber;
import org.apache.hop.core.row.value.ValueMetaNumber;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

class DataSetConstNumericCompareTest {

  @BeforeAll
  static void initHop() throws HopException {
    HopEnvironment.init();
  }

  @Test
  void maskForLength7Precision2MatchesDocumentedPattern() {
    assertEquals("00000.00;-0000.00", DataSetConst.buildNumericCompareMask(7, 2));
  }

  @Test
  void maskForLength7Precision4HasThreeIntegerDigits() {
    assertEquals("000.0000;-00.0000", DataSetConst.buildNumericCompareMask(7, 4));
  }

  @Test
  void maskFallsBackWhenLengthOrPrecisionMissing() {
    assertEquals(
        DataSetConst.NUMERIC_COMPARE_MASK_DEFAULT, DataSetConst.buildNumericCompareMask(-1, -1));
    assertEquals(
        DataSetConst.NUMERIC_COMPARE_MASK_DEFAULT, DataSetConst.buildNumericCompareMask(0, 2));
    assertEquals(
        DataSetConst.NUMERIC_COMPARE_MASK_DEFAULT, DataSetConst.buildNumericCompareMask(7, -1));
  }

  @Test
  void sqrtOfTwoMatchesGoldenAtDeclaredPrecision() throws HopValueException {
    IValueMeta meta = new ValueMetaNumber("sqrt", 7, 4);
    DecimalFormat format =
        DataSetConst.createNumericCompareFormat(
            DataSetConst.buildNumericCompareMask(meta.getLength(), meta.getPrecision()));

    assertTrue(
        DataSetConst.formattedNumericValuesEqual(format, meta, Math.sqrt(2), 1.4142),
        "sqrt(2) and 1.4142 must compare equal at length 7 precision 4");
    assertEquals("001.4142", format.format(Math.sqrt(2)));
    assertEquals("001.4142", format.format(1.4142));
  }

  @Test
  void sqrtOfTwoDoesNotMatchGoldenWithoutPrecision() throws HopValueException {
    IValueMeta meta = new ValueMetaNumber("sqrt");
    DecimalFormat format =
        DataSetConst.createNumericCompareFormat(
            DataSetConst.buildNumericCompareMask(meta.getLength(), meta.getPrecision()));

    assertFalse(
        DataSetConst.formattedNumericValuesEqual(format, meta, Math.sqrt(2), 1.4142),
        "without length/precision the extra digits of sqrt(2) must still fail");
  }

  @Test
  void valuesThatDifferAtDeclaredPrecisionAreNotEqual() throws HopValueException {
    IValueMeta meta = new ValueMetaNumber("amount", 7, 2);
    DecimalFormat format =
        DataSetConst.createNumericCompareFormat(DataSetConst.buildNumericCompareMask(7, 2));

    assertFalse(DataSetConst.formattedNumericValuesEqual(format, meta, 12.34, 12.35));
    assertTrue(DataSetConst.formattedNumericValuesEqual(format, meta, 12.344, 12.336));
  }

  @Test
  void bigNumberUsesSameMask() throws HopValueException {
    IValueMeta meta = new ValueMetaBigNumber("sqrt", 7, 4);
    DecimalFormat format =
        DataSetConst.createNumericCompareFormat(DataSetConst.buildNumericCompareMask(7, 4));

    assertTrue(
        DataSetConst.formattedNumericValuesEqual(
            format, meta, new BigDecimal("1.4142135623730951"), new BigDecimal("1.4142")));
  }

  @Test
  void defaultMaskTreatsOneAndOnePointZeroAsEqual() throws HopValueException {
    IValueMeta meta = new ValueMetaNumber("n");
    DecimalFormat format =
        DataSetConst.createNumericCompareFormat(DataSetConst.NUMERIC_COMPARE_MASK_DEFAULT);

    assertTrue(DataSetConst.formattedNumericValuesEqual(format, meta, 1.0d, 1.00d));
  }
}
