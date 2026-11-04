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

package org.apache.hop.core.row;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.math.BigDecimal;
import org.apache.hop.core.row.value.ValueMetaBigNumber;
import org.apache.hop.core.row.value.ValueMetaBoolean;
import org.apache.hop.core.row.value.ValueMetaInteger;
import org.apache.hop.core.row.value.ValueMetaString;
import org.junit.jupiter.api.Test;

/**
 * Calculator functions read every value with its own metadata and keep BigNumber and Integer
 * precision. Issue #9259.
 */
class ValueDataUtilPrecisionTest {

  private final IValueMeta big = new ValueMetaBigNumber("b");
  private final IValueMeta integer = new ValueMetaInteger("i");

  @Test
  void plus3ReadsTheThirdValueWithItsOwnMetadata() throws Exception {
    IValueMeta bool = new ValueMetaBoolean("a");
    IValueMeta string = new ValueMetaString("c");

    // dataC is "Y" described by a String meta; reading it through the Boolean meta of B fails
    assertEquals(Boolean.TRUE, ValueDataUtil.plus3(bool, false, bool, false, string, "Y"));
  }

  @Test
  void remainderReadsTheDivisorWithItsOwnMetadata() throws Exception {
    IValueMeta divisorAsString = new ValueMetaString("s");

    Object result = ValueDataUtil.remainder(big, new BigDecimal("10"), divisorAsString, "3");

    assertEquals(0, BigDecimal.ONE.compareTo((BigDecimal) result));
  }

  @Test
  void bigNumberFunctionsKeepPrecision() throws Exception {
    // 2^53 + 1 cannot be represented as a double
    BigDecimal huge = new BigDecimal("9007199254740993.4");

    assertEquals(new BigDecimal("9007199254740993"), ValueDataUtil.round(big, huge));
    assertEquals(new BigDecimal("9007199254740994.0"), ValueDataUtil.ceil(big, huge));
    assertEquals(new BigDecimal("9007199254740993.0"), ValueDataUtil.floor(big, huge));
    assertEquals(huge, ValueDataUtil.abs(big, huge.negate()));
  }

  @Test
  void bigNumberRoundKeepsMathRoundSemantics() throws Exception {
    assertEquals(new BigDecimal("3"), ValueDataUtil.round(big, new BigDecimal("2.5")));
    assertEquals(new BigDecimal("-2"), ValueDataUtil.round(big, new BigDecimal("-2.5")));
    assertEquals(new BigDecimal("-3"), ValueDataUtil.round(big, new BigDecimal("-2.6")));
  }

  @Test
  void bigNumberCeilAndFloorKeepTheirScale() throws Exception {
    // the result has always had one decimal, e.g. 6.0, which shows in JSON and other native output
    assertEquals(new BigDecimal("6.0"), ValueDataUtil.ceil(big, new BigDecimal("5.2")));
    assertEquals(new BigDecimal("5.0"), ValueDataUtil.floor(big, new BigDecimal("5.8")));
  }

  @Test
  void manyDecimalsGiveTheSameResultAsTheDoubleFunctions() throws Exception {
    String[] values = {
      "2.567",
      "-2.567",
      "5.001",
      "-5.001",
      "5.999",
      "-5.999",
      "0.4999",
      "-0.5001",
      "7.5000001",
      "-7.4999999",
      "123.456789",
      "-0.0001"
    };
    for (String text : values) {
      BigDecimal value = new BigDecimal(text);
      double d = value.doubleValue();

      assertEquals(
          0,
          BigDecimal.valueOf(Math.round(d)).compareTo((BigDecimal) ValueDataUtil.round(big, value)),
          "round " + text);
      assertEquals(
          0,
          BigDecimal.valueOf(Math.ceil(d)).compareTo((BigDecimal) ValueDataUtil.ceil(big, value)),
          "ceil " + text);
      assertEquals(
          0,
          BigDecimal.valueOf(Math.floor(d)).compareTo((BigDecimal) ValueDataUtil.floor(big, value)),
          "floor " + text);
      assertEquals(
          0, value.abs().compareTo((BigDecimal) ValueDataUtil.abs(big, value)), "abs " + text);
      assertEquals(1, ((BigDecimal) ValueDataUtil.ceil(big, value)).scale(), "ceil scale " + text);
      assertEquals(
          1, ((BigDecimal) ValueDataUtil.floor(big, value)).scale(), "floor scale " + text);
    }
  }

  @Test
  void integerAbsKeepsPrecision() throws Exception {
    assertEquals(9007199254740993L, ValueDataUtil.abs(integer, -9007199254740993L));
  }
}
