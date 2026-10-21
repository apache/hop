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

package org.apache.hop.openformula.util;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.math.MathContext;
import java.math.RoundingMode;
import org.apache.hop.openformula.EvaluationException;
import org.apache.hop.openformula.error.FormulaErrorValue;

/** High-precision numerical operations for OpenFormula evaluation. */
public final class NumberUtil {
  public static final int GLOBAL_SCALE = 40;
  public static final MathContext MATH_CONTEXT =
      new MathContext(GLOBAL_SCALE, RoundingMode.HALF_UP);
  private static final BigDecimal EPSILON = new BigDecimal("0.000000001");

  private NumberUtil() {}

  public static BigDecimal getAsBigDecimal(Object number) {
    if (number == null) {
      return BigDecimal.ZERO;
    }
    if (number instanceof BigDecimal bd) {
      return bd;
    }
    if (number instanceof BigInteger bi) {
      return new BigDecimal(bi);
    }
    if (number instanceof Number n) {
      return new BigDecimal(n.toString());
    }
    return new BigDecimal(number.toString().trim());
  }

  private static final BigDecimal INT_TEST_DELTA =
      new BigDecimal("0.00000000000000000000000000000000005");
  public static final int TUNE_SCALE = 34;

  public static BigDecimal divide(BigDecimal n1, BigDecimal n2) throws EvaluationException {
    if (n2.signum() == 0) {
      throw new EvaluationException(FormulaErrorValue.ERROR_ARITHMETIC_VALUE);
    }
    return normalize(n1.divide(n2, GLOBAL_SCALE, RoundingMode.HALF_UP));
  }

  public static BigDecimal multiply(BigDecimal n1, BigDecimal n2) {
    return normalize(n1.multiply(n2));
  }

  public static BigDecimal add(BigDecimal n1, BigDecimal n2) {
    return normalize(n1.add(n2));
  }

  public static BigDecimal subtract(BigDecimal n1, BigDecimal n2) {
    return normalize(n1.subtract(n2));
  }

  public static BigDecimal power(BigDecimal base, BigDecimal exponent) throws EvaluationException {
    try {
      double dBase = base.doubleValue();
      double dExp = exponent.doubleValue();
      double result = Math.pow(dBase, dExp);
      if (Double.isNaN(result) || Double.isInfinite(result)) {
        throw new EvaluationException(FormulaErrorValue.ERROR_ARITHMETIC_VALUE);
      }
      return normalize(BigDecimal.valueOf(result));
    } catch (Exception e) {
      throw new EvaluationException(FormulaErrorValue.ERROR_ARITHMETIC_VALUE, e);
    }
  }

  public static BigDecimal normalize(BigDecimal bd) {
    if (bd == null) return BigDecimal.ZERO;
    if (bd.signum() == 0) return BigDecimal.ZERO;
    BigDecimal stripped = bd.stripTrailingZeros();
    if (stripped.scale() < 0) {
      return stripped.setScale(0, RoundingMode.UNNECESSARY);
    }
    return stripped;
  }

  public static BigDecimal performMinuteRounding(BigDecimal minutes) {
    if (minutes == null) return BigDecimal.ZERO;
    BigDecimal rounded = minutes.setScale(0, RoundingMode.HALF_UP);
    BigDecimal diff = minutes.subtract(rounded).abs();
    if (diff.compareTo(EPSILON) < 0) {
      return rounded;
    }
    return minutes.setScale(0, RoundingMode.DOWN);
  }

  public static BigDecimal performTuneRounding(BigDecimal n) {
    if (n == null) return null;
    if (n.signum() == 0) return BigDecimal.ZERO;
    if (n.signum() > 0) {
      return normalize(n.add(INT_TEST_DELTA).setScale(TUNE_SCALE, RoundingMode.DOWN));
    } else {
      return normalize(n.subtract(INT_TEST_DELTA).setScale(TUNE_SCALE, RoundingMode.DOWN));
    }
  }
}
