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
import java.math.RoundingMode;
import java.util.ArrayList;
import java.util.List;
import org.apache.hop.openformula.EvaluationException;
import org.apache.hop.openformula.FormulaContext;
import org.apache.hop.openformula.error.FormulaErrorValue;
import org.apache.hop.openformula.function.DefaultFunctionRegistry;
import org.apache.hop.openformula.function.Function;
import org.apache.hop.openformula.function.FunctionCategory;
import org.apache.hop.openformula.function.FunctionDescription;
import org.apache.hop.openformula.function.ParameterCallback;
import org.apache.hop.openformula.typing.DataType;
import org.apache.hop.openformula.typing.TypedValue;
import org.apache.hop.openformula.util.NumberUtil;

public final class MathFunctions {
  private MathFunctions() {}

  public static void registerAll(DefaultFunctionRegistry registry) {
    registry.registerFunction(
        new AbsFunction(),
        new FunctionDescription(
            "ABS",
            FunctionCategory.MATH,
            "Absolute value of a number",
            DataType.NUMBER,
            1,
            false,
            List.of("number"),
            List.of("Number")));
    registry.registerFunction(
        new AcosFunction(),
        new FunctionDescription(
            "ACOS",
            FunctionCategory.MATH,
            "Arccosine of a number",
            DataType.NUMBER,
            1,
            false,
            List.of("number"),
            List.of("Cosine between -1 and 1")));
    registry.registerFunction(
        new AcoshFunction(),
        new FunctionDescription(
            "ACOSH",
            FunctionCategory.MATH,
            "Inverse hyperbolic cosine",
            DataType.NUMBER,
            1,
            false,
            List.of("number"),
            List.of("Number >= 1")));
    registry.registerFunction(
        new AsinFunction(),
        new FunctionDescription(
            "ASIN",
            FunctionCategory.MATH,
            "Arcsine of a number",
            DataType.NUMBER,
            1,
            false,
            List.of("number"),
            List.of("Sine between -1 and 1")));
    registry.registerFunction(
        new AtanFunction(),
        new FunctionDescription(
            "ATAN",
            FunctionCategory.MATH,
            "Arctangent of a number",
            DataType.NUMBER,
            1,
            false,
            List.of("number"),
            List.of("Number")));
    registry.registerFunction(
        new Atan2Function(),
        new FunctionDescription(
            "ATAN2",
            FunctionCategory.MATH,
            "Arctangent from x and y coordinates",
            DataType.NUMBER,
            2,
            false,
            List.of("x", "y"),
            List.of("x coordinate", "y coordinate")));
    registry.registerFunction(
        new AverageFunction(),
        new FunctionDescription(
            "AVERAGE",
            FunctionCategory.MATH,
            "Average of numerical values",
            DataType.NUMBER,
            1,
            true,
            List.of("number"),
            List.of("Numbers")));
    registry.registerFunction(
        new AverageAFunction(),
        new FunctionDescription(
            "AVERAGEA",
            FunctionCategory.MATH,
            "Average of all values",
            DataType.NUMBER,
            1,
            true,
            List.of("value"),
            List.of("Values")));
    registry.registerFunction(
        new CosFunction(),
        new FunctionDescription(
            "COS",
            FunctionCategory.MATH,
            "Cosine of an angle",
            DataType.NUMBER,
            1,
            false,
            List.of("number"),
            List.of("Angle in radians")));
    registry.registerFunction(
        new EvenFunction(),
        new FunctionDescription(
            "EVEN",
            FunctionCategory.MATH,
            "Rounds up to the nearest even integer",
            DataType.NUMBER,
            1,
            false,
            List.of("number"),
            List.of("Number to round")));
    registry.registerFunction(
        new ExpFunction(),
        new FunctionDescription(
            "EXP",
            FunctionCategory.MATH,
            "e raised to the power of number",
            DataType.NUMBER,
            1,
            false,
            List.of("number"),
            List.of("Exponent")));
    registry.registerFunction(
        new LnFunction(),
        new FunctionDescription(
            "LN",
            FunctionCategory.MATH,
            "Natural logarithm of a number",
            DataType.NUMBER,
            1,
            false,
            List.of("number"),
            List.of("Positive number")));
    registry.registerFunction(
        new LogFunction(),
        new FunctionDescription(
            "LOG",
            FunctionCategory.MATH,
            "Logarithm of a number to a specified base",
            DataType.NUMBER,
            1,
            false,
            List.of("number", "base"),
            List.of("Number", "Base (defaults to 10)")));
    registry.registerFunction(
        new Log10Function(),
        new FunctionDescription(
            "LOG10",
            FunctionCategory.MATH,
            "Base-10 logarithm",
            DataType.NUMBER,
            1,
            false,
            List.of("number"),
            List.of("Positive number")));
    registry.registerFunction(
        new MaxFunction(),
        new FunctionDescription(
            "MAX",
            FunctionCategory.MATH,
            "Maximum value in a list of numbers",
            DataType.NUMBER,
            1,
            true,
            List.of("number"),
            List.of("Numbers")));
    registry.registerFunction(
        new MaxAFunction(),
        new FunctionDescription(
            "MAXA",
            FunctionCategory.MATH,
            "Maximum value including text/logicals",
            DataType.NUMBER,
            1,
            true,
            List.of("value"),
            List.of("Values")));
    registry.registerFunction(
        new MinFunction(),
        new FunctionDescription(
            "MIN",
            FunctionCategory.MATH,
            "Minimum value in a list of numbers",
            DataType.NUMBER,
            1,
            true,
            List.of("number"),
            List.of("Numbers")));
    registry.registerFunction(
        new MinAFunction(),
        new FunctionDescription(
            "MINA",
            FunctionCategory.MATH,
            "Minimum value including text/logicals",
            DataType.NUMBER,
            1,
            true,
            List.of("value"),
            List.of("Values")));
    registry.registerFunction(
        new ModFunction(),
        new FunctionDescription(
            "MOD",
            FunctionCategory.MATH,
            "Remainder of division",
            DataType.NUMBER,
            2,
            false,
            List.of("number", "divisor"),
            List.of("Dividend", "Divisor")));
    registry.registerFunction(
        new NFunction(),
        new FunctionDescription(
            "N",
            FunctionCategory.MATH,
            "Converts value to a number",
            DataType.NUMBER,
            1,
            false,
            List.of("value"),
            List.of("Value to convert")));
    registry.registerFunction(
        new OddFunction(),
        new FunctionDescription(
            "ODD",
            FunctionCategory.MATH,
            "Rounds up to the nearest odd integer",
            DataType.NUMBER,
            1,
            false,
            List.of("number"),
            List.of("Number to round")));
    registry.registerFunction(
        new PiFunction(),
        new FunctionDescription(
            "PI",
            FunctionCategory.MATH,
            "Returns the value of PI",
            DataType.NUMBER,
            0,
            false,
            List.of(),
            List.of()));
    registry.registerFunction(
        new PowerFunction(),
        new FunctionDescription(
            "POWER",
            FunctionCategory.MATH,
            "Calculates base raised to power",
            DataType.NUMBER,
            2,
            false,
            List.of("base", "power"),
            List.of("Base", "Exponent")));
    registry.registerFunction(
        new SinFunction(),
        new FunctionDescription(
            "SIN",
            FunctionCategory.MATH,
            "Sine of an angle",
            DataType.NUMBER,
            1,
            false,
            List.of("number"),
            List.of("Angle in radians")));
    registry.registerFunction(
        new SqrtFunction(),
        new FunctionDescription(
            "SQRT",
            FunctionCategory.MATH,
            "Square root of a number",
            DataType.NUMBER,
            1,
            false,
            List.of("number"),
            List.of("Positive number")));
    registry.registerFunction(
        new SumFunction(),
        new FunctionDescription(
            "SUM",
            FunctionCategory.MATH,
            "Sum of numerical values",
            DataType.NUMBER,
            1,
            true,
            List.of("number"),
            List.of("Numbers to sum")));
    registry.registerFunction(
        new SumAFunction(),
        new FunctionDescription(
            "SUMA",
            FunctionCategory.MATH,
            "Sum of values including text/logicals",
            DataType.NUMBER,
            1,
            true,
            List.of("value"),
            List.of("Values to sum")));
    registry.registerFunction(
        new VarFunction(),
        new FunctionDescription(
            "VAR",
            FunctionCategory.MATH,
            "Sample variance",
            DataType.NUMBER,
            1,
            true,
            List.of("number"),
            List.of("Numbers")));
  }

  public static class AbsFunction implements Function {
    @Override
    public String getCanonicalName() {
      return "ABS";
    }

    @Override
    public DataType getReturnType() {
      return DataType.NUMBER;
    }

    @Override
    public TypedValue evaluate(FormulaContext context, ParameterCallback params)
        throws EvaluationException {
      if (params.getParameterCount() != 1)
        throw new EvaluationException(FormulaErrorValue.ERROR_ARGUMENTS_VALUE);
      BigDecimal n =
          context.getTypeRegistry().convertToNumber(params.getType(0), params.getValue(0));
      return TypedValue.ofNumber(n.abs());
    }
  }

  public static class AcosFunction implements Function {
    @Override
    public String getCanonicalName() {
      return "ACOS";
    }

    @Override
    public DataType getReturnType() {
      return DataType.NUMBER;
    }

    @Override
    public TypedValue evaluate(FormulaContext context, ParameterCallback params)
        throws EvaluationException {
      if (params.getParameterCount() != 1)
        throw new EvaluationException(FormulaErrorValue.ERROR_ARGUMENTS_VALUE);
      BigDecimal n =
          context.getTypeRegistry().convertToNumber(params.getType(0), params.getValue(0));
      double d = n.doubleValue();
      if (d < -1.0 || d > 1.0)
        throw new EvaluationException(FormulaErrorValue.ERROR_INVALID_ARGUMENT_VALUE);
      return TypedValue.ofNumber(NumberUtil.normalize(BigDecimal.valueOf(Math.acos(d))));
    }
  }

  public static class AcoshFunction implements Function {
    @Override
    public String getCanonicalName() {
      return "ACOSH";
    }

    @Override
    public DataType getReturnType() {
      return DataType.NUMBER;
    }

    @Override
    public TypedValue evaluate(FormulaContext context, ParameterCallback params)
        throws EvaluationException {
      if (params.getParameterCount() != 1)
        throw new EvaluationException(FormulaErrorValue.ERROR_ARGUMENTS_VALUE);
      BigDecimal n =
          context.getTypeRegistry().convertToNumber(params.getType(0), params.getValue(0));
      double d = n.doubleValue();
      if (d < 1.0) throw new EvaluationException(FormulaErrorValue.ERROR_INVALID_ARGUMENT_VALUE);
      double res = Math.log(d + Math.sqrt(d * d - 1.0));
      return TypedValue.ofNumber(NumberUtil.normalize(BigDecimal.valueOf(res)));
    }
  }

  public static class AsinFunction implements Function {
    @Override
    public String getCanonicalName() {
      return "ASIN";
    }

    @Override
    public DataType getReturnType() {
      return DataType.NUMBER;
    }

    @Override
    public TypedValue evaluate(FormulaContext context, ParameterCallback params)
        throws EvaluationException {
      if (params.getParameterCount() != 1)
        throw new EvaluationException(FormulaErrorValue.ERROR_ARGUMENTS_VALUE);
      BigDecimal n =
          context.getTypeRegistry().convertToNumber(params.getType(0), params.getValue(0));
      double d = n.doubleValue();
      if (d < -1.0 || d > 1.0)
        throw new EvaluationException(FormulaErrorValue.ERROR_INVALID_ARGUMENT_VALUE);
      return TypedValue.ofNumber(NumberUtil.normalize(BigDecimal.valueOf(Math.asin(d))));
    }
  }

  public static class AtanFunction implements Function {
    @Override
    public String getCanonicalName() {
      return "ATAN";
    }

    @Override
    public DataType getReturnType() {
      return DataType.NUMBER;
    }

    @Override
    public TypedValue evaluate(FormulaContext context, ParameterCallback params)
        throws EvaluationException {
      if (params.getParameterCount() != 1)
        throw new EvaluationException(FormulaErrorValue.ERROR_ARGUMENTS_VALUE);
      BigDecimal n =
          context.getTypeRegistry().convertToNumber(params.getType(0), params.getValue(0));
      return TypedValue.ofNumber(
          NumberUtil.normalize(BigDecimal.valueOf(Math.atan(n.doubleValue()))));
    }
  }

  public static class Atan2Function implements Function {
    @Override
    public String getCanonicalName() {
      return "ATAN2";
    }

    @Override
    public DataType getReturnType() {
      return DataType.NUMBER;
    }

    @Override
    public TypedValue evaluate(FormulaContext context, ParameterCallback params)
        throws EvaluationException {
      if (params.getParameterCount() != 2)
        throw new EvaluationException(FormulaErrorValue.ERROR_ARGUMENTS_VALUE);
      BigDecimal x =
          context.getTypeRegistry().convertToNumber(params.getType(0), params.getValue(0));
      BigDecimal y =
          context.getTypeRegistry().convertToNumber(params.getType(1), params.getValue(1));
      if (x.signum() == 0 && y.signum() == 0)
        throw new EvaluationException(FormulaErrorValue.ERROR_ARITHMETIC_VALUE);
      return TypedValue.ofNumber(
          NumberUtil.normalize(BigDecimal.valueOf(Math.atan2(y.doubleValue(), x.doubleValue()))));
    }
  }

  public static class AverageFunction implements Function {
    @Override
    public String getCanonicalName() {
      return "AVERAGE";
    }

    @Override
    public DataType getReturnType() {
      return DataType.NUMBER;
    }

    @Override
    public TypedValue evaluate(FormulaContext context, ParameterCallback params)
        throws EvaluationException {
      if (params.getParameterCount() == 0)
        throw new EvaluationException(FormulaErrorValue.ERROR_ARGUMENTS_VALUE);
      BigDecimal sum = BigDecimal.ZERO;
      long count = 0;
      for (int i = 0; i < params.getParameterCount(); i++) {
        List<BigDecimal> nums =
            context
                .getTypeRegistry()
                .convertToNumberSequence(params.getType(i), params.getValue(i), false);
        for (BigDecimal bd : nums) {
          sum = sum.add(bd);
          count++;
        }
      }
      if (count == 0) throw new EvaluationException(FormulaErrorValue.ERROR_ARITHMETIC_VALUE);
      return TypedValue.ofNumber(NumberUtil.divide(sum, BigDecimal.valueOf(count)));
    }
  }

  public static class AverageAFunction implements Function {
    @Override
    public String getCanonicalName() {
      return "AVERAGEA";
    }

    @Override
    public DataType getReturnType() {
      return DataType.NUMBER;
    }

    @Override
    public TypedValue evaluate(FormulaContext context, ParameterCallback params)
        throws EvaluationException {
      if (params.getParameterCount() == 0)
        throw new EvaluationException(FormulaErrorValue.ERROR_ARGUMENTS_VALUE);
      BigDecimal sum = BigDecimal.ZERO;
      long count = 0;
      for (int i = 0; i < params.getParameterCount(); i++) {
        List<BigDecimal> nums =
            context
                .getTypeRegistry()
                .convertToNumberSequence(params.getType(i), params.getValue(i), true);
        for (BigDecimal bd : nums) {
          sum = sum.add(bd);
          count++;
        }
      }
      if (count == 0) throw new EvaluationException(FormulaErrorValue.ERROR_ARITHMETIC_VALUE);
      return TypedValue.ofNumber(NumberUtil.divide(sum, BigDecimal.valueOf(count)));
    }
  }

  public static class CosFunction implements Function {
    @Override
    public String getCanonicalName() {
      return "COS";
    }

    @Override
    public DataType getReturnType() {
      return DataType.NUMBER;
    }

    @Override
    public TypedValue evaluate(FormulaContext context, ParameterCallback params)
        throws EvaluationException {
      if (params.getParameterCount() != 1)
        throw new EvaluationException(FormulaErrorValue.ERROR_ARGUMENTS_VALUE);
      BigDecimal n =
          context.getTypeRegistry().convertToNumber(params.getType(0), params.getValue(0));
      return TypedValue.ofNumber(
          NumberUtil.normalize(BigDecimal.valueOf(Math.cos(n.doubleValue()))));
    }
  }

  public static class EvenFunction implements Function {
    @Override
    public String getCanonicalName() {
      return "EVEN";
    }

    @Override
    public DataType getReturnType() {
      return DataType.NUMBER;
    }

    @Override
    public TypedValue evaluate(FormulaContext context, ParameterCallback params)
        throws EvaluationException {
      if (params.getParameterCount() != 1)
        throw new EvaluationException(FormulaErrorValue.ERROR_ARGUMENTS_VALUE);
      BigDecimal n =
          context.getTypeRegistry().convertToNumber(params.getType(0), params.getValue(0));
      long val = n.setScale(0, RoundingMode.UP).longValue();
      if (val % 2 != 0) {
        val = (val >= 0) ? val + 1 : val - 1;
      }
      return TypedValue.ofNumber(BigDecimal.valueOf(val));
    }
  }

  public static class ExpFunction implements Function {
    @Override
    public String getCanonicalName() {
      return "EXP";
    }

    @Override
    public DataType getReturnType() {
      return DataType.NUMBER;
    }

    @Override
    public TypedValue evaluate(FormulaContext context, ParameterCallback params)
        throws EvaluationException {
      if (params.getParameterCount() != 1)
        throw new EvaluationException(FormulaErrorValue.ERROR_ARGUMENTS_VALUE);
      BigDecimal n =
          context.getTypeRegistry().convertToNumber(params.getType(0), params.getValue(0));
      return TypedValue.ofNumber(
          NumberUtil.normalize(BigDecimal.valueOf(Math.exp(n.doubleValue()))));
    }
  }

  public static class LnFunction implements Function {
    @Override
    public String getCanonicalName() {
      return "LN";
    }

    @Override
    public DataType getReturnType() {
      return DataType.NUMBER;
    }

    @Override
    public TypedValue evaluate(FormulaContext context, ParameterCallback params)
        throws EvaluationException {
      if (params.getParameterCount() != 1)
        throw new EvaluationException(FormulaErrorValue.ERROR_ARGUMENTS_VALUE);
      BigDecimal n =
          context.getTypeRegistry().convertToNumber(params.getType(0), params.getValue(0));
      if (n.signum() <= 0)
        throw new EvaluationException(FormulaErrorValue.ERROR_INVALID_ARGUMENT_VALUE);
      return TypedValue.ofNumber(
          NumberUtil.normalize(BigDecimal.valueOf(Math.log(n.doubleValue()))));
    }
  }

  public static class LogFunction implements Function {
    @Override
    public String getCanonicalName() {
      return "LOG";
    }

    @Override
    public DataType getReturnType() {
      return DataType.NUMBER;
    }

    @Override
    public TypedValue evaluate(FormulaContext context, ParameterCallback params)
        throws EvaluationException {
      if (params.getParameterCount() < 1 || params.getParameterCount() > 2)
        throw new EvaluationException(FormulaErrorValue.ERROR_ARGUMENTS_VALUE);
      BigDecimal n =
          context.getTypeRegistry().convertToNumber(params.getType(0), params.getValue(0));
      if (n.signum() <= 0)
        throw new EvaluationException(FormulaErrorValue.ERROR_INVALID_ARGUMENT_VALUE);
      double base = 10.0;
      if (params.getParameterCount() == 2) {
        BigDecimal b =
            context.getTypeRegistry().convertToNumber(params.getType(1), params.getValue(1));
        if (b.signum() <= 0 || b.compareTo(BigDecimal.ONE) == 0)
          throw new EvaluationException(FormulaErrorValue.ERROR_INVALID_ARGUMENT_VALUE);
        base = b.doubleValue();
      }
      double res = Math.log(n.doubleValue()) / Math.log(base);
      return TypedValue.ofNumber(NumberUtil.normalize(BigDecimal.valueOf(res)));
    }
  }

  public static class Log10Function implements Function {
    @Override
    public String getCanonicalName() {
      return "LOG10";
    }

    @Override
    public DataType getReturnType() {
      return DataType.NUMBER;
    }

    @Override
    public TypedValue evaluate(FormulaContext context, ParameterCallback params)
        throws EvaluationException {
      if (params.getParameterCount() != 1)
        throw new EvaluationException(FormulaErrorValue.ERROR_ARGUMENTS_VALUE);
      BigDecimal n =
          context.getTypeRegistry().convertToNumber(params.getType(0), params.getValue(0));
      if (n.signum() <= 0)
        throw new EvaluationException(FormulaErrorValue.ERROR_INVALID_ARGUMENT_VALUE);
      return TypedValue.ofNumber(
          NumberUtil.normalize(BigDecimal.valueOf(Math.log10(n.doubleValue()))));
    }
  }

  public static class MaxFunction implements Function {
    @Override
    public String getCanonicalName() {
      return "MAX";
    }

    @Override
    public DataType getReturnType() {
      return DataType.NUMBER;
    }

    @Override
    public TypedValue evaluate(FormulaContext context, ParameterCallback params)
        throws EvaluationException {
      BigDecimal max = null;
      for (int i = 0; i < params.getParameterCount(); i++) {
        List<BigDecimal> nums =
            context
                .getTypeRegistry()
                .convertToNumberSequence(params.getType(i), params.getValue(i), false);
        for (BigDecimal n : nums) {
          if (max == null || n.compareTo(max) > 0) max = n;
        }
      }
      return TypedValue.ofNumber(max != null ? max : BigDecimal.ZERO);
    }
  }

  public static class MaxAFunction implements Function {
    @Override
    public String getCanonicalName() {
      return "MAXA";
    }

    @Override
    public DataType getReturnType() {
      return DataType.NUMBER;
    }

    @Override
    public TypedValue evaluate(FormulaContext context, ParameterCallback params)
        throws EvaluationException {
      BigDecimal max = null;
      for (int i = 0; i < params.getParameterCount(); i++) {
        List<BigDecimal> nums =
            context
                .getTypeRegistry()
                .convertToNumberSequence(params.getType(i), params.getValue(i), true);
        for (BigDecimal n : nums) {
          if (max == null || n.compareTo(max) > 0) max = n;
        }
      }
      return TypedValue.ofNumber(max != null ? max : BigDecimal.ZERO);
    }
  }

  public static class MinFunction implements Function {
    @Override
    public String getCanonicalName() {
      return "MIN";
    }

    @Override
    public DataType getReturnType() {
      return DataType.NUMBER;
    }

    @Override
    public TypedValue evaluate(FormulaContext context, ParameterCallback params)
        throws EvaluationException {
      BigDecimal min = null;
      for (int i = 0; i < params.getParameterCount(); i++) {
        List<BigDecimal> nums =
            context
                .getTypeRegistry()
                .convertToNumberSequence(params.getType(i), params.getValue(i), false);
        for (BigDecimal n : nums) {
          if (min == null || n.compareTo(min) < 0) min = n;
        }
      }
      return TypedValue.ofNumber(min != null ? min : BigDecimal.ZERO);
    }
  }

  public static class MinAFunction implements Function {
    @Override
    public String getCanonicalName() {
      return "MINA";
    }

    @Override
    public DataType getReturnType() {
      return DataType.NUMBER;
    }

    @Override
    public TypedValue evaluate(FormulaContext context, ParameterCallback params)
        throws EvaluationException {
      BigDecimal min = null;
      for (int i = 0; i < params.getParameterCount(); i++) {
        List<BigDecimal> nums =
            context
                .getTypeRegistry()
                .convertToNumberSequence(params.getType(i), params.getValue(i), true);
        for (BigDecimal n : nums) {
          if (min == null || n.compareTo(min) < 0) min = n;
        }
      }
      return TypedValue.ofNumber(min != null ? min : BigDecimal.ZERO);
    }
  }

  public static class ModFunction implements Function {
    @Override
    public String getCanonicalName() {
      return "MOD";
    }

    @Override
    public DataType getReturnType() {
      return DataType.NUMBER;
    }

    @Override
    public TypedValue evaluate(FormulaContext context, ParameterCallback params)
        throws EvaluationException {
      if (params.getParameterCount() != 2)
        throw new EvaluationException(FormulaErrorValue.ERROR_ARGUMENTS_VALUE);
      BigDecimal n =
          context.getTypeRegistry().convertToNumber(params.getType(0), params.getValue(0));
      BigDecimal d =
          context.getTypeRegistry().convertToNumber(params.getType(1), params.getValue(1));
      if (d.signum() == 0) throw new EvaluationException(FormulaErrorValue.ERROR_ARITHMETIC_VALUE);
      BigDecimal quot = n.divide(d, 0, RoundingMode.FLOOR);
      BigDecimal mod = n.subtract(d.multiply(quot));
      return TypedValue.ofNumber(NumberUtil.normalize(mod));
    }
  }

  public static class NFunction implements Function {
    @Override
    public String getCanonicalName() {
      return "N";
    }

    @Override
    public DataType getReturnType() {
      return DataType.NUMBER;
    }

    @Override
    public TypedValue evaluate(FormulaContext context, ParameterCallback params)
        throws EvaluationException {
      if (params.getParameterCount() != 1)
        throw new EvaluationException(FormulaErrorValue.ERROR_ARGUMENTS_VALUE);
      Object val = params.getValue(0);
      if (val instanceof Number)
        return TypedValue.ofNumber(
            context.getTypeRegistry().convertToNumber(params.getType(0), val));
      if (val instanceof Boolean b)
        return TypedValue.ofNumber(b ? BigDecimal.ONE : BigDecimal.ZERO);
      if (val instanceof java.util.Date)
        return TypedValue.ofNumber(
            context.getTypeRegistry().convertToNumber(params.getType(0), val));
      return TypedValue.ofNumber(BigDecimal.ZERO);
    }
  }

  public static class OddFunction implements Function {
    @Override
    public String getCanonicalName() {
      return "ODD";
    }

    @Override
    public DataType getReturnType() {
      return DataType.NUMBER;
    }

    @Override
    public TypedValue evaluate(FormulaContext context, ParameterCallback params)
        throws EvaluationException {
      if (params.getParameterCount() != 1)
        throw new EvaluationException(FormulaErrorValue.ERROR_ARGUMENTS_VALUE);
      BigDecimal n =
          context.getTypeRegistry().convertToNumber(params.getType(0), params.getValue(0));
      long val = n.setScale(0, RoundingMode.UP).longValue();
      if (val % 2 == 0) {
        val = (val >= 0) ? val + 1 : val - 1;
      }
      return TypedValue.ofNumber(BigDecimal.valueOf(val));
    }
  }

  public static class PiFunction implements Function {
    @Override
    public String getCanonicalName() {
      return "PI";
    }

    @Override
    public DataType getReturnType() {
      return DataType.NUMBER;
    }

    @Override
    public TypedValue evaluate(FormulaContext context, ParameterCallback params) {
      return TypedValue.ofNumber(NumberUtil.normalize(BigDecimal.valueOf(Math.PI)));
    }
  }

  public static class PowerFunction implements Function {
    @Override
    public String getCanonicalName() {
      return "POWER";
    }

    @Override
    public DataType getReturnType() {
      return DataType.NUMBER;
    }

    @Override
    public TypedValue evaluate(FormulaContext context, ParameterCallback params)
        throws EvaluationException {
      if (params.getParameterCount() != 2)
        throw new EvaluationException(FormulaErrorValue.ERROR_ARGUMENTS_VALUE);
      BigDecimal base =
          context.getTypeRegistry().convertToNumber(params.getType(0), params.getValue(0));
      BigDecimal exp =
          context.getTypeRegistry().convertToNumber(params.getType(1), params.getValue(1));
      return TypedValue.ofNumber(NumberUtil.power(base, exp));
    }
  }

  public static class SinFunction implements Function {
    @Override
    public String getCanonicalName() {
      return "SIN";
    }

    @Override
    public DataType getReturnType() {
      return DataType.NUMBER;
    }

    @Override
    public TypedValue evaluate(FormulaContext context, ParameterCallback params)
        throws EvaluationException {
      if (params.getParameterCount() != 1)
        throw new EvaluationException(FormulaErrorValue.ERROR_ARGUMENTS_VALUE);
      BigDecimal n =
          context.getTypeRegistry().convertToNumber(params.getType(0), params.getValue(0));
      return TypedValue.ofNumber(
          NumberUtil.normalize(BigDecimal.valueOf(Math.sin(n.doubleValue()))));
    }
  }

  public static class SqrtFunction implements Function {
    @Override
    public String getCanonicalName() {
      return "SQRT";
    }

    @Override
    public DataType getReturnType() {
      return DataType.NUMBER;
    }

    @Override
    public TypedValue evaluate(FormulaContext context, ParameterCallback params)
        throws EvaluationException {
      if (params.getParameterCount() != 1)
        throw new EvaluationException(FormulaErrorValue.ERROR_ARGUMENTS_VALUE);
      BigDecimal n =
          context.getTypeRegistry().convertToNumber(params.getType(0), params.getValue(0));
      if (n.signum() < 0)
        throw new EvaluationException(FormulaErrorValue.ERROR_INVALID_ARGUMENT_VALUE);
      return TypedValue.ofNumber(
          NumberUtil.normalize(BigDecimal.valueOf(Math.sqrt(n.doubleValue()))));
    }
  }

  public static class SumFunction implements Function {
    @Override
    public String getCanonicalName() {
      return "SUM";
    }

    @Override
    public DataType getReturnType() {
      return DataType.NUMBER;
    }

    @Override
    public TypedValue evaluate(FormulaContext context, ParameterCallback params)
        throws EvaluationException {
      if (params.getParameterCount() == 0)
        throw new EvaluationException(FormulaErrorValue.ERROR_ARGUMENTS_VALUE);
      BigDecimal sum = BigDecimal.ZERO;
      for (int i = 0; i < params.getParameterCount(); i++) {
        List<BigDecimal> nums =
            context
                .getTypeRegistry()
                .convertToNumberSequence(params.getType(i), params.getValue(i), false);
        for (BigDecimal bd : nums) {
          sum = sum.add(bd);
        }
      }
      return TypedValue.ofNumber(sum);
    }
  }

  public static class SumAFunction implements Function {
    @Override
    public String getCanonicalName() {
      return "SUMA";
    }

    @Override
    public DataType getReturnType() {
      return DataType.NUMBER;
    }

    @Override
    public TypedValue evaluate(FormulaContext context, ParameterCallback params)
        throws EvaluationException {
      if (params.getParameterCount() == 0)
        throw new EvaluationException(FormulaErrorValue.ERROR_ARGUMENTS_VALUE);
      BigDecimal sum = BigDecimal.ZERO;
      for (int i = 0; i < params.getParameterCount(); i++) {
        List<BigDecimal> nums =
            context
                .getTypeRegistry()
                .convertToNumberSequence(params.getType(i), params.getValue(i), true);
        for (BigDecimal bd : nums) {
          sum = sum.add(bd);
        }
      }
      return TypedValue.ofNumber(sum);
    }
  }

  public static class VarFunction implements Function {
    @Override
    public String getCanonicalName() {
      return "VAR";
    }

    @Override
    public DataType getReturnType() {
      return DataType.NUMBER;
    }

    @Override
    public TypedValue evaluate(FormulaContext context, ParameterCallback params)
        throws EvaluationException {
      List<BigDecimal> all = new ArrayList<>();
      for (int i = 0; i < params.getParameterCount(); i++) {
        all.addAll(
            context
                .getTypeRegistry()
                .convertToNumberSequence(params.getType(i), params.getValue(i), false));
      }
      if (all.size() < 2) throw new EvaluationException(FormulaErrorValue.ERROR_ARITHMETIC_VALUE);
      BigDecimal sum = BigDecimal.ZERO;
      for (BigDecimal n : all) sum = sum.add(n);
      BigDecimal mean = NumberUtil.divide(sum, BigDecimal.valueOf(all.size()));
      BigDecimal sumSq = BigDecimal.ZERO;
      for (BigDecimal n : all) {
        BigDecimal diff = n.subtract(mean);
        sumSq = sumSq.add(diff.multiply(diff));
      }
      return TypedValue.ofNumber(NumberUtil.divide(sumSq, BigDecimal.valueOf(all.size() - 1)));
    }
  }
}
