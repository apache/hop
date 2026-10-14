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

package org.apache.hop.openformula.function.logical;

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

public final class LogicalFunctions {
  private LogicalFunctions() {}

  public static void registerAll(DefaultFunctionRegistry registry) {
    registry.registerFunction(
        new AndFunction(),
        new FunctionDescription(
            "AND",
            FunctionCategory.LOGICAL,
            "Returns TRUE if all arguments are TRUE",
            DataType.LOGICAL,
            1,
            true,
            List.of("logical"),
            List.of("Conditions")));
    registry.registerFunction(
        new OrFunction(),
        new FunctionDescription(
            "OR",
            FunctionCategory.LOGICAL,
            "Returns TRUE if any argument is TRUE",
            DataType.LOGICAL,
            1,
            true,
            List.of("logical"),
            List.of("Conditions")));
    registry.registerFunction(
        new NotFunction(),
        new FunctionDescription(
            "NOT",
            FunctionCategory.LOGICAL,
            "Inverts a logical value",
            DataType.LOGICAL,
            1,
            false,
            List.of("logical"),
            List.of("Logical expression")));
    registry.registerFunction(
        new XorFunction(),
        new FunctionDescription(
            "XOR",
            FunctionCategory.LOGICAL,
            "Returns TRUE if an odd number of arguments evaluate to TRUE",
            DataType.LOGICAL,
            1,
            true,
            List.of("logical"),
            List.of("Conditions")));
    registry.registerFunction(
        new TrueFunction(),
        new FunctionDescription(
            "TRUE",
            FunctionCategory.LOGICAL,
            "Returns logical TRUE",
            DataType.LOGICAL,
            0,
            false,
            List.of(),
            List.of()));
    registry.registerFunction(
        new FalseFunction(),
        new FunctionDescription(
            "FALSE",
            FunctionCategory.LOGICAL,
            "Returns logical FALSE",
            DataType.LOGICAL,
            0,
            false,
            List.of(),
            List.of()));
    registry.registerFunction(
        new IfFunction(),
        new FunctionDescription(
            "IF",
            FunctionCategory.LOGICAL,
            "Conditional evaluation",
            DataType.ANY,
            2,
            false,
            List.of("test", "then_value", "otherwise_value"),
            List.of("Test condition", "Value if true", "Value if false")));
    registry.registerFunction(
        new IfNaFunction(),
        new FunctionDescription(
            "IFNA",
            FunctionCategory.LOGICAL,
            "Returns alternate value if expression is #N/A",
            DataType.ANY,
            2,
            false,
            List.of("value", "value_if_na"),
            List.of("Expression to check", "Fallback if #N/A")));
  }

  public static class AndFunction implements Function {
    @Override
    public String getCanonicalName() {
      return "AND";
    }

    @Override
    public DataType getReturnType() {
      return DataType.LOGICAL;
    }

    @Override
    public TypedValue evaluate(FormulaContext context, ParameterCallback params)
        throws EvaluationException {
      int count = params.getParameterCount();
      if (count == 0) throw new EvaluationException(FormulaErrorValue.ERROR_ARGUMENTS_VALUE);
      for (int i = 0; i < count; i++) {
        Object val = params.getValue(i);
        DataType type = params.getType(i);
        List<Object> seq = context.getTypeRegistry().convertToSequence(type, val);
        for (Object item : seq) {
          if (item != null) {
            boolean b =
                context
                    .getTypeRegistry()
                    .convertToLogical(context.getTypeRegistry().guessType(item), item);
            if (!b) return TypedValue.FALSE;
          }
        }
      }
      return TypedValue.TRUE;
    }
  }

  public static class OrFunction implements Function {
    @Override
    public String getCanonicalName() {
      return "OR";
    }

    @Override
    public DataType getReturnType() {
      return DataType.LOGICAL;
    }

    @Override
    public TypedValue evaluate(FormulaContext context, ParameterCallback params)
        throws EvaluationException {
      int count = params.getParameterCount();
      if (count == 0) throw new EvaluationException(FormulaErrorValue.ERROR_ARGUMENTS_VALUE);
      for (int i = 0; i < count; i++) {
        Object val = params.getValue(i);
        DataType type = params.getType(i);
        List<Object> seq = context.getTypeRegistry().convertToSequence(type, val);
        for (Object item : seq) {
          if (item != null) {
            boolean b =
                context
                    .getTypeRegistry()
                    .convertToLogical(context.getTypeRegistry().guessType(item), item);
            if (b) return TypedValue.TRUE;
          }
        }
      }
      return TypedValue.FALSE;
    }
  }

  public static class NotFunction implements Function {
    @Override
    public String getCanonicalName() {
      return "NOT";
    }

    @Override
    public DataType getReturnType() {
      return DataType.LOGICAL;
    }

    @Override
    public TypedValue evaluate(FormulaContext context, ParameterCallback params)
        throws EvaluationException {
      if (params.getParameterCount() != 1)
        throw new EvaluationException(FormulaErrorValue.ERROR_ARGUMENTS_VALUE);
      boolean b = context.getTypeRegistry().convertToLogical(params.getType(0), params.getValue(0));
      return TypedValue.ofLogical(!b);
    }
  }

  public static class XorFunction implements Function {
    @Override
    public String getCanonicalName() {
      return "XOR";
    }

    @Override
    public DataType getReturnType() {
      return DataType.LOGICAL;
    }

    @Override
    public TypedValue evaluate(FormulaContext context, ParameterCallback params)
        throws EvaluationException {
      int count = params.getParameterCount();
      if (count == 0) throw new EvaluationException(FormulaErrorValue.ERROR_ARGUMENTS_VALUE);
      int trueCount = 0;
      for (int i = 0; i < count; i++) {
        Object val = params.getValue(i);
        DataType type = params.getType(i);
        List<Object> seq = context.getTypeRegistry().convertToSequence(type, val);
        for (Object item : seq) {
          if (item != null) {
            boolean b =
                context
                    .getTypeRegistry()
                    .convertToLogical(context.getTypeRegistry().guessType(item), item);
            if (b) trueCount++;
          }
        }
      }
      return TypedValue.ofLogical((trueCount % 2) != 0);
    }
  }

  public static class TrueFunction implements Function {
    @Override
    public String getCanonicalName() {
      return "TRUE";
    }

    @Override
    public DataType getReturnType() {
      return DataType.LOGICAL;
    }

    @Override
    public TypedValue evaluate(FormulaContext context, ParameterCallback params) {
      return TypedValue.TRUE;
    }
  }

  public static class FalseFunction implements Function {
    @Override
    public String getCanonicalName() {
      return "FALSE";
    }

    @Override
    public DataType getReturnType() {
      return DataType.LOGICAL;
    }

    @Override
    public TypedValue evaluate(FormulaContext context, ParameterCallback params) {
      return TypedValue.FALSE;
    }
  }

  public static class IfFunction implements Function {
    @Override
    public String getCanonicalName() {
      return "IF";
    }

    @Override
    public TypedValue evaluate(FormulaContext context, ParameterCallback params)
        throws EvaluationException {
      int count = params.getParameterCount();
      if (count < 2 || count > 3)
        throw new EvaluationException(FormulaErrorValue.ERROR_ARGUMENTS_VALUE);
      boolean condition =
          context.getTypeRegistry().convertToLogical(params.getType(0), params.getValue(0));
      if (condition) {
        Object v = params.getValue(1);
        if (v == null) {
          return TypedValue.FALSE;
        }
        DataType t = params.getType(1);
        return TypedValue.of(t, v);
      } else {
        if (count == 3) {
          Object v = params.getValue(2);
          if (v == null) {
            return TypedValue.FALSE;
          }
          DataType t = params.getType(2);
          return TypedValue.of(t, v);
        }
        return TypedValue.FALSE;
      }
    }
  }

  public static class IfNaFunction implements Function {
    @Override
    public String getCanonicalName() {
      return "IFNA";
    }

    @Override
    public TypedValue evaluate(FormulaContext context, ParameterCallback params)
        throws EvaluationException {
      if (params.getParameterCount() != 2)
        throw new EvaluationException(FormulaErrorValue.ERROR_ARGUMENTS_VALUE);
      try {
        Object val = params.getValue(0);
        if (val instanceof FormulaErrorValue ev
            && ev.getErrorCode() == FormulaErrorValue.ERROR_NA) {
          return TypedValue.of(params.getType(1), params.getValue(1));
        }
        return TypedValue.of(params.getType(0), val);
      } catch (EvaluationException e) {
        if (e.getErrorValue() != null
            && e.getErrorValue().getErrorCode() == FormulaErrorValue.ERROR_NA) {
          return TypedValue.of(params.getType(1), params.getValue(1));
        }
        throw e;
      }
    }
  }
}
