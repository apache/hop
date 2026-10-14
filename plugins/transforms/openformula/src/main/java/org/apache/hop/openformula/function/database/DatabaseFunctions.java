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

package org.apache.hop.openformula.function.database;

import java.util.List;
import java.util.regex.Pattern;
import org.apache.hop.openformula.EvaluationException;
import org.apache.hop.openformula.FormulaContext;
import org.apache.hop.openformula.error.FormulaErrorValue;
import org.apache.hop.openformula.function.DefaultFunctionRegistry;
import org.apache.hop.openformula.function.Function;
import org.apache.hop.openformula.function.FunctionCategory;
import org.apache.hop.openformula.function.FunctionDescription;
import org.apache.hop.openformula.function.ParameterCallback;
import org.apache.hop.openformula.typing.DataType;
import org.apache.hop.openformula.typing.ExtendedComparator;
import org.apache.hop.openformula.typing.TypeRegistry;
import org.apache.hop.openformula.typing.TypedValue;

public final class DatabaseFunctions {
  private DatabaseFunctions() {}

  public static void registerAll(DefaultFunctionRegistry registry) {
    registry.registerFunction(
        new BeginsWithFunction(),
        new FunctionDescription(
            "BEGINSWITH",
            FunctionCategory.DATABASE,
            "Tests if text begins with a given prefix",
            DataType.LOGICAL,
            2,
            false,
            List.of("text", "prefix"),
            List.of("Source text", "Prefix to check")));
    registry.registerFunction(
        new ContainsFunction(),
        new FunctionDescription(
            "CONTAINS",
            FunctionCategory.DATABASE,
            "Tests if text contains a given substring",
            DataType.LOGICAL,
            2,
            false,
            List.of("text", "substring"),
            List.of("Source text", "Substring to check")));
    registry.registerFunction(
        new EndsWithFunction(),
        new FunctionDescription(
            "ENDSWITH",
            FunctionCategory.DATABASE,
            "Tests if text ends with a given suffix",
            DataType.LOGICAL,
            2,
            false,
            List.of("text", "suffix"),
            List.of("Source text", "Suffix to check")));
    registry.registerFunction(
        new EqualsFunction(),
        new FunctionDescription(
            "EQUALS",
            FunctionCategory.DATABASE,
            "Tests if two values are equal",
            DataType.LOGICAL,
            2,
            false,
            List.of("text1", "text2"),
            List.of("First value", "Second value")));
    registry.registerFunction(
        new InFunction(),
        new FunctionDescription(
            "IN",
            FunctionCategory.DATABASE,
            "Tests if a value is contained in a list",
            DataType.LOGICAL,
            2,
            true,
            List.of("value", "item"),
            List.of("Value to test", "Candidates")));
    registry.registerFunction(
        new LikeFunction(),
        new FunctionDescription(
            "LIKE",
            FunctionCategory.DATABASE,
            "Tests SQL LIKE pattern match",
            DataType.LOGICAL,
            2,
            false,
            List.of("text", "pattern"),
            List.of("Text to search", "Pattern with % and _")));
  }

  public static class BeginsWithFunction implements Function {
    @Override
    public String getCanonicalName() {
      return "BEGINSWITH";
    }

    @Override
    public DataType getReturnType() {
      return DataType.LOGICAL;
    }

    @Override
    public TypedValue evaluate(FormulaContext context, ParameterCallback params)
        throws EvaluationException {
      if (params.getParameterCount() != 2)
        throw new EvaluationException(FormulaErrorValue.ERROR_ARGUMENTS_VALUE);
      String text = context.getTypeRegistry().convertToText(params.getType(0), params.getValue(0));
      String prefix =
          context.getTypeRegistry().convertToText(params.getType(1), params.getValue(1));
      return TypedValue.ofLogical(text.startsWith(prefix));
    }
  }

  public static class ContainsFunction implements Function {
    @Override
    public String getCanonicalName() {
      return "CONTAINS";
    }

    @Override
    public DataType getReturnType() {
      return DataType.LOGICAL;
    }

    @Override
    public TypedValue evaluate(FormulaContext context, ParameterCallback params)
        throws EvaluationException {
      if (params.getParameterCount() != 2)
        throw new EvaluationException(FormulaErrorValue.ERROR_ARGUMENTS_VALUE);
      String text = context.getTypeRegistry().convertToText(params.getType(0), params.getValue(0));
      String substring =
          context.getTypeRegistry().convertToText(params.getType(1), params.getValue(1));
      return TypedValue.ofLogical(text.contains(substring));
    }
  }

  public static class EndsWithFunction implements Function {
    @Override
    public String getCanonicalName() {
      return "ENDSWITH";
    }

    @Override
    public DataType getReturnType() {
      return DataType.LOGICAL;
    }

    @Override
    public TypedValue evaluate(FormulaContext context, ParameterCallback params)
        throws EvaluationException {
      if (params.getParameterCount() != 2)
        throw new EvaluationException(FormulaErrorValue.ERROR_ARGUMENTS_VALUE);
      String text = context.getTypeRegistry().convertToText(params.getType(0), params.getValue(0));
      String suffix =
          context.getTypeRegistry().convertToText(params.getType(1), params.getValue(1));
      return TypedValue.ofLogical(text.endsWith(suffix));
    }
  }

  public static class EqualsFunction implements Function {
    @Override
    public String getCanonicalName() {
      return "EQUALS";
    }

    @Override
    public DataType getReturnType() {
      return DataType.LOGICAL;
    }

    @Override
    public TypedValue evaluate(FormulaContext context, ParameterCallback params)
        throws EvaluationException {
      if (params.getParameterCount() != 2)
        throw new EvaluationException(FormulaErrorValue.ERROR_ARGUMENTS_VALUE);
      String text1 = context.getTypeRegistry().convertToText(params.getType(0), params.getValue(0));
      String text2 = context.getTypeRegistry().convertToText(params.getType(1), params.getValue(1));
      return TypedValue.ofLogical(text1.equals(text2));
    }
  }

  public static class InFunction implements Function {
    @Override
    public String getCanonicalName() {
      return "IN";
    }

    @Override
    public DataType getReturnType() {
      return DataType.LOGICAL;
    }

    @Override
    public TypedValue evaluate(FormulaContext context, ParameterCallback params)
        throws EvaluationException {
      int count = params.getParameterCount();
      if (count < 2) throw new EvaluationException(FormulaErrorValue.ERROR_ARGUMENTS_VALUE);
      Object target = params.getValue(0);
      DataType targetType = params.getType(0);
      if (target == null) throw new EvaluationException(FormulaErrorValue.ERROR_NA_VALUE);

      TypeRegistry reg = context.getTypeRegistry();
      for (int i = 1; i < count; i++) {
        Object candidate = params.getValue(i);
        if (candidate == null) throw new EvaluationException(FormulaErrorValue.ERROR_NA_VALUE);
        DataType candType = params.getType(i);
        ExtendedComparator cmp = reg.getComparator(targetType, candType);
        if (cmp.isEqual(targetType, target, candType, candidate)) {
          return TypedValue.TRUE;
        }
      }
      return TypedValue.FALSE;
    }
  }

  public static class LikeFunction implements Function {
    @Override
    public String getCanonicalName() {
      return "LIKE";
    }

    @Override
    public DataType getReturnType() {
      return DataType.LOGICAL;
    }

    @Override
    public TypedValue evaluate(FormulaContext context, ParameterCallback params)
        throws EvaluationException {
      if (params.getParameterCount() != 2)
        throw new EvaluationException(FormulaErrorValue.ERROR_ARGUMENTS_VALUE);
      String text = context.getTypeRegistry().convertToText(params.getType(0), params.getValue(0));
      String pattern =
          context.getTypeRegistry().convertToText(params.getType(1), params.getValue(1));

      // Translate SQL LIKE pattern to regex
      StringBuilder regex = new StringBuilder("^");
      for (int i = 0; i < pattern.length(); i++) {
        char c = pattern.charAt(i);
        if (c == '%' || c == '*') {
          regex.append(".*");
        } else if (c == '_' || c == '?') {
          regex.append(".");
        } else if ("\\.[]{}()+^$|".indexOf(c) != -1) {
          regex.append('\\').append(c);
        } else {
          regex.append(c);
        }
      }
      regex.append("$");

      boolean matches =
          Pattern.compile(regex.toString(), Pattern.CASE_INSENSITIVE | Pattern.DOTALL)
              .matcher(text)
              .find();
      return TypedValue.ofLogical(matches);
    }
  }
}
