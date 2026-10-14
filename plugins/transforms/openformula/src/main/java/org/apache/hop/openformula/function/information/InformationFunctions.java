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

package org.apache.hop.openformula.function.information;

import java.math.BigDecimal;
import java.util.Collection;
import java.util.List;
import org.apache.hop.openformula.EvaluationException;
import org.apache.hop.openformula.FormulaContext;
import org.apache.hop.openformula.ast.ReferenceExpr;
import org.apache.hop.openformula.error.FormulaErrorValue;
import org.apache.hop.openformula.function.DefaultFunctionRegistry;
import org.apache.hop.openformula.function.Function;
import org.apache.hop.openformula.function.FunctionCategory;
import org.apache.hop.openformula.function.FunctionDescription;
import org.apache.hop.openformula.function.ParameterCallback;
import org.apache.hop.openformula.typing.ArrayCallback;
import org.apache.hop.openformula.typing.DataType;
import org.apache.hop.openformula.typing.ExtendedComparator;
import org.apache.hop.openformula.typing.TypedValue;

public final class InformationFunctions {
  private InformationFunctions() {}

  public static void registerAll(DefaultFunctionRegistry registry) {
    registry.registerFunction(
        new ChooseFunction(),
        new FunctionDescription(
            "CHOOSE",
            FunctionCategory.INFORMATION,
            "Returns a value from a list based on an index",
            DataType.ANY,
            2,
            true,
            List.of("index", "choice"),
            List.of("Index number", "Values to choose from")));
    registry.registerFunction(
        new CountFunction(),
        new FunctionDescription(
            "COUNT",
            FunctionCategory.INFORMATION,
            "Counts how many numbers are in the list",
            DataType.NUMBER,
            1,
            true,
            List.of("value"),
            List.of("Values or ranges to count")));
    registry.registerFunction(
        new CountAFunction(),
        new FunctionDescription(
            "COUNTA",
            FunctionCategory.INFORMATION,
            "Counts how many values are non-empty in the list",
            DataType.NUMBER,
            1,
            true,
            List.of("value"),
            List.of("Values or ranges to count")));
    registry.registerFunction(
        new CountBlankFunction(),
        new FunctionDescription(
            "COUNTBLANK",
            FunctionCategory.INFORMATION,
            "Counts how many blank cells are in the list",
            DataType.NUMBER,
            1,
            true,
            List.of("value"),
            List.of("Values or ranges to count")));
    registry.registerFunction(
        new ErrorFunction(),
        new FunctionDescription(
            "ERROR",
            FunctionCategory.INFORMATION,
            "Returns an error value with given code",
            DataType.ERROR,
            1,
            false,
            List.of("code"),
            List.of("Error code number")));
    registry.registerFunction(
        new HasChangedFunction(),
        new FunctionDescription(
            "HASCHANGED",
            FunctionCategory.INFORMATION,
            "Tests if a reference value has changed",
            DataType.LOGICAL,
            1,
            true,
            List.of("reference"),
            List.of("References to test")));
    registry.registerFunction(
        new IndexFunction(),
        new FunctionDescription(
            "INDEX",
            FunctionCategory.INFORMATION,
            "Returns an element from an array given row and column",
            DataType.ANY,
            2,
            false,
            List.of("array", "row", "column"),
            List.of("Array table", "Row index (1-based)", "Column index (1-based)")));
    registry.registerFunction(
        new IsBlankFunction(),
        new FunctionDescription(
            "ISBLANK",
            FunctionCategory.INFORMATION,
            "Tests if a value is blank / empty",
            DataType.LOGICAL,
            1,
            false,
            List.of("value"),
            List.of("Value to check")));
    registry.registerFunction(
        new IsErrFunction(),
        new FunctionDescription(
            "ISERR",
            FunctionCategory.INFORMATION,
            "Tests if a value is any error except #N/A",
            DataType.LOGICAL,
            1,
            false,
            List.of("value"),
            List.of("Value to check")));
    registry.registerFunction(
        new IsErrorFunction(),
        new FunctionDescription(
            "ISERROR",
            FunctionCategory.INFORMATION,
            "Tests if a value is any error including #N/A",
            DataType.LOGICAL,
            1,
            false,
            List.of("value"),
            List.of("Value to check")));
    registry.registerFunction(
        new IsEvenFunction(),
        new FunctionDescription(
            "ISEVEN",
            FunctionCategory.INFORMATION,
            "Tests if a number is even",
            DataType.LOGICAL,
            1,
            false,
            List.of("number"),
            List.of("Number to check")));
    registry.registerFunction(
        new IsLogicalFunction(),
        new FunctionDescription(
            "ISLOGICAL",
            FunctionCategory.INFORMATION,
            "Tests if a value is boolean",
            DataType.LOGICAL,
            1,
            false,
            List.of("value"),
            List.of("Value to check")));
    registry.registerFunction(
        new IsNaFunction(),
        new FunctionDescription(
            "ISNA",
            FunctionCategory.INFORMATION,
            "Tests if a value is #N/A error",
            DataType.LOGICAL,
            1,
            false,
            List.of("value"),
            List.of("Value to check")));
    registry.registerFunction(
        new IsNonTextFunction(),
        new FunctionDescription(
            "ISNONTEXT",
            FunctionCategory.INFORMATION,
            "Tests if a value is not text",
            DataType.LOGICAL,
            1,
            false,
            List.of("value"),
            List.of("Value to check")));
    registry.registerFunction(
        new IsNumberFunction(),
        new FunctionDescription(
            "ISNUMBER",
            FunctionCategory.INFORMATION,
            "Tests if a value is numeric",
            DataType.LOGICAL,
            1,
            false,
            List.of("value"),
            List.of("Value to check")));
    registry.registerFunction(
        new IsOddFunction(),
        new FunctionDescription(
            "ISODD",
            FunctionCategory.INFORMATION,
            "Tests if a number is odd",
            DataType.LOGICAL,
            1,
            false,
            List.of("number"),
            List.of("Number to check")));
    registry.registerFunction(
        new IsRefFunction(),
        new FunctionDescription(
            "ISREF",
            FunctionCategory.INFORMATION,
            "Tests if argument is a reference",
            DataType.LOGICAL,
            1,
            false,
            List.of("value"),
            List.of("Expression to check")));
    registry.registerFunction(
        new IsTextFunction(),
        new FunctionDescription(
            "ISTEXT",
            FunctionCategory.INFORMATION,
            "Tests if a value is text",
            DataType.LOGICAL,
            1,
            false,
            List.of("value"),
            List.of("Value to check")));
    registry.registerFunction(
        new LookupFunction(),
        new FunctionDescription(
            "LOOKUP",
            FunctionCategory.INFORMATION,
            "Looks up a value in a vector and returns value from another vector",
            DataType.ANY,
            2,
            false,
            List.of("lookup_value", "lookup_vector", "result_vector"),
            List.of("Value to find", "Vector to search", "Vector of results")));
    registry.registerFunction(
        new NaFunction(),
        new FunctionDescription(
            "NA",
            FunctionCategory.INFORMATION,
            "Returns #N/A error",
            DataType.ERROR,
            0,
            false,
            List.of(),
            List.of()));
    registry.registerFunction(
        new ValueFunction(),
        new FunctionDescription(
            "VALUE",
            FunctionCategory.INFORMATION,
            "Converts text representation of a number to a number",
            DataType.NUMBER,
            1,
            false,
            List.of("text"),
            List.of("Text string to convert")));
  }

  public static class ChooseFunction implements Function {
    @Override
    public String getCanonicalName() {
      return "CHOOSE";
    }

    @Override
    public TypedValue evaluate(FormulaContext context, ParameterCallback params)
        throws EvaluationException {
      if (params.getParameterCount() < 2)
        throw new EvaluationException(FormulaErrorValue.ERROR_ARGUMENTS_VALUE);
      BigDecimal idxNum =
          context.getTypeRegistry().convertToNumber(params.getType(0), params.getValue(0));
      int idx = idxNum.intValue();
      if (idx < 1 || idx >= params.getParameterCount()) {
        throw new EvaluationException(FormulaErrorValue.ERROR_INVALID_ARGUMENT_VALUE);
      }
      return TypedValue.of(params.getType(idx), params.getValue(idx));
    }
  }

  public static class CountFunction implements Function {
    @Override
    public String getCanonicalName() {
      return "COUNT";
    }

    @Override
    public DataType getReturnType() {
      return DataType.NUMBER;
    }

    @Override
    public TypedValue evaluate(FormulaContext context, ParameterCallback params)
        throws EvaluationException {
      long count = 0;
      for (int i = 0; i < params.getParameterCount(); i++) {
        List<Object> seq =
            context.getTypeRegistry().convertToSequence(params.getType(i), params.getValue(i));
        for (Object item : seq) {
          if (item instanceof Number) count++;
        }
      }
      return TypedValue.ofNumber(BigDecimal.valueOf(count));
    }
  }

  public static class CountAFunction implements Function {
    @Override
    public String getCanonicalName() {
      return "COUNTA";
    }

    @Override
    public DataType getReturnType() {
      return DataType.NUMBER;
    }

    @Override
    public TypedValue evaluate(FormulaContext context, ParameterCallback params)
        throws EvaluationException {
      long count = 0;
      for (int i = 0; i < params.getParameterCount(); i++) {
        count += countNonEmpty(params.getValue(i));
      }
      return TypedValue.ofNumber(BigDecimal.valueOf(count));
    }

    private static long countNonEmpty(Object item) throws EvaluationException {
      if (item == null) return 0;
      if (item instanceof String s && s.isEmpty()) return 0;
      if (item instanceof ArrayCallback ac) {
        long c = 0;
        for (int r = 0; r < ac.getRowCount(); r++) {
          for (int col = 0; col < ac.getColumnCount(); col++) {
            c += countNonEmpty(ac.getValue(r, col));
          }
        }
        return c;
      }
      if (item instanceof Collection<?> coll) {
        long c = 0;
        for (Object el : coll) {
          c += countNonEmpty(el);
        }
        return c;
      }
      if (item.getClass().isArray()) {
        int len = java.lang.reflect.Array.getLength(item);
        long c = 0;
        for (int i = 0; i < len; i++) {
          c += countNonEmpty(java.lang.reflect.Array.get(item, i));
        }
        return c;
      }
      return 1;
    }
  }

  public static class CountBlankFunction implements Function {
    @Override
    public String getCanonicalName() {
      return "COUNTBLANK";
    }

    @Override
    public DataType getReturnType() {
      return DataType.NUMBER;
    }

    @Override
    public TypedValue evaluate(FormulaContext context, ParameterCallback params)
        throws EvaluationException {
      long count = 0;
      for (int i = 0; i < params.getParameterCount(); i++) {
        List<Object> seq =
            context.getTypeRegistry().convertToSequence(params.getType(i), params.getValue(i));
        for (Object item : seq) {
          if (item == null || (item instanceof String s && s.isEmpty())) count++;
        }
      }
      return TypedValue.ofNumber(BigDecimal.valueOf(count));
    }
  }

  public static class ErrorFunction implements Function {
    @Override
    public String getCanonicalName() {
      return "ERROR";
    }

    @Override
    public DataType getReturnType() {
      return DataType.ERROR;
    }

    @Override
    public TypedValue evaluate(FormulaContext context, ParameterCallback params)
        throws EvaluationException {
      if (params.getParameterCount() != 1)
        throw new EvaluationException(FormulaErrorValue.ERROR_ARGUMENTS_VALUE);
      BigDecimal code =
          context.getTypeRegistry().convertToNumber(params.getType(0), params.getValue(0));
      return TypedValue.ofError(code.intValue());
    }
  }

  public static class HasChangedFunction implements Function {
    @Override
    public String getCanonicalName() {
      return "HASCHANGED";
    }

    @Override
    public DataType getReturnType() {
      return DataType.LOGICAL;
    }

    @Override
    public TypedValue evaluate(FormulaContext context, ParameterCallback params)
        throws EvaluationException {
      for (int i = 0; i < params.getParameterCount(); i++) {
        Object ref = params.getValue(i);
        if (ref != null && context.isReferenceDirty(ref)) {
          return TypedValue.TRUE;
        }
      }
      return TypedValue.FALSE;
    }
  }

  public static class IndexFunction implements Function {
    @Override
    public String getCanonicalName() {
      return "INDEX";
    }

    @Override
    public TypedValue evaluate(FormulaContext context, ParameterCallback params)
        throws EvaluationException {
      if (params.getParameterCount() < 2 || params.getParameterCount() > 3) {
        throw new EvaluationException(FormulaErrorValue.ERROR_ARGUMENTS_VALUE);
      }
      Object arrObj = params.getValue(0);
      if (!(arrObj instanceof ArrayCallback ac)) {
        throw new EvaluationException(FormulaErrorValue.ERROR_ILLEGAL_ARRAY_VALUE);
      }
      int r = 0;
      if (params.getParameterCount() >= 2 && params.getValue(1) != null) {
        BigDecimal rowNum =
            context.getTypeRegistry().convertToNumber(params.getType(1), params.getValue(1));
        r = rowNum.intValue() - 1;
      }
      int c = 0;
      if (params.getParameterCount() == 3 && params.getValue(2) != null) {
        BigDecimal colNum =
            context.getTypeRegistry().convertToNumber(params.getType(2), params.getValue(2));
        c = colNum.intValue() - 1;
      }
      if (r < 0 || r >= ac.getRowCount() || c < 0 || c >= ac.getColumnCount()) {
        throw new EvaluationException(FormulaErrorValue.ERROR_INVALID_ARGUMENT_VALUE);
      }
      return TypedValue.of(ac.getType(r, c), ac.getValue(r, c));
    }
  }

  public static class IsBlankFunction implements Function {
    @Override
    public String getCanonicalName() {
      return "ISBLANK";
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
      Object val = params.getValue(0);
      return TypedValue.ofLogical(val == null);
    }
  }

  public static class IsErrFunction implements Function {
    @Override
    public String getCanonicalName() {
      return "ISERR";
    }

    @Override
    public DataType getReturnType() {
      return DataType.LOGICAL;
    }

    @Override
    public TypedValue evaluate(FormulaContext context, ParameterCallback params) {
      try {
        Object val = params.getValue(0);
        if (val instanceof FormulaErrorValue ev) {
          return TypedValue.ofLogical(ev.getErrorCode() != FormulaErrorValue.ERROR_NA);
        }
        return TypedValue.FALSE;
      } catch (EvaluationException e) {
        FormulaErrorValue ev = e.getErrorValue();
        return TypedValue.ofLogical(ev != null && ev.getErrorCode() != FormulaErrorValue.ERROR_NA);
      }
    }
  }

  public static class IsErrorFunction implements Function {
    @Override
    public String getCanonicalName() {
      return "ISERROR";
    }

    @Override
    public DataType getReturnType() {
      return DataType.LOGICAL;
    }

    @Override
    public TypedValue evaluate(FormulaContext context, ParameterCallback params) {
      try {
        Object val = params.getValue(0);
        return TypedValue.ofLogical(val instanceof FormulaErrorValue);
      } catch (EvaluationException e) {
        return TypedValue.TRUE;
      }
    }
  }

  public static class IsEvenFunction implements Function {
    @Override
    public String getCanonicalName() {
      return "ISEVEN";
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
      BigDecimal n =
          context.getTypeRegistry().convertToNumber(params.getType(0), params.getValue(0));
      return TypedValue.ofLogical((n.longValue() % 2) == 0);
    }
  }

  public static class IsLogicalFunction implements Function {
    @Override
    public String getCanonicalName() {
      return "ISLOGICAL";
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
      return TypedValue.ofLogical(params.getValue(0) instanceof Boolean);
    }
  }

  public static class IsNaFunction implements Function {
    @Override
    public String getCanonicalName() {
      return "ISNA";
    }

    @Override
    public DataType getReturnType() {
      return DataType.LOGICAL;
    }

    @Override
    public TypedValue evaluate(FormulaContext context, ParameterCallback params) {
      try {
        Object val = params.getValue(0);
        if (val instanceof FormulaErrorValue ev) {
          return TypedValue.ofLogical(ev.getErrorCode() == FormulaErrorValue.ERROR_NA);
        }
        return TypedValue.FALSE;
      } catch (EvaluationException e) {
        FormulaErrorValue ev = e.getErrorValue();
        return TypedValue.ofLogical(ev != null && ev.getErrorCode() == FormulaErrorValue.ERROR_NA);
      }
    }
  }

  public static class IsNonTextFunction implements Function {
    @Override
    public String getCanonicalName() {
      return "ISNONTEXT";
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
      return TypedValue.ofLogical(!(params.getValue(0) instanceof String));
    }
  }

  public static class IsNumberFunction implements Function {
    @Override
    public String getCanonicalName() {
      return "ISNUMBER";
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
      return TypedValue.ofLogical(params.getValue(0) instanceof Number);
    }
  }

  public static class IsOddFunction implements Function {
    @Override
    public String getCanonicalName() {
      return "ISODD";
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
      BigDecimal n =
          context.getTypeRegistry().convertToNumber(params.getType(0), params.getValue(0));
      return TypedValue.ofLogical((n.longValue() % 2) != 0);
    }
  }

  public static class IsRefFunction implements Function {
    @Override
    public String getCanonicalName() {
      return "ISREF";
    }

    @Override
    public DataType getReturnType() {
      return DataType.LOGICAL;
    }

    @Override
    public TypedValue evaluate(FormulaContext context, ParameterCallback params)
        throws EvaluationException {
      if (params.getParameterCount() != 1) return TypedValue.FALSE;
      Object val = params.getValue(0);
      if (val instanceof FormulaErrorValue ev) {
        throw new EvaluationException(ev);
      }
      return TypedValue.ofLogical(params.getRaw(0) instanceof ReferenceExpr);
    }
  }

  public static class IsTextFunction implements Function {
    @Override
    public String getCanonicalName() {
      return "ISTEXT";
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
      return TypedValue.ofLogical(params.getValue(0) instanceof String);
    }
  }

  public static class LookupFunction implements Function {
    @Override
    public String getCanonicalName() {
      return "LOOKUP";
    }

    @Override
    public TypedValue evaluate(FormulaContext context, ParameterCallback params)
        throws EvaluationException {
      if (params.getParameterCount() < 2 || params.getParameterCount() > 3) {
        throw new EvaluationException(FormulaErrorValue.ERROR_ARGUMENTS_VALUE);
      }
      Object lookupVal = params.getValue(0);
      DataType lookupType = params.getType(0);
      List<Object> searchVector =
          context.getTypeRegistry().convertToSequence(params.getType(1), params.getValue(1));
      List<Object> resultVector =
          (params.getParameterCount() == 3)
              ? context.getTypeRegistry().convertToSequence(params.getType(2), params.getValue(2))
              : searchVector;

      ExtendedComparator cmp = context.getTypeRegistry().getComparator(lookupType, lookupType);
      int foundIndex = -1;
      for (int i = 0; i < searchVector.size(); i++) {
        Object item = searchVector.get(i);
        DataType itemType = context.getTypeRegistry().guessType(item);
        if (cmp.compare(lookupType, lookupVal, itemType, item) >= 0) {
          foundIndex = i;
        } else {
          break;
        }
      }
      if (foundIndex < 0 || foundIndex >= resultVector.size()) {
        throw new EvaluationException(FormulaErrorValue.ERROR_NA_VALUE);
      }
      Object res = resultVector.get(foundIndex);
      return TypedValue.of(context.getTypeRegistry().guessType(res), res);
    }
  }

  public static class NaFunction implements Function {
    @Override
    public String getCanonicalName() {
      return "NA";
    }

    @Override
    public DataType getReturnType() {
      return DataType.ERROR;
    }

    @Override
    public TypedValue evaluate(FormulaContext context, ParameterCallback params) {
      return TypedValue.ofError(FormulaErrorValue.ERROR_NA_VALUE);
    }
  }

  public static class ValueFunction implements Function {
    @Override
    public String getCanonicalName() {
      return "VALUE";
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
      BigDecimal num =
          context.getTypeRegistry().convertToNumber(params.getType(0), params.getValue(0));
      return TypedValue.ofNumber(num);
    }
  }
}
