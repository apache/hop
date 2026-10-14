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

package org.apache.hop.openformula.function.userdefined;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.List;
import org.apache.hop.openformula.EvaluationException;
import org.apache.hop.openformula.FormulaContext;
import org.apache.hop.openformula.error.FormulaErrorValue;
import org.apache.hop.openformula.function.DefaultFunctionRegistry;
import org.apache.hop.openformula.function.Function;
import org.apache.hop.openformula.function.FunctionCategory;
import org.apache.hop.openformula.function.FunctionDescription;
import org.apache.hop.openformula.function.ParameterCallback;
import org.apache.hop.openformula.typing.ArrayCallback;
import org.apache.hop.openformula.typing.DataType;
import org.apache.hop.openformula.typing.ExtendedComparator;
import org.apache.hop.openformula.typing.StaticArrayCallback;
import org.apache.hop.openformula.typing.TypedValue;

public final class UserDefinedFunctions {
  private UserDefinedFunctions() {}

  public static void registerAll(DefaultFunctionRegistry registry) {
    registry.registerFunction(
        new ArrayConcatenateFunction(),
        new FunctionDescription(
            "ARRAYCONCATENATE",
            FunctionCategory.USER_DEFINED,
            "Concatenates multiple arrays/sequences into one",
            DataType.ARRAY,
            1,
            true,
            List.of("array"),
            List.of("Arrays or values to concatenate")));
    registry.registerFunction(
        new ArrayContainsFunction(),
        new FunctionDescription(
            "ARRAYCONTAINS",
            FunctionCategory.USER_DEFINED,
            "Checks if array contains a specific value",
            DataType.LOGICAL,
            2,
            false,
            List.of("array", "value"),
            List.of("Array or sequence", "Value to find")));
    registry.registerFunction(
        new ArrayLeftFunction(),
        new FunctionDescription(
            "ARRAYLEFT",
            FunctionCategory.USER_DEFINED,
            "Returns first N elements from array",
            DataType.ARRAY,
            2,
            false,
            List.of("array", "count"),
            List.of("Array", "Count of elements")));
    registry.registerFunction(
        new ArrayMidFunction(),
        new FunctionDescription(
            "ARRAYMID",
            FunctionCategory.USER_DEFINED,
            "Returns a slice of an array",
            DataType.ARRAY,
            3,
            false,
            List.of("array", "start", "count"),
            List.of("Array", "Start index (1-based)", "Count of elements")));
    registry.registerFunction(
        new ArrayRightFunction(),
        new FunctionDescription(
            "ARRAYRIGHT",
            FunctionCategory.USER_DEFINED,
            "Returns last N elements from array",
            DataType.ARRAY,
            2,
            false,
            List.of("array", "count"),
            List.of("Array", "Count of elements")));
    registry.registerFunction(
        new CsvArrayFunction(),
        new FunctionDescription(
            "CSVARRAY",
            FunctionCategory.USER_DEFINED,
            "Parses CSV text into a 1D array of elements",
            DataType.ARRAY,
            1,
            false,
            List.of("csv_text", "delimiter", "quote"),
            List.of(
                "CSV text string",
                "Delimiter (defaults to ,)",
                "Quote character (defaults to \")")));
    registry.registerFunction(
        new CsvTextFunction(),
        new FunctionDescription(
            "CSVTEXT",
            FunctionCategory.USER_DEFINED,
            "Formats an array or sequence as CSV text",
            DataType.TEXT,
            1,
            false,
            List.of("array", "delimiter", "quote"),
            List.of(
                "Array or sequence",
                "Delimiter (defaults to ,)",
                "Quote character (defaults to \")")));
    registry.registerFunction(
        new NormalizeArrayFunction(),
        new FunctionDescription(
            "NORMALIZEARRAY",
            FunctionCategory.USER_DEFINED,
            "Flattens array/sequence into 1D array",
            DataType.ARRAY,
            1,
            false,
            List.of("array"),
            List.of("Array to normalize")));
    registry.registerFunction(
        new NullFunction(),
        new FunctionDescription(
            "NULL",
            FunctionCategory.USER_DEFINED,
            "Returns null",
            DataType.ANY,
            0,
            false,
            List.of(),
            List.of()));
    registry.registerFunction(
        new ParseDateFunction(),
        new FunctionDescription(
            "PARSEDATE",
            FunctionCategory.USER_DEFINED,
            "Parses text into date using specified pattern",
            DataType.DATETIME,
            2,
            false,
            List.of("text", "pattern"),
            List.of("Text string", "Date pattern (e.g. yyyy-MM-dd)")));
    registry.registerFunction(
        new SequenceQuoterFunction(),
        new FunctionDescription(
            "SEQUENCEQUOTER",
            FunctionCategory.USER_DEFINED,
            "Wraps each element in sequence with quote character",
            DataType.ARRAY,
            1,
            false,
            List.of("sequence", "quote"),
            List.of("Array or sequence", "Quote character (defaults to \")")));
  }

  private static void flatten(Object val, List<Object> target) throws EvaluationException {
    if (val == null) {
      return;
    }
    if (val instanceof ArrayCallback ac) {
      for (int r = 0; r < ac.getRowCount(); r++) {
        for (int c = 0; c < ac.getColumnCount(); c++) {
          flatten(ac.getValue(r, c), target);
        }
      }
    } else if (val instanceof Collection<?> coll) {
      for (Object item : coll) {
        flatten(item, target);
      }
    } else if (val.getClass().isArray()) {
      int len = java.lang.reflect.Array.getLength(val);
      for (int i = 0; i < len; i++) {
        flatten(java.lang.reflect.Array.get(val, i), target);
      }
    } else {
      target.add(val);
    }
  }

  public static class ArrayConcatenateFunction implements Function {
    @Override
    public String getCanonicalName() {
      return "ARRAYCONCATENATE";
    }

    @Override
    public DataType getReturnType() {
      return DataType.ARRAY;
    }

    @Override
    public TypedValue evaluate(FormulaContext context, ParameterCallback params)
        throws EvaluationException {
      List<Object> combined = new ArrayList<>();
      for (int i = 0; i < params.getParameterCount(); i++) {
        flatten(params.getValue(i), combined);
      }
      Object[][] grid = new Object[1][combined.size()];
      for (int i = 0; i < combined.size(); i++) {
        grid[0][i] = combined.get(i);
      }
      return TypedValue.ofArray(new StaticArrayCallback(grid));
    }
  }

  public static class ArrayContainsFunction implements Function {
    @Override
    public String getCanonicalName() {
      return "ARRAYCONTAINS";
    }

    @Override
    public DataType getReturnType() {
      return DataType.LOGICAL;
    }

    @Override
    public TypedValue evaluate(FormulaContext context, ParameterCallback params)
        throws EvaluationException {
      if (params.getParameterCount() < 2)
        throw new EvaluationException(FormulaErrorValue.ERROR_ARGUMENTS_VALUE);
      List<Object> haystack = new ArrayList<>();
      flatten(params.getValue(0), haystack);

      for (int p = 1; p < params.getParameterCount(); p++) {
        Object needle = params.getValue(p);
        DataType needleType = params.getType(p);
        boolean found = false;
        for (Object item : haystack) {
          DataType itemType = context.getTypeRegistry().guessType(item);
          ExtendedComparator cmp = context.getTypeRegistry().getComparator(itemType, needleType);
          if (cmp.isEqual(itemType, item, needleType, needle)) {
            found = true;
            break;
          }
        }
        if (!found) {
          return TypedValue.FALSE;
        }
      }
      return TypedValue.TRUE;
    }
  }

  public static class ArrayLeftFunction implements Function {
    @Override
    public String getCanonicalName() {
      return "ARRAYLEFT";
    }

    @Override
    public DataType getReturnType() {
      return DataType.ARRAY;
    }

    @Override
    public TypedValue evaluate(FormulaContext context, ParameterCallback params)
        throws EvaluationException {
      if (params.getParameterCount() != 2)
        throw new EvaluationException(FormulaErrorValue.ERROR_ARGUMENTS_VALUE);
      List<Object> seq = new ArrayList<>();
      flatten(params.getValue(0), seq);
      int count =
          context
              .getTypeRegistry()
              .convertToNumber(params.getType(1), params.getValue(1))
              .intValue();
      if (count < 0) throw new EvaluationException(FormulaErrorValue.ERROR_INVALID_ARGUMENT_VALUE);
      int take = Math.min(count, seq.size());
      Object[][] grid = new Object[1][take];
      for (int i = 0; i < take; i++) grid[0][i] = seq.get(i);
      return TypedValue.ofArray(new StaticArrayCallback(grid));
    }
  }

  public static class ArrayMidFunction implements Function {
    @Override
    public String getCanonicalName() {
      return "ARRAYMID";
    }

    @Override
    public DataType getReturnType() {
      return DataType.ARRAY;
    }

    @Override
    public TypedValue evaluate(FormulaContext context, ParameterCallback params)
        throws EvaluationException {
      if (params.getParameterCount() != 3)
        throw new EvaluationException(FormulaErrorValue.ERROR_ARGUMENTS_VALUE);
      List<Object> seq = new ArrayList<>();
      flatten(params.getValue(0), seq);
      int start =
          context
              .getTypeRegistry()
              .convertToNumber(params.getType(1), params.getValue(1))
              .intValue();
      int count =
          context
              .getTypeRegistry()
              .convertToNumber(params.getType(2), params.getValue(2))
              .intValue();
      if (start < 1 || count < 0)
        throw new EvaluationException(FormulaErrorValue.ERROR_INVALID_ARGUMENT_VALUE);
      int startIdx = start - 1;
      int take = (startIdx >= seq.size()) ? 0 : Math.min(count, seq.size() - startIdx);
      Object[][] grid = new Object[1][take];
      for (int i = 0; i < take; i++) grid[0][i] = seq.get(startIdx + i);
      return TypedValue.ofArray(new StaticArrayCallback(grid));
    }
  }

  public static class ArrayRightFunction implements Function {
    @Override
    public String getCanonicalName() {
      return "ARRAYRIGHT";
    }

    @Override
    public DataType getReturnType() {
      return DataType.ARRAY;
    }

    @Override
    public TypedValue evaluate(FormulaContext context, ParameterCallback params)
        throws EvaluationException {
      if (params.getParameterCount() != 2)
        throw new EvaluationException(FormulaErrorValue.ERROR_ARGUMENTS_VALUE);
      List<Object> seq = new ArrayList<>();
      flatten(params.getValue(0), seq);
      int count =
          context
              .getTypeRegistry()
              .convertToNumber(params.getType(1), params.getValue(1))
              .intValue();
      if (count < 0) throw new EvaluationException(FormulaErrorValue.ERROR_INVALID_ARGUMENT_VALUE);
      int take = Math.min(count, seq.size());
      int startIdx = seq.size() - take;
      Object[][] grid = new Object[1][take];
      for (int i = 0; i < take; i++) grid[0][i] = seq.get(startIdx + i);
      return TypedValue.ofArray(new StaticArrayCallback(grid));
    }
  }

  public static class CsvArrayFunction implements Function {
    @Override
    public String getCanonicalName() {
      return "CSVARRAY";
    }

    @Override
    public DataType getReturnType() {
      return DataType.ARRAY;
    }

    @Override
    public TypedValue evaluate(FormulaContext context, ParameterCallback params)
        throws EvaluationException {
      if (params.getParameterCount() < 1 || params.getParameterCount() > 4)
        throw new EvaluationException(FormulaErrorValue.ERROR_ARGUMENTS_VALUE);
      String text = context.getTypeRegistry().convertToText(params.getType(0), params.getValue(0));
      boolean quoting = false;
      String delim = ",";
      char quote = '"';

      if (params.getParameterCount() == 2) {
        Object p1 = params.getValue(1);
        if (p1 instanceof Boolean b) {
          quoting = b;
        } else {
          delim = context.getTypeRegistry().convertToText(params.getType(1), p1);
        }
      } else if (params.getParameterCount() == 3) {
        Object p1 = params.getValue(1);
        if (p1 instanceof Boolean b) {
          quoting = b;
          delim = context.getTypeRegistry().convertToText(params.getType(2), params.getValue(2));
        } else {
          delim = context.getTypeRegistry().convertToText(params.getType(1), p1);
          String qStr =
              context.getTypeRegistry().convertToText(params.getType(2), params.getValue(2));
          if (!qStr.isEmpty()) quote = qStr.charAt(0);
        }
      } else if (params.getParameterCount() == 4) {
        quoting = context.getTypeRegistry().convertToLogical(params.getType(1), params.getValue(1));
        delim = context.getTypeRegistry().convertToText(params.getType(2), params.getValue(2));
        String qStr =
            context.getTypeRegistry().convertToText(params.getType(3), params.getValue(3));
        if (!qStr.isEmpty()) quote = qStr.charAt(0);
      }

      List<String> tokens = parseCsv(text, delim, quote, quoting);
      Object[][] grid = new Object[1][tokens.size()];
      for (int i = 0; i < tokens.size(); i++) grid[0][i] = tokens.get(i);
      return TypedValue.ofArray(new StaticArrayCallback(grid));
    }

    private static List<String> parseCsv(
        String text, String delimiter, char quote, boolean quoting) {
      List<String> list = new ArrayList<>();
      if (text == null || text.isEmpty()) return list;
      StringBuilder current = new StringBuilder();
      boolean inQuote = false;
      int i = 0;
      while (i < text.length()) {
        char c = text.charAt(i);
        if (quoting && inQuote) {
          if (c == quote) {
            if (i + 1 < text.length() && text.charAt(i + 1) == quote) {
              current.append(quote);
              i += 2;
            } else {
              inQuote = false;
              i++;
            }
          } else {
            current.append(c);
            i++;
          }
        } else {
          if (quoting && c == quote) {
            inQuote = true;
            i++;
          } else if (text.startsWith(delimiter, i)) {
            list.add(current.toString());
            current.setLength(0);
            i += delimiter.length();
          } else {
            current.append(c);
            i++;
          }
        }
      }
      list.add(current.toString());
      return list;
    }
  }

  public static class CsvTextFunction implements Function {
    @Override
    public String getCanonicalName() {
      return "CSVTEXT";
    }

    @Override
    public DataType getReturnType() {
      return DataType.TEXT;
    }

    @Override
    public TypedValue evaluate(FormulaContext context, ParameterCallback params)
        throws EvaluationException {
      if (params.getParameterCount() < 1 || params.getParameterCount() > 4)
        throw new EvaluationException(FormulaErrorValue.ERROR_ARGUMENTS_VALUE);
      List<Object> seq = new ArrayList<>();
      flatten(params.getValue(0), seq);
      boolean quoting = true;
      String delim = ",";
      String quote = "\"";

      if (params.getParameterCount() == 2) {
        Object p1 = params.getValue(1);
        if (p1 instanceof Boolean b) {
          quoting = b;
        } else {
          delim = context.getTypeRegistry().convertToText(params.getType(1), p1);
        }
      } else if (params.getParameterCount() == 3) {
        Object p1 = params.getValue(1);
        if (p1 instanceof Boolean b) {
          quoting = b;
          delim = context.getTypeRegistry().convertToText(params.getType(2), params.getValue(2));
        } else {
          delim = context.getTypeRegistry().convertToText(params.getType(1), p1);
          quote = context.getTypeRegistry().convertToText(params.getType(2), params.getValue(2));
        }
      } else if (params.getParameterCount() == 4) {
        quoting = context.getTypeRegistry().convertToLogical(params.getType(1), params.getValue(1));
        delim = context.getTypeRegistry().convertToText(params.getType(2), params.getValue(2));
        quote = context.getTypeRegistry().convertToText(params.getType(3), params.getValue(3));
      }

      StringBuilder sb = new StringBuilder();
      for (int i = 0; i < seq.size(); i++) {
        if (i > 0) sb.append(delim);
        Object item = seq.get(i);
        String s =
            (item != null)
                ? context
                    .getTypeRegistry()
                    .convertToText(context.getTypeRegistry().guessType(item), item)
                : "";
        if (quoting && !quote.isEmpty()) {
          sb.append(quote).append(s.replace(quote, quote + quote)).append(quote);
        } else {
          sb.append(s);
        }
      }
      return TypedValue.ofText(sb.toString());
    }
  }

  public static class NormalizeArrayFunction implements Function {
    @Override
    public String getCanonicalName() {
      return "NORMALIZEARRAY";
    }

    @Override
    public DataType getReturnType() {
      return DataType.ARRAY;
    }

    @Override
    public TypedValue evaluate(FormulaContext context, ParameterCallback params)
        throws EvaluationException {
      if (params.getParameterCount() != 1)
        throw new EvaluationException(FormulaErrorValue.ERROR_ARGUMENTS_VALUE);
      List<Object> seq = new ArrayList<>();
      flatten(params.getValue(0), seq);
      Object[][] grid = new Object[1][seq.size()];
      for (int i = 0; i < seq.size(); i++) grid[0][i] = seq.get(i);
      return TypedValue.ofArray(new StaticArrayCallback(grid));
    }
  }

  public static class NullFunction implements Function {
    @Override
    public String getCanonicalName() {
      return "NULL";
    }

    @Override
    public DataType getReturnType() {
      return DataType.ANY;
    }

    @Override
    public TypedValue evaluate(FormulaContext context, ParameterCallback params) {
      return TypedValue.NULL;
    }
  }

  public static class ParseDateFunction implements Function {
    @Override
    public String getCanonicalName() {
      return "PARSEDATE";
    }

    @Override
    public DataType getReturnType() {
      return DataType.DATETIME;
    }

    @Override
    public TypedValue evaluate(FormulaContext context, ParameterCallback params)
        throws EvaluationException {
      if (params.getParameterCount() != 2)
        throw new EvaluationException(FormulaErrorValue.ERROR_ARGUMENTS_VALUE);
      String text = context.getTypeRegistry().convertToText(params.getType(0), params.getValue(0));
      String pattern =
          context.getTypeRegistry().convertToText(params.getType(1), params.getValue(1));
      try {
        SimpleDateFormat sdf = new SimpleDateFormat(pattern, context.getLocale());
        sdf.setTimeZone(context.getTimeZone());
        Date date = sdf.parse(text);
        return TypedValue.ofDate(date);
      } catch (ParseException e) {
        throw new EvaluationException(FormulaErrorValue.ERROR_INVALID_ARGUMENT_VALUE, e);
      }
    }
  }

  public static class SequenceQuoterFunction implements Function {
    @Override
    public String getCanonicalName() {
      return "SEQUENCEQUOTER";
    }

    @Override
    public DataType getReturnType() {
      return DataType.ARRAY;
    }

    @Override
    public TypedValue evaluate(FormulaContext context, ParameterCallback params)
        throws EvaluationException {
      if (params.getParameterCount() < 1 || params.getParameterCount() > 2)
        throw new EvaluationException(FormulaErrorValue.ERROR_ARGUMENTS_VALUE);
      List<Object> seq =
          context.getTypeRegistry().convertToSequence(params.getType(0), params.getValue(0));
      String quote = "\"";
      if (params.getParameterCount() == 2 && params.getValue(1) != null) {
        quote = context.getTypeRegistry().convertToText(params.getType(1), params.getValue(1));
      }
      Object[][] grid = new Object[1][seq.size()];
      for (int i = 0; i < seq.size(); i++) {
        Object item = seq.get(i);
        String s =
            (item != null)
                ? context
                    .getTypeRegistry()
                    .convertToText(context.getTypeRegistry().guessType(item), item)
                : "";
        grid[0][i] = quote + s + quote;
      }
      return TypedValue.ofArray(new StaticArrayCallback(grid));
    }
  }
}
