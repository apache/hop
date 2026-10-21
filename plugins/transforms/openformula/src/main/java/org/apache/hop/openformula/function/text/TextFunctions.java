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

package org.apache.hop.openformula.function.text;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.text.DecimalFormat;
import java.text.DecimalFormatSymbols;
import java.text.MessageFormat;
import java.text.SimpleDateFormat;
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
import org.apache.hop.openformula.typing.DataType;
import org.apache.hop.openformula.typing.TypedValue;

public final class TextFunctions {
  private TextFunctions() {}

  public static void registerAll(DefaultFunctionRegistry registry) {
    registry.registerFunction(
        new AscFunction(),
        new FunctionDescription(
            "ASC",
            FunctionCategory.TEXT,
            "Converts full-width characters to half-width",
            DataType.TEXT,
            1,
            false,
            List.of("text"),
            List.of("Text to convert")));
    registry.registerFunction(
        new CharFunction(),
        new FunctionDescription(
            "CHAR",
            FunctionCategory.TEXT,
            "Returns character specified by code point",
            DataType.TEXT,
            1,
            false,
            List.of("code"),
            List.of("Character code")));
    registry.registerFunction(
        new CleanFunction(),
        new FunctionDescription(
            "CLEAN",
            FunctionCategory.TEXT,
            "Removes non-printable characters from text",
            DataType.TEXT,
            1,
            false,
            List.of("text"),
            List.of("Text to clean")));
    registry.registerFunction(
        new CodeFunction(),
        new FunctionDescription(
            "CODE",
            FunctionCategory.TEXT,
            "Returns numeric code for first character in text",
            DataType.NUMBER,
            1,
            false,
            List.of("text"),
            List.of("Text string")));
    registry.registerFunction(
        new ConcatenateFunction(),
        new FunctionDescription(
            "CONCATENATE",
            FunctionCategory.TEXT,
            "Concatenates multiple text values",
            DataType.TEXT,
            1,
            true,
            List.of("text"),
            List.of("Text items to join")));
    registry.registerFunction(
        new DollarFunction(),
        new FunctionDescription(
            "DOLLAR",
            FunctionCategory.TEXT,
            "Formats a number as currency text",
            DataType.TEXT,
            1,
            false,
            List.of("number", "decimals"),
            List.of("Number", "Decimals (defaults to 2)")));
    registry.registerFunction(
        new ExactFunction(),
        new FunctionDescription(
            "EXACT",
            FunctionCategory.TEXT,
            "Case-sensitive comparison of two text strings",
            DataType.LOGICAL,
            2,
            false,
            List.of("text1", "text2"),
            List.of("First text", "Second text")));
    registry.registerFunction(
        new FindFunction(),
        new FunctionDescription(
            "FIND",
            FunctionCategory.TEXT,
            "Case-sensitive search for a substring within text",
            DataType.NUMBER,
            2,
            false,
            List.of("find_text", "within_text", "start_num"),
            List.of("Text to find", "Text to search", "Starting position (1-based)")));
    registry.registerFunction(
        new FixedFunction(),
        new FunctionDescription(
            "FIXED",
            FunctionCategory.TEXT,
            "Formats a number as text with a fixed number of decimals",
            DataType.TEXT,
            1,
            false,
            List.of("number", "decimals", "no_commas"),
            List.of("Number", "Decimals (defaults to 2)", "Omit commas if true")));
    registry.registerFunction(
        new LeftFunction(),
        new FunctionDescription(
            "LEFT",
            FunctionCategory.TEXT,
            "Returns leftmost characters from text",
            DataType.TEXT,
            1,
            false,
            List.of("text", "num_chars"),
            List.of("Source text", "Number of characters")));
    registry.registerFunction(
        new LenFunction(),
        new FunctionDescription(
            "LEN",
            FunctionCategory.TEXT,
            "Returns character count of text",
            DataType.NUMBER,
            1,
            false,
            List.of("text"),
            List.of("Source text")));
    registry.registerFunction(
        new LowerFunction(),
        new FunctionDescription(
            "LOWER",
            FunctionCategory.TEXT,
            "Converts text to lowercase",
            DataType.TEXT,
            1,
            false,
            List.of("text"),
            List.of("Source text")));
    registry.registerFunction(
        new MessageFunction(),
        new FunctionDescription(
            "MESSAGE",
            FunctionCategory.TEXT,
            "Formats arguments using MessageFormat pattern",
            DataType.TEXT,
            1,
            true,
            List.of("pattern", "arg"),
            List.of("Pattern string", "Arguments")));
    registry.registerFunction(
        new MidFunction(),
        new FunctionDescription(
            "MID",
            FunctionCategory.TEXT,
            "Extracts a substring from text",
            DataType.TEXT,
            3,
            false,
            List.of("text", "start_num", "num_chars"),
            List.of("Source text", "Start position (1-based)", "Number of characters")));
    registry.registerFunction(
        new ProperFunction(),
        new FunctionDescription(
            "PROPER",
            FunctionCategory.TEXT,
            "Capitalizes each word in text",
            DataType.TEXT,
            1,
            false,
            List.of("text"),
            List.of("Source text")));
    registry.registerFunction(
        new ReplaceFunction(),
        new FunctionDescription(
            "REPLACE",
            FunctionCategory.TEXT,
            "Replaces characters within text by position",
            DataType.TEXT,
            4,
            false,
            List.of("old_text", "start_num", "num_chars", "new_text"),
            List.of("Source text", "Start position", "Length", "Replacement text")));
    registry.registerFunction(
        new ReptFunction(),
        new FunctionDescription(
            "REPT",
            FunctionCategory.TEXT,
            "Repeats text a given number of times",
            DataType.TEXT,
            2,
            false,
            List.of("text", "count"),
            List.of("Source text", "Number of repetitions")));
    registry.registerFunction(
        new RightFunction(),
        new FunctionDescription(
            "RIGHT",
            FunctionCategory.TEXT,
            "Returns rightmost characters from text",
            DataType.TEXT,
            1,
            false,
            List.of("text", "num_chars"),
            List.of("Source text", "Number of characters")));
    registry.registerFunction(
        new SearchFunction(),
        new FunctionDescription(
            "SEARCH",
            FunctionCategory.TEXT,
            "Case-insensitive search with wildcards",
            DataType.NUMBER,
            2,
            false,
            List.of("find_text", "within_text", "start_num"),
            List.of("Search pattern", "Text to search", "Start position (1-based)")));
    registry.registerFunction(
        new StringCountFunction(),
        new FunctionDescription(
            "STRINGCOUNT",
            FunctionCategory.TEXT,
            "Counts non-overlapping occurrences of substring",
            DataType.NUMBER,
            2,
            false,
            List.of("text", "search_str"),
            List.of("Source text", "Substring to count")));
    registry.registerFunction(
        new SubstituteFunction(),
        new FunctionDescription(
            "SUBSTITUTE",
            FunctionCategory.TEXT,
            "Substitutes occurrences of substring with new text",
            DataType.TEXT,
            3,
            false,
            List.of("text", "old_text", "new_text", "instance_num"),
            List.of("Source text", "Old substring", "New substring", "Occurrence (optional)")));
    registry.registerFunction(
        new TFunction(),
        new FunctionDescription(
            "T",
            FunctionCategory.TEXT,
            "Returns text if value is text, otherwise empty string",
            DataType.TEXT,
            1,
            false,
            List.of("value"),
            List.of("Value to test")));
    registry.registerFunction(
        new TextFunction(),
        new FunctionDescription(
            "TEXT",
            FunctionCategory.TEXT,
            "Formats a number or date using a format pattern",
            DataType.TEXT,
            2,
            false,
            List.of("value", "format_pattern"),
            List.of("Value to format", "Format pattern")));
    registry.registerFunction(
        new TrimFunction(),
        new FunctionDescription(
            "TRIM",
            FunctionCategory.TEXT,
            "Trims whitespace from text and collapses spaces",
            DataType.TEXT,
            1,
            false,
            List.of("text"),
            List.of("Source text")));
    registry.registerFunction(
        new UnicharFunction(),
        new FunctionDescription(
            "UNICHAR",
            FunctionCategory.TEXT,
            "Returns Unicode character specified by integer code point",
            DataType.TEXT,
            1,
            false,
            List.of("code"),
            List.of("Unicode code point")));
    registry.registerFunction(
        new UnicodeFunction(),
        new FunctionDescription(
            "UNICODE",
            FunctionCategory.TEXT,
            "Returns Unicode code point of first character in text",
            DataType.NUMBER,
            1,
            false,
            List.of("text"),
            List.of("Source text")));
    registry.registerFunction(
        new UpperFunction(),
        new FunctionDescription(
            "UPPER",
            FunctionCategory.TEXT,
            "Converts text to uppercase",
            DataType.TEXT,
            1,
            false,
            List.of("text"),
            List.of("Source text")));
    registry.registerFunction(
        new URLBuilderFunction(),
        new FunctionDescription(
            "URLBUILDER",
            FunctionCategory.TEXT,
            "Builds URL with encoded query parameters",
            DataType.TEXT,
            1,
            true,
            List.of("url", "key", "val"),
            List.of("Base URL", "Parameter keys and values")));
    registry.registerFunction(
        new URLEncodeFunction(),
        new FunctionDescription(
            "URLENCODE",
            FunctionCategory.TEXT,
            "URL encodes a string",
            DataType.TEXT,
            1,
            false,
            List.of("text"),
            List.of("Text to encode")));
  }

  public static class AscFunction implements Function {
    @Override
    public String getCanonicalName() {
      return "ASC";
    }

    @Override
    public DataType getReturnType() {
      return DataType.TEXT;
    }

    @Override
    public TypedValue evaluate(FormulaContext context, ParameterCallback params)
        throws EvaluationException {
      if (params.getParameterCount() != 1)
        throw new EvaluationException(FormulaErrorValue.ERROR_ARGUMENTS_VALUE);
      String text = context.getTypeRegistry().convertToText(params.getType(0), params.getValue(0));
      StringBuilder sb = new StringBuilder();
      for (int i = 0; i < text.length(); i++) {
        char c = text.charAt(i);
        if (c >= 0xFF01 && c <= 0xFF5E) {
          sb.append((char) (c - 0xFEE0));
        } else if (c == 0x3000) {
          sb.append(' ');
        } else {
          sb.append(c);
        }
      }
      return TypedValue.ofText(sb.toString());
    }
  }

  public static class CharFunction implements Function {
    @Override
    public String getCanonicalName() {
      return "CHAR";
    }

    @Override
    public DataType getReturnType() {
      return DataType.TEXT;
    }

    @Override
    public TypedValue evaluate(FormulaContext context, ParameterCallback params)
        throws EvaluationException {
      if (params.getParameterCount() != 1)
        throw new EvaluationException(FormulaErrorValue.ERROR_ARGUMENTS_VALUE);
      BigDecimal code =
          context.getTypeRegistry().convertToNumber(params.getType(0), params.getValue(0));
      int val = code.intValue();
      if (val < 1 || val > 255)
        throw new EvaluationException(FormulaErrorValue.ERROR_INVALID_ARGUMENT_VALUE);
      return TypedValue.ofText(String.valueOf((char) val));
    }
  }

  public static class CleanFunction implements Function {
    @Override
    public String getCanonicalName() {
      return "CLEAN";
    }

    @Override
    public DataType getReturnType() {
      return DataType.TEXT;
    }

    @Override
    public TypedValue evaluate(FormulaContext context, ParameterCallback params)
        throws EvaluationException {
      if (params.getParameterCount() != 1)
        throw new EvaluationException(FormulaErrorValue.ERROR_ARGUMENTS_VALUE);
      String text = context.getTypeRegistry().convertToText(params.getType(0), params.getValue(0));
      StringBuilder sb = new StringBuilder();
      for (int i = 0; i < text.length(); i++) {
        char c = text.charAt(i);
        if (c >= 32) sb.append(c);
      }
      return TypedValue.ofText(sb.toString());
    }
  }

  public static class CodeFunction implements Function {
    @Override
    public String getCanonicalName() {
      return "CODE";
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
      String text = context.getTypeRegistry().convertToText(params.getType(0), params.getValue(0));
      if (text.isEmpty())
        throw new EvaluationException(FormulaErrorValue.ERROR_INVALID_ARGUMENT_VALUE);
      return TypedValue.ofNumber(BigDecimal.valueOf(text.charAt(0)));
    }
  }

  public static class ConcatenateFunction implements Function {
    @Override
    public String getCanonicalName() {
      return "CONCATENATE";
    }

    @Override
    public DataType getReturnType() {
      return DataType.TEXT;
    }

    @Override
    public TypedValue evaluate(FormulaContext context, ParameterCallback params)
        throws EvaluationException {
      StringBuilder sb = new StringBuilder();
      for (int i = 0; i < params.getParameterCount(); i++) {
        List<Object> seq =
            context.getTypeRegistry().convertToSequence(params.getType(i), params.getValue(i));
        for (Object item : seq) {
          if (item != null) {
            sb.append(
                context
                    .getTypeRegistry()
                    .convertToText(context.getTypeRegistry().guessType(item), item));
          }
        }
      }
      return TypedValue.ofText(sb.toString());
    }
  }

  public static class DollarFunction implements Function {
    @Override
    public String getCanonicalName() {
      return "DOLLAR";
    }

    @Override
    public DataType getReturnType() {
      return DataType.TEXT;
    }

    @Override
    public TypedValue evaluate(FormulaContext context, ParameterCallback params)
        throws EvaluationException {
      if (params.getParameterCount() < 1 || params.getParameterCount() > 2)
        throw new EvaluationException(FormulaErrorValue.ERROR_ARGUMENTS_VALUE);
      BigDecimal n =
          context.getTypeRegistry().convertToNumber(params.getType(0), params.getValue(0));
      int dec = 2;
      if (params.getParameterCount() == 2) {
        dec =
            context
                .getTypeRegistry()
                .convertToNumber(params.getType(1), params.getValue(1))
                .intValue();
      }
      StringBuilder pattern = new StringBuilder("$#,##0");
      if (dec > 0) {
        pattern.append(".");
        pattern.append("0".repeat(dec));
      }
      DecimalFormat df =
          new DecimalFormat(pattern.toString(), new DecimalFormatSymbols(context.getLocale()));
      if (dec < 0) {
        n = n.setScale(dec, RoundingMode.HALF_UP);
      }
      return TypedValue.ofText(df.format(n));
    }
  }

  public static class ExactFunction implements Function {
    @Override
    public String getCanonicalName() {
      return "EXACT";
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
      String t1 = context.getTypeRegistry().convertToText(params.getType(0), params.getValue(0));
      String t2 = context.getTypeRegistry().convertToText(params.getType(1), params.getValue(1));
      if (t1.equals(t2)) {
        return TypedValue.TRUE;
      }
      Object v0 = params.getValue(0);
      Object v1 = params.getValue(1);
      if (v0 instanceof Number && v1 instanceof Number) {
        double d0 = ((Number) v0).doubleValue();
        double d1 = ((Number) v1).doubleValue();
        if (Math.abs(d0 - d1) < 0.00005) {
          return TypedValue.TRUE;
        }
      }
      return TypedValue.FALSE;
    }
  }

  public static class FindFunction implements Function {
    @Override
    public String getCanonicalName() {
      return "FIND";
    }

    @Override
    public DataType getReturnType() {
      return DataType.NUMBER;
    }

    @Override
    public TypedValue evaluate(FormulaContext context, ParameterCallback params)
        throws EvaluationException {
      if (params.getParameterCount() < 2 || params.getParameterCount() > 3)
        throw new EvaluationException(FormulaErrorValue.ERROR_ARGUMENTS_VALUE);
      String find = context.getTypeRegistry().convertToText(params.getType(0), params.getValue(0));
      String within =
          context.getTypeRegistry().convertToText(params.getType(1), params.getValue(1));
      int start = 1;
      if (params.getParameterCount() == 3) {
        start =
            context
                .getTypeRegistry()
                .convertToNumber(params.getType(2), params.getValue(2))
                .intValue();
      }
      if (start < 1 || start > within.length() + 1)
        throw new EvaluationException(FormulaErrorValue.ERROR_INVALID_ARGUMENT_VALUE);
      int idx = within.indexOf(find, start - 1);
      if (idx < 0) throw new EvaluationException(FormulaErrorValue.ERROR_NOT_FOUND_VALUE);
      return TypedValue.ofNumber(BigDecimal.valueOf(idx + 1));
    }
  }

  public static class FixedFunction implements Function {
    @Override
    public String getCanonicalName() {
      return "FIXED";
    }

    @Override
    public DataType getReturnType() {
      return DataType.TEXT;
    }

    @Override
    public TypedValue evaluate(FormulaContext context, ParameterCallback params)
        throws EvaluationException {
      if (params.getParameterCount() < 1 || params.getParameterCount() > 3)
        throw new EvaluationException(FormulaErrorValue.ERROR_ARGUMENTS_VALUE);
      BigDecimal n =
          context.getTypeRegistry().convertToNumber(params.getType(0), params.getValue(0));
      int dec = 2;
      if (params.getParameterCount() >= 2 && params.getValue(1) != null) {
        dec =
            context
                .getTypeRegistry()
                .convertToNumber(params.getType(1), params.getValue(1))
                .intValue();
      }
      boolean noCommas = false;
      if (params.getParameterCount() == 3 && params.getValue(2) != null) {
        noCommas =
            context.getTypeRegistry().convertToLogical(params.getType(2), params.getValue(2));
      }
      StringBuilder pat = new StringBuilder(noCommas ? "0" : "#,##0");
      if (dec > 0) {
        pat.append(".");
        pat.append("0".repeat(dec));
      }
      DecimalFormat df =
          new DecimalFormat(pat.toString(), new DecimalFormatSymbols(context.getLocale()));
      if (dec < 0) {
        n = n.setScale(dec, RoundingMode.HALF_UP);
      }
      return TypedValue.ofText(df.format(n));
    }
  }

  public static class LeftFunction implements Function {
    @Override
    public String getCanonicalName() {
      return "LEFT";
    }

    @Override
    public DataType getReturnType() {
      return DataType.TEXT;
    }

    @Override
    public TypedValue evaluate(FormulaContext context, ParameterCallback params)
        throws EvaluationException {
      if (params.getParameterCount() < 1 || params.getParameterCount() > 2)
        throw new EvaluationException(FormulaErrorValue.ERROR_ARGUMENTS_VALUE);
      String text = context.getTypeRegistry().convertToText(params.getType(0), params.getValue(0));
      int count = 1;
      if (params.getParameterCount() == 2) {
        BigDecimal bd =
            context.getTypeRegistry().convertToNumber(params.getType(1), params.getValue(1));
        if (bd.signum() < 0)
          throw new EvaluationException(FormulaErrorValue.ERROR_INVALID_ARGUMENT_VALUE);
        count = bd.intValue();
      }
      if (count >= text.length()) return TypedValue.ofText(text);
      return TypedValue.ofText(text.substring(0, count));
    }
  }

  public static class LenFunction implements Function {
    @Override
    public String getCanonicalName() {
      return "LEN";
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
      String text = context.getTypeRegistry().convertToText(params.getType(0), params.getValue(0));
      return TypedValue.ofNumber(BigDecimal.valueOf(text.length()));
    }
  }

  public static class LowerFunction implements Function {
    @Override
    public String getCanonicalName() {
      return "LOWER";
    }

    @Override
    public DataType getReturnType() {
      return DataType.TEXT;
    }

    @Override
    public TypedValue evaluate(FormulaContext context, ParameterCallback params)
        throws EvaluationException {
      if (params.getParameterCount() != 1)
        throw new EvaluationException(FormulaErrorValue.ERROR_ARGUMENTS_VALUE);
      String text = context.getTypeRegistry().convertToText(params.getType(0), params.getValue(0));
      return TypedValue.ofText(text.toLowerCase(context.getLocale()));
    }
  }

  public static class MessageFunction implements Function {
    @Override
    public String getCanonicalName() {
      return "MESSAGE";
    }

    @Override
    public DataType getReturnType() {
      return DataType.TEXT;
    }

    @Override
    public TypedValue evaluate(FormulaContext context, ParameterCallback params)
        throws EvaluationException {
      if (params.getParameterCount() < 1)
        throw new EvaluationException(FormulaErrorValue.ERROR_ARGUMENTS_VALUE);
      String pattern =
          context.getTypeRegistry().convertToText(params.getType(0), params.getValue(0));
      Object[] args = new Object[params.getParameterCount() - 1];
      for (int i = 1; i < params.getParameterCount(); i++) {
        args[i - 1] = params.getValue(i);
      }
      MessageFormat mf = new MessageFormat(pattern, context.getLocale());
      return TypedValue.ofText(mf.format(args));
    }
  }

  public static class MidFunction implements Function {
    @Override
    public String getCanonicalName() {
      return "MID";
    }

    @Override
    public DataType getReturnType() {
      return DataType.TEXT;
    }

    @Override
    public TypedValue evaluate(FormulaContext context, ParameterCallback params)
        throws EvaluationException {
      if (params.getParameterCount() != 3)
        throw new EvaluationException(FormulaErrorValue.ERROR_ARGUMENTS_VALUE);
      String text = context.getTypeRegistry().convertToText(params.getType(0), params.getValue(0));
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
      if (startIdx >= text.length()) return TypedValue.ofText("");
      int endIdx = Math.min(startIdx + count, text.length());
      return TypedValue.ofText(text.substring(startIdx, endIdx));
    }
  }

  public static class ProperFunction implements Function {
    @Override
    public String getCanonicalName() {
      return "PROPER";
    }

    @Override
    public DataType getReturnType() {
      return DataType.TEXT;
    }

    @Override
    public TypedValue evaluate(FormulaContext context, ParameterCallback params)
        throws EvaluationException {
      if (params.getParameterCount() != 1)
        throw new EvaluationException(FormulaErrorValue.ERROR_ARGUMENTS_VALUE);
      String text = context.getTypeRegistry().convertToText(params.getType(0), params.getValue(0));
      StringBuilder sb = new StringBuilder();
      boolean nextUpper = true;
      for (int i = 0; i < text.length(); i++) {
        char c = text.charAt(i);
        if (Character.isLetter(c)) {
          sb.append(nextUpper ? Character.toUpperCase(c) : Character.toLowerCase(c));
          nextUpper = false;
        } else {
          sb.append(c);
          nextUpper = true;
        }
      }
      return TypedValue.ofText(sb.toString());
    }
  }

  public static class ReplaceFunction implements Function {
    @Override
    public String getCanonicalName() {
      return "REPLACE";
    }

    @Override
    public DataType getReturnType() {
      return DataType.TEXT;
    }

    @Override
    public TypedValue evaluate(FormulaContext context, ParameterCallback params)
        throws EvaluationException {
      if (params.getParameterCount() != 4)
        throw new EvaluationException(FormulaErrorValue.ERROR_ARGUMENTS_VALUE);
      String oldText =
          context.getTypeRegistry().convertToText(params.getType(0), params.getValue(0));
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
      String newText =
          context.getTypeRegistry().convertToText(params.getType(3), params.getValue(3));
      if (start < 1 || count < 0)
        throw new EvaluationException(FormulaErrorValue.ERROR_INVALID_ARGUMENT_VALUE);
      int startIdx = Math.min(start - 1, oldText.length());
      int endIdx = Math.min(startIdx + count, oldText.length());
      String result = oldText.substring(0, startIdx) + newText + oldText.substring(endIdx);
      return TypedValue.ofText(result);
    }
  }

  public static class ReptFunction implements Function {
    @Override
    public String getCanonicalName() {
      return "REPT";
    }

    @Override
    public DataType getReturnType() {
      return DataType.TEXT;
    }

    @Override
    public TypedValue evaluate(FormulaContext context, ParameterCallback params)
        throws EvaluationException {
      if (params.getParameterCount() != 2)
        throw new EvaluationException(FormulaErrorValue.ERROR_ARGUMENTS_VALUE);
      String text = context.getTypeRegistry().convertToText(params.getType(0), params.getValue(0));
      int count =
          context
              .getTypeRegistry()
              .convertToNumber(params.getType(1), params.getValue(1))
              .intValue();
      if (count < 0) throw new EvaluationException(FormulaErrorValue.ERROR_INVALID_ARGUMENT_VALUE);
      return TypedValue.ofText(text.repeat(count));
    }
  }

  public static class RightFunction implements Function {
    @Override
    public String getCanonicalName() {
      return "RIGHT";
    }

    @Override
    public DataType getReturnType() {
      return DataType.TEXT;
    }

    @Override
    public TypedValue evaluate(FormulaContext context, ParameterCallback params)
        throws EvaluationException {
      if (params.getParameterCount() < 1 || params.getParameterCount() > 2)
        throw new EvaluationException(FormulaErrorValue.ERROR_ARGUMENTS_VALUE);
      String text = context.getTypeRegistry().convertToText(params.getType(0), params.getValue(0));
      int count = 1;
      if (params.getParameterCount() == 2) {
        BigDecimal bd =
            context.getTypeRegistry().convertToNumber(params.getType(1), params.getValue(1));
        if (bd.signum() < 0)
          throw new EvaluationException(FormulaErrorValue.ERROR_INVALID_ARGUMENT_VALUE);
        count = bd.intValue();
      }
      if (count >= text.length()) return TypedValue.ofText(text);
      return TypedValue.ofText(text.substring(text.length() - count));
    }
  }

  public static class SearchFunction implements Function {
    @Override
    public String getCanonicalName() {
      return "SEARCH";
    }

    @Override
    public DataType getReturnType() {
      return DataType.NUMBER;
    }

    @Override
    public TypedValue evaluate(FormulaContext context, ParameterCallback params)
        throws EvaluationException {
      if (params.getParameterCount() < 2 || params.getParameterCount() > 3)
        throw new EvaluationException(FormulaErrorValue.ERROR_ARGUMENTS_VALUE);
      String find = context.getTypeRegistry().convertToText(params.getType(0), params.getValue(0));
      String within =
          context.getTypeRegistry().convertToText(params.getType(1), params.getValue(1));
      int start = 1;
      if (params.getParameterCount() == 3) {
        start =
            context
                .getTypeRegistry()
                .convertToNumber(params.getType(2), params.getValue(2))
                .intValue();
      }
      if (start < 1 || start > within.length() + 1)
        throw new EvaluationException(FormulaErrorValue.ERROR_INVALID_ARGUMENT_VALUE);
      String withinLower = within.substring(start - 1).toLowerCase(context.getLocale());
      String findLower = find.toLowerCase(context.getLocale());

      // Simple wildcard check (? and *)
      if (!findLower.contains("?") && !findLower.contains("*")) {
        int idx = withinLower.indexOf(findLower);
        if (idx < 0) throw new EvaluationException(FormulaErrorValue.ERROR_NOT_FOUND_VALUE);
        return TypedValue.ofNumber(BigDecimal.valueOf(idx + start));
      }

      StringBuilder regex = new StringBuilder();
      for (char c : findLower.toCharArray()) {
        if (c == '?') regex.append(".");
        else if (c == '*') regex.append(".*?");
        else if ("\\.[]{}()+^$|".indexOf(c) != -1) regex.append('\\').append(c);
        else regex.append(c);
      }
      java.util.regex.Matcher m =
          java.util.regex.Pattern.compile(regex.toString()).matcher(withinLower);
      if (m.find()) {
        return TypedValue.ofNumber(BigDecimal.valueOf(m.start() + start));
      }
      throw new EvaluationException(FormulaErrorValue.ERROR_NOT_FOUND_VALUE);
    }
  }

  public static class StringCountFunction implements Function {
    @Override
    public String getCanonicalName() {
      return "STRINGCOUNT";
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
      String text = context.getTypeRegistry().convertToText(params.getType(0), params.getValue(0));
      String sub = context.getTypeRegistry().convertToText(params.getType(1), params.getValue(1));
      if (sub.isEmpty()) return TypedValue.ofNumber(BigDecimal.ZERO);
      int count = 0;
      int idx = 0;
      while ((idx = text.indexOf(sub, idx)) != -1) {
        count++;
        idx += sub.length();
      }
      return TypedValue.ofNumber(BigDecimal.valueOf(count));
    }
  }

  public static class SubstituteFunction implements Function {
    @Override
    public String getCanonicalName() {
      return "SUBSTITUTE";
    }

    @Override
    public DataType getReturnType() {
      return DataType.TEXT;
    }

    @Override
    public TypedValue evaluate(FormulaContext context, ParameterCallback params)
        throws EvaluationException {
      if (params.getParameterCount() < 3 || params.getParameterCount() > 4)
        throw new EvaluationException(FormulaErrorValue.ERROR_ARGUMENTS_VALUE);
      String text = context.getTypeRegistry().convertToText(params.getType(0), params.getValue(0));
      String oldText =
          context.getTypeRegistry().convertToText(params.getType(1), params.getValue(1));
      String newText =
          context.getTypeRegistry().convertToText(params.getType(2), params.getValue(2));
      if (oldText.isEmpty()) return TypedValue.ofText(text);

      if (params.getParameterCount() == 4) {
        int occurrence =
            context
                .getTypeRegistry()
                .convertToNumber(params.getType(3), params.getValue(3))
                .intValue();
        if (occurrence < 1)
          throw new EvaluationException(FormulaErrorValue.ERROR_INVALID_ARGUMENT_VALUE);
        int idx = 0;
        int current = 0;
        while ((idx = text.indexOf(oldText, idx)) != -1) {
          current++;
          if (current == occurrence) {
            return TypedValue.ofText(
                text.substring(0, idx) + newText + text.substring(idx + oldText.length()));
          }
          idx++;
        }
        return TypedValue.ofText(text);
      }
      return TypedValue.ofText(text.replace(oldText, newText));
    }
  }

  public static class TFunction implements Function {
    @Override
    public String getCanonicalName() {
      return "T";
    }

    @Override
    public DataType getReturnType() {
      return DataType.TEXT;
    }

    @Override
    public TypedValue evaluate(FormulaContext context, ParameterCallback params)
        throws EvaluationException {
      if (params.getParameterCount() != 1)
        throw new EvaluationException(FormulaErrorValue.ERROR_ARGUMENTS_VALUE);
      Object val = params.getValue(0);
      if (val instanceof String s) return TypedValue.ofText(s);
      return TypedValue.ofText("");
    }
  }

  public static class TextFunction implements Function {
    @Override
    public String getCanonicalName() {
      return "TEXT";
    }

    @Override
    public DataType getReturnType() {
      return DataType.TEXT;
    }

    @Override
    public TypedValue evaluate(FormulaContext context, ParameterCallback params)
        throws EvaluationException {
      if (params.getParameterCount() < 1 || params.getParameterCount() > 2)
        throw new EvaluationException(FormulaErrorValue.ERROR_ARGUMENTS_VALUE);
      if (params.getParameterCount() == 1) {
        return TypedValue.ofText(
            context.getTypeRegistry().convertToText(params.getType(0), params.getValue(0)));
      }
      Object val = params.getValue(0);
      String pattern =
          context.getTypeRegistry().convertToText(params.getType(1), params.getValue(1));
      if (val instanceof Date d) {
        SimpleDateFormat sdf = new SimpleDateFormat(pattern, context.getLocale());
        sdf.setTimeZone(context.getTimeZone());
        return TypedValue.ofText(sdf.format(d));
      }
      if (val instanceof Number || val instanceof String) {
        BigDecimal num = context.getTypeRegistry().convertToNumber(params.getType(0), val);
        DecimalFormat df =
            new DecimalFormat(pattern, new DecimalFormatSymbols(context.getLocale()));
        return TypedValue.ofText(df.format(num));
      }
      return TypedValue.ofText(String.valueOf(val));
    }
  }

  public static class TrimFunction implements Function {
    @Override
    public String getCanonicalName() {
      return "TRIM";
    }

    @Override
    public DataType getReturnType() {
      return DataType.TEXT;
    }

    @Override
    public TypedValue evaluate(FormulaContext context, ParameterCallback params)
        throws EvaluationException {
      if (params.getParameterCount() != 1)
        throw new EvaluationException(FormulaErrorValue.ERROR_ARGUMENTS_VALUE);
      String text = context.getTypeRegistry().convertToText(params.getType(0), params.getValue(0));
      return TypedValue.ofText(text.trim().replaceAll("\\s+", " "));
    }
  }

  public static class UnicharFunction implements Function {
    @Override
    public String getCanonicalName() {
      return "UNICHAR";
    }

    @Override
    public DataType getReturnType() {
      return DataType.TEXT;
    }

    @Override
    public TypedValue evaluate(FormulaContext context, ParameterCallback params)
        throws EvaluationException {
      if (params.getParameterCount() != 1)
        throw new EvaluationException(FormulaErrorValue.ERROR_ARGUMENTS_VALUE);
      BigDecimal code =
          context.getTypeRegistry().convertToNumber(params.getType(0), params.getValue(0));
      long cpLong = code.longValue();
      if (cpLong < 1
          || cpLong > Character.MAX_CODE_POINT
          || !Character.isValidCodePoint((int) cpLong)) {
        throw new EvaluationException(FormulaErrorValue.ERROR_INVALID_ARGUMENT_VALUE);
      }
      return TypedValue.ofText(new String(Character.toChars((int) cpLong)));
    }
  }

  public static class UnicodeFunction implements Function {
    @Override
    public String getCanonicalName() {
      return "UNICODE";
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
      String text = context.getTypeRegistry().convertToText(params.getType(0), params.getValue(0));
      if (text.isEmpty())
        throw new EvaluationException(FormulaErrorValue.ERROR_INVALID_ARGUMENT_VALUE);
      return TypedValue.ofNumber(BigDecimal.valueOf(text.codePointAt(0)));
    }
  }

  public static class UpperFunction implements Function {
    @Override
    public String getCanonicalName() {
      return "UPPER";
    }

    @Override
    public DataType getReturnType() {
      return DataType.TEXT;
    }

    @Override
    public TypedValue evaluate(FormulaContext context, ParameterCallback params)
        throws EvaluationException {
      if (params.getParameterCount() != 1)
        throw new EvaluationException(FormulaErrorValue.ERROR_ARGUMENTS_VALUE);
      String text = context.getTypeRegistry().convertToText(params.getType(0), params.getValue(0));
      return TypedValue.ofText(text.toUpperCase(context.getLocale()));
    }
  }

  public static class URLBuilderFunction implements Function {
    @Override
    public String getCanonicalName() {
      return "URLBUILDER";
    }

    @Override
    public DataType getReturnType() {
      return DataType.TEXT;
    }

    @Override
    public TypedValue evaluate(FormulaContext context, ParameterCallback params)
        throws EvaluationException {
      if (params.getParameterCount() < 1)
        throw new EvaluationException(FormulaErrorValue.ERROR_ARGUMENTS_VALUE);
      StringBuilder url =
          new StringBuilder(
              context.getTypeRegistry().convertToText(params.getType(0), params.getValue(0)));
      boolean hasQuery = url.indexOf("?") != -1;
      for (int i = 1; i < params.getParameterCount(); i += 2) {
        String key = context.getTypeRegistry().convertToText(params.getType(i), params.getValue(i));
        String val =
            (i + 1 < params.getParameterCount())
                ? context
                    .getTypeRegistry()
                    .convertToText(params.getType(i + 1), params.getValue(i + 1))
                : "";
        url.append(hasQuery ? "&" : "?");
        hasQuery = true;
        url.append(URLEncoder.encode(key, StandardCharsets.UTF_8))
            .append("=")
            .append(URLEncoder.encode(val, StandardCharsets.UTF_8));
      }
      return TypedValue.ofText(url.toString());
    }
  }

  public static class URLEncodeFunction implements Function {
    @Override
    public String getCanonicalName() {
      return "URLENCODE";
    }

    @Override
    public DataType getReturnType() {
      return DataType.TEXT;
    }

    @Override
    public TypedValue evaluate(FormulaContext context, ParameterCallback params)
        throws EvaluationException {
      if (params.getParameterCount() != 1)
        throw new EvaluationException(FormulaErrorValue.ERROR_ARGUMENTS_VALUE);
      String text = context.getTypeRegistry().convertToText(params.getType(0), params.getValue(0));
      return TypedValue.ofText(URLEncoder.encode(text, StandardCharsets.UTF_8));
    }
  }
}
