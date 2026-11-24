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

package org.apache.hop.core.database;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.text.DecimalFormat;
import java.text.DecimalFormatSymbols;
import java.text.NumberFormat;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Date;
import java.util.GregorianCalendar;
import java.util.TimeZone;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.core.row.value.ValueMetaBase;
import org.apache.hop.core.row.value.ValueMetaBigNumber;
import org.apache.hop.core.row.value.ValueMetaDate;
import org.apache.hop.core.row.value.ValueMetaInteger;
import org.apache.hop.core.row.value.ValueMetaNumber;

/**
 * Renders a value as text without going through {@link DecimalFormat} or {@link SimpleDateFormat},
 * for the common configurations where the result is provably the same as {@link
 * IValueMeta#getString(Object)} (or {@link SimpleDateFormat#format(Date)}) would give.
 *
 * <p>The formatter is worked out once from the value metadata, after which {@link
 * #append(StringBuilder, Object)} either appends exactly the text the slow path would produce and
 * returns {@code true}, or appends nothing and returns {@code false}, and the caller formats the
 * value the usual way. A formatter is only handed out when the metadata uses:
 *
 * <ul>
 *   <li>Integer: the default mask {@code ####0;-####0} with ASCII digits and minus sign. Any long.
 *   <li>Number: the default mask {@code ####0.0#########;-####0.0#########} with ASCII symbols and
 *       half-even rounding. Values below 2<sup>18</sup> in magnitude whose shortest representation
 *       has at most 10 fraction digits; the others take the slow path.
 *   <li>BigNumber: the default mask with ASCII symbols. Values with at most 20 fraction digits.
 *   <li>Date: a mask built only from {@code y} (not {@code yy}), {@code M}/{@code MM}, {@code d},
 *       {@code H}, {@code m}, {@code s}, {@code S}, ASCII punctuation and quoted ASCII text, on a
 *       Gregorian calendar with ASCII digits. Instants from the year 1600 to 9999 in the
 *       formatter's time zone.
 * </ul>
 *
 * <p>The decision is made from the metadata as it is when the formatter is created. Create a new
 * one when the metadata changes. Instances are immutable and may be shared between threads.
 */
public abstract class FastValueFormatter {

  static final String DEFAULT_INTEGER_MASK = "####0;-####0";
  static final String DEFAULT_NUMBER_MASK = "####0.0#########;-####0.0#########";
  static final String DEFAULT_BIG_NUMBER_MASK =
      "######0.0###################;-######0.0###################";

  /** Below this magnitude a double's shortest digits survive the default number mask unchanged. */
  private static final double NUMBER_FAST_LIMIT = 262_144.0; // 2^18

  private static final int NUMBER_MAX_FRACTION_DIGITS = 10;
  private static final int BIG_NUMBER_MAX_FRACTION_DIGITS = 20;

  FastValueFormatter() {}

  /**
   * Appends the text of a non-null value.
   *
   * @param sb the builder to append to
   * @param value the value in its normal storage (not null)
   * @return true when the text was appended, false when the value needs the slow path (nothing was
   *     appended)
   */
  public abstract boolean append(StringBuilder sb, Object value);

  /**
   * @param valueMeta the metadata of the value
   * @return a formatter that renders values exactly as {@code valueMeta.getString(value)} does, or
   *     null when the metadata needs the slow path
   */
  public static FastValueFormatter forValueMeta(IValueMeta valueMeta) {
    if (valueMeta == null
        || valueMeta.getStorageType() != IValueMeta.STORAGE_TYPE_NORMAL
        || valueMeta.getConversionMetadata() != null
        || (valueMeta.isOutputPaddingEnabled() && valueMeta.getLength() > 0)) {
      return null;
    }
    try {
      Class<?> clazz = valueMeta.getClass();
      if (clazz == ValueMetaInteger.class) {
        DecimalFormat format = valueMeta.getDecimalFormat(false);
        return isDefault(format, DEFAULT_INTEGER_MASK, false) ? IntegerFormatter.INSTANCE : null;
      }
      if (clazz == ValueMetaNumber.class) {
        // convertNumberToString raises the maximum fraction digits of the format in this case
        if (valueMeta.getConversionMask() != null && valueMeta.getStorageMetadata() != null) {
          return null;
        }
        DecimalFormat format = valueMeta.getDecimalFormat(false);
        return isDefault(format, DEFAULT_NUMBER_MASK, true) ? NumberFormatter.INSTANCE : null;
      }
      if (clazz == ValueMetaBigNumber.class) {
        DecimalFormat format =
            valueMeta.getDecimalFormat(((ValueMetaBase) valueMeta).isBigNumberFormatting());
        return isDefault(format, DEFAULT_BIG_NUMBER_MASK, false)
            ? BigNumberFormatter.INSTANCE
            : null;
      }
      if (clazz == ValueMetaDate.class) {
        return forDateFormat(valueMeta.getDateFormat());
      }
    } catch (Exception e) {
      // Whatever the metadata cannot do, the slow path will report.
      return null;
    }
    return null;
  }

  /**
   * @param dateFormat a date format
   * @return a formatter that renders dates exactly as {@code dateFormat.format(date)} does, or null
   *     when the pattern or the calendar needs the slow path
   */
  public static FastValueFormatter forDateFormat(SimpleDateFormat dateFormat) {
    if (dateFormat == null) {
      return null;
    }
    // A subclass (BuddhistCalendar) counts its years differently.
    if (dateFormat.getCalendar().getClass() != GregorianCalendar.class) {
      return null;
    }
    GregorianCalendar calendar = (GregorianCalendar) dateFormat.getCalendar();
    if (calendar.getGregorianChange().getTime()
        != new GregorianCalendar().getGregorianChange().getTime()) {
      return null;
    }
    NumberFormat numberFormat = dateFormat.getNumberFormat();
    if (!(numberFormat instanceof DecimalFormat decimalFormat)
        || decimalFormat.getDecimalFormatSymbols().getZeroDigit() != '0'
        || numberFormat.isGroupingUsed()) {
      return null;
    }
    return DateFormatter.compile(dateFormat.toPattern(), dateFormat.getTimeZone());
  }

  /**
   * @return the formatter of a Number in the default mask with ASCII symbols and half-even rounding
   */
  static FastValueFormatter defaultNumber() {
    return NumberFormatter.INSTANCE;
  }

  /**
   * @return the formatter of a BigNumber in the default mask with ASCII symbols
   */
  static FastValueFormatter defaultBigNumber() {
    return BigNumberFormatter.INSTANCE;
  }

  /**
   * True when the format renders like a format built from the mask with ASCII digits, a period as
   * decimal separator and a plain minus sign.
   */
  private static boolean isDefault(DecimalFormat format, String mask, boolean halfEven) {
    if (format == null) {
      return false;
    }
    DecimalFormatSymbols symbols = format.getDecimalFormatSymbols();
    if (symbols.getZeroDigit() != '0' || symbols.getDecimalSeparator() != '.') {
      return false;
    }
    DecimalFormat reference = new DecimalFormat(mask, symbols);
    reference.setRoundingMode(format.getRoundingMode());
    reference.setParseBigDecimal(format.isParseBigDecimal());
    if (!reference.equals(format)) {
      return false;
    }
    if (halfEven && format.getRoundingMode() != RoundingMode.HALF_EVEN) {
      return false;
    }
    return format.getPositivePrefix().isEmpty()
        && format.getPositiveSuffix().isEmpty()
        && "-".equals(format.getNegativePrefix())
        && format.getNegativeSuffix().isEmpty();
  }

  /** Integer, default mask: the decimal digits of the long. */
  private static final class IntegerFormatter extends FastValueFormatter {
    static final IntegerFormatter INSTANCE = new IntegerFormatter();

    @Override
    public boolean append(StringBuilder sb, Object value) {
      if (value instanceof Long l) {
        sb.append(l.longValue());
        return true;
      }
      return false;
    }
  }

  /**
   * Number, default mask. The mask keeps up to 10 fraction digits, rounding half-even, and at least
   * one. Below 2<sup>18</sup> the spacing of doubles is under 3e-11, so any decimal that reads back
   * as the double - the digits DecimalFormat starts from as much as the shortest ones {@link
   * Double#toString(double)} prints - rounds to the same 10-digit value. When the shortest digits
   * have no more than 10 fraction digits they are that value.
   */
  private static final class NumberFormatter extends FastValueFormatter {
    static final NumberFormatter INSTANCE = new NumberFormatter();

    @Override
    public boolean append(StringBuilder sb, Object value) {
      if (!(value instanceof Double d)) {
        return false;
      }
      double v = d;
      double abs = Math.abs(v);
      // Double.toString is plain (no exponent) from 1e-3 up to 1e7, and zero prints as 0.0.
      if (!(abs < NUMBER_FAST_LIMIT) || (abs < 1e-3 && abs != 0.0)) {
        return false;
      }
      int start = sb.length();
      sb.append(v);
      int dot = -1;
      for (int i = start; i < sb.length(); i++) {
        char c = sb.charAt(i);
        if (c == '.') {
          dot = i;
        } else if (c == 'E') {
          sb.setLength(start);
          return false;
        }
      }
      if (dot < 0 || sb.length() - dot - 1 > NUMBER_MAX_FRACTION_DIGITS) {
        sb.setLength(start);
        return false;
      }
      return true;
    }
  }

  /**
   * BigNumber, default mask: no grouping, at least one and up to 20 fraction digits, trailing zeros
   * removed. A value with at most 20 fraction digits is printed without rounding.
   */
  private static final class BigNumberFormatter extends FastValueFormatter {
    static final BigNumberFormatter INSTANCE = new BigNumberFormatter();

    @Override
    public boolean append(StringBuilder sb, Object value) {
      if (!(value instanceof BigDecimal big)) {
        return false;
      }
      if (big.signum() == 0) {
        sb.append("0.0");
        return true;
      }
      BigDecimal stripped = big.stripTrailingZeros();
      int scale = stripped.scale();
      if (scale > BIG_NUMBER_MAX_FRACTION_DIGITS) {
        return false;
      }
      if (scale <= 0) {
        sb.append(stripped.toBigInteger()).append(".0");
      } else {
        sb.append(stripped.toPlainString());
      }
      return true;
    }
  }

  /** A date pattern of numeric fields and literal punctuation, compiled into steps. */
  private static final class DateFormatter extends FastValueFormatter {
    private static final char LITERAL = 0;
    private static final long MILLIS_PER_DAY = 86_400_000L;
    private static final int MIN_YEAR = 1600;
    private static final int MAX_YEAR = 9999;

    /** Per step: the field letter, or {@link #LITERAL}. */
    private final char[] fields;

    /** Per step: the minimum number of digits, or the literal character. */
    private final int[] widths;

    private final TimeZone timeZone;

    private DateFormatter(char[] fields, int[] widths, TimeZone timeZone) {
      this.fields = fields;
      this.widths = widths;
      this.timeZone = timeZone;
    }

    static DateFormatter compile(String pattern, TimeZone timeZone) {
      if (pattern == null || pattern.isEmpty() || timeZone == null) {
        return null;
      }
      char[] fields = new char[pattern.length()];
      int[] widths = new int[pattern.length()];
      int steps = 0;
      int i = 0;
      while (i < pattern.length()) {
        char c = pattern.charAt(i);
        if (c == '\'') {
          // quoted literal text, '' is a single quote
          int end = i + 1;
          if (end < pattern.length() && pattern.charAt(end) == '\'') {
            fields[steps] = LITERAL;
            widths[steps++] = '\'';
            i += 2;
            continue;
          }
          while (true) {
            if (end >= pattern.length()) {
              return null; // unterminated
            }
            char q = pattern.charAt(end);
            if (q == '\'') {
              if (end + 1 < pattern.length() && pattern.charAt(end + 1) == '\'') {
                fields[steps] = LITERAL;
                widths[steps++] = '\'';
                end += 2;
                continue;
              }
              break;
            }
            if (q < 0x20 || q > 0x7e) {
              return null;
            }
            fields[steps] = LITERAL;
            widths[steps++] = q;
            end++;
          }
          i = end + 1;
          continue;
        }
        int count = 1;
        while (i + count < pattern.length() && pattern.charAt(i + count) == c) {
          count++;
        }
        switch (c) {
          case 'y':
            // yy keeps two digits only
            if (count == 2) {
              return null;
            }
            break;
          case 'M':
            // MMM and longer are month names
            if (count > 2) {
              return null;
            }
            break;
          case 'd', 'H', 'm', 's', 'S':
            break;
          default:
            if (c < 0x20 || c > 0x7e || c == '\'' || Character.isLetter(c)) {
              return null;
            }
            // literal punctuation, one step per character
            for (int k = 0; k < count; k++) {
              fields[steps] = LITERAL;
              widths[steps++] = c;
            }
            i += count;
            continue;
        }
        fields[steps] = c;
        widths[steps++] = count;
        i += count;
      }
      return new DateFormatter(
          Arrays.copyOf(fields, steps), Arrays.copyOf(widths, steps), (TimeZone) timeZone.clone());
    }

    @Override
    public boolean append(StringBuilder sb, Object value) {
      if (!(value instanceof Date date)) {
        return false;
      }
      long millis = date.getTime();
      long local;
      try {
        local = Math.addExact(millis, timeZone.getOffset(millis));
      } catch (ArithmeticException e) {
        return false;
      }
      long epochDay = Math.floorDiv(local, MILLIS_PER_DAY);
      int millisOfDay = (int) Math.floorMod(local, MILLIS_PER_DAY);

      // Proleptic Gregorian civil date from the day number (H. Hinnant's algorithm).
      long z = epochDay + 719_468L;
      long era = Math.floorDiv(z, 146_097L);
      long doe = z - era * 146_097L;
      long yoe = (doe - doe / 1460 + doe / 36_524 - doe / 146_096) / 365;
      long doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
      long mp = (5 * doy + 2) / 153;
      int day = (int) (doy - (153 * mp + 2) / 5 + 1);
      int month = (int) (mp < 10 ? mp + 3 : mp - 9);
      long year = yoe + era * 400 + (month <= 2 ? 1 : 0);
      if (year < MIN_YEAR || year > MAX_YEAR) {
        return false;
      }

      for (int k = 0; k < fields.length; k++) {
        int v;
        switch (fields[k]) {
          case LITERAL:
            sb.append((char) widths[k]);
            continue;
          case 'y':
            v = (int) year;
            break;
          case 'M':
            v = month;
            break;
          case 'd':
            v = day;
            break;
          case 'H':
            v = millisOfDay / 3_600_000;
            break;
          case 'm':
            v = (millisOfDay / 60_000) % 60;
            break;
          case 's':
            v = (millisOfDay / 1000) % 60;
            break;
          default: // 'S'
            v = millisOfDay % 1000;
            break;
        }
        appendPadded(sb, v, widths[k]);
      }
      return true;
    }

    private static void appendPadded(StringBuilder sb, int value, int minDigits) {
      int digits = value < 10 ? 1 : value < 100 ? 2 : value < 1000 ? 3 : value < 10_000 ? 4 : 10;
      if (digits == 10) {
        digits = Integer.toString(value).length();
      }
      for (int p = digits; p < minDigits; p++) {
        sb.append('0');
      }
      sb.append(value);
    }
  }
}
