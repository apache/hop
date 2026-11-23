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

package org.apache.hop.core.database;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.text.DecimalFormat;
import java.text.DecimalFormatSymbols;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Locale;
import org.apache.hop.core.exception.HopValueException;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.core.row.value.ValueMetaBase;

/**
 * Writes number and date values as the text of a SQL literal, the same in every locale.
 *
 * <ul>
 *   <li>Integer: the decimal digits of the long, with an ASCII minus sign. A mask is ignored.
 *   <li>Number and BigNumber: a period as decimal separator, no grouping, an ASCII minus sign and
 *       no currency, percent or other decoration. Of the format mask only the rounding is kept: the
 *       minimum and maximum number of fraction digits it implies, and the rounding mode of the
 *       value. Without a mask that is the default mask, so in en_US the text is what {@link
 *       IValueMeta#getString(Object)} gives. A mask in scientific notation keeps its exponent. NaN
 *       and infinity, which no database reads as a number, are written as {@code null}.
 *   <li>Date: the pattern of the value's date format, or of the pattern given, on a Gregorian
 *       calendar with ASCII digits and English names. The time zone is the one of the value's date
 *       format, or the default time zone for a pattern.
 * </ul>
 *
 * <p>Default masks are rendered through {@link FastValueFormatter}. Instances are not thread-safe.
 */
public abstract class SqlLiteralFormatter {

  /**
   * The locale of the number symbols. Locale.ROOT and Locale.US share them; ROOT says that no
   * region is meant.
   */
  private static final DecimalFormatSymbols SYMBOLS = DecimalFormatSymbols.getInstance(Locale.ROOT);

  /**
   * The locale of dates. Unlike Locale.ROOT it spells MMMM and G as en_US always did ("January",
   * "AD"), where ROOT writes "Jan" and "CE".
   */
  private static final Locale DATE_LOCALE = Locale.US;

  /** Fraction digits of the default Number and BigNumber masks. */
  private static final int NUMBER_MAX_FRACTION_DIGITS = 10;

  private static final int BIG_NUMBER_MAX_FRACTION_DIGITS = 20;

  /** As ValueMetaBase.convertNumberToString when the conversion mask differs from the storage. */
  private static final int NUMBER_LOSSLESS_FRACTION_DIGITS = 50;

  private static final String NULL = "null";

  /** The metadata of the values. */
  protected final IValueMeta valueMeta;

  SqlLiteralFormatter(IValueMeta valueMeta) {
    this.valueMeta = valueMeta;
  }

  /**
   * Appends the literal text of a non-null value, without quotes.
   *
   * @param sb the builder to append to
   * @param value the value, in the storage of the value metadata (not null)
   * @throws HopValueException when the value cannot be converted
   */
  public abstract void append(StringBuilder sb, Object value) throws HopValueException;

  /**
   * @param valueMeta the metadata of the values
   * @return the formatter for an Integer, Number, BigNumber or Date, or null for another type
   */
  public static SqlLiteralFormatter forValueMeta(IValueMeta valueMeta) {
    return switch (valueMeta.getType()) {
      case IValueMeta.TYPE_INTEGER -> new IntegerLiteral(valueMeta);
      case IValueMeta.TYPE_NUMBER -> new NumberLiteral(valueMeta);
      case IValueMeta.TYPE_BIGNUMBER -> new BigNumberLiteral(valueMeta);
      case IValueMeta.TYPE_DATE -> new DateLiteral(valueMeta, valueMeta.getDateFormat());
      default -> null;
    };
  }

  /**
   * @param valueMeta the metadata of the values, a Date
   * @param pattern a date pattern that overrides the format of the value metadata
   * @return the formatter that writes the dates of the value metadata in the pattern, in the
   *     default time zone
   * @throws IllegalArgumentException when the pattern is invalid
   */
  public static SqlLiteralFormatter forDatePattern(IValueMeta valueMeta, String pattern) {
    return new DateLiteral(valueMeta, newDateFormat(pattern));
  }

  /**
   * @param pattern a date pattern
   * @return a date format for the pattern on a Gregorian calendar with ASCII digits and English
   *     names, in the default time zone
   * @throws IllegalArgumentException when the pattern is invalid
   */
  public static SimpleDateFormat newDateFormat(String pattern) {
    return new SimpleDateFormat(pattern, DATE_LOCALE);
  }

  /**
   * @param value a value in the storage of the value metadata, may be null
   * @return the literal text of the value without quotes, or "null"
   * @throws HopValueException when the value cannot be converted
   */
  public String format(Object value) throws HopValueException {
    if (valueMeta.isNull(value)) {
      return NULL;
    }
    StringBuilder sb = new StringBuilder();
    append(sb, value);
    return sb.toString();
  }

  /**
   * A format that keeps the rounding of the value's format and nothing else.
   *
   * @param source the format of the value metadata
   * @param minimumMaxFractionDigits a lower bound for the maximum number of fraction digits
   */
  static DecimalFormat plainFormat(DecimalFormat source, int minimumMaxFractionDigits) {
    if (isScientific(source)) {
      DecimalFormat format = (DecimalFormat) source.clone();
      format.setDecimalFormatSymbols(SYMBOLS);
      format.setGroupingUsed(false);
      format.setMultiplier(1);
      // after the symbols, which expand the affixes of the pattern again
      format.setPositivePrefix("");
      format.setPositiveSuffix("");
      format.setNegativePrefix("-");
      format.setNegativeSuffix("");
      return format;
    }
    // A percent or permille mask rounds the multiplied value; keep those digits.
    int shift = decimalShift(source.getMultiplier());
    DecimalFormat format = new DecimalFormat("0", SYMBOLS);
    format.setGroupingUsed(false);
    format.setMinimumIntegerDigits(1);
    format.setMaximumFractionDigits(
        Math.max(source.getMaximumFractionDigits() + shift, minimumMaxFractionDigits));
    format.setMinimumFractionDigits(source.getMinimumFractionDigits() + shift);
    format.setRoundingMode(source.getRoundingMode());
    return format;
  }

  private static boolean isScientific(DecimalFormat source) {
    DecimalFormat probe = (DecimalFormat) source.clone();
    probe.setPositivePrefix("");
    probe.setPositiveSuffix("");
    probe.setNegativePrefix("-");
    probe.setNegativeSuffix("");
    return probe.toPattern().indexOf('E') >= 0;
  }

  /** k for a multiplier of 10^k (k &gt; 0), otherwise 0. */
  private static int decimalShift(int multiplier) {
    int shift = 0;
    int m = multiplier;
    while (m > 1 && m % 10 == 0) {
      m /= 10;
      shift++;
    }
    return m == 1 ? shift : 0;
  }

  /** True when the format renders like the default mask with the given fraction digits. */
  private static boolean isDefault(DecimalFormat format, int maxFractionDigits) {
    return !isScientific(format)
        && format.getMinimumFractionDigits() == 1
        && format.getMaximumFractionDigits() == maxFractionDigits
        && format.getMinimumIntegerDigits() == 1;
  }

  /** The digits of the long. */
  private static final class IntegerLiteral extends SqlLiteralFormatter {
    IntegerLiteral(IValueMeta valueMeta) {
      super(valueMeta);
    }

    @Override
    public void append(StringBuilder sb, Object value) throws HopValueException {
      sb.append(valueMeta.getInteger(value).longValue());
    }
  }

  private static final class NumberLiteral extends SqlLiteralFormatter {
    private final DecimalFormat format;
    private final FastValueFormatter fast;

    NumberLiteral(IValueMeta valueMeta) {
      super(valueMeta);
      int minimumMax = 0;
      String mask = valueMeta.getConversionMask();
      IValueMeta storage = valueMeta.getStorageMetadata();
      if (mask != null && storage != null && !mask.equals(storage.getConversionMask())) {
        minimumMax = NUMBER_LOSSLESS_FRACTION_DIGITS;
      }
      this.format = plainFormat(valueMeta.getDecimalFormat(false), minimumMax);
      this.fast =
          isDefault(format, NUMBER_MAX_FRACTION_DIGITS)
                  && format.getRoundingMode() == RoundingMode.HALF_EVEN
              ? FastValueFormatter.defaultNumber()
              : null;
    }

    @Override
    public void append(StringBuilder sb, Object value) throws HopValueException {
      Double number = valueMeta.getNumber(value);
      if (number.isNaN() || number.isInfinite()) {
        sb.append(NULL);
      } else if (fast == null || !fast.append(sb, number)) {
        sb.append(format.format(number));
      }
    }
  }

  private static final class BigNumberLiteral extends SqlLiteralFormatter {
    private final DecimalFormat format;
    private final FastValueFormatter fast;

    BigNumberLiteral(IValueMeta valueMeta) {
      super(valueMeta);
      boolean bigNumberFormatting =
          !(valueMeta instanceof ValueMetaBase base) || base.isBigNumberFormatting();
      this.format = plainFormat(valueMeta.getDecimalFormat(bigNumberFormatting), 0);
      // The fast path only writes values that need no rounding, so any rounding mode will do.
      this.fast =
          isDefault(format, BIG_NUMBER_MAX_FRACTION_DIGITS)
              ? FastValueFormatter.defaultBigNumber()
              : null;
    }

    @Override
    public void append(StringBuilder sb, Object value) throws HopValueException {
      BigDecimal number = valueMeta.getBigNumber(value);
      if (fast == null || !fast.append(sb, number)) {
        sb.append(format.format(number));
      }
    }
  }

  private static final class DateLiteral extends SqlLiteralFormatter {
    private final SimpleDateFormat format;
    private final FastValueFormatter fast;

    /**
     * @param source the pattern and time zone to use; its locale and calendar are not
     */
    DateLiteral(IValueMeta valueMeta, SimpleDateFormat source) {
      super(valueMeta);
      this.format = newDateFormat(source.toPattern());
      this.format.setTimeZone(source.getTimeZone());
      this.fast = FastValueFormatter.forDateFormat(format);
    }

    @Override
    public void append(StringBuilder sb, Object value) throws HopValueException {
      Date date = valueMeta.getDate(value);
      if (fast == null || !fast.append(sb, date)) {
        sb.append(format.format(date));
      }
    }
  }
}
