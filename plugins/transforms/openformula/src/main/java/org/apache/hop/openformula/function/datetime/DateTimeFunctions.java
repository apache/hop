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

package org.apache.hop.openformula.function.datetime;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.Calendar;
import java.util.Date;
import java.util.GregorianCalendar;
import java.util.List;
import java.util.TimeZone;
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
import org.apache.hop.openformula.util.DateUtil;
import org.apache.hop.openformula.util.NumberUtil;

public final class DateTimeFunctions {
  private DateTimeFunctions() {}

  public static void registerAll(DefaultFunctionRegistry registry) {
    registry.registerFunction(
        new DateFunction(),
        new FunctionDescription(
            "DATE",
            FunctionCategory.DATETIME,
            "Constructs a date from year, month, and day",
            DataType.DATETIME,
            3,
            false,
            List.of("year", "month", "day"),
            List.of("Year", "Month (1-12)", "Day of month (1-31)")));
    registry.registerFunction(
        new DateDifFunction(),
        new FunctionDescription(
            "DATEDIF",
            FunctionCategory.DATETIME,
            "Calculates difference between two dates",
            DataType.NUMBER,
            3,
            false,
            List.of("start_date", "end_date", "format"),
            List.of("Start date", "End date", "Format code (Y, M, D, YM, YD, MD)")));
    registry.registerFunction(
        new DateTimeValueFunction(),
        new FunctionDescription(
            "DATETIMEVALUE",
            FunctionCategory.DATETIME,
            "Converts text representation to date/time serial",
            DataType.NUMBER,
            1,
            false,
            List.of("text"),
            List.of("Text string representing date and time")));
    registry.registerFunction(
        new DateValueFunction(),
        new FunctionDescription(
            "DATEVALUE",
            FunctionCategory.DATETIME,
            "Converts text representation to date serial",
            DataType.NUMBER,
            1,
            false,
            List.of("text"),
            List.of("Text string representing date")));
    registry.registerFunction(
        new DayFunction(),
        new FunctionDescription(
            "DAY",
            FunctionCategory.DATETIME,
            "Extracts day of month",
            DataType.NUMBER,
            1,
            false,
            List.of("date"),
            List.of("Date or serial value")));
    registry.registerFunction(
        new DaysFunction(),
        new FunctionDescription(
            "DAYS",
            FunctionCategory.DATETIME,
            "Calculates days between two dates",
            DataType.NUMBER,
            2,
            false,
            List.of("end_date", "start_date"),
            List.of("End date", "Start date")));
    registry.registerFunction(
        new HourFunction(),
        new FunctionDescription(
            "HOUR",
            FunctionCategory.DATETIME,
            "Extracts hour of day (0-23)",
            DataType.NUMBER,
            1,
            false,
            List.of("time"),
            List.of("Time or date value")));
    registry.registerFunction(
        new MinuteFunction(),
        new FunctionDescription(
            "MINUTE",
            FunctionCategory.DATETIME,
            "Extracts minute of hour (0-59)",
            DataType.NUMBER,
            1,
            false,
            List.of("time"),
            List.of("Time or date value")));
    registry.registerFunction(
        new MonthFunction(),
        new FunctionDescription(
            "MONTH",
            FunctionCategory.DATETIME,
            "Extracts month of year (1-12)",
            DataType.NUMBER,
            1,
            false,
            List.of("date"),
            List.of("Date or serial value")));
    registry.registerFunction(
        new MonthEndFunction(),
        new FunctionDescription(
            "MONTHEND",
            FunctionCategory.DATETIME,
            "Returns last day of month",
            DataType.DATETIME,
            1,
            false,
            List.of("date"),
            List.of("Date or serial value")));
    registry.registerFunction(
        new NowFunction(),
        new FunctionDescription(
            "NOW",
            FunctionCategory.DATETIME,
            "Current date and time",
            DataType.DATETIME,
            0,
            false,
            List.of(),
            List.of()));
    registry.registerFunction(
        new PrevWeekdayFunction(),
        new FunctionDescription(
            "PREVWEEKDAY",
            FunctionCategory.DATETIME,
            "Returns previous weekday (Monday-Friday)",
            DataType.DATETIME,
            1,
            false,
            List.of("date"),
            List.of("Date or serial value")));
    registry.registerFunction(
        new SecondFunction(),
        new FunctionDescription(
            "SECOND",
            FunctionCategory.DATETIME,
            "Extracts second of minute (0-59)",
            DataType.NUMBER,
            1,
            false,
            List.of("time"),
            List.of("Time or date value")));
    registry.registerFunction(
        new TimeFunction(),
        new FunctionDescription(
            "TIME",
            FunctionCategory.DATETIME,
            "Constructs a time fraction from hour, minute, second",
            DataType.NUMBER,
            3,
            false,
            List.of("hour", "minute", "second"),
            List.of("Hour (0-23)", "Minute (0-59)", "Second (0-59)")));
    registry.registerFunction(
        new TimeValueFunction(),
        new FunctionDescription(
            "TIMEVALUE",
            FunctionCategory.DATETIME,
            "Converts text representation of time to fractional day",
            DataType.NUMBER,
            1,
            false,
            List.of("text"),
            List.of("Text string representing time")));
    registry.registerFunction(
        new TodayFunction(),
        new FunctionDescription(
            "TODAY",
            FunctionCategory.DATETIME,
            "Current date with time set to 00:00:00",
            DataType.DATETIME,
            0,
            false,
            List.of(),
            List.of()));
    registry.registerFunction(
        new WeekDayFunction(),
        new FunctionDescription(
            "WEEKDAY",
            FunctionCategory.DATETIME,
            "Day of week",
            DataType.NUMBER,
            1,
            false,
            List.of("date", "type"),
            List.of("Date or serial value", "Return type (1=Sun..Sat, 2=Mon..Sun, 3=0..6)")));
    registry.registerFunction(
        new YearFunction(),
        new FunctionDescription(
            "YEAR",
            FunctionCategory.DATETIME,
            "Extracts year from date",
            DataType.NUMBER,
            1,
            false,
            List.of("date"),
            List.of("Date or serial value")));
    registry.registerFunction(
        new YesterdayFunction(),
        new FunctionDescription(
            "YESTERDAY",
            FunctionCategory.DATETIME,
            "Yesterday's date",
            DataType.DATETIME,
            0,
            false,
            List.of(),
            List.of()));
  }

  private static Calendar toCalendar(FormulaContext context, ParameterCallback params, int index)
      throws EvaluationException {
    Date d = context.getTypeRegistry().convertToDate(params.getType(index), params.getValue(index));
    if (d == null) throw new EvaluationException(FormulaErrorValue.ERROR_INVALID_ARGUMENT_VALUE);
    Calendar cal = new GregorianCalendar(context.getTimeZone(), context.getLocale());
    cal.setTime(d);
    return cal;
  }

  public static class DateFunction implements Function {
    @Override
    public String getCanonicalName() {
      return "DATE";
    }

    @Override
    public DataType getReturnType() {
      return DataType.DATETIME;
    }

    @Override
    public TypedValue evaluate(FormulaContext context, ParameterCallback params)
        throws EvaluationException {
      if (params.getParameterCount() != 3)
        throw new EvaluationException(FormulaErrorValue.ERROR_ARGUMENTS_VALUE);
      BigDecimal y =
          context.getTypeRegistry().convertToNumber(params.getType(0), params.getValue(0));
      BigDecimal m =
          context.getTypeRegistry().convertToNumber(params.getType(1), params.getValue(1));
      BigDecimal d =
          context.getTypeRegistry().convertToNumber(params.getType(2), params.getValue(2));
      int year = y.intValue();
      if (year < 1900) year += 1900;
      Date date = DateUtil.createDate(year, m.intValue(), d.intValue(), context.getTimeZone());
      return TypedValue.ofDate(date);
    }
  }

  public static class DateDifFunction implements Function {
    @Override
    public String getCanonicalName() {
      return "DATEDIF";
    }

    @Override
    public DataType getReturnType() {
      return DataType.NUMBER;
    }

    @Override
    public TypedValue evaluate(FormulaContext context, ParameterCallback params)
        throws EvaluationException {
      if (params.getParameterCount() != 3)
        throw new EvaluationException(FormulaErrorValue.ERROR_ARGUMENTS_VALUE);
      Date start = context.getTypeRegistry().convertToDate(params.getType(0), params.getValue(0));
      Date end = context.getTypeRegistry().convertToDate(params.getType(1), params.getValue(1));
      String format =
          context
              .getTypeRegistry()
              .convertToText(params.getType(2), params.getValue(2))
              .trim()
              .toLowerCase();

      Calendar s = new GregorianCalendar(context.getTimeZone(), context.getLocale());
      s.setTime(start);
      Calendar e = new GregorianCalendar(context.getTimeZone(), context.getLocale());
      e.setTime(end);

      if (s.after(e)) throw new EvaluationException(FormulaErrorValue.ERROR_INVALID_ARGUMENT_VALUE);

      return switch (format) {
        case "y" -> {
          int diff = e.get(Calendar.YEAR) - s.get(Calendar.YEAR);
          if (e.get(Calendar.MONTH) < s.get(Calendar.MONTH)
              || (e.get(Calendar.MONTH) == s.get(Calendar.MONTH)
                  && e.get(Calendar.DAY_OF_MONTH) < s.get(Calendar.DAY_OF_MONTH))) {
            diff--;
          }
          yield TypedValue.ofNumber(BigDecimal.valueOf(diff));
        }
        case "m" -> {
          int diff =
              (e.get(Calendar.YEAR) - s.get(Calendar.YEAR)) * 12
                  + e.get(Calendar.MONTH)
                  - s.get(Calendar.MONTH);
          if (e.get(Calendar.DAY_OF_MONTH) < s.get(Calendar.DAY_OF_MONTH)) {
            diff--;
          }
          yield TypedValue.ofNumber(BigDecimal.valueOf(diff));
        }
        case "d" ->
            TypedValue.ofNumber(
                BigDecimal.valueOf(wholeDays(e.getTime(), s.getTime(), context.getTimeZone())));
        case "ym" -> {
          int diff = e.get(Calendar.MONTH) - s.get(Calendar.MONTH);
          if (e.get(Calendar.DAY_OF_MONTH) < s.get(Calendar.DAY_OF_MONTH)) diff--;
          if (diff < 0) diff += 12;
          yield TypedValue.ofNumber(BigDecimal.valueOf(diff));
        }
        case "yd" -> {
          Calendar eSameYear = (Calendar) e.clone();
          eSameYear.set(Calendar.YEAR, s.get(Calendar.YEAR));
          if (eSameYear.before(s)) eSameYear.add(Calendar.YEAR, 1);
          yield TypedValue.ofNumber(
              BigDecimal.valueOf(
                  wholeDays(eSameYear.getTime(), s.getTime(), context.getTimeZone())));
        }
        case "md" -> {
          int sDay = s.get(Calendar.DAY_OF_MONTH);
          int eDay = e.get(Calendar.DAY_OF_MONTH);
          int diff = eDay - sDay;
          if (diff < 0) {
            Calendar prevMonth = (Calendar) e.clone();
            prevMonth.add(Calendar.MONTH, -1);
            int maxDays = prevMonth.getActualMaximum(Calendar.DAY_OF_MONTH);
            diff = maxDays - sDay + eDay;
          }
          yield TypedValue.ofNumber(BigDecimal.valueOf(diff));
        }
        default -> throw new EvaluationException(FormulaErrorValue.ERROR_INVALID_ARGUMENT_VALUE);
      };
    }
  }

  public static class DateTimeValueFunction implements Function {
    @Override
    public String getCanonicalName() {
      return "DATETIMEVALUE";
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
      Date d = DateUtil.parseDate(text, context.getLocale(), context.getTimeZone());
      if (d == null) throw new EvaluationException(FormulaErrorValue.ERROR_INVALID_ARGUMENT_VALUE);
      return TypedValue.ofNumber(DateUtil.dateToSerial(d, context.getTimeZone()));
    }
  }

  public static class DateValueFunction implements Function {
    @Override
    public String getCanonicalName() {
      return "DATEVALUE";
    }

    @Override
    public DataType getReturnType() {
      return DataType.DATETIME;
    }

    @Override
    public TypedValue evaluate(FormulaContext context, ParameterCallback params)
        throws EvaluationException {
      if (params.getParameterCount() != 1)
        throw new EvaluationException(FormulaErrorValue.ERROR_ARGUMENTS_VALUE);
      Date d = context.getTypeRegistry().convertToDate(params.getType(0), params.getValue(0));
      if (d == null) throw new EvaluationException(FormulaErrorValue.ERROR_INVALID_ARGUMENT_VALUE);
      Calendar cal = Calendar.getInstance(context.getTimeZone());
      cal.setTime(d);
      cal.set(Calendar.HOUR_OF_DAY, 0);
      cal.set(Calendar.MINUTE, 0);
      cal.set(Calendar.SECOND, 0);
      cal.set(Calendar.MILLISECOND, 0);
      return TypedValue.ofDate(cal.getTime());
    }
  }

  public static class DayFunction implements Function {
    @Override
    public String getCanonicalName() {
      return "DAY";
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
      Calendar cal = toCalendar(context, params, 0);
      return TypedValue.ofNumber(BigDecimal.valueOf(cal.get(Calendar.DAY_OF_MONTH)));
    }
  }

  public static class DaysFunction implements Function {
    @Override
    public String getCanonicalName() {
      return "DAYS";
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
      // Kettle/PDI subtracts the first date from the second. Part 2 section 6.10.6 is the reverse.
      Date first = context.getTypeRegistry().convertToDate(params.getType(0), params.getValue(0));
      Date second = context.getTypeRegistry().convertToDate(params.getType(1), params.getValue(1));
      long diffDays = wholeDays(second, first, context.getTimeZone());
      return TypedValue.ofNumber(BigDecimal.valueOf(diffDays));
    }
  }

  private static long wholeDays(Date end, Date start, TimeZone timeZone) {
    return Math.round(DateUtil.daysBetween(end, start, timeZone).doubleValue());
  }

  public static class HourFunction implements Function {
    @Override
    public String getCanonicalName() {
      return "HOUR";
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
      Calendar cal = toCalendar(context, params, 0);
      return TypedValue.ofNumber(BigDecimal.valueOf(cal.get(Calendar.HOUR_OF_DAY)));
    }
  }

  public static class MinuteFunction implements Function {
    @Override
    public String getCanonicalName() {
      return "MINUTE";
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
      Object raw = params.getValue(0);
      if (raw instanceof Number) {
        BigDecimal bd = context.getTypeRegistry().convertToNumber(params.getType(0), raw);
        // Fractional part of day in minutes
        BigDecimal dayFraction = bd.remainder(BigDecimal.ONE).abs();
        BigDecimal minutes = dayFraction.multiply(BigDecimal.valueOf(24 * 60));
        BigDecimal rounded = NumberUtil.performMinuteRounding(minutes);
        return TypedValue.ofNumber(rounded.remainder(BigDecimal.valueOf(60)));
      }
      Calendar cal = toCalendar(context, params, 0);
      return TypedValue.ofNumber(BigDecimal.valueOf(cal.get(Calendar.MINUTE)));
    }
  }

  public static class MonthFunction implements Function {
    @Override
    public String getCanonicalName() {
      return "MONTH";
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
      Calendar cal = toCalendar(context, params, 0);
      return TypedValue.ofNumber(BigDecimal.valueOf(cal.get(Calendar.MONTH) + 1));
    }
  }

  public static class MonthEndFunction implements Function {
    @Override
    public String getCanonicalName() {
      return "MONTHEND";
    }

    @Override
    public DataType getReturnType() {
      return DataType.DATETIME;
    }

    @Override
    public TypedValue evaluate(FormulaContext context, ParameterCallback params)
        throws EvaluationException {
      if (params.getParameterCount() < 1 || params.getParameterCount() > 2)
        throw new EvaluationException(FormulaErrorValue.ERROR_ARGUMENTS_VALUE);
      Calendar cal = toCalendar(context, params, 0);
      if (params.getParameterCount() == 2) {
        BigDecimal addMonths =
            context.getTypeRegistry().convertToNumber(params.getType(1), params.getValue(1));
        cal.add(Calendar.MONTH, addMonths.intValue());
      }
      cal.set(Calendar.DAY_OF_MONTH, cal.getActualMaximum(Calendar.DAY_OF_MONTH));
      return TypedValue.ofDate(cal.getTime());
    }
  }

  public static class NowFunction implements Function {
    @Override
    public String getCanonicalName() {
      return "NOW";
    }

    @Override
    public DataType getReturnType() {
      return DataType.DATETIME;
    }

    @Override
    public TypedValue evaluate(FormulaContext context, ParameterCallback params) {
      return TypedValue.ofDate(context.getCurrentDate());
    }
  }

  public static class PrevWeekdayFunction implements Function {
    @Override
    public String getCanonicalName() {
      return "PREVWEEKDAY";
    }

    @Override
    public DataType getReturnType() {
      return DataType.DATETIME;
    }

    @Override
    public TypedValue evaluate(FormulaContext context, ParameterCallback params)
        throws EvaluationException {
      if (params.getParameterCount() != 1)
        throw new EvaluationException(FormulaErrorValue.ERROR_ARGUMENTS_VALUE);
      Calendar cal = toCalendar(context, params, 0);
      do {
        cal.add(Calendar.DAY_OF_MONTH, -1);
      } while (cal.get(Calendar.DAY_OF_WEEK) == Calendar.SATURDAY
          || cal.get(Calendar.DAY_OF_WEEK) == Calendar.SUNDAY);
      return TypedValue.ofDate(cal.getTime());
    }
  }

  public static class SecondFunction implements Function {
    @Override
    public String getCanonicalName() {
      return "SECOND";
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
      Calendar cal = toCalendar(context, params, 0);
      return TypedValue.ofNumber(BigDecimal.valueOf(cal.get(Calendar.SECOND)));
    }
  }

  public static class TimeFunction implements Function {
    @Override
    public String getCanonicalName() {
      return "TIME";
    }

    @Override
    public DataType getReturnType() {
      return DataType.NUMBER;
    }

    @Override
    public TypedValue evaluate(FormulaContext context, ParameterCallback params)
        throws EvaluationException {
      if (params.getParameterCount() != 3)
        throw new EvaluationException(FormulaErrorValue.ERROR_ARGUMENTS_VALUE);
      BigDecimal h =
          context.getTypeRegistry().convertToNumber(params.getType(0), params.getValue(0));
      BigDecimal m =
          context.getTypeRegistry().convertToNumber(params.getType(1), params.getValue(1));
      BigDecimal s =
          context.getTypeRegistry().convertToNumber(params.getType(2), params.getValue(2));
      long totalSeconds = h.longValue() * 3600 + m.longValue() * 60 + s.longValue();
      BigDecimal frac =
          BigDecimal.valueOf(totalSeconds)
              .divide(BigDecimal.valueOf(86400), NumberUtil.GLOBAL_SCALE, RoundingMode.HALF_UP);
      return TypedValue.ofNumber(NumberUtil.normalize(frac));
    }
  }

  public static class TimeValueFunction implements Function {
    @Override
    public String getCanonicalName() {
      return "TIMEVALUE";
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
      Date d = context.getTypeRegistry().convertToDate(params.getType(0), params.getValue(0));
      if (d == null) throw new EvaluationException(FormulaErrorValue.ERROR_INVALID_ARGUMENT_VALUE);
      Calendar cal = Calendar.getInstance(context.getTimeZone());
      cal.setTime(d);
      long seconds =
          cal.get(Calendar.HOUR_OF_DAY) * 3600L
              + cal.get(Calendar.MINUTE) * 60L
              + cal.get(Calendar.SECOND);
      BigDecimal frac =
          BigDecimal.valueOf(seconds)
              .divide(BigDecimal.valueOf(86400), NumberUtil.GLOBAL_SCALE, RoundingMode.HALF_UP);
      return TypedValue.ofNumber(NumberUtil.normalize(frac));
    }
  }

  public static class TodayFunction implements Function {
    @Override
    public String getCanonicalName() {
      return "TODAY";
    }

    @Override
    public DataType getReturnType() {
      return DataType.DATETIME;
    }

    @Override
    public TypedValue evaluate(FormulaContext context, ParameterCallback params) {
      Calendar cal = Calendar.getInstance(context.getTimeZone());
      cal.setTime(context.getCurrentDate());
      cal.set(Calendar.HOUR_OF_DAY, 0);
      cal.set(Calendar.MINUTE, 0);
      cal.set(Calendar.SECOND, 0);
      cal.set(Calendar.MILLISECOND, 0);
      return TypedValue.ofDate(cal.getTime());
    }
  }

  public static class WeekDayFunction implements Function {
    @Override
    public String getCanonicalName() {
      return "WEEKDAY";
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
      Calendar cal = toCalendar(context, params, 0);
      int type = 1;
      if (params.getParameterCount() == 2) {
        BigDecimal t =
            context.getTypeRegistry().convertToNumber(params.getType(1), params.getValue(1));
        type = t.intValue();
      }
      int dow = cal.get(Calendar.DAY_OF_WEEK); // 1 = Sunday .. 7 = Saturday
      int res;
      try {
        res = convertType(dow, type);
      } catch (IllegalArgumentException e) {
        throw new EvaluationException(FormulaErrorValue.ERROR_INVALID_ARGUMENT_VALUE);
      }
      return TypedValue.ofNumber(BigDecimal.valueOf(res));
    }

    public int convertType(int dow, int type) {
      return switch (type) {
        case 1 -> dow; // 1 (Sunday) to 7 (Saturday)
        case 2 -> (dow == Calendar.SUNDAY) ? 7 : (dow - 1); // 1 (Monday) to 7 (Sunday)
        case 3 -> (dow == Calendar.SUNDAY) ? 6 : (dow - 2); // 0 (Monday) to 6 (Sunday)
        default -> throw new IllegalArgumentException("Invalid type: " + type);
      };
    }
  }

  public static class YearFunction implements Function {
    @Override
    public String getCanonicalName() {
      return "YEAR";
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
      Calendar cal = toCalendar(context, params, 0);
      return TypedValue.ofNumber(BigDecimal.valueOf(cal.get(Calendar.YEAR)));
    }
  }

  public static class YesterdayFunction implements Function {
    @Override
    public String getCanonicalName() {
      return "YESTERDAY";
    }

    @Override
    public DataType getReturnType() {
      return DataType.DATETIME;
    }

    @Override
    public TypedValue evaluate(FormulaContext context, ParameterCallback params) {
      Calendar cal = Calendar.getInstance(context.getTimeZone());
      cal.setTime(context.getCurrentDate());
      cal.add(Calendar.DAY_OF_MONTH, -1);
      cal.set(Calendar.HOUR_OF_DAY, 0);
      cal.set(Calendar.MINUTE, 0);
      cal.set(Calendar.SECOND, 0);
      cal.set(Calendar.MILLISECOND, 0);
      return TypedValue.ofDate(cal.getTime());
    }
  }
}
