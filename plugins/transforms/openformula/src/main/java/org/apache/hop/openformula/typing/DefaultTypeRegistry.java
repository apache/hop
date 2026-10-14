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

package org.apache.hop.openformula.typing;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.List;
import java.util.Locale;
import java.util.TimeZone;
import org.apache.hop.openformula.EvaluationException;
import org.apache.hop.openformula.error.FormulaErrorValue;
import org.apache.hop.openformula.util.DateUtil;
import org.apache.hop.openformula.util.NumberUtil;

/** Default implementation of TypeRegistry. */
public class DefaultTypeRegistry implements TypeRegistry {
  private final Locale locale;
  private final TimeZone timeZone;
  private final ExtendedComparator comparator;

  public DefaultTypeRegistry() {
    this(Locale.US, TimeZone.getDefault());
  }

  public DefaultTypeRegistry(Locale locale, TimeZone timeZone) {
    this.locale = locale != null ? locale : Locale.US;
    this.timeZone = timeZone != null ? timeZone : TimeZone.getDefault();
    this.comparator = new DefaultComparator(this);
  }

  @Override
  public ExtendedComparator getComparator(DataType type1, DataType type2) {
    return comparator;
  }

  @Override
  public String convertToText(DataType type, Object value) throws EvaluationException {
    if (value == null) {
      return "";
    }
    if (value instanceof FormulaErrorValue ev) {
      throw new EvaluationException(ev);
    }
    if (value instanceof ArrayCallback ac) {
      if (ac.getRowCount() == 1 && ac.getColumnCount() == 1) {
        return convertToText(ac.getType(0, 0), ac.getValue(0, 0));
      }
      StringBuilder sb = new StringBuilder();
      boolean first = true;
      for (int r = 0; r < ac.getRowCount(); r++) {
        for (int c = 0; c < ac.getColumnCount(); c++) {
          Object cell = ac.getValue(r, c);
          if (cell != null) {
            if (!first) sb.append(", ");
            sb.append(convertToText(ac.getType(r, c), cell));
            first = false;
          }
        }
      }
      return sb.toString();
    }
    if (value instanceof Collection<?> coll) {
      StringBuilder sb = new StringBuilder();
      boolean first = true;
      for (Object cell : coll) {
        if (cell != null) {
          if (!first) sb.append(", ");
          sb.append(convertToText(guessType(cell), cell));
          first = false;
        }
      }
      return sb.toString();
    }
    if (value.getClass().isArray()) {
      int len = java.lang.reflect.Array.getLength(value);
      StringBuilder sb = new StringBuilder();
      boolean first = true;
      for (int i = 0; i < len; i++) {
        Object cell = java.lang.reflect.Array.get(value, i);
        if (cell != null) {
          if (!first) sb.append(", ");
          sb.append(convertToText(guessType(cell), cell));
          first = false;
        }
      }
      return sb.toString();
    }
    if (value instanceof Boolean b) {
      return b ? "TRUE" : "FALSE";
    }
    if (value instanceof BigDecimal bd) {
      return bd.stripTrailingZeros().toPlainString();
    }
    if (value instanceof Number n) {
      return n.toString();
    }
    if (value instanceof Date d) {
      return d.toString();
    }
    return value.toString();
  }

  @Override
  public BigDecimal convertToNumber(DataType type, Object value) throws EvaluationException {
    if (value == null) {
      return BigDecimal.ZERO;
    }
    if (value instanceof FormulaErrorValue ev) {
      throw new EvaluationException(ev);
    }
    if (value instanceof ArrayCallback ac) {
      if (ac.getRowCount() == 1 && ac.getColumnCount() == 1) {
        return convertToNumber(ac.getType(0, 0), ac.getValue(0, 0));
      }
      throw new EvaluationException(FormulaErrorValue.ERROR_ILLEGAL_ARRAY_VALUE);
    }
    if (value instanceof BigDecimal bd) {
      return NumberUtil.normalize(bd);
    }
    if (value instanceof Number n) {
      return NumberUtil.getAsBigDecimal(n);
    }
    if (value instanceof Boolean b) {
      return b ? BigDecimal.ONE : BigDecimal.ZERO;
    }
    if (value instanceof Date d) {
      return DateUtil.dateToSerial(d, timeZone);
    }
    if (value instanceof String s) {
      String trimmed = s.trim();
      if (trimmed.isEmpty()) {
        return BigDecimal.ZERO;
      }
      try {
        return NumberUtil.normalize(new BigDecimal(trimmed));
      } catch (NumberFormatException e) {
        // Try parsing as date
        Date parsed = DateUtil.parseDate(trimmed, locale, timeZone);
        if (parsed != null) {
          return DateUtil.dateToSerial(parsed, timeZone);
        }
        throw new EvaluationException(FormulaErrorValue.ERROR_INVALID_ARGUMENT_VALUE, e);
      }
    }
    throw new EvaluationException(FormulaErrorValue.ERROR_INVALID_ARGUMENT_VALUE);
  }

  @Override
  public Boolean convertToLogical(DataType type, Object value) throws EvaluationException {
    if (value == null) {
      return Boolean.FALSE;
    }
    if (value instanceof FormulaErrorValue ev) {
      throw new EvaluationException(ev);
    }
    if (value instanceof ArrayCallback ac) {
      if (ac.getRowCount() == 1 && ac.getColumnCount() == 1) {
        return convertToLogical(ac.getType(0, 0), ac.getValue(0, 0));
      }
      throw new EvaluationException(FormulaErrorValue.ERROR_ILLEGAL_ARRAY_VALUE);
    }
    if (value instanceof Boolean b) {
      return b;
    }
    if (value instanceof Number n) {
      return n.doubleValue() != 0.0;
    }
    if (value instanceof String s) {
      String trimmed = s.trim();
      if ("TRUE".equalsIgnoreCase(trimmed)) {
        return Boolean.TRUE;
      }
      if ("FALSE".equalsIgnoreCase(trimmed)) {
        return Boolean.FALSE;
      }
      throw new EvaluationException(FormulaErrorValue.ERROR_INVALID_ARGUMENT_VALUE);
    }
    throw new EvaluationException(FormulaErrorValue.ERROR_INVALID_ARGUMENT_VALUE);
  }

  @Override
  public Date convertToDate(DataType type, Object value) throws EvaluationException {
    if (value == null) {
      return null;
    }
    if (value instanceof FormulaErrorValue ev) {
      throw new EvaluationException(ev);
    }
    if (value instanceof ArrayCallback ac) {
      if (ac.getRowCount() == 1 && ac.getColumnCount() == 1) {
        return convertToDate(ac.getType(0, 0), ac.getValue(0, 0));
      }
      throw new EvaluationException(FormulaErrorValue.ERROR_ILLEGAL_ARRAY_VALUE);
    }
    if (value instanceof Date d) {
      return d;
    }
    if (value instanceof Number n) {
      BigDecimal bd = NumberUtil.getAsBigDecimal(n);
      return DateUtil.serialToDate(bd, timeZone);
    }
    if (value instanceof String s) {
      Date d = DateUtil.parseDate(s, locale, timeZone);
      if (d != null) {
        return d;
      }
      // Check if numeric string
      try {
        BigDecimal bd = new BigDecimal(s.trim());
        return DateUtil.serialToDate(bd, timeZone);
      } catch (NumberFormatException e) {
        throw new EvaluationException(FormulaErrorValue.ERROR_INVALID_ARGUMENT_VALUE, e);
      }
    }
    throw new EvaluationException(FormulaErrorValue.ERROR_INVALID_ARGUMENT_VALUE);
  }

  @Override
  public List<Object> convertToSequence(DataType type, Object value) throws EvaluationException {
    List<Object> list = new ArrayList<>();
    if (value == null) {
      return list;
    }
    if (value instanceof ArrayCallback ac) {
      int rows = ac.getRowCount();
      int cols = ac.getColumnCount();
      for (int r = 0; r < rows; r++) {
        for (int c = 0; c < cols; c++) {
          list.add(ac.getValue(r, c));
        }
      }
    } else if (value instanceof Collection<?> coll) {
      list.addAll(coll);
    } else if (value.getClass().isArray()) {
      int len = java.lang.reflect.Array.getLength(value);
      for (int i = 0; i < len; i++) {
        list.add(java.lang.reflect.Array.get(value, i));
      }
    } else {
      list.add(value);
    }
    return list;
  }

  @Override
  public List<BigDecimal> convertToNumberSequence(
      DataType type, Object value, boolean includeLogicalAsNumbers) throws EvaluationException {
    List<BigDecimal> numbers = new ArrayList<>();
    if (value == null) {
      return numbers;
    }
    if (value instanceof FormulaErrorValue ev) {
      throw new EvaluationException(ev);
    }

    boolean isArray =
        (value instanceof ArrayCallback)
            || (value instanceof Collection<?>)
            || value.getClass().isArray();

    if (isArray) {
      List<Object> raw = convertToSequence(type, value);
      for (Object obj : raw) {
        if (obj == null) {
          continue;
        }
        if (obj instanceof FormulaErrorValue ev) {
          throw new EvaluationException(ev);
        }
        if (obj instanceof Number) {
          numbers.add(convertToNumber(DataType.NUMBER, obj));
        } else if (obj instanceof Date) {
          numbers.add(convertToNumber(DataType.DATETIME, obj));
        } else if (obj instanceof Boolean b && includeLogicalAsNumbers) {
          numbers.add(b ? BigDecimal.ONE : BigDecimal.ZERO);
        } else if (obj instanceof String s && includeLogicalAsNumbers) {
          String trimmed = s.trim();
          try {
            numbers.add(new BigDecimal(trimmed));
          } catch (NumberFormatException e) {
            numbers.add(BigDecimal.ZERO);
          }
        }
      }
    } else {
      // Direct scalar argument
      if (value instanceof Number) {
        numbers.add(convertToNumber(type, value));
      } else if (value instanceof Date) {
        numbers.add(convertToNumber(type, value));
      } else if (value instanceof Boolean b) {
        numbers.add(b ? BigDecimal.ONE : BigDecimal.ZERO);
      } else if (value instanceof String s) {
        if (includeLogicalAsNumbers) {
          String trimmed = s.trim();
          try {
            numbers.add(new BigDecimal(trimmed));
          } catch (NumberFormatException e) {
            numbers.add(BigDecimal.ZERO);
          }
        } else {
          numbers.add(convertToNumber(type, value));
        }
      }
    }
    return numbers;
  }

  @Override
  public DataType guessType(Object value) {
    if (value == null) return DataType.ANY;
    if (value instanceof Number) return DataType.NUMBER;
    if (value instanceof String) return DataType.TEXT;
    if (value instanceof Boolean) return DataType.LOGICAL;
    if (value instanceof Date) return DataType.DATETIME;
    if (value instanceof ArrayCallback) return DataType.ARRAY;
    if (value instanceof FormulaErrorValue) return DataType.ERROR;
    return DataType.ANY;
  }
}
