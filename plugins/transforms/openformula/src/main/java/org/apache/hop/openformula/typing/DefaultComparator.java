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
import java.util.Date;
import org.apache.hop.openformula.EvaluationException;
import org.apache.hop.openformula.error.FormulaErrorValue;

/**
 * Standard comparator following OpenFormula rules.
 *
 * <ul>
 *   <li>Numbers compare numerically.
 *   <li>Text compares lexicographically (case-insensitive for equals and comparison).
 *   <li>Dates compare by timestamp.
 *   <li>Booleans compare false before true.
 *   <li>Mixed types order as number, then text, then boolean.
 * </ul>
 */
public class DefaultComparator implements ExtendedComparator {
  private final TypeRegistry typeRegistry;

  public DefaultComparator(TypeRegistry typeRegistry) {
    this.typeRegistry = typeRegistry;
  }

  @Override
  public boolean isEqual(DataType type1, Object value1, DataType type2, Object value2)
      throws EvaluationException {
    if (value1 == null && value2 == null) {
      return true;
    }
    if (value1 == null || value2 == null) {
      return false;
    }

    if (value1 instanceof FormulaErrorValue || value2 instanceof FormulaErrorValue) {
      return value1.equals(value2);
    }

    // Unbox 1x1 arrays if needed
    if (value1 instanceof ArrayCallback ac && ac.getRowCount() == 1 && ac.getColumnCount() == 1) {
      value1 = ac.getValue(0, 0);
      type1 = ac.getType(0, 0);
    }
    if (value2 instanceof ArrayCallback ac && ac.getRowCount() == 1 && ac.getColumnCount() == 1) {
      value2 = ac.getValue(0, 0);
      type2 = ac.getType(0, 0);
    }

    // If both are numbers or numeric
    if (isNumeric(value1) && isNumeric(value2)) {
      BigDecimal n1 = typeRegistry.convertToNumber(type1, value1);
      BigDecimal n2 = typeRegistry.convertToNumber(type2, value2);
      return n1.compareTo(n2) == 0;
    }

    // If both are booleans
    if (value1 instanceof Boolean b1 && value2 instanceof Boolean b2) {
      return b1.equals(b2);
    }

    // If both are dates
    if (value1 instanceof Date d1 && value2 instanceof Date d2) {
      return d1.getTime() == d2.getTime();
    }

    // Try numeric/date coercion if one is string and other is number or date
    try {
      if ((isNumeric(value1) || value1 instanceof Date) && value2 instanceof String) {
        BigDecimal n1 = typeRegistry.convertToNumber(type1, value1);
        BigDecimal n2 = typeRegistry.convertToNumber(type2, value2);
        return n1.compareTo(n2) == 0;
      }
      if (value1 instanceof String && (isNumeric(value2) || value2 instanceof Date)) {
        BigDecimal n1 = typeRegistry.convertToNumber(type1, value1);
        BigDecimal n2 = typeRegistry.convertToNumber(type2, value2);
        return n1.compareTo(n2) == 0;
      }
    } catch (EvaluationException ignored) {
    }

    // Date and number comparison
    if ((value1 instanceof Date && isNumeric(value2))
        || (isNumeric(value1) && value2 instanceof Date)) {
      BigDecimal n1 = typeRegistry.convertToNumber(type1, value1);
      BigDecimal n2 = typeRegistry.convertToNumber(type2, value2);
      return n1.compareTo(n2) == 0;
    }

    // If either is string or different types, compare as text (case-insensitive in OpenFormula)
    String s1 = typeRegistry.convertToText(type1, value1);
    String s2 = typeRegistry.convertToText(type2, value2);
    return s1.equalsIgnoreCase(s2);
  }

  @Override
  public int compare(DataType type1, Object value1, DataType type2, Object value2)
      throws EvaluationException {
    if (value1 == null && value2 == null) return 0;
    if (value1 == null) return -1;
    if (value2 == null) return 1;

    // Both numeric
    if (isNumeric(value1) && isNumeric(value2)) {
      BigDecimal n1 = typeRegistry.convertToNumber(type1, value1);
      BigDecimal n2 = typeRegistry.convertToNumber(type2, value2);
      return n1.compareTo(n2);
    }

    // Date comparisons
    if (value1 instanceof Date d1 && value2 instanceof Date d2) {
      return d1.compareTo(d2);
    }
    if ((value1 instanceof Date && isNumeric(value2))
        || (isNumeric(value1) && value2 instanceof Date)) {
      BigDecimal n1 = typeRegistry.convertToNumber(type1, value1);
      BigDecimal n2 = typeRegistry.convertToNumber(type2, value2);
      return n1.compareTo(n2);
    }

    // Both booleans
    if (value1 instanceof Boolean b1 && value2 instanceof Boolean b2) {
      return b1.compareTo(b2);
    }

    // Both strings
    if (value1 instanceof String s1 && value2 instanceof String s2) {
      return s1.compareToIgnoreCase(s2);
    }

    // Mixed types: OpenFormula ordering Number < Text < Logical
    int rank1 = typeRank(value1);
    int rank2 = typeRank(value2);
    if (rank1 != rank2) {
      return Integer.compare(rank1, rank2);
    }

    String s1 = typeRegistry.convertToText(type1, value1);
    String s2 = typeRegistry.convertToText(type2, value2);
    return s1.compareToIgnoreCase(s2);
  }

  private static boolean isNumeric(Object o) {
    return o instanceof Number;
  }

  private static int typeRank(Object o) {
    if (o instanceof Number || o instanceof Date) return 1;
    if (o instanceof String) return 2;
    if (o instanceof Boolean) return 3;
    return 4;
  }
}
