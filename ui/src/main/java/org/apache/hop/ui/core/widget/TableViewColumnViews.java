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
package org.apache.hop.ui.core.widget;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.Date;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import org.apache.commons.lang3.StringUtils;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.core.row.value.ValueMetaBigNumber;
import org.apache.hop.core.row.value.ValueMetaBoolean;
import org.apache.hop.core.row.value.ValueMetaDate;
import org.apache.hop.core.row.value.ValueMetaInteger;
import org.apache.hop.core.row.value.ValueMetaNumber;
import org.apache.hop.core.row.value.ValueMetaString;
import org.apache.hop.core.row.value.ValueMetaTimestamp;

/**
 * Matching helpers for named {@link TableView} column views and their column filters. Kept free of
 * SWT so the logic can be unit tested.
 */
public final class TableViewColumnViews {
  /** Label shown in the UI for a null or empty value in a column filter. */
  public static final String NULL_VALUE_LABEL = "<null>";

  private TableViewColumnViews() {}

  /**
   * Resolve view column names to indices into {@code availableNames}.
   *
   * <p>Matching is case-sensitive (Hop field names). Duplicate available names take the first
   * unused match. Names that are not present are skipped.
   *
   * @param availableNames column names of the current table, in creation order
   * @param viewNames column names in the desired display order
   * @return matching indices into {@code availableNames}, in view order
   */
  public static List<Integer> resolveColumnIndices(
      String[] availableNames, List<String> viewNames) {
    List<Integer> indices = new ArrayList<>();
    if (availableNames == null || viewNames == null) {
      return indices;
    }
    boolean[] used = new boolean[availableNames.length];
    for (String viewName : viewNames) {
      if (viewName == null) {
        continue;
      }
      for (int i = 0; i < availableNames.length; i++) {
        if (used[i]) {
          continue;
        }
        if (viewName.equals(availableNames[i])) {
          used[i] = true;
          indices.add(i);
          break;
        }
      }
    }
    return indices;
  }

  /**
   * Columns the table views dialog lists under "Columns in view" when it opens.
   *
   * <p>Every column is included. Columns that are on screen come first, in that order, and the
   * others follow in creation order. A filter from the column header does not change this, so the
   * dialog does not open with an empty view.
   *
   * @param visibleInOrder column names currently shown, in display order
   * @param allColumns every column of the table, in creation order
   * @return the names to put in "Columns in view"
   */
  public static List<String> columnsInViewOnOpen(List<String> visibleInOrder, String[] allColumns) {
    List<String> shown = distinctNames(visibleInOrder);
    List<String> all = distinctNames(allColumns == null ? List.of() : Arrays.asList(allColumns));
    if (shown.isEmpty()) {
      return all;
    }
    Set<String> seen = new LinkedHashSet<>(shown);
    for (String name : all) {
      if (seen.add(name)) {
        shown.add(name);
      }
    }
    return shown;
  }

  private static List<String> distinctNames(List<String> names) {
    if (names == null || names.isEmpty()) {
      return new ArrayList<>();
    }
    Set<String> seen = new LinkedHashSet<>();
    for (String name : names) {
      if (StringUtils.isNotEmpty(name)) {
        seen.add(name);
      }
    }
    return new ArrayList<>(seen);
  }

  /**
   * Render a filter the way it is shown in the applied-filters list, for example {@code Country :
   * BE,FR,<null>}.
   *
   * @param filter the filter to render
   * @return {@code "column : value1,value2"} or an empty string when the filter is empty
   */
  public static String formatFilter(TableViewColumnFilter filter) {
    if (filter == null || StringUtils.isEmpty(filter.getColumnName())) {
      return "";
    }
    List<String> values = filter.getValues();
    if (values == null || values.isEmpty()) {
      return "";
    }
    return filter.getColumnName() + " : " + String.join(",", values);
  }

  /**
   * True when a row passes every filter. A filter on a column that is not in {@code columnNames} is
   * ignored, as is a filter without values.
   *
   * <p>Row values are matched exactly on the full cell value; an empty cell counts as {@link
   * #NULL_VALUE_LABEL}.
   *
   * @param filters the filters to apply, in order
   * @param columnNames the column names of {@code rowValues}, in the same order
   * @param rowValues the values of one row
   * @return true when the row matches all applicable filters
   */
  public static boolean matchesFilters(
      List<TableViewColumnFilter> filters, String[] columnNames, String[] rowValues) {
    if (filters == null || filters.isEmpty()) {
      return true;
    }
    if (columnNames == null || rowValues == null) {
      return true;
    }
    for (TableViewColumnFilter filter : filters) {
      if (filter == null
          || StringUtils.isEmpty(filter.getColumnName())
          || filter.getValues() == null
          || filter.getValues().isEmpty()) {
        continue;
      }
      int index = -1;
      for (int i = 0; i < columnNames.length; i++) {
        if (filter.getColumnName().equals(columnNames[i])) {
          index = i;
          break;
        }
      }
      if (index < 0 || index >= rowValues.length) {
        continue;
      }
      if (!filter.getValues().contains(normalizeValue(rowValues[index]))) {
        return false;
      }
    }
    return true;
  }

  /**
   * Map a raw cell value to its filter representation: empty or null becomes {@link
   * #NULL_VALUE_LABEL}, anything else is kept as-is.
   *
   * @param value the raw cell value
   * @return the value to compare against filter values
   */
  public static String normalizeValue(String value) {
    if (value == null || value.isEmpty()) {
      return NULL_VALUE_LABEL;
    }
    return value;
  }

  /**
   * Comparator for the unique values of a column, sorted the way that column's value meta sorts.
   * Numbers, dates and booleans are parsed with the meta's mask, decimal and grouping symbols, date
   * locale and time zone — the same conversion a column sort uses. A value that does not parse
   * sorts after the ones that do, and two unparsed values sort as text, so the order stays
   * transitive. The null label always sorts first.
   *
   * @param columnMeta the column's value meta, or null to sort as text
   * @return a comparator over the displayed values
   */
  public static Comparator<String> valueComparator(IValueMeta columnMeta) {
    IValueMeta parser = null;
    if (columnMeta != null && columnMeta.getType() != IValueMeta.TYPE_STRING) {
      parser = new ValueMetaString("filter-value");
      parser.setConversionMetadata(columnMeta);
    }
    IValueMeta source = parser;
    return (left, right) -> compareValues(source, left, right);
  }

  /**
   * Same ordering as {@link #valueComparator(IValueMeta)} for a type and mask. Numeric symbols are
   * fixed to {@code '.'} and {@code ','} so the order does not follow the JVM locale.
   *
   * @param valueType one of the {@link IValueMeta} TYPE_* constants
   * @param conversionMask the conversion mask of the column, or null when it has none
   * @return a comparator over the displayed values
   */
  public static Comparator<String> valueComparator(int valueType, String conversionMask) {
    IValueMeta columnMeta =
        switch (valueType) {
          case IValueMeta.TYPE_NUMBER -> new ValueMetaNumber("value");
          case IValueMeta.TYPE_INTEGER -> new ValueMetaInteger("value");
          case IValueMeta.TYPE_BIGNUMBER -> new ValueMetaBigNumber("value");
          case IValueMeta.TYPE_DATE -> new ValueMetaDate("value");
          case IValueMeta.TYPE_TIMESTAMP -> new ValueMetaTimestamp("value");
          case IValueMeta.TYPE_BOOLEAN -> new ValueMetaBoolean("value");
          default -> new ValueMetaString("value");
        };
    if (columnMeta.isNumeric()) {
      columnMeta.setDecimalSymbol(".");
      columnMeta.setGroupingSymbol(",");
    }
    columnMeta.setConversionMask(conversionMask);
    return valueComparator(columnMeta);
  }

  private static int compareValues(IValueMeta parser, String left, String right) {
    if (left == null || right == null) {
      return left == right ? 0 : (left == null ? -1 : 1);
    }
    boolean leftNull = NULL_VALUE_LABEL.equals(left);
    boolean rightNull = NULL_VALUE_LABEL.equals(right);
    if (leftNull || rightNull) {
      return leftNull == rightNull ? 0 : (leftNull ? -1 : 1);
    }
    Object leftParsed = tryParse(parser, left);
    Object rightParsed = tryParse(parser, right);
    if (leftParsed != null && rightParsed != null) {
      Integer compared = compareParsed(leftParsed, rightParsed);
      if (compared != null) {
        return compared;
      }
    }
    // One parsed value always sorts before one that did not. Two failures sort as text.
    if (leftParsed != null && rightParsed == null) {
      return -1;
    }
    if (leftParsed == null && rightParsed != null) {
      return 1;
    }
    return left.compareTo(right);
  }

  /** Parse with the column meta. Null when the meta does not apply or the text does not parse. */
  private static Object tryParse(IValueMeta parser, String text) {
    if (parser == null) {
      return null;
    }
    try {
      return parser.convertDataUsingConversionMetaData(text);
    } catch (Exception e) {
      return null;
    }
  }

  private static Integer compareParsed(Object left, Object right) {
    if (left instanceof BigDecimal leftNumber && right instanceof BigDecimal rightNumber) {
      return leftNumber.compareTo(rightNumber);
    }
    if (left instanceof Number leftNumber && right instanceof Number rightNumber) {
      return Double.compare(leftNumber.doubleValue(), rightNumber.doubleValue());
    }
    if (left instanceof Date leftDate && right instanceof Date rightDate) {
      return leftDate.compareTo(rightDate);
    }
    if (left instanceof Boolean leftFlag && right instanceof Boolean rightFlag) {
      return Boolean.compare(leftFlag, rightFlag);
    }
    return null;
  }
}
