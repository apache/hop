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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Locale;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.core.row.value.ValueMetaDate;
import org.apache.hop.core.row.value.ValueMetaNumber;
import org.junit.jupiter.api.Test;

class TableViewColumnViewsTest {

  @Test
  void resolvesNamesInViewOrder() {
    String[] available = {"id", "name", "email", "amount"};
    List<Integer> indices =
        TableViewColumnViews.resolveColumnIndices(available, List.of("email", "id"));

    assertEquals(List.of(2, 0), indices);
  }

  @Test
  void skipsMissingNames() {
    String[] available = {"id", "name", "email"};
    List<Integer> indices =
        TableViewColumnViews.resolveColumnIndices(
            available, List.of("email", "missing", "id", "also_missing"));

    assertEquals(List.of(2, 0), indices);
  }

  @Test
  void usesFirstUnusedMatchForDuplicateAvailableNames() {
    String[] available = {"id", "name", "id"};
    List<Integer> indices =
        TableViewColumnViews.resolveColumnIndices(available, List.of("id", "id"));

    assertEquals(List.of(0, 2), indices);
  }

  @Test
  void emptyViewOrTableYieldsEmptyResult() {
    assertTrue(TableViewColumnViews.resolveColumnIndices(new String[0], List.of("id")).isEmpty());
    assertTrue(
        TableViewColumnViews.resolveColumnIndices(new String[] {"id"}, Collections.emptyList())
            .isEmpty());
    assertTrue(TableViewColumnViews.resolveColumnIndices(null, List.of("id")).isEmpty());
    assertTrue(TableViewColumnViews.resolveColumnIndices(new String[] {"id"}, null).isEmpty());
  }

  @Test
  void zeroMatchesIsEmpty() {
    List<Integer> indices =
        TableViewColumnViews.resolveColumnIndices(
            new String[] {"id", "name"}, Arrays.asList("other", null));

    assertTrue(indices.isEmpty());
  }

  @Test
  void columnsInViewOnOpenKeepsTheVisibleOrderAndAddsTheRest() {
    assertEquals(
        List.of("email", "id", "name"),
        TableViewColumnViews.columnsInViewOnOpen(
            List.of("email", "id", "", "email"), new String[] {"id", "name", "email"}));
  }

  @Test
  void columnsInViewOnOpenUsesEveryColumnWhenNoneAreVisible() {
    assertEquals(
        List.of("id", "name", "email"),
        TableViewColumnViews.columnsInViewOnOpen(
            List.of(), new String[] {"id", "name", "", "email"}));
    assertEquals(
        List.of("id"), TableViewColumnViews.columnsInViewOnOpen(null, new String[] {"id"}));
  }

  @Test
  void formatsFilterWithColumnAndValues() {
    TableViewColumnFilter filter =
        new TableViewColumnFilter(
            "Country", List.of("BE", "FR", TableViewColumnViews.NULL_VALUE_LABEL));

    assertEquals("Country : BE,FR,<null>", TableViewColumnViews.formatFilter(filter));
  }

  @Test
  void formatsEmptyFilterAsEmptyString() {
    assertEquals("", TableViewColumnViews.formatFilter(null));
    assertEquals("", TableViewColumnViews.formatFilter(new TableViewColumnFilter()));
    assertEquals(
        "", TableViewColumnViews.formatFilter(new TableViewColumnFilter("Country", List.of())));
  }

  @Test
  void rowPassesWithoutFilters() {
    String[] row = {"1", "Jane"};

    assertTrue(TableViewColumnViews.matchesFilters(null, new String[] {"id", "name"}, row));
    assertTrue(
        TableViewColumnViews.matchesFilters(
            Collections.emptyList(), new String[] {"id", "name"}, row));
  }

  @Test
  void rowMatchesWhenValueIsSelected() {
    TableViewColumnFilter filter = new TableViewColumnFilter("Country", List.of("BE", "FR"));
    List<TableViewColumnFilter> filters = List.of(filter);

    assertTrue(
        TableViewColumnViews.matchesFilters(
            filters, new String[] {"id", "Country"}, new String[] {"1", "BE"}));
    assertTrue(
        TableViewColumnViews.matchesFilters(
            filters, new String[] {"id", "Country"}, new String[] {"2", "FR"}));
    assertFalse(
        TableViewColumnViews.matchesFilters(
            filters, new String[] {"id", "Country"}, new String[] {"3", "NL"}));
  }

  @Test
  void emptyOrNullCellMatchesNullLabel() {
    TableViewColumnFilter filter = new TableViewColumnFilter("Country", List.of("<null>"));

    assertTrue(
        TableViewColumnViews.matchesFilters(
            List.of(filter), new String[] {"Country"}, new String[] {""}));
    assertTrue(
        TableViewColumnViews.matchesFilters(
            List.of(filter), new String[] {"Country"}, new String[] {null}));
    assertFalse(
        TableViewColumnViews.matchesFilters(
            List.of(filter), new String[] {"Country"}, new String[] {"BE"}));
  }

  @Test
  void allFiltersMustPass() {
    List<TableViewColumnFilter> filters =
        List.of(
            new TableViewColumnFilter("Country", List.of("BE")),
            new TableViewColumnFilter("Type", List.of("A", "B")));

    String[] columns = {"Country", "Type"};
    assertTrue(TableViewColumnViews.matchesFilters(filters, columns, new String[] {"BE", "A"}));
    assertFalse(TableViewColumnViews.matchesFilters(filters, columns, new String[] {"BE", "C"}));
    assertFalse(TableViewColumnViews.matchesFilters(filters, columns, new String[] {"NL", "A"}));
  }

  @Test
  void filterOnUnknownOrEmptyColumnIsIgnored() {
    List<TableViewColumnFilter> filters =
        List.of(
            new TableViewColumnFilter("Missing", List.of("X")),
            new TableViewColumnFilter("Country", List.of("BE")),
            new TableViewColumnFilter(null, List.of("X")),
            new TableViewColumnFilter("Empty", List.of()));

    assertTrue(
        TableViewColumnViews.matchesFilters(
            filters, new String[] {"Country"}, new String[] {"BE"}));
    assertFalse(
        TableViewColumnViews.matchesFilters(
            filters, new String[] {"Country"}, new String[] {"NL"}));
  }

  @Test
  void numericValuesSortNumerically() {
    Comparator<String> comparator =
        TableViewColumnViews.valueComparator(IValueMeta.TYPE_NUMBER, null);

    assertTrue(comparator.compare("9", "10") < 0);
    assertTrue(comparator.compare("10", "10.0") == 0);
    assertTrue(comparator.compare("-2", "2") < 0);
  }

  @Test
  void nullLabelSortsFirstForEveryType() {
    for (int type :
        new int[] {
          IValueMeta.TYPE_STRING,
          IValueMeta.TYPE_NUMBER,
          IValueMeta.TYPE_DATE,
          IValueMeta.TYPE_BOOLEAN
        }) {
      Comparator<String> comparator = TableViewColumnViews.valueComparator(type, null);
      assertTrue(
          comparator.compare(TableViewColumnViews.NULL_VALUE_LABEL, "anything") < 0,
          "type " + type);
      assertEquals(
          0,
          comparator.compare(
              TableViewColumnViews.NULL_VALUE_LABEL, TableViewColumnViews.NULL_VALUE_LABEL));
    }
  }

  @Test
  void datesSortByConversionMask() {
    Comparator<String> comparator =
        TableViewColumnViews.valueComparator(IValueMeta.TYPE_DATE, "dd-MM-yyyy");

    assertTrue(comparator.compare("01-03-2024", "15-02-2024") > 0);
    assertTrue(comparator.compare("15-02-2024", "16-02-2024") < 0);
  }

  @Test
  void datesSortByTheColumnMask() {
    Comparator<String> comparator =
        TableViewColumnViews.valueComparator(IValueMeta.TYPE_DATE, "yyyy-MM-dd");

    assertTrue(comparator.compare("2024-02-15", "2024-03-01") < 0);
  }

  @Test
  void datesUseTheColumnLocale() {
    ValueMetaDate column = new ValueMetaDate("when");
    column.setConversionMask("dd MMM yyyy");
    column.setDateFormatLocale(Locale.ENGLISH);
    Comparator<String> comparator = TableViewColumnViews.valueComparator(column);

    // Text order would put "01 ..." before "15 ...". The column locale parses the month names.
    assertTrue(comparator.compare("15 Feb 2024", "01 Mar 2024") < 0);
  }

  @Test
  void numbersUseTheColumnDecimalSymbolAndStayTransitive() {
    ValueMetaNumber column = new ValueMetaNumber("amount");
    column.setConversionMask("#,##0.0#");
    column.setDecimalSymbol(",");
    column.setGroupingSymbol(".");

    Comparator<String> comparator = TableViewColumnViews.valueComparator(column);
    assertTrue(comparator.compare("2,0", "10,0") < 0);
    assertTrue(comparator.compare("1.234,5", "10,0") > 0);

    List<String> values = new ArrayList<>(List.of("10", "9", "8,5", "nope", "2,0"));
    values.sort(comparator);
    assertEquals(List.of("2,0", "8,5", "9", "10", "nope"), values);
  }

  @Test
  void unparseableValuesFallBackToText() {
    Comparator<String> comparator =
        TableViewColumnViews.valueComparator(IValueMeta.TYPE_DATE, "dd-MM-yyyy");

    assertTrue(comparator.compare("abc", "abd") < 0);
  }

  @Test
  void booleansSortByTruth() {
    Comparator<String> comparator =
        TableViewColumnViews.valueComparator(IValueMeta.TYPE_BOOLEAN, null);

    assertTrue(comparator.compare("false", "true") < 0);
    assertTrue(comparator.compare("N", "Y") < 0);
    assertTrue(comparator.compare("Y", "Y") == 0);
  }

  @Test
  void textSortsNaturally() {
    Comparator<String> comparator =
        TableViewColumnViews.valueComparator(IValueMeta.TYPE_STRING, null);

    assertTrue(comparator.compare("apple", "banana") < 0);
    assertTrue(comparator.compare("b", "a") > 0);
  }
}
