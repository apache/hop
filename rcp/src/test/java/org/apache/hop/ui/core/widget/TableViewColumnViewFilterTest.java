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

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.hop.core.row.value.ValueMetaNumber;
import org.apache.hop.core.row.value.ValueMetaString;
import org.apache.hop.core.variables.Variables;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.ui.core.PropsUi;
import org.apache.hop.ui.core.dialog.TableViewColumnViewDialog;
import org.apache.hop.ui.testing.SwtBotTestBase;
import org.eclipse.swt.SWT;
import org.eclipse.swt.graphics.Point;
import org.eclipse.swt.graphics.Rectangle;
import org.eclipse.swt.layout.FillLayout;
import org.eclipse.swt.widgets.Control;
import org.eclipse.swt.widgets.Event;
import org.eclipse.swt.widgets.Label;
import org.eclipse.swt.widgets.Menu;
import org.eclipse.swt.widgets.MenuItem;
import org.eclipse.swt.widgets.ScrollBar;
import org.eclipse.swt.widgets.Shell;
import org.eclipse.swt.widgets.Table;
import org.eclipse.swt.widgets.TableColumn;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

/**
 * Drives the column-view filters on a real {@link TableView}: the rows that do not pass the filters
 * of a table view leave the table, "Show all columns" puts them back exactly as they were, and the
 * filtered column headers carry the filter icon while the view is applied.
 *
 * <p>The matching and sorting logic itself is covered SWT-free by {@link TableViewColumnViewsTest};
 * this test is about what only works against a live table: taking rows out and putting them back,
 * with their values, row numbers and header images intact.
 */
@Tag("uitest")
class TableViewColumnViewFilterTest extends SwtBotTestBase {

  /** name, country, amount - the country of row 2 is empty, which is the null value. */
  private static final String[][] ROWS = {
    {"aaa", "BE", "1.5"},
    {"bbb", "NL", "12"},
    {"ccc", "", "2.5"},
    {"ddd", "FR", "2"},
    {"eee", "NL", "10"},
  };

  @Test
  void filterHidesNonMatchingRowsAndResetBringsThemBack() {
    withScene(
        shell -> {
          TableView tableView = newTableView(shell);

          TableViewColumnFilter filter =
              new TableViewColumnFilter("Country", List.of("BE", "FR", "<null>"));
          assertTrue(
              tableView.applyColumnView(List.of("Name", "Country", "Amount"), List.of(filter)),
              "the view applies to the table");

          // aaa (BE), ccc (empty), ddd (FR) stay; bbb and eee (NL) are taken out.
          assertEquals(3, tableView.table.getItemCount());
          assertEquals("aaa", tableView.table.getItem(0).getText(1));
          assertEquals("ccc", tableView.table.getItem(1).getText(1));
          assertEquals("ddd", tableView.table.getItem(2).getText(1));

          tableView.resetColumnView();

          assertEquals(5, tableView.table.getItemCount());
          for (int i = 0; i < ROWS.length; i++) {
            assertArrayEquals(ROWS[i], rowValues(tableView.table.getItem(i)));
          }
          assertTrue(tableView.getAppliedColumnFilters().isEmpty());
        },
        bot -> {
          // The whole scenario is synchronous: nothing waits for the event loop.
        });
  }

  @Test
  void applyingAnotherViewWithoutFiltersRestoresTheRows() {
    withScene(
        shell -> {
          TableView tableView = newTableView(shell);

          tableView.applyColumnView(
              List.of("Name", "Country"),
              List.of(new TableViewColumnFilter("Country", List.of("NL"))));
          assertEquals(2, tableView.table.getItemCount());

          tableView.applyColumnView(List.of("Name"));
          assertEquals(5, tableView.table.getItemCount());
          assertEquals(List.of("Name"), tableView.getVisibleColumnNamesInOrder());
          assertTrue(tableView.getAppliedColumnFilters().isEmpty());
        },
        bot -> {
          // Nothing asynchronous here either.
        });
  }

  @Test
  void uniqueValuesStayCompleteWhileAFilterIsApplied() {
    withScene(
        shell -> {
          TableView tableView = newTableView(shell);

          tableView.applyColumnView(
              List.of("Name", "Country", "Amount"),
              List.of(new TableViewColumnFilter("Country", List.of("BE"))));
          assertEquals(1, tableView.table.getItemCount());

          // The hidden rows are still part of the original distinct list.
          assertEquals(List.of("<null>", "BE", "FR", "NL"), tableView.getUniqueColumnValues(1));
          assertEquals(List.of("1.5", "2", "2.5", "10", "12"), tableView.getUniqueColumnValues(2));
        },
        bot -> {
          // No event-loop interaction.
        });
  }

  @Test
  void uniqueValuesSortByColumnType() {
    withScene(
        shell -> {
          TableView tableView = newTableView(shell);

          // Numeric column: 10 and 12 sort after 2.5, not before it.
          assertEquals(List.of("1.5", "2", "2.5", "10", "12"), tableView.getUniqueColumnValues(2));

          // Text column: natural order, the empty country first as the null value.
          assertEquals(List.of("<null>", "BE", "FR", "NL"), tableView.getUniqueColumnValues(1));
        },
        bot -> {
          // No event-loop interaction.
        });
  }

  @Test
  void filteredColumnsGetAFilterIcon() {
    withScene(
        shell -> {
          TableView tableView = newTableView(shell);

          tableView.applyColumnView(
              List.of("Name", "Country", "Amount"),
              List.of(new TableViewColumnFilter("Country", List.of("NL"))));

          TableColumn countryColumn = tableView.table.getColumn(2);
          assertNotNull(countryColumn.getImage(), "the filtered column carries the filter icon");
          assertNull(tableView.table.getColumn(1).getImage());
          assertFalse(countryColumn.getToolTipText().isEmpty());

          tableView.resetColumnView();

          assertNull(tableView.table.getColumn(2).getImage(), "the icon goes away again");
        },
        bot -> {
          // No event-loop interaction.
        });
  }

  @Test
  void filtersStayOffUnlessTheGridOptsIn() {
    AtomicReference<TableView> ref = new AtomicReference<>();
    withScene(
        shell -> ref.set(newTableView(shell, false)),
        bot ->
            display.syncExec(
                () -> {
                  TableView tableView = ref.get();
                  tableView.getShell().layout(true, true);

                  Menu headerMenu = openHeaderMenu(tableView, 2);
                  assertFalse(menuHasItem(headerMenu, "TableView.menu.AddFilter"));
                  assertFalse(menuHasItem(headerMenu, "TableView.menu.RemoveFilter"));

                  tableView.applyColumnView(
                      List.of("Name", "Country"),
                      List.of(new TableViewColumnFilter("Country", List.of("NL"))));
                  assertEquals(ROWS.length, tableView.table.getItemCount());
                  assertTrue(tableView.getAppliedColumnFilters().isEmpty());
                }));
  }

  @Test
  void headerMenuAddsAndRemovesAFilterForThatColumn() {
    AtomicReference<TableView> ref = new AtomicReference<>();
    withScene(
        shell -> ref.set(newTableView(shell)),
        bot ->
            display.syncExec(
                () -> {
                  TableView tableView = ref.get();
                  tableView.getShell().layout(true, true);

                  Menu countryMenu = openHeaderMenu(tableView, 2);
                  assertTrue(menuItem(countryMenu, "TableView.menu.AddFilter").isEnabled());
                  assertFalse(menuItem(countryMenu, "TableView.menu.RemoveFilter").isEnabled());

                  Menu numberMenu = openHeaderMenu(tableView, 0);
                  assertFalse(menuItem(numberMenu, "TableView.menu.AddFilter").isEnabled());
                  assertFalse(menuItem(numberMenu, "TableView.menu.RemoveFilter").isEnabled());
                  assertFalse(rowMenuHasAddFilter(tableView));

                  // Data columns are 0 Name, 1 Country, 2 Amount. The menu column index counts the
                  // # column.
                  tableView.replaceColumnFilter(1, List.of("NL"));
                  assertEquals(2, tableView.table.getItemCount());
                  assertEquals(
                      List.of("Name", "Country", "Amount"),
                      tableView.getVisibleColumnNamesInOrder());

                  tableView.replaceColumnFilter(2, List.of("12"));
                  assertEquals(1, tableView.table.getItemCount());
                  assertEquals("bbb", tableView.table.getItem(0).getText(1));

                  Menu amountMenu = openHeaderMenu(tableView, 3);
                  assertTrue(menuItem(amountMenu, "TableView.menu.RemoveFilter").isEnabled());
                  Menu nameMenu = openHeaderMenu(tableView, 1);
                  assertTrue(menuItem(nameMenu, "TableView.menu.AddFilter").isEnabled());
                  assertFalse(menuItem(nameMenu, "TableView.menu.RemoveFilter").isEnabled());

                  countryMenu = openHeaderMenu(tableView, 2);
                  assertTrue(menuItem(countryMenu, "TableView.menu.RemoveFilter").isEnabled());
                  menuItem(countryMenu, "TableView.menu.RemoveFilter")
                      .notifyListeners(SWT.Selection, new Event());

                  // Amount 12 stays filtered; the country filter and its header icon are gone.
                  assertEquals(1, tableView.table.getItemCount());
                  assertEquals("bbb", tableView.table.getItem(0).getText(1));
                  assertEquals(
                      "Amount", tableView.getAppliedColumnFilters().get(0).getColumnName());
                  assertNull(tableView.table.getColumn(2).getImage());
                  assertNotNull(tableView.table.getColumn(3).getImage());
                }));
  }

  @Test
  void matchingRowCountCountsTheVisibleRowsBeforeApplying() {
    withScene(
        shell -> {
          TableView tableView = newTableView(shell);

          List<TableViewColumnFilter> filters =
              List.of(new TableViewColumnFilter("Country", List.of("NL")));
          assertEquals(2, tableView.getMatchingRowCount(filters));
          assertEquals(5, tableView.getMatchingRowCount(null));

          tableView.applyColumnView(List.of("Name", "Country", "Amount"), filters);
          assertEquals(2, tableView.table.getItemCount());
          // Hidden rows still count: BE matches one hidden row, and the total is all five.
          assertEquals(2, tableView.getMatchingRowCount(filters));
          assertEquals(
              1,
              tableView.getMatchingRowCount(
                  List.of(new TableViewColumnFilter("Country", List.of("BE")))));
          assertEquals(5, tableView.getMatchingRowCount(null));
          assertEquals(5, tableView.getUnfilteredRowCount());
        },
        bot -> {
          // No event-loop interaction.
        });
  }

  @Test
  void sortThenShowAllKeepsEveryRowInSortOrder() {
    withScene(
        shell -> {
          TableView tableView = newTableView(shell);
          tableView.applyColumnView(
              List.of("Name", "Country", "Amount"),
              List.of(new TableViewColumnFilter("Country", List.of("NL"))));

          // Column 3 is Amount. The sort covers the hidden rows, then the filter goes back on.
          tableView.sortTable(3);
          assertEquals(2, tableView.table.getItemCount());
          assertEquals("eee", tableView.table.getItem(0).getText(1));
          assertEquals("bbb", tableView.table.getItem(1).getText(1));

          tableView.resetColumnView();

          assertEquals(
              List.of("aaa", "ddd", "ccc", "eee", "bbb"),
              List.of(
                  tableView.table.getItem(0).getText(1),
                  tableView.table.getItem(1).getText(1),
                  tableView.table.getItem(2).getText(1),
                  tableView.table.getItem(3).getText(1),
                  tableView.table.getItem(4).getText(1)));
          // The preview line number stays with its row.
          assertEquals("1", tableView.table.getItem(0).getText(0));
          assertEquals("4", tableView.table.getItem(1).getText(0));
          assertEquals("2", tableView.table.getItem(4).getText(0));
        },
        bot -> {
          // No event-loop interaction.
        });
  }

  @Test
  void deletingAVisibleRowThenShowAllDoesNotUseAStaleIndex() {
    withScene(
        shell -> {
          TableView tableView = newTableView(shell);
          tableView.applyColumnView(
              List.of("Name", "Country", "Amount"),
              List.of(new TableViewColumnFilter("Country", List.of("NL"))));

          // Visible row 0 is bbb, the first NL row. The hidden rows are aaa, ccc and ddd.
          tableView.table.remove(0);
          assertEquals(1, tableView.table.getItemCount());
          assertEquals("eee", tableView.table.getItem(0).getText(1));

          tableView.resetColumnView();

          assertEquals(4, tableView.table.getItemCount());
          assertEquals("aaa", tableView.table.getItem(0).getText(1));
          assertEquals("ccc", tableView.table.getItem(1).getText(1));
          assertEquals("ddd", tableView.table.getItem(2).getText(1));
          assertEquals("eee", tableView.table.getItem(3).getText(1));
        },
        bot -> {
          // No event-loop interaction.
        });
  }

  @Test
  void replacingTheTableDropsHiddenRows() {
    withScene(
        shell -> {
          TableView tableView = newTableView(shell);
          tableView.applyColumnView(
              List.of("Name", "Country", "Amount"),
              List.of(new TableViewColumnFilter("Country", List.of("NL"))));

          tableView.table.removeAll();
          org.eclipse.swt.widgets.TableItem item =
              new org.eclipse.swt.widgets.TableItem(tableView.table, SWT.NONE);
          item.setText(1, "new");
          tableView.resetColumnView();

          assertEquals(1, tableView.table.getItemCount());
          assertEquals("new", tableView.table.getItem(0).getText(1));
          assertTrue(tableView.getAppliedColumnFilters().isEmpty());
        },
        bot -> {
          // No event-loop interaction.
        });
  }

  @Test
  void aViewThatMatchesNoColumnLeavesTheFilterInPlace() {
    withScene(
        shell -> {
          TableView tableView = newTableView(shell);
          tableView.applyColumnView(
              List.of("Name", "Country", "Amount"),
              List.of(new TableViewColumnFilter("Country", List.of("NL"))));

          assertFalse(
              tableView.applyColumnView(
                  List.of("DoesNotExist"),
                  List.of(new TableViewColumnFilter("Name", List.of("aaa")))));

          assertEquals(2, tableView.table.getItemCount());
          assertEquals("Country", tableView.getAppliedColumnFilters().get(0).getColumnName());
          assertNotNull(tableView.table.getColumn(2).getImage());
        },
        bot -> {
          // No event-loop interaction.
        });
  }

  @Test
  void dialogStartsFromTheHeaderFilterAndCountsHiddenRows() {
    withDialog(
        parent -> {
          TableView tableView = newTableView(parent);
          tableView.applyColumnView(
              List.of("Name", "Country", "Amount"),
              List.of(new TableViewColumnFilter("Country", List.of("NL"))));
          new TableViewColumnViewDialog(parent, tableView).open();
        },
        bot -> {
          bot.shell("Table views").activate();
          display.syncExec(
              () -> {
                Shell dialog = findShell("Table views");
                assertTrue(widgetListContains(dialog, "Country : NL"));
                String status = statusText(dialog);
                assertTrue(status.contains("2 of 5"), status);
              });
          bot.button("Use current layout").click();
          display.syncExec(
              () -> {
                Shell dialog = findShell("Table views");
                assertTrue(widgetListContains(dialog, "Country : NL"));
                String status = statusText(dialog);
                assertTrue(status.contains("3 of 3"), status);
                assertTrue(status.contains("2 of 5"), status);
              });
          bot.button(buttonLabel("System.Button.Close")).click();
        });
  }

  // --- scene -----------------------------------------------------------------------------------

  private TableView newTableView(Shell shell) {
    return newTableView(shell, true);
  }

  private TableView newTableView(Shell shell, boolean filtersSupported) {
    shell.setLayout(new FillLayout());
    shell.setSize(760, 320);
    ColumnInfo name = new ColumnInfo("Name", ColumnInfo.COLUMN_TYPE_TEXT, false, false);
    name.setValueMeta(new ValueMetaString("Name"));
    ColumnInfo country = new ColumnInfo("Country", ColumnInfo.COLUMN_TYPE_TEXT, false, false);
    country.setValueMeta(new ValueMetaString("Country"));
    ColumnInfo amount = new ColumnInfo("Amount", ColumnInfo.COLUMN_TYPE_TEXT, true, false);
    ValueMetaNumber amountMeta = new ValueMetaNumber("Amount");
    amountMeta.setDecimalSymbol(".");
    amountMeta.setGroupingSymbol(",");
    amount.setValueMeta(amountMeta);

    TableView tableView =
        new TableView(
            new Variables(),
            shell,
            SWT.BORDER | SWT.FULL_SELECTION,
            new ColumnInfo[] {name, country, amount},
            ROWS.length,
            null,
            PropsUi.getInstance());
    if (filtersSupported) {
      tableView.setFiltersSupported(true);
    }
    for (int i = 0; i < ROWS.length; i++) {
      tableView.table.getItem(i).setText(1, ROWS[i][0]);
      tableView.table.getItem(i).setText(2, ROWS[i][1]);
      tableView.table.getItem(i).setText(3, ROWS[i][2]);
    }
    return tableView;
  }

  private static String[] rowValues(org.eclipse.swt.widgets.TableItem item) {
    return new String[] {item.getText(1), item.getText(2), item.getText(3)};
  }

  /** Open the column-header menu on one table column (0 is the row-number column). */
  private static Menu openHeaderMenu(TableView view, int tableColumnIndex) {
    Table table = view.table;
    int headerHeight = table.getHeaderHeight();
    if (headerHeight <= 0) {
      throw new AssertionError("header has no height");
    }
    Rectangle client = table.getClientArea();
    int y = client.y + headerHeight / 2;
    table.notifyListeners(
        SWT.MenuDetect, menuDetect(table, columnCenterX(table, tableColumnIndex), y));
    return table.getMenu();
  }

  /** The menu opened below the header is the row menu, which does not filter a column. */
  private static boolean rowMenuHasAddFilter(TableView view) {
    Table table = view.table;
    Rectangle client = table.getClientArea();
    int y = client.y + table.getHeaderHeight() + 4;
    table.notifyListeners(SWT.MenuDetect, menuDetect(table, 10, y));
    String label = BaseMessages.getString(TableView.class, "TableView.menu.AddFilter");
    for (MenuItem item : table.getMenu().getItems()) {
      if (label.equals(item.getText())) {
        return true;
      }
    }
    return false;
  }

  private static Event menuDetect(Table table, int x, int y) {
    Point displayPoint = table.toDisplay(x, y);
    Event detect = new Event();
    detect.x = displayPoint.x;
    detect.y = displayPoint.y;
    detect.detail = SWT.MENU_MOUSE;
    return detect;
  }

  private static int columnCenterX(Table table, int tableColumnIndex) {
    int x = 0;
    ScrollBar bar = table.getHorizontalBar();
    if (bar != null && !bar.isDisposed()) {
      x -= bar.getSelection();
    }
    for (int index : table.getColumnOrder()) {
      int width = table.getColumn(index).getWidth();
      if (index == tableColumnIndex) {
        if (width <= 0) {
          throw new AssertionError("column " + tableColumnIndex + " has no width");
        }
        return x + width / 2;
      }
      x += width;
    }
    throw new AssertionError("missing column " + tableColumnIndex);
  }

  private Shell findShell(String title) {
    for (Shell open : display.getShells()) {
      if (!open.isDisposed() && title.equals(open.getText())) {
        return open;
      }
    }
    throw new AssertionError("no shell named " + title);
  }

  private static boolean widgetListContains(Control control, String item) {
    if (control instanceof org.eclipse.swt.widgets.List list) {
      for (String text : list.getItems()) {
        if (item.equals(text)) {
          return true;
        }
      }
    }
    if (control instanceof org.eclipse.swt.widgets.Composite composite) {
      for (Control child : composite.getChildren()) {
        if (widgetListContains(child, item)) {
          return true;
        }
      }
    }
    return false;
  }

  private static String statusText(Control control) {
    if (control instanceof Label label && label.getText().contains("rows match")) {
      return label.getText();
    }
    if (control instanceof org.eclipse.swt.widgets.Composite composite) {
      for (Control child : composite.getChildren()) {
        String text = statusText(child);
        if (text != null) {
          return text;
        }
      }
    }
    return null;
  }

  private static boolean menuHasItem(Menu menu, String key) {
    String label = BaseMessages.getString(TableView.class, key);
    for (MenuItem item : menu.getItems()) {
      if (label.equals(item.getText())) {
        return true;
      }
    }
    return false;
  }

  private static MenuItem menuItem(Menu menu, String key) {
    String label = BaseMessages.getString(TableView.class, key);
    for (MenuItem item : menu.getItems()) {
      if (label.equals(item.getText())) {
        return item;
      }
    }
    throw new AssertionError("no '" + label + "' item");
  }
}
