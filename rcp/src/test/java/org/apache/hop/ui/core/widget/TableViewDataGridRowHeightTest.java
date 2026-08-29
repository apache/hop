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

package org.apache.hop.ui.core.widget;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;
import org.apache.hop.core.variables.Variables;
import org.apache.hop.ui.core.PropsUi;
import org.apache.hop.ui.testing.SwtBotTestBase;
import org.eclipse.swt.SWT;
import org.eclipse.swt.layout.FillLayout;
import org.eclipse.swt.widgets.TableItem;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

/**
 * Guards the row height of an <em>editable</em> data grid — the Data tab of the Data grid
 * transform, and the edit-rows dialogs.
 *
 * <p>Row geometry is not uniform on every platform: GTK measures each row from the text its cells
 * hold, so a line break that reaches the cell grows that row to as many lines as the value has
 * (issue #8155). macOS gives every row the same height, so these can only fail on GTK.
 */
@Tag("uitest")
class TableViewDataGridRowHeightTest extends SwtBotTestBase {

  private static final String MULTI_LINE = "first line\nsecond line\nthird line\nfourth line";
  private static final int VALUE_COLUMN = 1;

  @Test
  void aMultiLineValueDoesNotGrowItsRow() {
    AtomicReference<TableView> gridRef = new AtomicReference<>();
    withScene(
        shell -> gridRef.set(buildGrid(shell)),
        bot -> {
          TableView grid = gridRef.get();
          int singleLine = onUi(() -> grid.table.getItem(0).getBounds(VALUE_COLUMN).height);
          int multiLine = onUi(() -> grid.table.getItem(1).getBounds(VALUE_COLUMN).height);

          assertEquals(
              singleLine,
              multiLine,
              "a multi-line value must not make its row taller than a single-line one");
        });
  }

  @Test
  void theGridHandsBackTheCompleteValue() {
    AtomicReference<TableView> gridRef = new AtomicReference<>();
    withScene(
        shell -> gridRef.set(buildGrid(shell)),
        bot ->
            assertEquals(
                MULTI_LINE,
                onUi(() -> TableView.getCellValue(gridRef.get().table.getItem(1), VALUE_COLUMN)),
                "the value must survive being drawn shortened, every line of it"));
  }

  /**
   * The value kept aside is only good while the cell still shows the text it was derived from. A
   * write that goes straight to the item — any path that does not know about the grid's own value
   * accessors — has to win, so that the worst such a path can cause is a value drawn in full again,
   * never a stale one handed back and saved.
   */
  @Test
  void aValueWrittenStraightToTheCellTakesOver() {
    AtomicReference<TableView> gridRef = new AtomicReference<>();
    withScene(
        shell -> gridRef.set(buildGrid(shell)),
        bot -> {
          TableItem item = onUi(() -> gridRef.get().table.getItem(1));
          onUi(
              () -> {
                item.setText(VALUE_COLUMN, "written around the grid");
                return null;
              });

          assertEquals(
              "written around the grid",
              onUi(() -> TableView.getCellValue(item, VALUE_COLUMN)),
              "a cell written directly must be read back as it stands, not as it was");
        });
  }

  private TableView buildGrid(org.eclipse.swt.widgets.Shell shell) {
    shell.setLayout(new FillLayout());
    shell.setSize(900, 320);
    ColumnInfo[] columns = {
      new ColumnInfo("Value", ColumnInfo.COLUMN_TYPE_TEXT, false, false),
    };
    TableView grid =
        new TableView(
            new Variables(),
            shell,
            SWT.BORDER | SWT.FULL_SELECTION,
            columns,
            3,
            null,
            PropsUi.getInstance());
    // What the Data grid's Data tab and the edit-rows dialogs do: data rows, drawn shortened.
    grid.setShortenDisplayedValues(true);
    grid.setCellValue(grid.table.getItem(0), VALUE_COLUMN, "single line");
    grid.setCellValue(grid.table.getItem(1), VALUE_COLUMN, MULTI_LINE);
    grid.setCellValue(grid.table.getItem(2), VALUE_COLUMN, "another single line");
    grid.optWidth(true);
    return grid;
  }

  private static <T> T onUi(Supplier<T> supplier) {
    AtomicReference<T> result = new AtomicReference<>();
    AtomicReference<RuntimeException> failure = new AtomicReference<>();
    display.syncExec(
        () -> {
          try {
            result.set(supplier.get());
          } catch (RuntimeException e) {
            failure.set(e);
          }
        });
    if (failure.get() != null) {
      throw failure.get();
    }
    return result.get();
  }
}
