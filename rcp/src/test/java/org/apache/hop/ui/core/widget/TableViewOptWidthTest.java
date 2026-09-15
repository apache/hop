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
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiConsumer;
import java.util.function.Supplier;
import org.apache.hop.core.variables.Variables;
import org.apache.hop.ui.core.PropsUi;
import org.apache.hop.ui.testing.SwtBotTestBase;
import org.eclipse.swt.SWT;
import org.eclipse.swt.layout.FillLayout;
import org.eclipse.swt.widgets.TableItem;
import org.eclipse.swtbot.swt.finder.SWTBot;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

/**
 * Width-stability contracts for {@link TableView#optWidth}. Live metrics refresh used to set each
 * column twice (and bounce pack-then-restore); these cases pin down the behaviour Windows 11 needs
 * even though this agent runs under GTK.
 */
@Tag("uitest")
class TableViewOptWidthTest extends SwtBotTestBase {

  private static final int NAME_COLUMN = 1;
  private static final int VALUE_COLUMN = 2;
  private static final int PREFERRED_WIDTH = 250;

  @Test
  void optWidthTwiceWithUnchangedContentLeavesColumnWidthsAlone() {
    withTableView(
        (view, bot) -> {
          // Pack after the shell is open: GTK stretches the last column on first layout.
          onUi(
              () -> {
                view.optWidth(true);
                return null;
              });
          int[] first = snapshotWidths(view);
          onUi(
              () -> {
                view.optWidth(true);
                return null;
              });
          int[] second = snapshotWidths(view);
          assertEquals(first.length, second.length);
          for (int i = 0; i < first.length; i++) {
            assertEquals(first[i], second[i], "column " + i + " must not move on a no-op pack");
          }
        });
  }

  @Test
  void optWidthHonorsExplicitColumnWidth() {
    withTableView(
        (view, bot) -> {
          onUi(
              () -> {
                view.setPreferredColumnWidth(VALUE_COLUMN, PREFERRED_WIDTH);
                view.optWidth(true);
                return null;
              });
          assertEquals(PREFERRED_WIDTH, columnWidth(view, VALUE_COLUMN));
          onUi(
              () -> {
                view.optWidth(true);
                return null;
              });
          assertEquals(
              PREFERRED_WIDTH,
              columnWidth(view, VALUE_COLUMN),
              "a second pack must not override the preferred width");
        });
  }

  @Test
  void growOnlyWidensForLongerTextButNeverShrinks() {
    withTableView(
        (view, bot) -> {
          onUi(
              () -> {
                view.table.getItem(0).setText(VALUE_COLUMN, "1");
                view.optWidth(true);
                return null;
              });
          int packedShort = columnWidth(view, VALUE_COLUMN);

          onUi(
              () -> {
                view.table.getItem(0).setText(VALUE_COLUMN, "1,234,567,890,123");
                view.optWidth(true, 0, true);
                return null;
              });
          int afterGrow = columnWidth(view, VALUE_COLUMN);
          assertTrue(
              afterGrow > packedShort,
              "grow-only should widen for a much longer value (short="
                  + packedShort
                  + ", grown="
                  + afterGrow
                  + ")");

          onUi(
              () -> {
                view.table.getItem(0).setText(VALUE_COLUMN, "1");
                view.optWidth(true, 0, true);
                return null;
              });
          assertEquals(
              afterGrow,
              columnWidth(view, VALUE_COLUMN),
              "grow-only must not shrink when the cell text gets shorter");
        });
  }

  @Test
  void growOnlyDoesNotTouchUserSizedColumns() {
    withTableView(
        (view, bot) -> {
          onUi(
              () -> {
                view.setPreferredColumnWidth(VALUE_COLUMN, PREFERRED_WIDTH);
                view.optWidth(true);
                view.table.getItem(0).setText(VALUE_COLUMN, "1,234,567,890,123");
                view.optWidth(true, 0, true);
                return null;
              });
          assertEquals(
              PREFERRED_WIDTH,
              columnWidth(view, VALUE_COLUMN),
              "grow-only must leave a user-sized column alone");
        });
  }

  private void withTableView(BiConsumer<TableView, SWTBot> body) {
    AtomicReference<TableView> viewRef = new AtomicReference<>();
    withScene(
        shell -> {
          shell.setLayout(new FillLayout());
          shell.setSize(700, 280);
          ColumnInfo[] columns = {
            new ColumnInfo("Name", ColumnInfo.COLUMN_TYPE_TEXT, false, true),
            new ColumnInfo("Value", ColumnInfo.COLUMN_TYPE_TEXT, true, true),
          };
          TableView view =
              new TableView(
                  new Variables(),
                  shell,
                  SWT.BORDER | SWT.FULL_SELECTION,
                  columns,
                  1,
                  true,
                  null,
                  PropsUi.getInstance());
          TableItem row = view.table.getItem(0);
          row.setText(NAME_COLUMN, "rows");
          row.setText(VALUE_COLUMN, "1");
          view.optWidth(true);
          viewRef.set(view);
        },
        bot -> body.accept(viewRef.get(), bot));
  }

  private static int[] snapshotWidths(TableView view) {
    return onUi(
        () -> {
          int n = view.table.getColumnCount();
          int[] widths = new int[n];
          for (int i = 0; i < n; i++) {
            widths[i] = view.table.getColumn(i).getWidth();
          }
          return widths;
        });
  }

  private static int columnWidth(TableView view, int index) {
    return onUi(() -> view.table.getColumn(index).getWidth());
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
