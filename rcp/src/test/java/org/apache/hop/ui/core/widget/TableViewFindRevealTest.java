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
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;

import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;
import org.apache.hop.core.variables.Variables;
import org.apache.hop.ui.core.PropsUi;
import org.apache.hop.ui.testing.SwtBotTestBase;
import org.eclipse.swt.SWT;
import org.eclipse.swt.graphics.Point;
import org.eclipse.swt.layout.FillLayout;
import org.eclipse.swt.widgets.Composite;
import org.eclipse.swt.widgets.Control;
import org.eclipse.swt.widgets.Text;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

/**
 * {@link TableView#revealFoundCell(int, int)} selects editable values and only shows read-only
 * ones.
 */
@Tag("uitest")
class TableViewFindRevealTest extends SwtBotTestBase {

  @Test
  void editableGridSelectsTheCellValue() {
    AtomicReference<TableView> viewRef = new AtomicReference<>();
    withScene(
        shell -> {
          shell.setLayout(new FillLayout());
          TableView view = newTableView(shell, false);
          view.table.getItem(0).setText(2, "one");
          view.table.getItem(1).setText(2, "needle");
          viewRef.set(view);
        },
        bot -> {
          Revealed revealed =
              onUi(
                  () -> {
                    TableView view = viewRef.get();
                    view.revealFoundCell(1, 1);
                    Text editor = findText(view.table);
                    return new Revealed(
                        view.table.getSelectionIndex(),
                        view.getActiveTableColumn(),
                        editor == null ? null : editor.getText(),
                        editor == null ? null : editor.getSelection());
                  });
          assertEquals(1, revealed.row());
          assertEquals(2, revealed.column());
          assertEquals("needle", revealed.text());
          assertNotNull(revealed.selection());
          assertEquals(new Point(0, "needle".length()), revealed.selection());
        });
  }

  @Test
  void readOnlyColumnShowsTheRowWithoutAnEditor() {
    AtomicReference<TableView> viewRef = new AtomicReference<>();
    withScene(
        shell -> {
          shell.setLayout(new FillLayout());
          viewRef.set(newTableView(shell, true));
        },
        bot -> {
          Revealed revealed =
              onUi(
                  () -> {
                    TableView view = viewRef.get();
                    view.table.getItem(0).setText(1, "alpha");
                    view.revealFoundCell(0, 0);
                    Text editor = findText(view.table);
                    return new Revealed(
                        view.table.getSelectionIndex(),
                        view.getActiveTableColumn(),
                        editor == null ? null : editor.getText(),
                        null);
                  });
          assertEquals(0, revealed.row());
          assertEquals(1, revealed.column());
          assertNull(revealed.text());
        });
  }

  @Test
  void readOnlyTableShowsTheRowWithoutAnEditor() {
    AtomicReference<TableView> viewRef = new AtomicReference<>();
    withScene(
        shell -> {
          shell.setLayout(new FillLayout());
          TableView view = newTableView(shell, false);
          view.setReadonly(true);
          viewRef.set(view);
        },
        bot -> {
          Revealed revealed =
              onUi(
                  () -> {
                    TableView view = viewRef.get();
                    view.table.getItem(0).setText(1, "alpha");
                    view.revealFoundCell(0, 0);
                    return new Revealed(
                        view.table.getSelectionIndex(),
                        view.getActiveTableColumn(),
                        findText(view.table) == null ? null : "open",
                        null);
                  });
          assertEquals(0, revealed.row());
          assertEquals(1, revealed.column());
          assertNull(revealed.text());
        });
  }

  private record Revealed(int row, int column, String text, Point selection) {}

  private static TableView newTableView(Composite parent, boolean columnReadOnly) {
    ColumnInfo[] columns = {
      new ColumnInfo("Name", ColumnInfo.COLUMN_TYPE_TEXT, false, columnReadOnly),
      new ColumnInfo("Value", ColumnInfo.COLUMN_TYPE_TEXT, false, columnReadOnly),
    };
    return new TableView(
        new Variables(),
        parent,
        SWT.BORDER | SWT.FULL_SELECTION,
        columns,
        2,
        null,
        PropsUi.getInstance());
  }

  private static Text findText(Composite parent) {
    for (Control child : parent.getChildren()) {
      if (child instanceof Text text && !text.isDisposed()) {
        return text;
      }
      if (child instanceof Composite composite) {
        Text found = findText(composite);
        if (found != null) {
          return found;
        }
      }
    }
    return null;
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
