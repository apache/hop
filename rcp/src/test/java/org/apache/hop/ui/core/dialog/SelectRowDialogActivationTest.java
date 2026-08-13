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

package org.apache.hop.ui.core.dialog;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;
import org.apache.hop.core.RowMetaAndData;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.RowMeta;
import org.apache.hop.core.row.value.ValueMetaString;
import org.apache.hop.core.variables.Variables;
import org.apache.hop.ui.testing.SwtBotTestBase;
import org.eclipse.swt.SWT;
import org.eclipse.swt.graphics.Rectangle;
import org.eclipse.swt.widgets.Composite;
import org.eclipse.swt.widgets.Control;
import org.eclipse.swt.widgets.Event;
import org.eclipse.swt.widgets.Shell;
import org.eclipse.swt.widgets.Table;
import org.eclipse.swt.widgets.TableItem;
import org.eclipse.swt.widgets.Text;
import org.eclipse.swtbot.swt.finder.SWTBot;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

/**
 * A read-only grid puts a view-only editor on the clicked cell so the full value stays selectable.
 * That editor covers the cell, so the table no longer sees the double-click (or Enter) that picks a
 * row - {@link SelectRowDialog} listens for exactly that. The editor has to pass it on.
 */
@Tag("uitest")
class SelectRowDialogActivationTest extends SwtBotTestBase {

  @Test
  void doubleClickingACellStillPicksTheRow() {
    IRowMeta rowMeta = new RowMeta();
    rowMeta.addValueMeta(new ValueMetaString("filename"));
    List<RowMetaAndData> rows = new ArrayList<>();
    rows.add(new RowMetaAndData(rowMeta, "first-file.hpl"));
    rows.add(new RowMetaAndData(rowMeta, "second-file.hpl"));

    AtomicReference<RowMetaAndData> picked = new AtomicReference<>();

    withDialog(
        parent -> picked.set(new SelectRowDialog(parent, new Variables(), SWT.NONE, rows).open()),
        bot -> {
          Table table = awaitTable(bot);
          assertNotNull(table, "the SelectRowDialog table should open");

          // First click activates the cell and drops the view-only editor on it.
          clickFirstDataCell(table);
          Text editor = awaitCellEditor(bot, table);
          assertNotNull(editor, "clicking a read-only cell should open the view-only editor");

          // The second click of the double-click lands on that editor, not on the table.
          onUi(
              () -> {
                editor.notifyListeners(SWT.MouseDoubleClick, new Event());
                return null;
              });
          bot.sleep(200);
        });

    assertNotNull(picked.get(), "double-clicking a cell should have picked the row");
    assertEquals(
        "first-file.hpl",
        picked.get().getData()[0],
        "the row under the double-clicked cell should be the one that is picked");
  }

  /** Fires a left mouse-down at the centre of the first data cell (row 0, first value column). */
  private void clickFirstDataCell(Table table) {
    onUi(
        () -> {
          TableItem item = table.getItem(0);
          Rectangle bounds = item.getBounds(1); // column 0 is the row number
          Event event = new Event();
          event.button = 1;
          event.x = bounds.x + bounds.width / 2;
          event.y = bounds.y + bounds.height / 2;
          table.notifyListeners(SWT.MouseDown, event);
          return null;
        });
  }

  private Table awaitTable(SWTBot bot) {
    for (int attempt = 0; attempt < 40; attempt++) {
      Table found = onUi(SelectRowDialogActivationTest::findTableOnUi);
      if (found != null) {
        return found;
      }
      bot.sleep(50);
    }
    return null;
  }

  private Text awaitCellEditor(SWTBot bot, Table table) {
    for (int attempt = 0; attempt < 40; attempt++) {
      Text found = onUi(() -> findText(table));
      if (found != null) {
        return found;
      }
      bot.sleep(50);
    }
    return null;
  }

  private static Table findTableOnUi() {
    for (Shell openShell : display.getShells()) {
      Table found = findTable(openShell);
      if (found != null) {
        return found;
      }
    }
    return null;
  }

  private static Table findTable(Composite parent) {
    for (Control child : parent.getChildren()) {
      if (child instanceof Table table) {
        return table;
      }
      if (child instanceof Composite composite) {
        Table found = findTable(composite);
        if (found != null) {
          return found;
        }
      }
    }
    return null;
  }

  /** The editor the grid parents onto the table, inside the holder carrying the expand icon. */
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

  /** Runs {@code supplier} on the UI thread and hands its result back to the SWTBot worker. */
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
