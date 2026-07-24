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
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.RowMeta;
import org.apache.hop.core.row.value.ValueMetaString;
import org.apache.hop.core.variables.Variables;
import org.apache.hop.ui.testing.SwtBotTestBase;
import org.eclipse.swt.SWT;
import org.eclipse.swt.graphics.Rectangle;
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
 * Guards in-place cell selection in {@link ShowRowsDialog}: clicking a cell must drop a read-only,
 * selectable text field on it that holds the cell's <em>full</em> value, so it can be copied even
 * when the grid only shows a truncated, single-lined version.
 *
 * <p>This is the behaviour the transform/hop "output data" preview lost when it moved off {@link
 * PreviewRowsDialog} (which has the same click-to-select editor) onto the simpler {@code
 * ShowRowsDialog}. Without the editor, the click delivered below leaves no {@link Text} on the
 * table and the assertions here fail.
 */
@Tag("uitest")
class ShowRowsDialogCellSelectionTest extends SwtBotTestBase {

  // Longer than the max preview cell length, so the grid truncates it but the editor must not.
  private static final String LONG_VALUE =
      "a very long value that the preview grid truncates for display but must stay selectable "
          + "in full so it can be copied out of the cell without any characters being lost at all";

  @Test
  void clickingACellDropsAReadOnlyEditorHoldingTheFullValue() {
    IRowMeta rowMeta = new RowMeta();
    rowMeta.addValueMeta(new ValueMetaString("text"));
    List<Object[]> rows = new ArrayList<>();
    rows.add(new Object[] {LONG_VALUE});

    withDialog(
        parent ->
            new ShowRowsDialog(parent, new Variables(), "Preview", "Output rows", rowMeta, rows)
                .open(),
        bot -> {
          Table table = awaitTable(bot);
          assertNotNull(table, "the ShowRowsDialog table should open");

          clickFirstDataCell(table);

          Text editor = awaitCellEditor(bot, table);
          assertNotNull(
              editor,
              "clicking a cell should drop a selectable text field holding the value in place");
          assertTrue(
              (onUi(editor::getStyle) & SWT.READ_ONLY) != 0,
              "the in-place cell editor must be read-only (a viewer, not an editor)");
          assertEquals(
              LONG_VALUE,
              onUi(editor::getText),
              "the editor must expose the full, untruncated cell value for copying");
        });
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

  /** Polls for the ShowRowsDialog's table across the open shells. */
  private Table awaitTable(SWTBot bot) {
    for (int attempt = 0; attempt < 40; attempt++) {
      Table found = onUi(ShowRowsDialogCellSelectionTest::findTableOnUi);
      if (found != null) {
        return found;
      }
      bot.sleep(50);
    }
    return null;
  }

  /** Polls for the read-only editor the click drops directly on the table. */
  private Text awaitCellEditor(SWTBot bot, Table table) {
    for (int attempt = 0; attempt < 40; attempt++) {
      Text found = onUi(() -> findDirectTextChild(table));
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

  private static Table findTable(org.eclipse.swt.widgets.Composite parent) {
    for (Control child : parent.getChildren()) {
      if (child instanceof Table table) {
        return table;
      }
      if (child instanceof org.eclipse.swt.widgets.Composite composite) {
        Table found = findTable(composite);
        if (found != null) {
          return found;
        }
      }
    }
    return null;
  }

  /** The cell editor is a {@link Text} parented straight onto the table (via a TableEditor). */
  private static Text findDirectTextChild(Table table) {
    for (Control child : table.getChildren()) {
      if (child instanceof Text text && !text.isDisposed()) {
        return text;
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
