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
import static org.junit.jupiter.api.Assertions.assertNotSame;
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
import org.eclipse.swt.dnd.Clipboard;
import org.eclipse.swt.dnd.TextTransfer;
import org.eclipse.swt.graphics.Rectangle;
import org.eclipse.swt.widgets.Composite;
import org.eclipse.swt.widgets.Control;
import org.eclipse.swt.widgets.Event;
import org.eclipse.swt.widgets.Label;
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
          String tooltip = onUi(editor::getToolTipText);
          assertNotNull(tooltip, "selected cell should show a column-metadata tooltip");
          assertTrue(
              tooltip.contains("text"),
              "cell tooltip should include the column name; was: " + tooltip);
        });
  }

  /**
   * Copy to clipboard, export to Excel/CSV and save-to-data-set all read the cell straight off the
   * table item, so the item has to hold the complete value: the grid only shortens it when drawing.
   */
  @Test
  void gridCellsHoldTheFullValueEvenThoughTheyAreDrawnShortened() {
    String multiLine = "first line\nsecond line\nthird line";

    IRowMeta rowMeta = new RowMeta();
    rowMeta.addValueMeta(new ValueMetaString("text"));
    rowMeta.addValueMeta(new ValueMetaString("lines"));
    List<Object[]> rows = new ArrayList<>();
    rows.add(new Object[] {LONG_VALUE, multiLine});

    withDialog(
        parent ->
            new ShowRowsDialog(parent, new Variables(), "Preview", "Output rows", rowMeta, rows)
                .open(),
        bot -> {
          Table table = awaitTable(bot);
          assertNotNull(table, "the ShowRowsDialog table should open");

          assertEquals(
              LONG_VALUE,
              onUi(() -> table.getItem(0).getText(1)),
              "a long cell value must be stored in full, not truncated with an ellipsis");
          assertEquals(
              multiLine,
              onUi(() -> table.getItem(0).getText(2)),
              "a multi-line cell value must keep all of its lines");
        });
  }

  /** Ctrl+C on selected rows must put the complete values on the clipboard, ellipsis-free. */
  @Test
  void copyingSelectedRowsPutsTheFullValuesOnTheClipboard() {
    String multiLine = "first line\nsecond line";

    IRowMeta rowMeta = new RowMeta();
    rowMeta.addValueMeta(new ValueMetaString("text"));
    rowMeta.addValueMeta(new ValueMetaString("lines"));
    List<Object[]> rows = new ArrayList<>();
    rows.add(new Object[] {LONG_VALUE, multiLine});

    withDialog(
        parent ->
            new ShowRowsDialog(parent, new Variables(), "Preview", "Output rows", rowMeta, rows)
                .open(),
        bot -> {
          Table table = awaitTable(bot);
          assertNotNull(table, "the ShowRowsDialog table should open");

          // A click makes the row the active one; the key handler ignores keys until then.
          clickFirstDataCell(table);

          String clipped =
              onUi(
                  () -> {
                    table.setSelection(0);
                    Event event = new Event();
                    event.keyCode = 'c';
                    event.stateMask = SWT.MOD1;
                    table.notifyListeners(SWT.KeyDown, event);
                    Clipboard clipboard = new Clipboard(display);
                    try {
                      return (String) clipboard.getContents(TextTransfer.getInstance());
                    } finally {
                      clipboard.dispose();
                    }
                  });

          assertNotNull(clipped, "Ctrl+C should put the selected row on the clipboard");
          assertTrue(
              clipped.contains(LONG_VALUE),
              "the clipboard must hold the full value, not the shortened one; was: " + clipped);
          assertTrue(
              clipped.contains(multiLine),
              "the clipboard must keep every line of a multi-line value; was: " + clipped);
        });
  }

  @Test
  void sortingKeepsFullValueLookupForTheSelectedCell() {
    IRowMeta rowMeta = new RowMeta();
    rowMeta.addValueMeta(new ValueMetaString("text"));
    List<Object[]> rows = new ArrayList<>();
    rows.add(new Object[] {"zebra-" + LONG_VALUE});
    rows.add(new Object[] {"alpha-" + LONG_VALUE});

    withDialog(
        parent ->
            new ShowRowsDialog(parent, new Variables(), "Preview", "Output rows", rowMeta, rows)
                .open(),
        bot -> {
          Table table = awaitTable(bot);
          assertNotNull(table, "the ShowRowsDialog table should open");

          // Sort by the data column (index 1; 0 is the row-number column).
          onUi(
              () -> {
                table.getColumn(1).notifyListeners(SWT.Selection, new Event());
                return null;
              });
          bot.sleep(100);

          // After ascending sort, "alpha-..." should be first. Click it and verify full value.
          clickFirstDataCell(table);
          Text editor = awaitCellEditor(bot, table);
          assertNotNull(editor, "cell editor should open after sort");
          assertEquals(
              "alpha-" + LONG_VALUE,
              onUi(editor::getText),
              "after sorting, the full buffer value for the clicked row must still be resolved");
        });
  }

  /**
   * The in-place editor must be the grid's own one - a text field wrapped in a holder that also
   * carries the "expand" icon opening the multi-line viewer - and not a hand-rolled field that only
   * reacts to a double-click.
   */
  @Test
  void theCellEditorCarriesTheGridsExpandIcon() {
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
          assertNotNull(editor, "clicking a cell should open the grid's in-place editor");

          Composite holder = onUi(editor::getParent);
          assertNotSame(table, holder, "the editor should sit in the holder that carries the icon");
          assertNotNull(
              onUi(() -> findLabelWithImage(holder)),
              "the editor holder should carry the expand icon opening the multi-line viewer");
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

  /**
   * The cell editor is a {@link Text} the grid parents onto the table via a TableEditor, wrapped in
   * a holder composite that also carries the "expand" icon.
   */
  private static Text findDirectTextChild(Composite parent) {
    for (Control child : parent.getChildren()) {
      if (child instanceof Text text && !text.isDisposed()) {
        return text;
      }
      if (child instanceof Composite composite) {
        Text found = findDirectTextChild(composite);
        if (found != null) {
          return found;
        }
      }
    }
    return null;
  }

  /** The expand icon: an image-carrying label next to the editor. */
  private static Label findLabelWithImage(Composite parent) {
    for (Control child : parent.getChildren()) {
      if (child instanceof Label label && label.getImage() != null) {
        return label;
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
