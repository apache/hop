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

import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiConsumer;
import java.util.function.Supplier;
import org.apache.hop.core.variables.Variables;
import org.apache.hop.ui.core.PropsUi;
import org.apache.hop.ui.testing.SwtBotTestBase;
import org.eclipse.swt.SWT;
import org.eclipse.swt.events.SelectionAdapter;
import org.eclipse.swt.graphics.Rectangle;
import org.eclipse.swt.layout.FillLayout;
import org.eclipse.swt.widgets.Composite;
import org.eclipse.swt.widgets.Control;
import org.eclipse.swt.widgets.Event;
import org.eclipse.swt.widgets.TableItem;
import org.eclipse.swt.widgets.Text;
import org.eclipse.swtbot.swt.finder.SWTBot;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

/**
 * Covers what happens to an open cell editor when the user leaves it by clicking the empty area
 * under the grid - the click that makes the grid add a row.
 *
 * <p>That click first moves the active cell to column 1 of the last row and only then commits, so
 * an editor the commit does not recognise is applied to the cell that was moved to rather than the
 * one it belongs to. The commit has to know every editor {@code edit()} can open; this pins down
 * the {@code COLUMN_TYPE_TEXT_BUTTON} one, which is a {@link TextVarButton} - a {@link TextVar},
 * not the plain {@link Text} that the type mismatch makes {@code getTextWidgetValue} cast to.
 *
 * <p>Tagged {@code uitest} so it is skipped on headless machines; run with {@code mvn -pl rcp
 * -Puitest test}.
 */
@Tag("uitest")
class TableViewLeaveEditorTest extends SwtBotTestBase {

  /** Grid layout: column 0 holds the row number, then the name and the browsable value. */
  private static final int NAME_COLUMN = 1;

  private static final int GENERATOR_COLUMN = 2;

  private static final int EDITED_ROW = 0;
  private static final int LAST_ROW = 1;

  private static final String TYPED = "uuid";

  /**
   * With a plain text column next to it the misapplied commit does not even reach the wrong cell:
   * it casts the browse editor to the widget type that column would have used.
   */
  @Test
  void aTextButtonCellIsCommittedToItsOwnCellNextToAPlainColumn() {
    assertTextButtonCellSurvivesLeavingTheEditor(false);
  }

  /** With a variables column next to it the same commit silently lands in the wrong cell. */
  @Test
  void aTextButtonCellIsCommittedToItsOwnCellNextToAVariablesColumn() {
    assertTextButtonCellSurvivesLeavingTheEditor(true);
  }

  private void assertTextButtonCellSurvivesLeavingTheEditor(boolean nameColumnUsesVariables) {
    withTableView(
        nameColumnUsesVariables,
        (tableView, bot) -> {
          editCell(tableView, EDITED_ROW, GENERATOR_COLUMN);
          setOpenEditorText(tableView, TYPED);

          clickTheEmptyAreaUnderTheGrid(tableView);

          assertEquals(
              TYPED,
              cellValue(tableView, EDITED_ROW, GENERATOR_COLUMN),
              "what was typed belongs to the cell it was typed in");
          assertEquals(
              "second",
              cellValue(tableView, LAST_ROW, NAME_COLUMN),
              "leaving the editor must not overwrite another row's name");
        });
  }

  // --- scene -----------------------------------------------------------------------------------

  /** Builds a two-row grid whose second column is browsable, the way e.g. the Fake dialog is. */
  private void withTableView(boolean nameColumnUsesVariables, BiConsumer<TableView, SWTBot> body) {
    AtomicReference<TableView> tableViewRef = new AtomicReference<>();
    withScene(
        shell -> {
          shell.setLayout(new FillLayout());
          shell.setSize(900, 320);
          ColumnInfo[] columns = {
            new ColumnInfo("Name", ColumnInfo.COLUMN_TYPE_TEXT, false, false),
            new ColumnInfo("Generator", ColumnInfo.COLUMN_TYPE_TEXT_BUTTON, false, false),
          };
          columns[0].setUsingVariables(nameColumnUsesVariables);
          // A TEXT_BUTTON column only renders its browse button when the column uses variables,
          // so its editor is a TextVarButton rather than a plain Text.
          columns[1].setUsingVariables(true);
          columns[1].setTextVarButtonSelectionListener(new SelectionAdapter() {});
          TableView tableView =
              new TableView(
                  new Variables(),
                  shell,
                  SWT.BORDER | SWT.FULL_SELECTION,
                  columns,
                  2,
                  null,
                  PropsUi.getInstance());
          TableItem first = tableView.table.getItem(EDITED_ROW);
          first.setText(NAME_COLUMN, "first");
          first.setText(GENERATOR_COLUMN, "random");
          TableItem second = tableView.table.getItem(LAST_ROW);
          second.setText(NAME_COLUMN, "second");
          tableView.optWidth(true);
          tableViewRef.set(tableView);
        },
        bot -> body.accept(tableViewRef.get(), bot));
  }

  // --- interactions ----------------------------------------------------------------------------

  private void editCell(TableView tableView, int rowNr, int colNr) {
    onUi(
        () -> {
          tableView.edit(rowNr, colNr);
          return null;
        });
  }

  /** Types into the editor the edit opened, wherever in the cell's holder it lives. */
  private void setOpenEditorText(TableView tableView, String value) {
    onUi(
        () -> {
          Text editor = findTextIn(tableView.table);
          assertNotNull(editor, "no editor is open on the grid");
          editor.setText(value);
          return null;
        });
  }

  /**
   * Clicks the empty space below the last row. The grid answers that with a new row, and it is the
   * commit done on the way there that can pick up the editor left open.
   */
  private void clickTheEmptyAreaUnderTheGrid(TableView tableView) {
    onUi(
        () -> {
          TableItem lastRow = tableView.table.getItem(tableView.table.getItemCount() - 1);
          Rectangle bounds = lastRow.getBounds(NAME_COLUMN);
          Event click = new Event();
          click.widget = tableView.table;
          click.button = 1;
          click.count = 1;
          click.x = bounds.x + bounds.width / 2;
          click.y = bounds.y + bounds.height + bounds.height / 2;
          tableView.table.notifyListeners(SWT.MouseDown, click);
          return null;
        });
  }

  // --- lookups ---------------------------------------------------------------------------------

  private String cellValue(TableView tableView, int rowNr, int colNr) {
    return onUi(() -> tableView.table.getItem(rowNr).getText(colNr));
  }

  private static Text findTextIn(Composite parent) {
    for (Control child : parent.getChildren()) {
      if (child instanceof Text text) {
        return text;
      }
      if (child instanceof Composite composite) {
        Text found = findTextIn(composite);
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
