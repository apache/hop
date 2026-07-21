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
import java.util.function.BiConsumer;
import java.util.function.Supplier;
import org.apache.hop.core.variables.Variables;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.ui.core.PropsUi;
import org.apache.hop.ui.testing.SwtBotTestBase;
import org.eclipse.swt.SWT;
import org.eclipse.swt.layout.FillLayout;
import org.eclipse.swt.widgets.Composite;
import org.eclipse.swt.widgets.Control;
import org.eclipse.swt.widgets.Event;
import org.eclipse.swt.widgets.Label;
import org.eclipse.swt.widgets.Shell;
import org.eclipse.swt.widgets.TableItem;
import org.eclipse.swt.widgets.Text;
import org.eclipse.swtbot.swt.finder.SWTBot;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

/**
 * Covers how a table cell is expanded into the floating multi-line editor: the "expand" icon on the
 * inline editor, the pop-out it opens, and committing / cancelling from there.
 *
 * <p>These paths break silently when touched. The icon has to stay a non-focusable {@link Label} -
 * a {@link org.eclipse.swt.widgets.Button} takes focus on mouse-down, which fires the editor's
 * focus-lost handler and disposes the editor before the click is ever handled. And the inline
 * editor is wrapped in a holder composite that has to come down together with it.
 *
 * <p>Clicks are delivered as synthetic events, which does not reproduce the platform's focus
 * transfer, so no test here can observe a focus-stealing icon directly. {@link
 * #expandingCarriesOverTextTypedInTheInlineEditor()} covers the consequence instead: an editor that
 * was committed and disposed before the click is handled cannot hand its in-flight text over.
 */
@Tag("uitest")
class TableViewMultilineEditorTest extends SwtBotTestBase {

  private static final String SHORT_VALUE = "a short single line value";
  private static final String MULTILINE_VALUE = "SELECT *\nFROM customers\nWHERE id > 100";

  private static final String EXPAND_TOOLTIP =
      BaseMessages.getString(TableView.class, "TableView.tooltip.ExpandValue");

  /** Row holding {@link #MULTILINE_VALUE}, row holding {@link #SHORT_VALUE}, value column. */
  private static final int MULTILINE_ROW = 0;

  private static final int SHORT_ROW = 1;
  private static final int VALUE_COLUMN = 2;

  @Test
  void textCellEditorOffersAnExpandIcon() {
    withTableView(
        false,
        (tableView, bot) -> {
          editCell(tableView, SHORT_ROW, VALUE_COLUMN);
          assertNotNull(
              findExpandIcon(tableView), "the inline editor of a text cell should carry the icon");
        });
  }

  @Test
  void passwordCellEditorHasNoExpandIcon() {
    withTableView(
        true,
        (tableView, bot) -> {
          editCell(tableView, SHORT_ROW, VALUE_COLUMN);
          assertNull(
              findExpandIcon(tableView), "a password cell must not offer a multi-line pop-out");
        });
  }

  @Test
  void clickingTheExpandIconOpensThePopOutWithTheCellValue() {
    withTableView(
        false,
        (tableView, bot) -> {
          editCell(tableView, SHORT_ROW, VALUE_COLUMN);
          clickExpandIcon(tableView);

          Text popOut = awaitPopOutText(bot);
          assertNotNull(popOut, "clicking the expand icon should open the multi-line editor");
          assertEquals(
              SHORT_VALUE, onUi(popOut::getText), "the pop-out should show the full cell value");
        });
  }

  /**
   * Expanding mid-edit has to carry the editor's current text over, not fall back to the value
   * stored in the cell - otherwise whatever was typed before reaching for the icon is lost.
   */
  @Test
  void expandingCarriesOverTextTypedInTheInlineEditor() {
    withTableView(
        false,
        (tableView, bot) -> {
          editCell(tableView, SHORT_ROW, VALUE_COLUMN);
          onUi(
              () -> {
                setInlineEditorText(tableView, "typed but not committed");
                return null;
              });

          clickExpandIcon(tableView);

          Text popOut = awaitPopOutText(bot);
          assertNotNull(popOut, "the pop-out should be open");
          assertEquals(
              "typed but not committed",
              onUi(popOut::getText),
              "the pop-out should continue the in-flight edit, not re-read the cell");
        });
  }

  @Test
  void enterInThePopOutCommitsTheEditedValueBackToTheCell() {
    withTableView(
        false,
        (tableView, bot) -> {
          editCell(tableView, SHORT_ROW, VALUE_COLUMN);
          clickExpandIcon(tableView);
          Text popOut = awaitPopOutText(bot);
          assertNotNull(popOut, "the pop-out should be open");

          onUi(
              () -> {
                popOut.setText("first line" + Text.DELIMITER + "second line");
                return pressEnter(popOut, SWT.NONE);
              });

          // Line breaks are stored platform-independently, whatever delimiter the Text used.
          assertEquals("first line\nsecond line", cellValue(tableView, SHORT_ROW, VALUE_COLUMN));
          assertNull(onUi(TableViewMultilineEditorTest::findPopOutTextOnUi), "Enter should close");
        });
  }

  /** Enter has to be swallowed, or the newline lands in the value that is about to be stored. */
  @Test
  void enterDoesNotLeakItsNewlineIntoTheStoredValue() {
    withTableView(
        false,
        (tableView, bot) -> {
          editCell(tableView, SHORT_ROW, VALUE_COLUMN);
          clickExpandIcon(tableView);
          Text popOut = awaitPopOutText(bot);
          assertNotNull(popOut, "the pop-out should be open");

          Boolean doit = onUi(() -> pressEnter(popOut, SWT.NONE));

          assertEquals(Boolean.FALSE, doit, "a committing Enter must not be typed into the text");
        });
  }

  @Test
  void shiftEnterInThePopOutAddsALineInsteadOfCommitting() {
    withTableView(
        false,
        (tableView, bot) -> {
          editCell(tableView, SHORT_ROW, VALUE_COLUMN);
          clickExpandIcon(tableView);
          Text popOut = awaitPopOutText(bot);
          assertNotNull(popOut, "the pop-out should be open");

          Boolean doit = onUi(() -> pressEnter(popOut, SWT.SHIFT));

          assertEquals(Boolean.TRUE, doit, "Shift+Enter must reach the Text to insert a line");
          assertNotNull(
              onUi(TableViewMultilineEditorTest::findPopOutTextOnUi),
              "Shift+Enter should leave the editor open");
          assertEquals(
              SHORT_VALUE,
              cellValue(tableView, SHORT_ROW, VALUE_COLUMN),
              "Shift+Enter should not commit");
        });
  }

  @Test
  void escapeInThePopOutLeavesTheCellUntouched() {
    withTableView(
        false,
        (tableView, bot) -> {
          editCell(tableView, SHORT_ROW, VALUE_COLUMN);
          clickExpandIcon(tableView);
          Text popOut = awaitPopOutText(bot);
          assertNotNull(popOut, "the pop-out should be open");

          onUi(
              () -> {
                popOut.setText("discarded");
                Event escape = new Event();
                escape.keyCode = SWT.ESC;
                popOut.notifyListeners(SWT.KeyDown, escape);
                return null;
              });

          assertEquals(SHORT_VALUE, cellValue(tableView, SHORT_ROW, VALUE_COLUMN));
        });
  }

  @Test
  void aValueThatAlreadyHasLineBreaksOpensThePopOutDirectly() {
    withTableView(
        false,
        (tableView, bot) -> {
          // A single-line inline editor cannot represent this value, so editing must skip it.
          editCell(tableView, MULTILINE_ROW, VALUE_COLUMN);

          Text popOut = awaitPopOutText(bot);
          assertNotNull(popOut, "a value with line breaks should open the pop-out straight away");
          assertEquals(MULTILINE_VALUE, toUnixLineBreaks(onUi(popOut::getText)));
          assertNull(
              findExpandIcon(tableView),
              "the inline editor should be skipped, not opened underneath the pop-out");
        });
  }

  /**
   * Double-click belongs to the text editor, where it selects a word. It used to open the pop-out -
   * both from the table and from the inline editor - which cost users word-select in every grid.
   */
  @Test
  void doubleClickingACellDoesNotOpenThePopOut() {
    withTableView(
        false,
        (tableView, bot) -> {
          // Establish an active cell first, then close the editor again: the handler this guards
          // against keyed off the active cell, so without one the test would pass vacuously.
          editCell(tableView, SHORT_ROW, VALUE_COLUMN);
          onUi(
              () -> {
                tableView.unEdit();
                tableView.table.notifyListeners(SWT.MouseDoubleClick, new Event());
                return null;
              });

          assertNull(
              awaitNoPopOut(bot),
              "double-clicking a cell should no longer open the multi-line box");
        });
  }

  @Test
  void doubleClickingInsideTheInlineEditorDoesNotOpenThePopOut() {
    withTableView(
        false,
        (tableView, bot) -> {
          editCell(tableView, SHORT_ROW, VALUE_COLUMN);

          onUi(
              () -> {
                Text inline = findTextIn(tableView.table);
                assertNotNull(inline, "precondition: the inline editor is open");
                inline.notifyListeners(SWT.MouseDown, new Event());
                inline.notifyListeners(SWT.MouseDoubleClick, new Event());
                return null;
              });

          assertNull(
              awaitNoPopOut(bot),
              "double-clicking inside the inline editor should select a word, not expand");
        });
  }

  /**
   * The inline editor of a text cell lives in a holder composite. However the edit ends, that
   * holder has to go with it - a leftover would sit on top of the table and swallow clicks.
   */
  @Test
  void closingTheEditorLeavesNoHolderBehindOnTheTable() {
    withTableView(
        false,
        (tableView, bot) -> {
          editCell(tableView, SHORT_ROW, VALUE_COLUMN);
          assertNotNull(findExpandIcon(tableView), "precondition: the inline editor is open");

          onUi(
              () -> {
                tableView.unEdit();
                return null;
              });

          assertEquals(
              0,
              (int) onUi(() -> countCompositesOnTable(tableView)),
              "the inline editor's holder composite should be disposed with the editor");
        });
  }

  // --- scene -----------------------------------------------------------------------------------

  /**
   * Builds a table holding a multi-line row and a short single-line row, then runs {@code body}.
   */
  private void withTableView(boolean passwordColumn, BiConsumer<TableView, SWTBot> body) {
    AtomicReference<TableView> tableViewRef = new AtomicReference<>();
    withScene(
        shell -> {
          shell.setLayout(new FillLayout());
          shell.setSize(900, 320);
          ColumnInfo[] columns = {
            new ColumnInfo("Name", ColumnInfo.COLUMN_TYPE_TEXT, false, false),
            new ColumnInfo("Value", ColumnInfo.COLUMN_TYPE_TEXT, false, false),
          };
          columns[1].setPasswordField(passwordColumn);
          TableView tableView =
              new TableView(
                  new Variables(),
                  shell,
                  SWT.BORDER | SWT.FULL_SELECTION,
                  columns,
                  3,
                  null,
                  PropsUi.getInstance());
          TableItem multiline = tableView.table.getItem(MULTILINE_ROW);
          multiline.setText(1, "query");
          multiline.setText(VALUE_COLUMN, MULTILINE_VALUE);
          TableItem single = tableView.table.getItem(SHORT_ROW);
          single.setText(1, "note");
          single.setText(VALUE_COLUMN, SHORT_VALUE);
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

  /**
   * Sends Enter to the pop-out and reports whether the key was left for the Text to consume, i.e.
   * whether it inserted a line break rather than committing.
   */
  private static Boolean pressEnter(Text popOut, int stateMask) {
    Event enter = new Event();
    enter.keyCode = SWT.CR;
    enter.stateMask = stateMask;
    enter.doit = true;
    popOut.notifyListeners(SWT.KeyDown, enter);
    return enter.doit;
  }

  /** Fires the icon's mouse-down the way a real click would. */
  private void clickExpandIcon(TableView tableView) {
    onUi(
        () -> {
          Label icon = findExpandIconOnUi(tableView);
          assertNotNull(icon, "no expand icon on the inline editor");
          icon.notifyListeners(SWT.MouseDown, new Event());
          return null;
        });
  }

  // --- lookups ---------------------------------------------------------------------------------

  private Label findExpandIcon(TableView tableView) {
    return onUi(() -> findExpandIconOnUi(tableView));
  }

  private static Label findExpandIconOnUi(TableView tableView) {
    return findLabelWithExpandTooltip(tableView.table);
  }

  private static Label findLabelWithExpandTooltip(Composite parent) {
    for (Control child : parent.getChildren()) {
      // Match on the tooltip: a TextVar carries an image label of its own (the $ variable hint).
      if (child instanceof Label label && EXPAND_TOOLTIP.equals(label.getToolTipText())) {
        return label;
      }
      if (child instanceof Composite composite) {
        Label found = findLabelWithExpandTooltip(composite);
        if (found != null) {
          return found;
        }
      }
    }
    return null;
  }

  /** Types into the open inline editor, i.e. the Text somewhere inside the holder on the table. */
  private static void setInlineEditorText(TableView tableView, String value) {
    Text editor = findTextIn(tableView.table);
    assertNotNull(editor, "no inline editor is open");
    editor.setText(value);
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

  /** Composites parented straight onto the table - i.e. surviving inline editor holders. */
  private static int countCompositesOnTable(TableView tableView) {
    int count = 0;
    for (Control child : tableView.table.getChildren()) {
      if (child instanceof Composite && !child.isDisposed()) {
        count++;
      }
    }
    return count;
  }

  private String cellValue(TableView tableView, int rowNr, int colNr) {
    return onUi(() -> tableView.table.getItem(rowNr).getText(colNr));
  }

  /**
   * The pop-out is a title-less shell of its own, so SWTBot cannot address it by name - find its
   * multi-line Text directly. Polls, because the icon handler opens the pop-out asynchronously.
   */
  private Text awaitPopOutText(SWTBot bot) {
    for (int attempt = 0; attempt < 40; attempt++) {
      Text found = onUi(TableViewMultilineEditorTest::findPopOutTextOnUi);
      if (found != null) {
        return found;
      }
      bot.sleep(50);
    }
    return null;
  }

  /**
   * Waits out the grace period a deferred handler would need to open a pop-out, then reports what
   * is actually there - for the cases where the answer should be "nothing".
   */
  private Text awaitNoPopOut(SWTBot bot) {
    bot.sleep(500);
    return onUi(TableViewMultilineEditorTest::findPopOutTextOnUi);
  }

  private static Text findPopOutTextOnUi() {
    for (Shell shell : display.getShells()) {
      for (Control child : shell.getChildren()) {
        if (child instanceof Text text && (text.getStyle() & SWT.MULTI) != 0) {
          return text;
        }
      }
    }
    return null;
  }

  // --- helpers ---------------------------------------------------------------------------------

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

  private static String toUnixLineBreaks(String value) {
    return value == null ? null : value.replace("\r\n", "\n").replace('\r', '\n');
  }
}
