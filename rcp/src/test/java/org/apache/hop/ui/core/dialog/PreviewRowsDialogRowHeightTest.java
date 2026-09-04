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
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.RowMeta;
import org.apache.hop.core.row.value.ValueMetaString;
import org.apache.hop.core.variables.Variables;
import org.apache.hop.ui.core.widget.TableView;
import org.apache.hop.ui.testing.SwtBotTestBase;
import org.eclipse.swt.SWT;
import org.eclipse.swt.widgets.Composite;
import org.eclipse.swt.widgets.Control;
import org.eclipse.swt.widgets.Shell;
import org.eclipse.swt.widgets.Table;
import org.eclipse.swtbot.swt.finder.SWTBot;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

/**
 * Guards the row height of the transform preview grid.
 *
 * <p>Row geometry is not uniform on every platform: GTK measures each row from the text its cells
 * hold, so a line break that reaches the cell grows that row to as many lines as the value has
 * (issue #8155). macOS gives every row the same height, so this can only ever fail on GTK — run it
 * there, e.g. through {@code tools/with-isolated-display.sh}.
 */
@Tag("uitest")
class PreviewRowsDialogRowHeightTest extends SwtBotTestBase {

  private static final String MULTI_LINE = "first line\nsecond line\nthird line\nfourth line";

  @Test
  void aMultiLineValueDoesNotGrowItsRow() {
    IRowMeta rowMeta = new RowMeta();
    rowMeta.addValueMeta(new ValueMetaString("text"));
    List<Object[]> rows = new ArrayList<>();
    rows.add(new Object[] {"single line"});
    rows.add(new Object[] {MULTI_LINE});
    rows.add(new Object[] {"another single line"});

    withDialog(
        parent ->
            new PreviewRowsDialog(parent, new Variables(), SWT.NONE, "transform", rowMeta, rows)
                .open(),
        bot -> {
          Table table = awaitTable(bot);
          assertNotNull(table, "the PreviewRowsDialog table should open");

          int singleLineHeight = onUi(() -> table.getItem(0).getBounds(1).height);
          int multiLineHeight = onUi(() -> table.getItem(1).getBounds(1).height);
          assertEquals(
              singleLineHeight,
              multiLineHeight,
              "a multi-line value must not make its row taller than a single-line one");
          assertEquals(
              MULTI_LINE,
              onUi(() -> TableView.getCellValue(table.getItem(1), 1)),
              "the grid must still hand back the value with all of its lines");
        });
  }

  private Table awaitTable(SWTBot bot) {
    for (int i = 0; i < 50; i++) {
      Table found = onUi(PreviewRowsDialogRowHeightTest::findTableOnUi);
      if (found != null && onUi(() -> found.getItemCount() >= 3)) {
        return found;
      }
      bot.sleep(100);
    }
    return null;
  }

  private static Table findTableOnUi() {
    for (Shell shell : display.getShells()) {
      Table table = findTable(shell);
      if (table != null) {
        return table;
      }
    }
    return null;
  }

  private static Table findTable(Composite composite) {
    for (Control child : composite.getChildren()) {
      if (child instanceof Table foundTable) {
        return foundTable;
      }
      if (child instanceof Composite childComposite) {
        Table found = findTable(childComposite);
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
