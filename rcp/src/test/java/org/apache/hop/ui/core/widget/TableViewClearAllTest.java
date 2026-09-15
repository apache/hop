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

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;

import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;
import org.apache.hop.core.variables.Variables;
import org.apache.hop.ui.core.PropsUi;
import org.apache.hop.ui.testing.SwtBotTestBase;
import org.eclipse.swt.SWT;
import org.eclipse.swt.layout.FillLayout;
import org.eclipse.swt.widgets.Composite;
import org.eclipse.swt.widgets.Control;
import org.eclipse.swt.widgets.Shell;
import org.eclipse.swt.widgets.Text;
import org.eclipse.swtbot.swt.finder.SWTBot;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

/**
 * {@link TableView#clearAll(boolean)} used to always start editing the first cell. That stole focus
 * from the run-dialog Launch button when Parameters were filled before the shell was open.
 */
@Tag("uitest")
class TableViewClearAllTest extends SwtBotTestBase {

  @Test
  void clearAllBeforeTheShellIsOpenDoesNotStartAnEditor() {
    AtomicReference<TableView> viewRef = new AtomicReference<>();

    withScene(
        parent -> {
          Shell dialog = new Shell(parent, SWT.DIALOG_TRIM);
          dialog.setLayout(new FillLayout());
          TableView view = newTableView(dialog);
          view.clearAll(false);
          viewRef.set(view);
        },
        bot -> {
          drainUi();
          bot.sleep(50);
          assertNull(
              onUi(() -> findText(viewRef.get().table)),
              "clearAll on a hidden shell must not open a cell editor");
        });
  }

  @Test
  void clearAllOnAVisibleGridStillStartsEditing() {
    AtomicReference<TableView> viewRef = new AtomicReference<>();

    withScene(
        shell -> {
          shell.setLayout(new FillLayout());
          viewRef.set(newTableView(shell));
        },
        bot -> {
          onUi(
              () -> {
                viewRef.get().clearAll(false);
                return null;
              });
          assertNotNull(
              waitForEditor(bot, viewRef.get()),
              "clearAll on a visible grid should still start editing");
        });
  }

  private static TableView newTableView(Composite parent) {
    ColumnInfo[] columns = {
      new ColumnInfo("Name", ColumnInfo.COLUMN_TYPE_TEXT, false, false),
      new ColumnInfo("Value", ColumnInfo.COLUMN_TYPE_TEXT, false, false),
    };
    return new TableView(
        new Variables(),
        parent,
        SWT.BORDER | SWT.FULL_SELECTION,
        columns,
        1,
        null,
        PropsUi.getInstance());
  }

  private static Text waitForEditor(SWTBot bot, TableView view) {
    for (int attempt = 0; attempt < 40; attempt++) {
      Text found = onUi(() -> findText(view.table));
      if (found != null) {
        return found;
      }
      bot.sleep(25);
    }
    return onUi(() -> findText(view.table));
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

  private static void drainUi() {
    display.syncExec(
        () -> {
          while (display.readAndDispatch()) {
            // flush clearAll asyncExec
          }
        });
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
