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

import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;
import org.apache.hop.ui.testing.SwtBotTestBase;
import org.eclipse.swt.SWT;
import org.eclipse.swt.layout.RowLayout;
import org.eclipse.swt.widgets.Button;
import org.eclipse.swt.widgets.Table;
import org.eclipse.swt.widgets.TableItem;
import org.eclipse.swtbot.swt.finder.SWTBot;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

/**
 * Space only launches when Launch has keyboard focus. SWT otherwise focuses the first child
 * (Cancel) or a table that queued {@code setFocus} from {@code clearAll()}.
 */
@Tag("uitest")
class ConfigurationDialogLaunchFocusTest extends SwtBotTestBase {

  @Test
  void launchKeepsKeyboardFocusDespiteALaterTableFocus() {
    AtomicReference<Button> launchRef = new AtomicReference<>();

    withScene(
        shell -> {
          shell.setLayout(new RowLayout());
          Button cancel = new Button(shell, SWT.PUSH);
          cancel.setText("Cancel");
          Button launch = new Button(shell, SWT.PUSH);
          launch.setText("Launch");
          Table table = new Table(shell, SWT.BORDER | SWT.FULL_SELECTION);
          new TableItem(table, SWT.NONE);
          shell.setDefaultButton(launch);
          // Same timing as TableView.clearAll(): an asyncExec queued before the shell is open.
          shell.getDisplay().asyncExec(table::setFocus);
          ConfigurationDialog.focusLaunchButtonWhenActivated(shell, launch);
          launchRef.set(launch);
        },
        bot ->
            assertTrue(
                waitUntil(bot, () -> onUi(() -> launchRef.get().isFocusControl())),
                "Launch must keep keyboard focus so Space activates it"));
  }

  private static boolean waitUntil(SWTBot bot, Supplier<Boolean> condition) {
    for (int attempt = 0; attempt < 40; attempt++) {
      if (Boolean.TRUE.equals(condition.get())) {
        return true;
      }
      bot.sleep(25);
    }
    return Boolean.TRUE.equals(condition.get());
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
