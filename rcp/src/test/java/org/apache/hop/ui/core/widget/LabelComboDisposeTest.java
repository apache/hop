/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hop.ui.core.widget;

import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import org.apache.hop.core.variables.Variables;
import org.apache.hop.ui.testing.SwtBotTestBase;
import org.eclipse.swt.custom.CCombo;
import org.eclipse.swt.layout.FillLayout;
import org.eclipse.swt.widgets.Shell;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

/**
 * Closing a dialog while the cursor is in a {@link LabelCombo} or {@link LabelComboVar} must not
 * fail. On GTK, disposing the focused combo makes SWT move the focus to its parent, the label
 * composite, whose {@code setFocus()} used to call into the combo that was already torn down.
 */
@Tag("uitest")
class LabelComboDisposeTest extends SwtBotTestBase {

  @Test
  void disposingFocusedLabelComboDoesNotFail() {
    AtomicReference<CCombo> combo = new AtomicReference<>();
    disposeWithFocusIn(
        shell -> combo.set(new LabelCombo(shell, "Field", "tooltip").getComboWidget()), combo);
  }

  @Test
  void disposingFocusedLabelComboVarDoesNotFail() {
    AtomicReference<CCombo> combo = new AtomicReference<>();
    disposeWithFocusIn(
        shell ->
            combo.set(
                new LabelComboVar(new Variables(), shell, "Field", "tooltip").getComboWidget()),
        combo);
  }

  private void disposeWithFocusIn(Consumer<Shell> build, AtomicReference<CCombo> combo) {
    AtomicBoolean focused = new AtomicBoolean();
    // withScene disposes the shell afterwards; before the fix that threw a NullPointerException.
    withScene(
        shell -> {
          shell.setLayout(new FillLayout());
          build.accept(shell);
        },
        bot -> {
          // A freshly opened shell is not always active right away: retry until the focus lands.
          long deadline = System.currentTimeMillis() + 2000;
          while (!focused.get() && System.currentTimeMillis() < deadline) {
            display.syncExec(
                () -> {
                  combo.get().getShell().forceActive();
                  combo.get().forceFocus();
                  focused.set(combo.get().isFocusControl());
                });
            if (!focused.get()) {
              try {
                Thread.sleep(100);
              } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return;
              }
            }
          }
        });
    assertTrue(focused.get(), "the combo must have had the focus for this test to mean anything");
  }
}
