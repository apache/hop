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
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Locale;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.hop.ui.testing.SwtBotTestBase;
import org.eclipse.swt.SWT;
import org.eclipse.swt.graphics.Point;
import org.eclipse.swt.layout.FillLayout;
import org.eclipse.swt.widgets.Combo;
import org.eclipse.swt.widgets.Control;
import org.eclipse.swt.widgets.Event;
import org.eclipse.swt.widgets.Shell;
import org.eclipse.swtbot.swt.finder.SWTBot;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

/**
 * Drives the filter with SWT events rather than OS keystrokes: SWTBot's keyboard cannot post key
 * events under -XstartOnFirstThread on macOS. The native drop-down list is never opened, because on
 * macOS it runs a modal tracking loop that blocks until a real user clicks.
 */
@Tag("uitest")
class ComboFilterPopupTest extends SwtBotTestBase {

  private final AtomicReference<Combo> comboRef = new AtomicReference<>();

  private void buildScene(Shell shell) {
    shell.setLayout(new FillLayout());
    Combo combo = new Combo(shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    combo.setItems(
        Arrays.stream(Locale.getAvailableLocales())
            .filter(l -> !l.getCountry().isEmpty())
            .map(l -> l.getDisplayName(Locale.US))
            .distinct()
            .sorted()
            .toArray(String[]::new));
    ComboFilterPopup.attach(combo, () -> Arrays.asList(combo.getItems()), null);
    comboRef.set(combo);
  }

  /** One keystroke at a time, leaving the caret after the text as real typing does. */
  private void type(SWTBot bot, String text) {
    for (int i = 1; i <= text.length(); i++) {
      String prefix = text.substring(0, i);
      display.syncExec(
          () -> {
            Combo combo = comboRef.get();
            combo.setText(prefix);
            combo.setSelection(new Point(prefix.length(), prefix.length()));
            combo.notifyListeners(SWT.Modify, new Event());
          });
      bot.sleep(50);
    }
  }

  /** The visible shells other than the scene's own, each with the items of its list. */
  private List<List<String>> popupLists() {
    List<List<String>> lists = new ArrayList<>();
    display.syncExec(
        () -> {
          Shell scene = comboRef.get().getShell();
          for (Shell shell : display.getShells()) {
            if (shell == scene || !shell.isVisible()) {
              continue;
            }
            for (Control child : shell.getChildren()) {
              if (child instanceof org.eclipse.swt.widgets.List list) {
                lists.add(Arrays.asList(list.getItems()));
              }
            }
          }
        });
    return lists;
  }

  @Test
  void typingOpensOneFilteredList() {
    withScene(
        this::buildScene,
        bot -> {
          display.syncExec(() -> comboRef.get().setFocus());
          type(bot, "dutch");

          List<List<String>> popups = popupLists();
          assertEquals(1, popups.size(), "Expected exactly one filter popup");
          List<String> items = popups.get(0);
          assertFalse(items.isEmpty());
          assertTrue(
              items.stream().allMatch(item -> item.startsWith("Dutch")),
              "Expected only Dutch locales, got " + items);
          assertTrue(items.contains("Dutch (Belgium)"));
        });
  }

  @Test
  void enterAppliesTheHighlightedMatchAndClosesThePopup() {
    withScene(
        this::buildScene,
        bot -> {
          display.syncExec(() -> comboRef.get().setFocus());
          type(bot, "dutch (belg");
          display.syncExec(
              () -> {
                Event enter = new Event();
                enter.keyCode = SWT.CR;
                enter.character = SWT.CR;
                comboRef.get().notifyListeners(SWT.KeyDown, enter);
              });
          bot.sleep(50);

          assertTrue(popupLists().isEmpty(), "Expected the popup to close");
          String[] text = new String[1];
          display.syncExec(() -> text[0] = comboRef.get().getText());
          assertEquals("Dutch (Belgium)", text[0]);
        });
  }
}
