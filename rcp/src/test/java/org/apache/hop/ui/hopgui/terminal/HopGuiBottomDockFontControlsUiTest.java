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

package org.apache.hop.ui.hopgui.terminal;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.mock;

import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;
import org.apache.hop.ui.hopgui.HopGui;
import org.apache.hop.ui.hopgui.HopGuiEnvironment;
import org.apache.hop.ui.testing.SwtBotTestBase;
import org.eclipse.swt.SWT;
import org.eclipse.swt.custom.CTabItem;
import org.eclipse.swt.layout.FillLayout;
import org.eclipse.swt.widgets.Display;
import org.eclipse.swt.widgets.Event;
import org.eclipse.swt.widgets.Label;
import org.eclipse.swt.widgets.ToolBar;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

/** The font-size buttons of the bottom dock resize terminal text, so they go with terminal tabs. */
@Tag("uitest")
class HopGuiBottomDockFontControlsUiTest extends SwtBotTestBase {

  /** Maximize and close. */
  private static final int PANEL_BUTTONS = 2;

  /** Larger, smaller, 100% and their separator. */
  private static final int FONT_BUTTONS = 4;

  @BeforeAll
  static void init() throws Exception {
    HopGuiEnvironment.init();
  }

  @Test
  void theFontButtonsAreShownOnTerminalTabsOnly() {
    AtomicReference<HopGuiBottomDock> dock = new AtomicReference<>();
    withScene(
        shell -> {
          shell.setLayout(new FillLayout());
          shell.setSize(900, 500);
          dock.set(new HopGuiBottomDock(shell, HopGui.getInstance(), true));
        },
        bot -> {
          assertEquals(PANEL_BUTTONS, onUi(() -> buttons(dock.get())), "no tab yet");

          onUi(
              () ->
                  dock.get()
                      .focusOrOpenToolTab(
                          "search", "Search", null, true, c -> new Label(c, SWT.NONE)));
          assertEquals(PANEL_BUTTONS, onUi(() -> buttons(dock.get())), "a tool tab");

          CTabItem terminal = onUi(() -> fakeTerminalTab(dock.get()));
          onUi(
              () -> {
                dock.get().selectTab(terminal);
                return null;
              });
          assertEquals(PANEL_BUTTONS + FONT_BUTTONS, onUi(() -> buttons(dock.get())), "terminal");

          // Clicking the tool tab: a Selection event.
          onUi(
              () -> {
                CTabItem search = dock.get().findToolTab("search");
                dock.get().getTabFolder().setSelection(search);
                Event event = new Event();
                event.item = search;
                dock.get().getTabFolder().notifyListeners(SWT.Selection, event);
                return null;
              });
          assertEquals(PANEL_BUTTONS, onUi(() -> buttons(dock.get())), "back on the tool tab");

          onUi(
              () -> {
                dock.get().selectTab(terminal);
                dock.get().closeTab(null, terminal);
                return null;
              });
          assertEquals(PANEL_BUTTONS, onUi(() -> buttons(dock.get())), "terminal closed");
        });
  }

  /** A tab that looks like a terminal to the dock, without starting a shell. */
  private static CTabItem fakeTerminalTab(HopGuiBottomDock dock) {
    CTabItem item = new CTabItem(dock.getTabFolder(), SWT.CLOSE);
    item.setText("Terminal");
    item.setData("terminalWidget", mock(ITerminalWidget.class));
    return item;
  }

  private static int buttons(HopGuiBottomDock dock) {
    return ((ToolBar) dock.getTabFolder().getTopRight()).getItemCount();
  }

  private static <T> T onUi(Supplier<T> supplier) {
    AtomicReference<T> result = new AtomicReference<>();
    Display.getDefault().syncExec(() -> result.set(supplier.get()));
    return result.get();
  }
}
