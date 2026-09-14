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

import java.util.function.Supplier;
import org.eclipse.swt.SWT;
import org.eclipse.swt.events.KeyAdapter;
import org.eclipse.swt.events.KeyEvent;
import org.eclipse.swt.widgets.Control;

/**
 * Dispatches widget-local key events to {@link TextWidgetShortcutRegistry}. No-op when no plugin is
 * registered.
 */
public class TextWidgetShortcutKeyAdapter extends KeyAdapter {

  private final Supplier<TextWidgetShortcutContext> contextSupplier;

  public TextWidgetShortcutKeyAdapter(Supplier<TextWidgetShortcutContext> contextSupplier) {
    this.contextSupplier = contextSupplier;
  }

  @Override
  public void keyPressed(KeyEvent event) {
    dispatch(event, contextSupplier.get());
  }

  /**
   * Try registered shortcuts against {@code event}. Returns true when one handled it.
   *
   * @param event key event
   * @param context widget context (ignored when null)
   * @return true if a shortcut consumed the event
   */
  public static boolean dispatch(KeyEvent event, TextWidgetShortcutContext context) {
    if (event == null || context == null) {
      return false;
    }
    for (ITextWidgetShortcut shortcut : TextWidgetShortcutRegistry.getInstance().getShortcuts()) {
      if (shortcut.isHotKey(event, context.isVariablesEnabled())) {
        event.doit = false;
        shortcut.apply(context);
        return true;
      }
    }
    return false;
  }

  /**
   * Make {@code indicator} clickable: hand cursor and dispatch {@link
   * ITextWidgetShortcut#onIndicatorClick(TextWidgetShortcutContext)}.
   *
   * @param indicator the N label
   * @param contextSupplier context for the owning widget
   */
  public static void attachIndicatorClick(
      Control indicator, Supplier<TextWidgetShortcutContext> contextSupplier) {
    if (indicator == null || indicator.isDisposed()) {
      return;
    }
    indicator.setCursor(indicator.getDisplay().getSystemCursor(SWT.CURSOR_HAND));
    indicator.addListener(
        SWT.MouseDown,
        e -> {
          TextWidgetShortcutContext context = contextSupplier.get();
          if (context == null) {
            return;
          }
          for (ITextWidgetShortcut shortcut :
              TextWidgetShortcutRegistry.getInstance().getShortcuts()) {
            shortcut.onIndicatorClick(context);
          }
        });
  }
}
