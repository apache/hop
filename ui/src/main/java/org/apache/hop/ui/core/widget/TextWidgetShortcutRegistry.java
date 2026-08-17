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

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

/** Process-wide list of {@link ITextWidgetShortcut} implementations (usually one plugin). */
public final class TextWidgetShortcutRegistry {

  private static final TextWidgetShortcutRegistry INSTANCE = new TextWidgetShortcutRegistry();

  private final List<ITextWidgetShortcut> shortcuts = new CopyOnWriteArrayList<>();

  private TextWidgetShortcutRegistry() {
    // singleton
  }

  public static TextWidgetShortcutRegistry getInstance() {
    return INSTANCE;
  }

  public void register(ITextWidgetShortcut shortcut) {
    if (shortcut != null && !shortcuts.contains(shortcut)) {
      shortcuts.add(shortcut);
    }
  }

  public void unregister(ITextWidgetShortcut shortcut) {
    shortcuts.remove(shortcut);
  }

  public List<ITextWidgetShortcut> getShortcuts() {
    return new ArrayList<>(shortcuts);
  }

  /** Test hook. */
  void clear() {
    shortcuts.clear();
  }
}
