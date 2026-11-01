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

import java.util.function.Consumer;
import java.util.function.Supplier;
import org.eclipse.swt.custom.CCombo;
import org.eclipse.swt.widgets.Combo;

/**
 * Replaces the items of a combo and writes the previous text back. {@code setItems} clears the
 * text, including a value that is not in the new list.
 */
public final class ComboItems {

  private ComboItems() {}

  public static void setItemsKeepingText(CCombo combo, String[] items) {
    apply(combo::getText, combo::setItems, combo::setText, items);
  }

  public static void setItemsKeepingText(Combo combo, String[] items) {
    apply(combo::getText, combo::setItems, combo::setText, items);
  }

  public static void setItemsKeepingText(ComboVar combo, String[] items) {
    apply(combo::getText, combo::setItems, combo::setText, items);
  }

  public static void setItemsKeepingText(LabelCombo combo, String[] items) {
    apply(combo::getText, combo::setItems, combo::setText, items);
  }

  /**
   * @param items the replacement list. {@code null} leaves the current items in place and still
   *     writes the previous text back.
   */
  static void apply(
      Supplier<String> currentText,
      Consumer<String[]> replaceItems,
      Consumer<String> restoreText,
      String[] items) {
    String text = currentText.get();
    if (items != null) {
      replaceItems.accept(items);
    }
    if (text != null) {
      restoreText.accept(text);
    }
  }
}
