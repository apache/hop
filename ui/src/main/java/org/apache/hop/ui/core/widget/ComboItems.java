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

/**
 * Replaces the items of a combo and writes the previous text back.
 *
 * <p>{@code CCombo.setItems} clears the text only when the combo is not editable. {@code
 * removeAll()} always clears it, including on an editable combo. Call this after a field lookup
 * instead of calling {@code removeAll()} first, so a failed lookup can leave the configured name in
 * place.
 *
 * <p>On an editable combo the previous text is kept, including a name that is not in the new list.
 * On a read-only combo {@code setText} only selects a name that is in the new list.
 */
public final class ComboItems {

  private ComboItems() {}

  public static void setItemsKeepingText(CCombo combo, String[] items) {
    apply(combo::getText, combo::setItems, combo::setText, items);
  }

  /**
   * @param items the replacement list. {@code CCombo.setItems} rejects {@code null}.
   */
  static void apply(
      Supplier<String> currentText,
      Consumer<String[]> replaceItems,
      Consumer<String> restoreText,
      String[] items) {
    String text = currentText.get();
    replaceItems.accept(items);
    // setText(null) is an SWT error. A null previous value is left as setItems left it.
    if (text != null) {
      restoreText.accept(text);
    }
  }
}
