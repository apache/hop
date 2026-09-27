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

import java.util.Arrays;
import org.eclipse.swt.SWT;
import org.eclipse.swt.custom.CCombo;
import org.eclipse.swt.widgets.Combo;
import org.eclipse.swt.widgets.Control;

/**
 * Replaces the drop-down items of a combo without touching the value the user configured.
 *
 * <p>SWT is surprisingly destructive here: {@link Combo#removeAll()} and {@link
 * Combo#setItems(String...)} clear the text on every platform, {@link CCombo#removeAll()} always
 * clears it, and {@link CCombo#setItems(String[])} clears it when the combo is not editable. A
 * dialog that refreshes its field list with those calls wipes the stored value, and pressing OK
 * then saves an empty field. Use {@link #setItemsKeepingText(Control, String[])} instead.
 */
public final class ComboItems {

  private ComboItems() {
    // static helper
  }

  /**
   * Sets the items of a combo and keeps its current text.
   *
   * <p>Works for {@link Combo}, {@link CCombo}, {@link ComboVar}, {@link LabelCombo} and {@link
   * LabelComboVar}. A read-only native {@link Combo} can only show a value that is one of its
   * items, so there a current value that is missing from {@code items} is added to the list. Other
   * controls are left alone.
   *
   * @param control the combo to fill
   * @param items the new items, {@code null} means no items
   */
  public static void setItemsKeepingText(Control control, String[] items) {
    String[] safeItems = items == null ? new String[0] : items;

    if (control instanceof ComboVar comboVar) {
      setItemsKeepingText(comboVar.getCComboWidget(), safeItems);
    } else if (control instanceof LabelCombo labelCombo) {
      setItemsKeepingText(labelCombo.getComboWidget(), safeItems);
    } else if (control instanceof LabelComboVar labelComboVar) {
      setItemsKeepingText(labelComboVar.getComboWidget(), safeItems);
    } else if (control instanceof CCombo cCombo) {
      String text = cCombo.getText();
      cCombo.setItems(safeItems);
      if (!text.equals(cCombo.getText())) {
        cCombo.setText(text);
      }
    } else if (control instanceof Combo combo) {
      String text = combo.getText();
      boolean readOnly = (combo.getStyle() & SWT.READ_ONLY) != 0;
      if (readOnly && !text.isEmpty() && !Arrays.asList(safeItems).contains(text)) {
        String[] withText = Arrays.copyOf(safeItems, safeItems.length + 1);
        withText[safeItems.length] = text;
        safeItems = withText;
      }
      combo.setItems(safeItems);
      if (!text.equals(combo.getText())) {
        combo.setText(text);
      }
    }
  }
}
