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

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import java.util.function.Function;
import org.apache.hop.core.variables.Variables;
import org.apache.hop.ui.testing.SwtBotTestBase;
import org.eclipse.swt.SWT;
import org.eclipse.swt.custom.CCombo;
import org.eclipse.swt.layout.FillLayout;
import org.eclipse.swt.widgets.Combo;
import org.eclipse.swt.widgets.Control;
import org.eclipse.swt.widgets.Shell;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

/**
 * {@link ComboItems#setItemsKeepingText(Control, String[])} must keep the configured value for
 * every kind of combo Hop dialogs use, including the ones where plain SWT clears it.
 */
@Tag("uitest")
class ComboItemsTest extends SwtBotTestBase {

  private static final String[] FIELDS = {"id", "name"};

  @Test
  void editableComboKeepsValueNotInList() {
    assertKeepsValue(shell -> new Combo(shell, SWT.BORDER), "renamed_field", FIELDS);
  }

  @Test
  void readOnlyComboKeepsValueByAddingItToTheList() {
    AtomicReference<String[]> items = new AtomicReference<>();
    assertKeepsValue(
        shell -> new Combo(shell, SWT.BORDER | SWT.READ_ONLY),
        "renamed_field",
        FIELDS,
        control -> items.set(((Combo) control).getItems()));
    assertArrayEquals(new String[] {"id", "name", "renamed_field"}, items.get());
  }

  @Test
  void readOnlyComboSelectsValueInList() {
    AtomicReference<String[]> items = new AtomicReference<>();
    assertKeepsValue(
        shell -> new Combo(shell, SWT.BORDER | SWT.READ_ONLY),
        "name",
        FIELDS,
        control -> items.set(((Combo) control).getItems()));
    assertArrayEquals(FIELDS, items.get());
  }

  @Test
  void editableCComboKeepsValue() {
    assertKeepsValue(shell -> new CCombo(shell, SWT.BORDER), "renamed_field", FIELDS);
  }

  @Test
  void nonEditableCComboKeepsValue() {
    assertKeepsValue(
        shell -> new CCombo(shell, SWT.BORDER | SWT.READ_ONLY), "renamed_field", FIELDS);
  }

  @Test
  void comboVarKeepsVariableExpression() {
    assertKeepsValue(
        shell -> new ComboVar(new Variables(), shell, SWT.BORDER | SWT.READ_ONLY),
        "${FIELD_NAME}",
        FIELDS);
  }

  @Test
  void labelComboKeepsValue() {
    assertKeepsValue(shell -> new LabelCombo(shell, "Field", "tooltip"), "renamed_field", FIELDS);
  }

  @Test
  void nullItemsMeanNoItemsAndKeepTheValue() {
    assertKeepsValue(shell -> new CCombo(shell, SWT.BORDER | SWT.READ_ONLY), "id", null);
  }

  private void assertKeepsValue(Function<Shell, Control> factory, String value, String[] newItems) {
    assertKeepsValue(factory, value, newItems, control -> {});
  }

  private void assertKeepsValue(
      Function<Shell, Control> factory,
      String value,
      String[] newItems,
      Consumer<Control> inspect) {
    AtomicReference<Control> control = new AtomicReference<>();
    AtomicReference<String> textAfter = new AtomicReference<>();
    withScene(
        shell -> {
          shell.setLayout(new FillLayout());
          Control combo = factory.apply(shell);
          ComboItems.setItemsKeepingText(combo, new String[] {"old", value});
          setText(combo, value);
          control.set(combo);
        },
        bot ->
            display.syncExec(
                () -> {
                  ComboItems.setItemsKeepingText(control.get(), newItems);
                  textAfter.set(getText(control.get()));
                  inspect.accept(control.get());
                }));
    assertEquals(value, textAfter.get(), "the configured value must survive new items");
  }

  private static void setText(Control control, String text) {
    if (control instanceof Combo combo) {
      combo.setText(text);
    } else if (control instanceof CCombo cCombo) {
      cCombo.setText(text);
    } else if (control instanceof ComboVar comboVar) {
      comboVar.setText(text);
    } else if (control instanceof LabelCombo labelCombo) {
      labelCombo.setText(text);
    }
  }

  private static String getText(Control control) {
    if (control instanceof Combo combo) {
      return combo.getText();
    } else if (control instanceof CCombo cCombo) {
      return cCombo.getText();
    } else if (control instanceof ComboVar comboVar) {
      return comboVar.getText();
    } else if (control instanceof LabelCombo labelCombo) {
      return labelCombo.getText();
    }
    throw new IllegalArgumentException(control.getClass().getName());
  }
}
