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

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;

import org.junit.jupiter.api.Test;

class ComboItemsTest {

  @Test
  void editableSetItemsDoesNotClearTheText() {
    FakeCombo combo = new FakeCombo(true);
    combo.text = "id_json";

    combo.setItems(new String[] {"other"});

    assertEquals("id_json", combo.text);
  }

  @Test
  void readOnlySetItemsClearsTheText() {
    FakeCombo combo = new FakeCombo(false);
    combo.text = "id_json";

    combo.setItems(new String[] {"other"});

    assertEquals("", combo.text);
  }

  @Test
  void editableComboKeepsANameThatIsNotInTheNewList() {
    FakeCombo combo = new FakeCombo(true);
    combo.text = "id_json";
    combo.items = new String[] {"old"};

    ComboItems.apply(combo::getText, combo::setItems, combo::setText, new String[] {"other"});

    assertArrayEquals(new String[] {"other"}, combo.items);
    assertEquals("id_json", combo.text);
  }

  @Test
  void readOnlyComboWritesBackANameThatIsInTheNewList() {
    FakeCombo combo = new FakeCombo(false);
    combo.text = "id_json";
    combo.items = new String[] {"old"};

    ComboItems.apply(
        combo::getText, combo::setItems, combo::setText, new String[] {"other", "id_json"});

    assertArrayEquals(new String[] {"other", "id_json"}, combo.items);
    assertEquals("id_json", combo.text);
  }

  @Test
  void readOnlyComboDoesNotRestoreANameMissingFromTheNewList() {
    FakeCombo combo = new FakeCombo(false);
    combo.text = "id_json";
    combo.items = new String[] {"old"};

    ComboItems.apply(combo::getText, combo::setItems, combo::setText, new String[] {"other"});

    assertArrayEquals(new String[] {"other"}, combo.items);
    assertEquals("", combo.text);
  }

  @Test
  void nullTextIsNotWrittenBack() {
    FakeCombo combo = new FakeCombo(false);
    combo.text = null;
    combo.items = new String[] {"old"};
    boolean[] wrote = {false};

    ComboItems.apply(
        combo::getText,
        combo::setItems,
        value -> {
          wrote[0] = true;
          combo.setText(value);
        },
        new String[] {"other"});

    assertFalse(wrote[0]);
    assertArrayEquals(new String[] {"other"}, combo.items);
    assertEquals("", combo.text);
  }

  /**
   * Models {@code CCombo}. {@code setItems} clears the text only when the combo is not editable.
   * {@code setText} on a read-only combo changes the text only when the value is one of the items.
   * {@code removeAll()} is the call that always clears the text. This fake does not implement it.
   */
  private static final class FakeCombo {
    private final boolean editable;
    private String text;
    private String[] items = new String[0];

    private FakeCombo(boolean editable) {
      this.editable = editable;
    }

    private String getText() {
      return text;
    }

    private void setItems(String[] items) {
      if (items == null) {
        throw new IllegalArgumentException("items");
      }
      this.items = items.clone();
      if (!editable) {
        this.text = "";
      }
    }

    private void setText(String text) {
      if (text == null) {
        throw new IllegalArgumentException("text");
      }
      if (!editable && indexOf(text) < 0) {
        return;
      }
      this.text = text;
    }

    private int indexOf(String value) {
      for (int i = 0; i < items.length; i++) {
        if (value.equals(items[i])) {
          return i;
        }
      }
      return -1;
    }
  }
}
