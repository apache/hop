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

import org.junit.jupiter.api.Test;

class ComboItemsTest {

  @Test
  void replacingItemsWritesThePreviousTextBack() {
    FakeCombo combo = new FakeCombo();
    combo.text = "id_json";
    combo.items = new String[] {"old"};

    ComboItems.apply(combo::getText, combo::setItems, combo::setText, new String[] {"other"});

    assertArrayEquals(new String[] {"other"}, combo.items);
    assertEquals("id_json", combo.text);
  }

  @Test
  void nullItemsLeaveTheListAndStillRestoreTheText() {
    FakeCombo combo = new FakeCombo();
    combo.text = "id_json";
    combo.items = new String[] {"kept"};

    ComboItems.apply(combo::getText, combo::setItems, combo::setText, null);

    assertArrayEquals(new String[] {"kept"}, combo.items);
    assertEquals("id_json", combo.text);
  }

  @Test
  void nullTextIsNotWrittenBack() {
    FakeCombo combo = new FakeCombo();
    combo.text = null;
    combo.items = new String[] {"old"};

    ComboItems.apply(combo::getText, combo::setItems, combo::setText, new String[] {"other"});

    assertArrayEquals(new String[] {"other"}, combo.items);
    // setItems cleared the text, and a null previous value is not written back.
    assertEquals("", combo.text);
  }

  private static final class FakeCombo {
    private String text;
    private String[] items;

    private String getText() {
      return text;
    }

    private void setItems(String[] items) {
      this.items = items;
      // SWT clears the text when the items change.
      this.text = "";
    }

    private void setText(String text) {
      this.text = text;
    }
  }
}
