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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Test;

class FindReplaceOperationsTest {

  @Test
  void findNextSelectsMatchAndWraps() {
    FakeTarget target = new FakeTarget("Select From from dual");

    assertTrue(FindReplaceOperations.find(target, "from", false, true));
    assertEquals("From", target.getSelectionText());

    assertTrue(FindReplaceOperations.find(target, "from", false, true));
    assertEquals("from", target.getSelectionText());

    assertTrue(FindReplaceOperations.find(target, "from", false, true));
    assertEquals("From", target.getSelectionText());
  }

  @Test
  void findPreviousSelectsEarlierMatch() {
    FakeTarget target = new FakeTarget("aaa bbb aaa");
    target.setCaretPosition(target.getText().length());

    assertTrue(FindReplaceOperations.find(target, "aaa", true, false));
    assertEquals(8, target.selStart);
    assertEquals("aaa", target.getSelectionText());
  }

  @Test
  void findReturnsFalseWhenMissing() {
    FakeTarget target = new FakeTarget("hello");
    assertFalse(FindReplaceOperations.find(target, "missing", false, true));
    assertFalse(FindReplaceOperations.find(target, "", false, true));
  }

  @Test
  void replaceOneReplacesSelectionThenFindsNext() {
    FakeTarget target = new FakeTarget("foo bar foo");
    assertTrue(FindReplaceOperations.find(target, "foo", true, true));
    assertTrue(FindReplaceOperations.replaceOne(target, "foo", "baz", true));
    assertEquals("baz bar foo", target.getText());
    assertEquals("foo", target.getSelectionText());
  }

  @Test
  void replaceAllReplacesEveryMatch() {
    FakeTarget target = new FakeTarget("Hello hello HELLO");
    assertEquals(3, FindReplaceOperations.replaceAll(target, "hello", "Hi", false));
    assertEquals("Hi Hi Hi", target.getText());
  }

  @Test
  void replaceDoesNothingWhenReadOnly() {
    FakeTarget target = new FakeTarget("foo foo");
    target.editable = false;
    assertFalse(FindReplaceOperations.replaceOne(target, "foo", "bar", true));
    assertEquals(0, FindReplaceOperations.replaceAll(target, "foo", "bar", true));
    assertEquals("foo foo", target.getText());
  }

  private static final class FakeTarget implements IFindReplaceTarget {
    private String text;
    private int selStart;
    private int selEnd;
    private boolean editable = true;
    private boolean disposed;

    private FakeTarget(String text) {
      this.text = text;
    }

    @Override
    public String getText() {
      return text;
    }

    @Override
    public void setText(String text) {
      this.text = text != null ? text : "";
    }

    @Override
    public String getSelectionText() {
      if (selEnd <= selStart || selStart >= text.length()) {
        return "";
      }
      return text.substring(selStart, Math.min(selEnd, text.length()));
    }

    @Override
    public int getSelectionCount() {
      return Math.max(0, selEnd - selStart);
    }

    @Override
    public void setSelection(int start, int end) {
      selStart = Math.max(0, start);
      selEnd = Math.max(selStart, end);
    }

    @Override
    public int getCaretPosition() {
      return selEnd;
    }

    @Override
    public void setCaretPosition(int position) {
      selStart = position;
      selEnd = position;
    }

    @Override
    public void insert(String replacement) {
      String safe = replacement != null ? replacement : "";
      text = text.substring(0, selStart) + safe + text.substring(selEnd);
      selStart = selStart + safe.length();
      selEnd = selStart;
    }

    @Override
    public boolean isEditable() {
      return editable;
    }

    @Override
    public boolean isDisposed() {
      return disposed;
    }

    @Override
    public boolean setFocus() {
      return !disposed;
    }
  }
}
