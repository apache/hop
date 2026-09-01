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

package org.apache.hop.ui.hopgui.perspective.database;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.apache.hop.ui.core.widget.editor.IContentEditorWidget;
import org.eclipse.swt.SWT;
import org.junit.jupiter.api.Test;

class SqlExecuteRangeTest {

  @Test
  void selectionWinsOverCaret() {
    String text = "SELECT 1;\n\nSELECT 2;";
    assertEquals("SELECT 2;", SqlExecuteRange.scriptToExecute(text, "SELECT 2;", 0));
  }

  @Test
  void blankSelectionFallsBackToBlock() {
    String text = "SELECT 1;\n\nSELECT 2;";
    assertEquals("SELECT 1;", SqlExecuteRange.scriptToExecute(text, "  ", 0));
  }

  @Test
  void singleStatementWithoutSemicolon() {
    String text = "SELECT *\nFROM foo\nWHERE x = 1";
    assertEquals(text, SqlExecuteRange.scriptToExecute(text, "", 10));
  }

  @Test
  void caretInFirstBlock() {
    String text = "SELECT 1;\n\nSELECT 2;";
    assertEquals("SELECT 1;", SqlExecuteRange.scriptToExecute(text, "", 0));
    assertEquals("SELECT 1;", SqlExecuteRange.scriptToExecute(text, "", 8));
  }

  @Test
  void caretInSecondBlock() {
    String text = "SELECT 1;\n\nSELECT 2;";
    int caret = text.indexOf("SELECT 2");
    assertEquals("SELECT 2;", SqlExecuteRange.scriptToExecute(text, "", caret));
  }

  @Test
  void caretOnBlankLineUsesBlockAbove() {
    String text = "SELECT 1;\n\nSELECT 2;";
    int blank = text.indexOf('\n') + 1;
    assertEquals("SELECT 1;", SqlExecuteRange.scriptToExecute(text, "", blank));
  }

  @Test
  void caretOnLeadingBlankLinesUsesBlockBelow() {
    String text = "\n\nSELECT 1;";
    assertEquals("SELECT 1;", SqlExecuteRange.scriptToExecute(text, "", 0));
  }

  @Test
  void lastStatementWithoutSemicolon() {
    String text = "SELECT 1;\n\nSELECT 2";
    int caret = text.indexOf("SELECT 2");
    assertEquals("SELECT 2", SqlExecuteRange.scriptToExecute(text, "", caret));
  }

  @Test
  void windowsNewlines() {
    String text = "SELECT 1;\r\n\r\nSELECT 2;";
    int caret = text.indexOf("SELECT 2");
    assertEquals("SELECT 2;", SqlExecuteRange.scriptToExecute(text, "", caret));
  }

  @Test
  void emptyText() {
    assertEquals("", SqlExecuteRange.scriptToExecute("", "", 0));
    assertEquals("", SqlExecuteRange.scriptToExecute(null, null, 0));
  }

  @Test
  void whitespaceOnlyBlock() {
    assertEquals("", SqlExecuteRange.scriptToExecute("   \n\n  ", "", 0));
  }

  @Test
  void selectedMultipleStatementsKeptIntact() {
    String selected = "SELECT 1;\nSELECT 2;";
    assertEquals(selected, SqlExecuteRange.scriptToExecute("ignored", selected, 0));
  }

  @Test
  void executeKeyIsCtrlEnter() {
    assertTrue(IContentEditorWidget.isExecuteKey(SWT.MOD1, SWT.CR, SWT.CR));
    assertTrue(IContentEditorWidget.isExecuteKey(SWT.CONTROL, SWT.CR, (char) 0));
    assertTrue(IContentEditorWidget.isExecuteKey(SWT.MOD1, SWT.KEYPAD_CR, (char) 0));
    assertTrue(IContentEditorWidget.isExecuteTraverse(SWT.TRAVERSE_RETURN, SWT.CONTROL));
    assertFalse(IContentEditorWidget.isExecuteKey(SWT.NONE, SWT.CR, SWT.CR));
    assertFalse(IContentEditorWidget.isExecuteKey(SWT.MOD1 | SWT.SHIFT, SWT.CR, SWT.CR));
    assertFalse(IContentEditorWidget.isExecuteTraverse(SWT.TRAVERSE_RETURN, SWT.NONE));
    assertTrue(IContentEditorWidget.isExecuteNewline(SWT.CR, (char) 0));
    assertTrue(IContentEditorWidget.isLineDelimiterText("\n"));
    assertTrue(IContentEditorWidget.isLineDelimiterText("\r"));
    assertTrue(IContentEditorWidget.isLineDelimiterText("\r\n"));
    assertFalse(IContentEditorWidget.isLineDelimiterText("SELECT 1"));
    assertFalse(IContentEditorWidget.isLineDelimiterText(""));
  }

  @Test
  void formatElapsed() {
    assertEquals("0 ms", DatabaseOperationsPanel.formatElapsed(0));
    assertEquals("12 ms", DatabaseOperationsPanel.formatElapsed(12));
    assertEquals("1.5 s", DatabaseOperationsPanel.formatElapsed(1500));
    assertEquals("1:05", DatabaseOperationsPanel.formatElapsed(65_000));
  }
}
