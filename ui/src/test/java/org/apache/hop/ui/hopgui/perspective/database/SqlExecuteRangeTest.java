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

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;
import org.apache.hop.core.Const;
import org.apache.hop.core.database.NoneDatabaseMeta;
import org.apache.hop.core.database.SqlScriptStatement;
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
  void rangeToExecuteKeepsOffsetsInEditor() {
    String text = "prefix\nSELECT 1;\n\nSELECT 2;";
    int caret = text.indexOf("SELECT 2");
    SqlExecuteRange.Range range = SqlExecuteRange.rangeToExecute(text, "", caret);
    assertEquals("SELECT 2;", range.script());
    assertEquals(caret, range.start());
    assertEquals(text.length(), range.end());
  }

  @Test
  void editorOffsetsIncludeTrailingSemicolonAndStopThere() {
    String script = "SELECT * FROM t;\nWHERE x = 1";
    SqlExecuteRange.Range range = new SqlExecuteRange.Range(script, 10, 10 + script.length());
    int semi = script.indexOf(';');
    List<SqlScriptStatement> statements =
        List.of(new SqlScriptStatement("SELECT * FROM t", 0, semi, true));
    int[] offsets = SqlExecuteRange.editorOffsets(range, statements);
    assertArrayEquals(new int[] {10, 10 + semi + 1}, offsets);
    assertEquals("SELECT * FROM t;", script.substring(offsets[0] - 10, offsets[1] - 10));
  }

  @Test
  void runAtCaretDoesNotSelectLeftoverAfterSemicolon() {
    String script = "SELECT * FROM t;\nWHERE x = 1";
    SqlExecuteRange.Range range = new SqlExecuteRange.Range(script, 0, script.length());
    List<SqlScriptStatement> parsed = parse(script);
    assertEquals(2, parsed.size());

    List<SqlScriptStatement> toRun =
        SqlExecuteRange.statementsToExecute(range, parsed, script.indexOf("FROM"));
    assertEquals(1, toRun.size());
    assertEquals("SELECT * FROM t", toRun.get(0).getStatement());

    int[] offsets = SqlExecuteRange.editorOffsets(range, toRun);
    assertEquals("SELECT * FROM t;", script.substring(offsets[0], offsets[1]));
  }

  @Test
  void caretOnLeftoverFallsBackToPreviousStatement() {
    String script = "SELECT * FROM t;\nWHERE x = 1";
    SqlExecuteRange.Range range = new SqlExecuteRange.Range(script, 0, script.length());
    List<SqlScriptStatement> toRun =
        SqlExecuteRange.statementsToExecute(range, parse(script), script.indexOf("WHERE"));
    assertEquals(1, toRun.size());
    assertEquals("SELECT * FROM t", toRun.get(0).getStatement());
    int[] offsets = SqlExecuteRange.editorOffsets(range, toRun);
    assertEquals("SELECT * FROM t;", script.substring(offsets[0], offsets[1]));
  }

  @Test
  void caretOnSecondQueryRunsThatQuery() {
    String script = "SELECT 1;\nSELECT 2;";
    SqlExecuteRange.Range range = new SqlExecuteRange.Range(script, 0, script.length());
    List<SqlScriptStatement> toRun =
        SqlExecuteRange.statementsToExecute(range, parse(script), script.indexOf("SELECT 2"));
    assertEquals(1, toRun.size());
    assertEquals("SELECT 2", toRun.get(0).getStatement());
  }

  @Test
  void runAllKeepsLeftoverFragment() {
    String script = "SELECT * FROM t;\nWHERE x = 1";
    SqlExecuteRange.Range range = new SqlExecuteRange.Range(script, 0, script.length());
    List<SqlScriptStatement> parsed = parse(script);
    List<SqlScriptStatement> toRun =
        SqlExecuteRange.statementsToExecute(range, parsed, SqlExecuteRange.ALL_STATEMENTS);
    assertEquals(parsed, toRun);
    int[] offsets = SqlExecuteRange.editorOffsets(range, toRun);
    assertEquals(script, script.substring(offsets[0], offsets[1]));
  }

  private static List<SqlScriptStatement> parse(String script) {
    return new NoneDatabaseMeta().getSqlScriptStatements(script + Const.CR);
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
    assertTrue(IContentEditorWidget.isExecuteAllKey(SWT.MOD1 | SWT.SHIFT, SWT.CR, SWT.CR));
    assertTrue(IContentEditorWidget.isExecuteAllKey(SWT.CONTROL | SWT.SHIFT, SWT.CR, (char) 0));
    assertTrue(
        IContentEditorWidget.isExecuteAllTraverse(SWT.TRAVERSE_RETURN, SWT.CONTROL | SWT.SHIFT));
    assertFalse(IContentEditorWidget.isExecuteAllKey(SWT.MOD1, SWT.CR, SWT.CR));
    assertFalse(IContentEditorWidget.isExecuteAllTraverse(SWT.TRAVERSE_RETURN, SWT.CONTROL));
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
