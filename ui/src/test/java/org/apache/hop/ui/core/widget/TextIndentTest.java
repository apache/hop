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

import java.lang.reflect.Field;
import java.util.Map;
import org.apache.hop.core.Const;
import org.apache.hop.core.config.HopConfig;
import org.apache.hop.core.variables.DescribedVariable;
import org.apache.hop.ui.core.widget.TextIndent.Edit;
import org.junit.jupiter.api.Test;

class TextIndentTest {

  @Test
  void caretIndentsTheCurrentLineAndMovesWithTheText() {
    Edit edit = TextIndent.edit("hello", 2, 2, 2, false);
    assertEquals("  hello", apply(edit, "hello"));
    assertEquals(4, edit.selectionStart());
    assertEquals(4, edit.selectionEnd());
  }

  @Test
  void caretAtTheStartOfTheLineLandsAfterTheNewSpaces() {
    Edit edit = TextIndent.edit("hello", 0, 0, 2, false);
    assertEquals("  hello", apply(edit, "hello"));
    assertEquals(2, edit.selectionStart());
  }

  @Test
  void emptyTextBecomesAnIndent() {
    Edit edit = TextIndent.edit("", 0, 0, 2, false);
    assertEquals("  ", edit.replacement());
    assertEquals(2, edit.selectionStart());
    assertEquals(2, edit.selectionEnd());
  }

  @Test
  void selectionThatEndsOnTheNextLineLeavesThatLineAlone() {
    String text = "a\nb\nc";
    Edit edit = TextIndent.edit(text, 0, 4, 2, false);
    assertEquals("  a\n  b\nc", apply(edit, text));
    Edit again =
        TextIndent.edit(apply(edit, text), edit.selectionStart(), edit.selectionEnd(), 2, false);
    assertEquals("    a\n    b\nc", apply(again, apply(edit, text)));
  }

  @Test
  void wholeBufferSelectionIndentsEveryLine() {
    String text = "a\nb\nc";
    Edit edit = TextIndent.edit(text, 0, text.length(), 2, false);
    assertEquals("  a\n  b\n  c", apply(edit, text));
  }

  @Test
  void backwardsSelectionIndentsTheSameLines() {
    String text = "a\nb";
    Edit forward = TextIndent.edit(text, 0, text.length(), 2, false);
    Edit backward = TextIndent.edit(text, text.length(), 0, 2, false);
    assertEquals(forward.replacement(), backward.replacement());
    assertEquals(apply(forward, text), apply(backward, text));
  }

  @Test
  void partialSelectionStillIndentsTheWholeLineAndKeepsTheWord() {
    String text = "hello";
    Edit edit = TextIndent.edit(text, 1, 4, 2, false);
    assertEquals("  hello", apply(edit, text));
    assertEquals("ell", apply(edit, text).substring(edit.selectionStart(), edit.selectionEnd()));
  }

  @Test
  void emptyLineInsideASelectionGainsSpaces() {
    String text = "a\n\nb";
    Edit edit = TextIndent.edit(text, 0, text.length(), 2, false);
    assertEquals("  a\n  \n  b", apply(edit, text));
  }

  @Test
  void caretOnTheTrailingEmptyLineIndentsThatLine() {
    String text = "ab\n";
    Edit edit = TextIndent.edit(text, text.length(), text.length(), 2, false);
    assertEquals("ab\n  ", apply(edit, text));
  }

  @Test
  void selectionEndingOnTheTrailingEmptyLineDoesNotIndentIt() {
    String text = "ab\n";
    Edit edit = TextIndent.edit(text, 0, text.length(), 2, false);
    assertEquals("  ab\n", apply(edit, text));
  }

  @Test
  void carriageReturnLineFeedStaysIntact() {
    String text = "a\r\nb";
    Edit edit = TextIndent.edit(text, 0, text.length(), 2, false);
    assertEquals("  a\r\n  b", apply(edit, text));
  }

  @Test
  void caretBetweenCarriageReturnAndLineFeedIndentsThatLine() {
    String text = "ab\r\ncd";
    Edit edit = TextIndent.edit(text, 3, 3, 2, false);
    assertEquals("  ab\r\ncd", apply(edit, text));
  }

  @Test
  void loneCarriageReturnIsALineBreak() {
    String text = "a\rb";
    Edit edit = TextIndent.edit(text, 0, text.length(), 2, false);
    assertEquals("  a\r  b", apply(edit, text));
  }

  @Test
  void outdentRemovesUpToTheIndentAndClampsTheCaret() {
    String text = "  hello";
    Edit edit = TextIndent.edit(text, 1, 1, 2, true);
    assertEquals("hello", apply(edit, text));
    assertEquals(0, edit.selectionStart());
  }

  @Test
  void outdentRemovesOneLeadingTab() {
    Edit edit = TextIndent.edit("\thello", 2, 2, 2, true);
    assertEquals("hello", apply(edit, "\thello"));
    assertEquals(1, edit.selectionStart());
  }

  @Test
  void outdentStopsAfterFewerSpacesThanTheIndent() {
    Edit edit = TextIndent.edit(" hello", 3, 3, 2, true);
    assertEquals("hello", apply(edit, " hello"));
  }

  @Test
  void outdentOfALineWithNoIndentChangesNothing() {
    Edit edit = TextIndent.edit("hello", 2, 2, 2, true);
    assertEquals("hello", edit.replacement());
    assertEquals(0, edit.replaceStart());
    assertEquals(2, edit.selectionStart());
  }

  @Test
  void outdentDoesNotRemoveSpacesAfterTheFirstTab() {
    Edit edit = TextIndent.edit("\t  x", 0, 4, 2, true);
    assertEquals("  x", apply(edit, "\t  x"));
  }

  @Test
  void configuredWidthIsUsedInsteadOfTwo() {
    Edit edit = TextIndent.edit("a\nb", 0, 3, 4, false);
    assertEquals("    a\n    b", apply(edit, "a\nb"));
  }

  @Test
  void lineThatStartsWithATabGainsAnotherTab() {
    String text = "\thello";
    Edit edit = TextIndent.edit(text, 2, 2, 2, false);
    assertEquals("\t\thello", apply(edit, text));
    assertEquals(3, edit.selectionStart());
  }

  @Test
  void mixedLinesKeepATabOnlyWhereTheLineAlreadyHasOne() {
    String text = "\ta\nb";
    Edit edit = TextIndent.edit(text, 0, text.length(), 2, false);
    assertEquals("\t\ta\n  b", apply(edit, text));
  }

  @Test
  void zeroWidthInsertsATab() {
    String text = "hello";
    Edit edit = TextIndent.edit(text, 0, 0, 0, false);
    assertEquals("\thello", apply(edit, text));
    assertEquals(1, edit.selectionStart());

    Edit outdent = TextIndent.edit("\thello", 1, 1, 0, true);
    assertEquals("hello", apply(outdent, "\thello"));
    Edit spaces = TextIndent.edit("  hello", 2, 2, 0, true);
    assertEquals("  hello", apply(spaces, "  hello"));
  }

  @Test
  void systemPropertyWinsOverTheEnvironmentAndUnusableValuesFallBack() {
    assertEquals(0, TextIndent.parseSize("0"));
    assertEquals(0, TextIndent.parseSize(" 0 "));
    assertEquals(1, TextIndent.parseSize("1"));
    assertEquals(32, TextIndent.parseSize(" 32 "));
    assertEquals(4, TextIndent.parseSize("4"));
    assertEquals(TextIndent.DEFAULT_SIZE, TextIndent.parseSize(null));
    assertEquals(TextIndent.DEFAULT_SIZE, TextIndent.parseSize(""));
    assertEquals(TextIndent.DEFAULT_SIZE, TextIndent.parseSize(" "));
    assertEquals(TextIndent.DEFAULT_SIZE, TextIndent.parseSize("nope"));
    assertEquals(TextIndent.DEFAULT_SIZE, TextIndent.parseSize("-3"));
    assertEquals(TextIndent.DEFAULT_SIZE, TextIndent.parseSize("33"));

    String name = Const.HOP_TEXT_TAB_SIZE;
    String previousProperty = System.getProperty(name);
    String previousEnvironment = System.getenv(name);
    DescribedVariable previousConfig = HopConfig.getInstance().findDescribedVariable(name);
    boolean hadConfig = previousConfig != null;
    String previousConfigValue = hadConfig ? previousConfig.getValue() : null;
    String previousConfigDescription = hadConfig ? previousConfig.getDescription() : null;
    try {
      setEnvironment(name, "4");
      HopConfig.getInstance().setDescribedVariable(new DescribedVariable(name, "2", "test"));
      System.setProperty(name, "8");
      assertEquals(8, TextIndent.tabSize());

      System.setProperty(name, "2");
      assertEquals(4, TextIndent.tabSize());

      System.clearProperty(name);
      assertEquals(4, TextIndent.tabSize());

      restoreEnvironment(name, null);
      assertEquals(2, TextIndent.tabSize());

      System.setProperty(name, "nope");
      assertEquals(TextIndent.DEFAULT_SIZE, TextIndent.tabSize());
      System.setProperty(name, "0");
      assertEquals(0, TextIndent.tabSize());
    } finally {
      if (previousProperty == null) {
        System.clearProperty(name);
      } else {
        System.setProperty(name, previousProperty);
      }
      restoreEnvironment(name, previousEnvironment);
      if (hadConfig) {
        HopConfig.getInstance()
            .setDescribedVariable(
                new DescribedVariable(name, previousConfigValue, previousConfigDescription));
      } else {
        HopConfig.getInstance()
            .getDescribedVariables()
            .removeIf(variable -> name.equals(variable.getName()));
      }
    }
  }

  private static String apply(Edit edit, String text) {
    return text.substring(0, edit.replaceStart())
        + edit.replacement()
        + text.substring(edit.replaceStart() + edit.replaceLength());
  }

  private static void setEnvironment(String name, String value) {
    environmentMap().put(name, value);
  }

  private static void restoreEnvironment(String name, String previous) {
    Map<String, String> environment = environmentMap();
    if (previous == null) {
      environment.remove(name);
    } else {
      environment.put(name, previous);
    }
  }

  @SuppressWarnings("unchecked")
  private static Map<String, String> environmentMap() {
    try {
      Map<String, String> view = System.getenv();
      Field field = view.getClass().getDeclaredField("m");
      field.setAccessible(true);
      return (Map<String, String>) field.get(view);
    } catch (ReflectiveOperationException e) {
      throw new IllegalStateException(e);
    }
  }
}
