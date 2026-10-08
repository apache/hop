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

import org.apache.hop.core.variables.Variables;
import org.apache.hop.ui.core.widget.editor.IContentEditorWidget;
import org.apache.hop.ui.hopgui.ContentEditorFacade;
import org.apache.hop.ui.testing.SwtBotTestBase;
import org.eclipse.swt.SWT;
import org.eclipse.swt.custom.StyledText;
import org.eclipse.swt.layout.FillLayout;
import org.eclipse.swt.widgets.Composite;
import org.eclipse.swt.widgets.Event;
import org.eclipse.swt.widgets.Listener;
import org.eclipse.swt.widgets.Shell;
import org.eclipse.swt.widgets.Text;
import org.eclipse.swt.widgets.Widget;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

/** Tab and Shift+Tab in multi-line text fields, issue #8653. */
@Tag("uitest")
class TextIndentWidgetTest extends SwtBotTestBase {

  @Test
  void tabIndentsStyledTextBeforeTheWidgetInsertsATabCharacter() {
    Shell shell = new Shell(display);
    shell.setLayout(new FillLayout());
    StyledText text = new StyledText(shell, SWT.MULTI | SWT.V_SCROLL);
    shell.open();
    text.setText("a\nb");
    text.setSelection(0, 0);
    TextIndent.attach(text);
    String pad = " ".repeat(TextIndent.tabSize());

    Listener filter = TextIndent::handleKey;
    display.addFilter(SWT.KeyDown, filter);
    try {
      Event tab = key(text, SWT.NONE);
      text.notifyListeners(SWT.KeyDown, tab);
      assertFalse(tab.doit);
      assertEquals(pad + "a\nb", text.getText());
      assertEquals(pad.length(), text.getCaretOffset());

      text.setSelection(0, pad.length() + 1);
      Event shiftTab = key(text, SWT.SHIFT);
      text.notifyListeners(SWT.KeyDown, shiftTab);
      assertEquals("a\nb", text.getText());
    } finally {
      display.removeFilter(SWT.KeyDown, filter);
      shell.dispose();
    }
  }

  @Test
  void shiftTabStaysInAnEditableStyledText() {
    Shell shell = new Shell(display);
    shell.setLayout(new FillLayout());
    StyledText text = new StyledText(shell, SWT.MULTI);
    shell.open();
    TextIndent.attach(text);

    Event shiftTab = new Event();
    shiftTab.widget = text;
    shiftTab.detail = SWT.TRAVERSE_TAB_PREVIOUS;
    shiftTab.stateMask = SWT.SHIFT;
    shiftTab.doit = true;
    text.notifyListeners(SWT.Traverse, shiftTab);
    assertFalse(shiftTab.doit, "Shift+Tab must not leave an editable multi-line field");
    assertEquals(SWT.TRAVERSE_NONE, shiftTab.detail);

    text.setEditable(false);
    Event leaving = new Event();
    leaving.widget = text;
    leaving.detail = SWT.TRAVERSE_TAB_PREVIOUS;
    leaving.stateMask = SWT.SHIFT;
    leaving.doit = true;
    text.notifyListeners(SWT.Traverse, leaving);
    assertTrue(leaving.doit, "A read-only field still gives Shift+Tab back to focus traversal");
    shell.dispose();
  }

  @Test
  void singleLineTextKeepsTabForFocusTraversal() {
    Shell shell = new Shell(display);
    shell.setLayout(new FillLayout());
    Text text = new Text(shell, SWT.SINGLE | SWT.BORDER);
    shell.open();
    text.setText("name");
    text.setSelection(1, 1);
    TextIndent.attach(text);

    Event tab = key(text, SWT.NONE);
    assertFalse(TextIndent.handleKey(tab));
    assertTrue(tab.doit);
    assertEquals("name", text.getText());

    Event traverse = new Event();
    traverse.widget = text;
    traverse.detail = SWT.TRAVERSE_TAB_NEXT;
    traverse.doit = true;
    text.notifyListeners(SWT.Traverse, traverse);
    assertTrue(traverse.doit);
    shell.dispose();
  }

  @Test
  void multiLineTextIndentsAndDoesNotMarkAnEmptyOutdentAsAChange() {
    Shell shell = new Shell(display);
    shell.setLayout(new FillLayout());
    Text text = new Text(shell, SWT.MULTI | SWT.BORDER);
    shell.open();
    text.setText("{\n}");
    text.setSelection(0, text.getCharCount());
    String pad = " ".repeat(TextIndent.tabSize());

    Event tab = key(text, SWT.NONE);
    assertTrue(TextIndent.handleKey(tab));
    assertEquals(pad + "{\n" + pad + "}", text.getText());

    text.setText("{\n}");
    text.setSelection(1, 1);
    Event shiftTab = key(text, SWT.SHIFT);
    assertTrue(TextIndent.handleKey(shiftTab));
    assertEquals("{\n}", text.getText());
    shell.dispose();
  }

  @Test
  void undoOfAnIndentStopsAtTheLoadedScript() {
    Shell shell = new Shell(display);
    shell.setLayout(new FillLayout());
    StyledTextVar text =
        new StyledTextVar(new Variables(), shell, SWT.MULTI | SWT.V_SCROLL, false, false, false);
    shell.open();
    String original = "function f() {\n  return 1;\n}\n";
    text.setText(original);
    assertFalse(text.canUndo(), "Loading the script must not be an undo step");

    StyledText widget = text.getTextWidget();
    widget.setSelection(0);
    assertTrue(TextIndent.handleKey(key(widget, SWT.NONE)));
    assertFalse(original.equals(text.getText()));

    text.undo();
    assertEquals(original, text.getText(), "The first undo must restore the loaded script");
    assertFalse(text.canUndo(), "Undo must not go past the loaded script");
    shell.dispose();
  }

  @Test
  void undoOfAnIndentStopsAtTheLoadedFile() {
    Shell shell = new Shell(display);
    shell.setLayout(new FillLayout());
    shell.setSize(600, 400);
    IContentEditorWidget editor = ContentEditorFacade.createContentEditor(shell, "javascript");
    shell.open();
    String original = "{\n  \"name\": \"hop\"\n}\n";
    editor.setText(original);
    editor.undo();
    assertEquals(original, editor.getText(), "Loading the file must not be an undo step");

    if (!(editor.getControl() instanceof Composite composite)) {
      throw new AssertionError("editor control");
    }
    StyledText widget = findStyledText(composite);
    widget.setFocus();
    widget.setSelection(0);
    assertTrue(TextIndent.handleKey(key(widget, SWT.NONE)));
    assertFalse(original.equals(editor.getText()));

    editor.undo();
    assertEquals(original, editor.getText(), "The first undo must restore the loaded file");
    editor.undo();
    assertEquals(original, editor.getText(), "Undo must not clear the loaded file");
    shell.dispose();
  }

  private static StyledText findStyledText(Composite composite) {
    for (org.eclipse.swt.widgets.Control child : composite.getChildren()) {
      if (child instanceof StyledText styledText) {
        return styledText;
      }
      if (child instanceof Composite nested) {
        StyledText found = findStyledText(nested);
        if (found != null) {
          return found;
        }
      }
    }
    return null;
  }

  private static Event key(Widget widget, int stateMask) {
    Event event = new Event();
    event.widget = widget;
    event.keyCode = SWT.TAB;
    event.character = '\t';
    event.stateMask = stateMask;
    event.doit = true;
    return event;
  }
}
