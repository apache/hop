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

package org.apache.hop.ui.hopgui.file.shared;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.lang.reflect.Method;
import org.apache.hop.core.gui.plugin.key.GuiKeyboardShortcut;
import org.apache.hop.core.gui.plugin.key.GuiOsxKeyboardShortcut;
import org.apache.hop.ui.hopgui.file.pipeline.HopGuiPipelineGraph;
import org.apache.hop.ui.hopgui.file.workflow.HopGuiWorkflowGraph;
import org.junit.jupiter.api.Test;

/** Issue #8605: hover an icon and press x, or Alt-click, to open the running execution. */
class OpenExecutionShortcutTest {

  @Test
  void altClickOpensExecutionOnlyWhileARunIsActive() {
    assertTrue(DrillDownGuiPlugin.altClickOpensExecution(true, true));
    assertFalse(DrillDownGuiPlugin.altClickOpensExecution(true, false));
    assertFalse(DrillDownGuiPlugin.altClickOpensExecution(false, true));
    assertFalse(DrillDownGuiPlugin.altClickOpensExecution(false, false));
  }

  @Test
  void pipelineAndWorkflowGraphsBindBareX() throws Exception {
    assertBareX(HopGuiPipelineGraph.class);
    assertBareX(HopGuiWorkflowGraph.class);
  }

  @Test
  void cutStaysOnCtrlOrCommandX() throws Exception {
    assertModifiedX(HopGuiPipelineGraph.class.getMethod("cutSelectedToClipboard"), true);
    assertModifiedX(HopGuiWorkflowGraph.class.getMethod("cutSelectedToClipboard"), true);
  }

  private static void assertBareX(Class<?> graphClass) throws Exception {
    Method method = graphClass.getMethod("openExecution");
    GuiKeyboardShortcut shortcut = method.getAnnotation(GuiKeyboardShortcut.class);
    GuiOsxKeyboardShortcut osx = method.getAnnotation(GuiOsxKeyboardShortcut.class);
    assertNotNull(shortcut, graphClass.getSimpleName());
    assertNotNull(osx, graphClass.getSimpleName());
    assertEquals('x', shortcut.key());
    assertEquals('x', osx.key());
    assertFalse(shortcut.control());
    assertFalse(shortcut.alt());
    assertFalse(shortcut.shift());
    assertFalse(shortcut.command());
    assertFalse(osx.control());
    assertFalse(osx.alt());
    assertFalse(osx.shift());
    assertFalse(osx.command());
  }

  private static void assertModifiedX(Method method, boolean commandOnOsx) {
    GuiKeyboardShortcut shortcut = method.getAnnotation(GuiKeyboardShortcut.class);
    GuiOsxKeyboardShortcut osx = method.getAnnotation(GuiOsxKeyboardShortcut.class);
    assertNotNull(shortcut);
    assertNotNull(osx);
    assertEquals('x', shortcut.key());
    assertTrue(shortcut.control());
    assertFalse(shortcut.alt());
    assertFalse(shortcut.shift());
    assertEquals(commandOnOsx, osx.command());
    assertFalse(osx.control());
  }
}
