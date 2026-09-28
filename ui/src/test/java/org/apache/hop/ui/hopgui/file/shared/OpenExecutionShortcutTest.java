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
import static org.mockito.Mockito.CALLS_REAL_METHODS;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.Method;
import java.text.MessageFormat;
import java.util.Properties;
import org.apache.hop.core.gui.plugin.key.GuiKeyboardShortcut;
import org.apache.hop.core.gui.plugin.key.GuiOsxKeyboardShortcut;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.engine.IPipelineEngine;
import org.apache.hop.ui.hopgui.file.pipeline.HopGuiPipelineGraph;
import org.apache.hop.ui.hopgui.file.workflow.HopGuiWorkflowGraph;
import org.apache.hop.workflow.WorkflowMeta;
import org.apache.hop.workflow.engine.IWorkflowEngine;
import org.junit.jupiter.api.Test;

/** Issue #8605: hover an icon and press x, or Alt-click, to open the running execution. */
class OpenExecutionShortcutTest {

  @Test
  void altClickOpensExecutionOnlyWhileARunIsActive() {
    HopGuiPipelineGraph pipelineGraph = mock(HopGuiPipelineGraph.class, CALLS_REAL_METHODS);
    @SuppressWarnings("unchecked")
    IPipelineEngine<PipelineMeta> pipeline = mock(IPipelineEngine.class);
    // A finished run leaves the engine set. isRunning() is false, so Alt-click must fall through
    // to error handling.
    pipelineGraph.pipeline = pipeline;
    when(pipeline.isRunning()).thenReturn(false);
    assertFalse(DrillDownGuiPlugin.altClickOpensExecution(pipelineGraph, true));

    when(pipeline.isRunning()).thenReturn(true);
    assertTrue(DrillDownGuiPlugin.altClickOpensExecution(pipelineGraph, true));
    assertFalse(DrillDownGuiPlugin.altClickOpensExecution(pipelineGraph, false));

    when(pipeline.isStopped()).thenReturn(true);
    assertFalse(DrillDownGuiPlugin.altClickOpensExecution(pipelineGraph, true));

    pipelineGraph.pipeline = null;
    assertFalse(DrillDownGuiPlugin.altClickOpensExecution(pipelineGraph, true));

    HopGuiWorkflowGraph workflowGraph = mock(HopGuiWorkflowGraph.class, CALLS_REAL_METHODS);
    @SuppressWarnings("unchecked")
    IWorkflowEngine<WorkflowMeta> workflow = mock(IWorkflowEngine.class);
    workflowGraph.setWorkflow(workflow);
    when(workflow.isFinished()).thenReturn(true);
    when(workflow.isActive()).thenReturn(true);
    assertFalse(DrillDownGuiPlugin.altClickOpensExecution(workflowGraph, true));

    when(workflow.isFinished()).thenReturn(false);
    when(workflow.isStopped()).thenReturn(false);
    when(workflow.isActive()).thenReturn(true);
    assertTrue(DrillDownGuiPlugin.altClickOpensExecution(workflowGraph, true));
    assertFalse(DrillDownGuiPlugin.altClickOpensExecution(workflowGraph, false));

    workflowGraph.setWorkflow(null);
    assertFalse(DrillDownGuiPlugin.altClickOpensExecution(workflowGraph, true));
  }

  @Test
  void openExecutionTooltipKeepsTheQuotedX() throws Exception {
    assertQuotedX("messages/messages_en_US.properties", "hit key 'x'", "hit key x");
    assertQuotedX("messages/messages_pt_BR.properties", "teclar 'x'", "teclar x");
  }

  private static void assertQuotedX(String resource, String quoted, String eaten)
      throws IOException {
    Properties properties = new Properties();
    try (InputStream in = DrillDownGuiPlugin.class.getResourceAsStream(resource)) {
      assertNotNull(in, resource);
      properties.load(in);
    }
    String formatted =
        MessageFormat.format(
            properties.getProperty("DrillDown.OpenExecution.Tooltip"), new Object[0]);
    assertTrue(formatted.contains(quoted), formatted);
    assertFalse(formatted.contains(eaten), formatted);
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
