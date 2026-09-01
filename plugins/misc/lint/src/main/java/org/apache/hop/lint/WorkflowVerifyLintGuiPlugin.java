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
package org.apache.hop.lint;

import org.apache.hop.core.gui.plugin.GuiPlugin;
import org.apache.hop.core.gui.plugin.key.GuiKeyboardShortcut;
import org.apache.hop.core.gui.plugin.key.GuiOsxKeyboardShortcut;
import org.apache.hop.core.gui.plugin.toolbar.GuiToolbarElement;
import org.apache.hop.ui.hopgui.HopGui;
import org.apache.hop.ui.hopgui.file.IHopFileTypeHandler;
import org.apache.hop.ui.hopgui.file.workflow.HopGuiWorkflowGraph;
import org.eclipse.swt.SWT;

/** Overrides the workflow verify toolbar action to include lint rules in the Problems tab. */
@GuiPlugin(
    id = "HopLintWorkflowVerifyGuiPlugin",
    description = "Workflow verify integration for Hop Lint Checker")
public class WorkflowVerifyLintGuiPlugin {

  private static WorkflowVerifyLintGuiPlugin instance;

  public static WorkflowVerifyLintGuiPlugin getInstance() {
    if (instance == null) {
      instance = new WorkflowVerifyLintGuiPlugin();
    }
    return instance;
  }

  public WorkflowVerifyLintGuiPlugin() {
    instance = this;
  }

  @GuiToolbarElement(
      root = HopGuiWorkflowGraph.GUI_PLUGIN_TOOLBAR_PARENT_ID,
      id = HopGuiWorkflowGraph.TOOLBAR_ITEM_CHECK,
      toolTip = "i18n:org.apache.hop.ui.hopgui:HopGui.Tooltip.VerifyWorkflow",
      image = "ui/images/check.svg",
      separator = true)
  @GuiKeyboardShortcut(key = SWT.F7)
  @GuiOsxKeyboardShortcut(key = SWT.F7)
  public void checkWorkflowWithLint() {
    IHopFileTypeHandler handler = HopGui.getInstance().getActiveFileTypeHandler();
    if (handler instanceof HopGuiWorkflowGraph workflowGraph) {
      WorkflowVerifyLintService.runVerify(workflowGraph);
    }
  }
}
